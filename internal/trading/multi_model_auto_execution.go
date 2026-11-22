package trading

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	walletpkg "binance_trader/internal/wallet"
)

// MultiModelEvaluation is an audit-friendly per-model outcome from one serial
// evaluation pass. A HOLD is not an order, and no row authorizes trading.
type MultiModelEvaluation struct {
	ModelID string `json:"model_id"`
	Action  string `json:"action,omitempty"`
	State   string `json:"state"`
}

// MultiModelAutoRuntime shares ONE coordinator, ownership journal, budget and
// lifecycle across all selected strategies. It is an internal, opt-in runtime:
// HTTP auto/start remains 423 LOCKED. The first durable order attempt ends the
// entire run (including on ambiguous failure) until a separate signed review.
// In particular it cannot dispatch one order per selected model in a pass.
type MultiModelAutoRuntime struct {
	mu          sync.Mutex
	paths       []*AutoExecutionPath
	control     *AutoRunControl
	lease       AutoRunLease
	closeShared func() error
	protection  *DemoProtectionRuntimeCoordinator
	terminal    bool
	closed      bool
}

func newMultiModelAutoRuntime(paths []*AutoExecutionPath, control *AutoRunControl, lease AutoRunLease, closeShared func() error) (*MultiModelAutoRuntime, error) {
	if len(paths) < 2 || len(paths) > 16 || control == nil || closeShared == nil {
		return nil, errors.New("다중 모델 실행에는 공유 coordinator, run control 및 2~16개 모델이 필요합니다")
	}
	if err := control.Guard(lease)(context.Background()); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(paths))
	first := paths[0]
	if first == nil {
		return nil, errors.New("다중 모델 실행 경로가 nil입니다")
	}
	for _, p := range paths {
		if p == nil || p.modelID == "" || seen[p.modelID] || p.coordinator != first.coordinator ||
			p.ownership != first.ownership || p.wallet != first.wallet || p.lifecycle != first.lifecycle ||
			p.control != control || p.lease != lease || p.guard == nil || p.executor == nil {
			return nil, errors.New("다중 모델 실행은 고유 모델 ID와 동일한 영구 risk/lifecycle/run lease를 요구합니다")
		}
		seen[p.modelID] = true
	}
	return &MultiModelAutoRuntime{paths: append([]*AutoExecutionPath(nil), paths...), control: control, lease: lease, closeShared: closeShared}, nil
}

// RunOnce serializes all evaluations with Close and other RunOnce calls. It
// checks the durable STOP generation before EACH model, not only at pass start.
// A failed model never falls through to a later model; a reservation or an
// ambiguous MAY_HAVE_SENT cannot be silently treated as HOLD. No automatic
// retry occurs after a dispatch. Controller interprets the supervised sentinel
// as a durable STOP requiring signed order/position/protection review.
func (r *MultiModelAutoRuntime) RunOnce(ctx context.Context) (AutoExecutionResult, error) {
	if r == nil {
		return AutoExecutionResult{}, errors.New("다중 모델 실행기가 없습니다")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.terminal {
		return AutoExecutionResult{}, errors.New("다중 모델 실행기가 종료되었거나 signed 계정 확인 대기 중입니다")
	}
	if err := r.control.Guard(r.lease)(ctx); err != nil {
		r.terminal = true
		return AutoExecutionResult{}, err
	}
	result := AutoExecutionResult{Decision: model.Decision{Action: model.ActionHold}, Evaluations: make([]MultiModelEvaluation, 0, len(r.paths))}
	for _, p := range r.paths {
		if err := r.control.Guard(r.lease)(ctx); err != nil {
			r.terminal = true
			return result, err
		}
		got, err := p.RunOnce(ctx)
		evaluation := MultiModelEvaluation{ModelID: p.modelID, Action: got.Decision.Action, State: "HOLD"}
		if err != nil {
			evaluation.State = "BLOCKED_SIGNED_REVIEW_REQUIRED"
		} else if got.Dispatched {
			evaluation.State = "DISPATCHED_SIGNED_REVIEW_REQUIRED"
		}
		result.Evaluations = append(result.Evaluations, evaluation)
		if err != nil || got.Dispatched || got.ClientOrderID != "" {
			r.terminal = true
			got.Evaluations = append([]MultiModelEvaluation(nil), result.Evaluations...)
			if err != nil {
				return got, fmt.Errorf("모델 %s 실행 중 차단 (후속 모델 실행 금지): %w", p.modelID, err)
			}
			if got.Dispatched && got.ClientOrderID != "" {
				return got, ErrSupervisedSingleOrderComplete
			}
			return got, errors.New("모델 주문 시도 후 상태가 불명확합니다: 후속 모델 및 재시도 차단")
		}
		if got.Decision.Action != model.ActionHold || got.ModelID != p.modelID {
			r.terminal = true
			return result, fmt.Errorf("모델 %s가 주문 없이 비정상 상태를 반환했습니다", p.modelID)
		}
	}
	return result, nil
}

func (r *MultiModelAutoRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.terminal = true
	return r.closeShared()
}

// NewDemoMultiModelAutoRuntimeWithRegistry composes selected frozen model
// strategies onto one shared Demo account and one durable coordinator lock.
// The server owns this constructor behind its HTTP 423 trading lock.
func NewDemoMultiModelAutoRuntimeWithRegistry(registry *model.Registry, dataDir string, ids []string, client *exchange.Client,
	credentials CredentialsProvider, market AutoMarketProvider, control *AutoRunControl, lease AutoRunLease) (*MultiModelAutoRuntime, error) {
	if registry == nil || dataDir == "" || client == nil || credentials == nil || market == nil || control == nil {
		return nil, errors.New("다중 모델 Demo 런타임 필수 구성요소가 없습니다")
	}
	if len(ids) < 2 || len(ids) > 16 {
		return nil, errors.New("다중 모델 Demo 런타임은 2~16개 모델이 필요합니다")
	}
	strategies := make([]model.Strategy, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return nil, fmt.Errorf("중복 모델 ID: %s", id)
		}
		seen[id] = true
		strategy, err := registry.Resolve(id)
		if err != nil {
			return nil, err
		}
		strategies = append(strategies, strategy)
	}
	if err := control.Guard(lease)(context.Background()); err != nil {
		return nil, err
	}
	coordinator, err := OpenCoordinator(dataDir)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*MultiModelAutoRuntime, error) { _ = coordinator.Close(); return nil, e }
	ownership, err := NewOwnershipStore(dataDir)
	if err != nil {
		return fail(err)
	}
	budget, err := walletpkg.Open(dataDir)
	if err != nil {
		return fail(err)
	}
	lifecycle, err := NewExecutionLifecycleStore(dataDir)
	if err != nil {
		return fail(err)
	}
	evidence, err := NewDemoAutoEvidenceProvider(client, credentials, ownership)
	if err != nil {
		return fail(err)
	}
	quoteProvider := func(ctx context.Context, symbol string) (Quote, error) {
		if symbol != "BTCUSDT" {
			return Quote{}, errors.New("자동매매 종목은 BTCUSDT로 고정되어 있습니다")
		}
		m, err := market(ctx)
		if err != nil {
			return Quote{}, err
		}
		return quoteFromLive(m)
	}
	paths := make([]*AutoExecutionPath, 0, len(strategies))
	for _, strategy := range strategies {
		id := strategy.ID()
		guard, err := NewAutoSignedTradingReadinessGuardForModel(id, dataDir, coordinator, budget, client, credentials, market)
		if err != nil {
			return fail(err)
		}
		// Each product strategy shares the same immutable safety boundary.
		// Internal test-only paths may use injected mock guards separately.
		guard = requireProductProtectionDispatchApproval(guard)
		gate, err := NewModelDemoLiveGateForModel(id, client, coordinator, budget)
		if err != nil {
			return fail(err)
		}
		executor, err := NewDemoExecutor(client, credentials, quoteProvider, gate)
		if err != nil {
			return fail(err)
		}
		path, err := NewAutoExecutionPathWithStrategy(strategy, coordinator, ownership, budget, lifecycle, executor, guard, evidence, market)
		if err != nil {
			return fail(err)
		}
		if err := path.bindRunControl(control, lease); err != nil {
			return fail(err)
		}
		path.recoverEntry = productSignedEntryRecovery(coordinator, lifecycle, ownership, client, credentials)
		path.prepareProtection = productProtectionPlanner(dataDir, coordinator, lifecycle, ownership, client, credentials, market)
		paths = append(paths, path)
	}
	runtime, err := newMultiModelAutoRuntime(paths, control, lease, coordinator.Close)
	if err != nil {
		return fail(err)
	}
	protection, err := NewDemoProtectionRuntimeCoordinator(dataDir, coordinator, client, credentials)
	if err != nil {
		return fail(err)
	}
	runtime.protection = protection
	return runtime, nil
}

func NewDemoMultiModelAutoRuntime(dataDir string, ids []string, client *exchange.Client,
	credentials CredentialsProvider, market AutoMarketProvider, control *AutoRunControl, lease AutoRunLease) (*MultiModelAutoRuntime, error) {
	return NewDemoMultiModelAutoRuntimeWithRegistry(model.DefaultRegistry(), dataDir, ids, client, credentials, market, control, lease)
}
