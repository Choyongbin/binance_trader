package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	walletpkg "binance_trader/internal/wallet"
)

// AutoExecutionEvidence is authenticated/read-only evidence used to turn one
// model candidate into a durable order intent. It is deliberately not accepted
// from HTTP/JSON. NewDemoAutoEvidenceProvider constructs it from signed Demo
// reads and the local immutable ownership ledger.
type AutoExecutionEvidence struct {
	Snapshot           exchange.Snapshot
	Orders             map[string]OrderObservation
	Rules              exchange.SymbolRules
	AccountFingerprint string
}

type AutoEvidenceProvider func(context.Context) (AutoExecutionEvidence, error)
type AutoMarketProvider func(context.Context) (exchange.LiveMarketSnapshot, error)
type AutoDecisionEvaluator func(exchange.LiveMarketSnapshot, model.OwnerState, time.Time) (model.Decision, error)

// AutoExecutionResult describes a single model evaluation. HOLD produces no
// reservation. A dispatched result means the internal order adapter was called;
// no server/HTTP route instantiates this path while trading remains LOCKED.
type AutoExecutionResult struct {
	ModelID       string                 `json:"model_id"`
	Decision      model.Decision         `json:"decision"`
	Intent        Intent                 `json:"intent,omitempty"`
	ClientOrderID string                 `json:"client_order_id,omitempty"`
	Lifecycle     ExecutionStage         `json:"lifecycle_stage,omitempty"`
	Submission    Submission             `json:"submission,omitempty"`
	Dispatched    bool                   `json:"dispatched"`
	Evaluations   []MultiModelEvaluation `json:"model_evaluations,omitempty"`
}

// AutoExecutionPath is the only internal model->order connection. The web
// server does not construct it yet. When Demo unlock is later approved, the
// controller must be long-lived so Coordinator ownership spans the full run.
type AutoExecutionPath struct {
	modelID     string
	coordinator *Coordinator
	ownership   *OwnershipStore
	wallet      *walletpkg.Store
	lifecycle   *ExecutionLifecycleStore
	executor    durableFinalGuardExecutor
	guard       func(context.Context, Intent) error
	evidence    AutoEvidenceProvider
	market      AutoMarketProvider
	evaluate    AutoDecisionEvaluator
	now         func() time.Time
	control     *AutoRunControl
	lease       AutoRunLease
}

func NewAutoExecutionPath(modelID string, coordinator *Coordinator, ownership *OwnershipStore, budget *walletpkg.Store,
	lifecycle *ExecutionLifecycleStore, executor durableFinalGuardExecutor, guard func(context.Context, Intent) error,
	evidence AutoEvidenceProvider, market AutoMarketProvider) (*AutoExecutionPath, error) {
	s, err := model.DefaultRegistry().Resolve(modelID)
	if err != nil {
		return nil, err
	}
	return NewAutoExecutionPathWithStrategy(s, coordinator, ownership, budget, lifecycle, executor, guard, evidence, market)
}

// NewAutoExecutionPathWithStrategy binds ONE pre-registered immutable strategy
// to the shared LONG/SHORT risk, ownership and lifecycle execution path.
// The runtime must resolve the strategy through a frozen registry first.
func NewAutoExecutionPathWithStrategy(s model.Strategy, coordinator *Coordinator, ownership *OwnershipStore, budget *walletpkg.Store,
	lifecycle *ExecutionLifecycleStore, executor durableFinalGuardExecutor, guard func(context.Context, Intent) error,
	evidence AutoEvidenceProvider, market AutoMarketProvider) (*AutoExecutionPath, error) {
	if s == nil || coordinator == nil || ownership == nil || budget == nil || lifecycle == nil || executor == nil || guard == nil || evidence == nil || market == nil {
		return nil, errors.New("자동매매 실행 경로 필수 구성요소가 없습니다")
	}
	modelID := s.ID()
	if coordinator.root == "" || !budget.BelongsTo(filepath.Dir(coordinator.root)) || filepath.Clean(lifecycle.root) != filepath.Clean(coordinator.root) || filepath.Clean(ownership.journalDir) != filepath.Join(filepath.Clean(coordinator.root), "requests") {
		return nil, errors.New("자동매매 실행 경로 데이터 디렉터리가 일치하지 않습니다")
	}
	return &AutoExecutionPath{modelID: modelID, coordinator: coordinator, ownership: ownership, wallet: budget,
		lifecycle: lifecycle, executor: executor, guard: guard, evidence: evidence, market: market,
		evaluate: s.EvaluateLive, now: time.Now}, nil
}

func autoRequestID(modelID string, candleMS int64, action string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("auto:%s:BTCUSDT:%d:%s", modelID, candleMS, action)))
	return "auto_" + hex.EncodeToString(sum[:])[:32]
}

func quoteFromLive(m exchange.LiveMarketSnapshot) (Quote, error) {
	if len(m.Candles) == 0 || m.Symbol != "BTCUSDT" {
		return Quote{}, errors.New("자동매매 실시간 BTCUSDT 가격이 없습니다")
	}
	received, err := time.Parse(time.RFC3339Nano, m.LastReceivedAt)
	if err != nil || received.IsZero() {
		return Quote{}, errors.New("자동매매 시장 수신 시각이 유효하지 않습니다")
	}
	price := strconv.FormatFloat(m.Candles[len(m.Candles)-1].Close, 'f', 8, 64)
	if _, err := pos(price); err != nil {
		return Quote{}, errors.New("자동매매 시장 가격이 유효하지 않습니다")
	}
	return Quote{Symbol: "BTCUSDT", Price: price, ReceivedAt: received}, nil
}

func ownerStateForModel(report OwnershipReport, reservations []Reservation, modelID string) (model.OwnerState, map[PositionSide]string, error) {
	state := model.OwnerState{Verified: report.ReadyForAssessment}
	quantities := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, p := range report.Owned {
		if p.Source != Model || p.ModelID != modelID || p.Symbol != "BTCUSDT" {
			continue
		}
		q, err := decimalRat(p.Quantity, true)
		if err != nil || q.Sign() < 0 {
			return state, nil, errors.New("모델 소유 수량이 유효하지 않습니다")
		}
		if p.Side != Long && p.Side != Short {
			return state, nil, errors.New("모델 소유 방향이 유효하지 않습니다")
		}
		quantities[p.Side].Add(quantities[p.Side], q)
	}
	state.LongOpen = quantities[Long].Sign() > 0
	state.ShortOpen = quantities[Short].Sign() > 0
	for _, r := range reservations {
		if r.Intent.Source == Model && r.Intent.ModelID == modelID && r.Intent.Action == Enter && r.Intent.SignalCandleMS > state.LastEntryCandleMS {
			state.LastEntryCandleMS = r.Intent.SignalCandleMS
		}
	}
	return state, map[PositionSide]string{Long: formatOwnershipDecimal(quantities[Long]), Short: formatOwnershipDecimal(quantities[Short])}, nil
}

func accountAvailableUSDT(s exchange.Snapshot) (string, error) {
	value := ""
	for _, b := range s.Balances {
		if b.Asset != "USDT" {
			continue
		}
		if value != "" {
			return "", errors.New("USDT 잔액이 중복되었습니다")
		}
		if _, err := decimalRat(b.Available, true); err != nil {
			return "", err
		}
		value = b.Available
	}
	if value == "" {
		return "", errors.New("USDT 가용 잔액이 없습니다")
	}
	return value, nil
}

func autoModelBudget(report OwnershipReport, reservations []Reservation, assigned, actualAvailable string) (walletpkg.Assessment, error) {
	owned := new(big.Rat)
	for _, p := range report.Owned {
		if p.Source != Model || p.Symbol != "BTCUSDT" {
			continue
		}
		q, err := decimalRat(p.Quantity, true)
		if err != nil {
			return walletpkg.Assessment{}, err
		}
		if q.Sign() == 0 {
			continue
		}
		avg, err := positiveRat(p.AverageEntry)
		if err != nil {
			return walletpkg.Assessment{}, err
		}
		owned.Add(owned, ratDiv(ratMul(q, avg), big.NewRat(15, 1)))
	}
	pending := new(big.Rat)
	for _, r := range reservations {
		if r.Intent.Source != Model || r.Intent.Action != Enter {
			continue
		}
		n, err := decMargin(r.MarginUSDT)
		if err != nil || n.Sign() < 0 {
			return walletpkg.Assessment{}, errors.New("모델 예약 증거금이 손상되었습니다")
		}
		// done reservations no longer consume budget
		pending.Add(pending, n)
	}
	return walletpkg.Assess(assigned, ceilingDecimal12(owned), ratToString(pending), "0", actualAvailable)
}

func (p *AutoExecutionPath) buildIntent(decision model.Decision, quantities map[PositionSide]string, report OwnershipReport, ev AutoExecutionEvidence, quote Quote, reservations []Reservation, now time.Time) (Intent, error) {
	if decision.ModelID != p.modelID || decision.Status != model.StatusReady || decision.CandleOpenMS <= 0 || decision.CandleOpenMS%60000 != 0 {
		return Intent{}, errors.New("검증된 모델 결정이 아닙니다")
	}
	i := Intent{RequestID: autoRequestID(p.modelID, decision.CandleOpenMS, decision.Action), Source: Model, ModelID: p.modelID,
		Symbol: "BTCUSDT", OrderType: Market, SignalCandleMS: decision.CandleOpenMS}
	switch decision.Action {
	case model.ActionEnterLong:
		i.PositionSide, i.Action = Long, Enter
	case model.ActionEnterShort:
		i.PositionSide, i.Action = Short, Enter
	case model.ActionExitLong:
		i.PositionSide, i.Action, i.Quantity = Long, Exit, quantities[Long]
	case model.ActionExitShort:
		i.PositionSide, i.Action, i.Quantity = Short, Exit, quantities[Short]
	default:
		return Intent{}, errors.New("HOLD 결정은 주문 intent로 변환할 수 없습니다")
	}
	if i.Action == Exit {
		if !validPositiveDecimal(i.Quantity) {
			return Intent{}, errors.New("청산할 모델 소유 수량이 없습니다")
		}
		return i, i.Validate()
	}
	assigned, err := p.wallet.SnapshotFor(ev.AccountFingerprint)
	if err != nil || !assigned.Verified {
		return Intent{}, errors.New("자동매매 배정 지갑 계정 검증 실패")
	}
	available, err := accountAvailableUSDT(ev.Snapshot)
	if err != nil {
		return Intent{}, err
	}
	assessment, err := autoModelBudget(report, activeReservations(p.coordinator, reservations), assigned.AssignedUSDT, available)
	if err != nil {
		return Intent{}, err
	}
	if !assessment.Fits || assessment.MaxNewModelEntryUSDT == "0" {
		return Intent{}, errors.New("자동매매 신규 진입 예산이 없습니다")
	}
	// SizeMarketEntry applies the 10% policy. Use the smaller of internal free
	// budget and actual exchange available, matching wallet.Assess.
	budget := assessment.FreeBudgetUSDT
	bq, _ := decimalRat(budget, true)
	aq, _ := decimalRat(available, true)
	if aq.Cmp(bq) < 0 {
		budget = available
	}
	size, err := SizeMarketEntry(ev.Rules, EntryBudget{AvailableUSDT: budget, SharePercent: 10, Leverage: 15, FeeReserveBPS: 20, PriceBufferBPS: 100, Quote: quote, Now: now})
	if err != nil {
		return Intent{}, err
	}
	i.Quantity = size.Quantity
	return i, i.Validate()
}

func activeReservations(c *Coordinator, all []Reservation) []Reservation {
	out := make([]Reservation, 0, len(all))
	for _, r := range all {
		if !c.isDone(r) {
			out = append(out, r)
		}
	}
	return out
}

func (p *AutoExecutionPath) bindRunControl(control *AutoRunControl, lease AutoRunLease) error {
	if p == nil || control == nil || lease.Generation == 0 || lease.RunID == "" {
		return errors.New("자동매매 run control/lease가 필요합니다")
	}
	if filepath.Clean(control.dataDir) != filepath.Clean(filepath.Dir(p.coordinator.root)) {
		return errors.New("자동매매 run control 데이터 경로가 runtime과 일치하지 않습니다")
	}
	if err := control.Guard(lease)(context.Background()); err != nil {
		return err
	}
	p.control, p.lease = control, lease
	return nil
}

func (p *AutoExecutionPath) executionGuard(ctx context.Context, intent Intent) error {
	if p.control != nil {
		if err := p.control.Guard(p.lease)(ctx); err != nil {
			return fmt.Errorf("자동매매 STOP/run lease 차단: %w", err)
		}
	}
	return p.guard(ctx, intent)
}

// RunOnce is the complete internal model signal -> durable dispatch path. The
// HTTP auto/start route stays 423 LOCKED, so this method is not reachable from
// the product UI until the later Demo E2E/unlock milestone.
func (p *AutoExecutionPath) RunOnce(ctx context.Context) (AutoExecutionResult, error) {
	if p == nil {
		return AutoExecutionResult{}, errors.New("자동매매 실행 경로가 없습니다")
	}
	if err := ctx.Err(); err != nil {
		return AutoExecutionResult{}, err
	}
	if p.control != nil {
		if err := p.control.Guard(p.lease)(ctx); err != nil {
			return AutoExecutionResult{}, err
		}
	}
	ev, err := p.evidence(ctx)
	if err != nil {
		return AutoExecutionResult{}, err
	}
	now := p.now().UTC()
	report, err := p.ownership.Audit(ev.Snapshot, ev.Orders, now)
	if err != nil || !report.ReadyForAssessment {
		return AutoExecutionResult{}, errors.New("모델 소유권 감사가 준비되지 않았습니다")
	}
	reservations, err := p.coordinator.scan()
	if err != nil {
		return AutoExecutionResult{}, err
	}
	owner, quantities, err := ownerStateForModel(report, reservations, p.modelID)
	if err != nil {
		return AutoExecutionResult{}, err
	}
	market, err := p.market(ctx)
	if err != nil {
		return AutoExecutionResult{}, err
	}
	decision, err := p.evaluate(market, owner, now)
	result := AutoExecutionResult{ModelID: p.modelID, Decision: decision}
	if err != nil {
		return result, err
	}
	if decision.ModelID != p.modelID || decision.TradingAuthorized || decision.Status != model.StatusReady {
		return result, errors.New("전략 결정의 model ID, readiness 또는 거래 권한 필드가 유효하지 않습니다")
	}
	switch decision.Action {
	case model.ActionHold, model.ActionEnterLong, model.ActionEnterShort, model.ActionExitLong, model.ActionExitShort:
	default:
		return result, errors.New("등록 전략이 알 수 없는 거래 신호를 반환했습니다")
	}
	if decision.Action == model.ActionHold {
		return result, nil
	}
	quote, err := quoteFromLive(market)
	if err != nil {
		return result, err
	}
	intent, err := p.buildIntent(decision, quantities, report, ev, quote, reservations, now)
	if err != nil {
		return result, err
	}
	result.Intent = intent

	// Pre-check before any reservation/lifecycle mutation. The same guard is
	// called again by DispatchOnce before MAY_HAVE_SENT and by the sealed
	// executor immediately before the exchange POST.
	if err := p.executionGuard(ctx, intent); err != nil {
		return result, fmt.Errorf("자동매매 예약 전 readiness 실패: %w", err)
	}
	var reservation Reservation
	var life ExecutionLifecycle
	reserveAndBegin := func() error {
		var reserveErr error
		reservation, _, reserveErr = p.coordinator.ReserveAuditedWithWallet(intent, p.ownership, p.wallet, ev.AccountFingerprint, ev.Snapshot, ev.Orders, quote, now)
		if reserveErr != nil {
			return reserveErr
		}
		life, reserveErr = p.lifecycle.Begin(reservation, now)
		return reserveErr
	}
	if p.control != nil {
		err = p.control.withLease(p.lease, reserveAndBegin)
	} else {
		err = reserveAndBegin()
	}
	if err != nil {
		return result, err
	}
	result.ClientOrderID = reservation.ClientOrderID
	result.Lifecycle = life.Stage
	ack, err := p.lifecycle.DispatchOnce(ctx, reservation, p.executor, p.executionGuard)
	result.Submission = ack
	if err != nil {
		return result, err
	}
	result.Dispatched = true
	result.Lifecycle = ExecutionMayHaveSent
	return result, nil
}

// NewDemoAutoEvidenceProvider creates the production signed/read-only evidence
// builder. It never submits or cancels orders.
func NewDemoAutoEvidenceProvider(client *exchange.Client, credentials CredentialsProvider, ownership *OwnershipStore) (AutoEvidenceProvider, error) {
	if client == nil || credentials == nil || ownership == nil {
		return nil, errors.New("자동매매 signed evidence 구성요소가 없습니다")
	}
	return func(ctx context.Context) (AutoExecutionEvidence, error) {
		creds, err := credentials(ctx)
		if err != nil {
			return AutoExecutionEvidence{}, err
		}
		if creds.APIKey == "" || creds.Secret == "" {
			return AutoExecutionEvidence{}, errors.New("Demo 키가 잠겨 있습니다")
		}
		observations, err := ownership.CollectDemoOrderObservations(ctx, client, creds)
		if err != nil {
			return AutoExecutionEvidence{}, err
		}
		snap, err := client.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
		if err != nil {
			return AutoExecutionEvidence{}, err
		}
		rules, err := client.FetchSymbolRules(ctx, "BTCUSDT")
		if err != nil {
			return AutoExecutionEvidence{}, err
		}
		converted, err := snapshotFromExecutionPreflight(snap)
		if err != nil {
			return AutoExecutionEvidence{}, err
		}
		sum := sha256.Sum256([]byte(creds.APIKey))
		return AutoExecutionEvidence{Snapshot: converted, Orders: observations, Rules: rules, AccountFingerprint: hex.EncodeToString(sum[:])}, nil
	}, nil
}

func snapshotFromExecutionPreflight(s exchange.ExecutionPreflightSnapshot) (exchange.Snapshot, error) {
	if s.Environment != "demo" || s.FinishedAt.IsZero() || s.Portfolio.Assets == nil || s.Portfolio.Positions == nil || s.OpenOrders == nil || s.OpenAlgos == nil {
		return exchange.Snapshot{}, errors.New("불완전한 signed execution snapshot")
	}
	// Preserve the distinction between a complete empty signed collection and
	// a missing collection. OwnershipStore.Audit intentionally treats nil
	// Balances/Positions/Orders/AlgoOrders as incomplete account evidence. A
	// flat account commonly has zero positions/orders, so converting a valid
	// [] response into nil here would permanently block the model execution
	// path before its first reservation.
	out := exchange.Snapshot{
		Status: "connected", Environment: "demo", AsOf: s.FinishedAt.UTC().Format(time.RFC3339Nano),
		Balances:   make([]exchange.Balance, 0, len(s.Portfolio.Assets)),
		Positions:  make([]exchange.Position, 0, 2),
		Orders:     append([]exchange.Order{}, s.OpenOrders...),
		AlgoOrders: append([]exchange.AlgoOrder{}, s.OpenAlgos...),
	}
	for _, a := range s.Portfolio.Assets {
		out.Balances = append(out.Balances, exchange.Balance{Asset: a.Asset, Wallet: string(a.WalletBalance), Available: string(a.AvailableBalance)})
	}
	for _, x := range s.Portfolio.Positions {
		if x.Symbol != "BTCUSDT" || (x.PositionSide != "LONG" && x.PositionSide != "SHORT") {
			continue
		}
		q, ok := new(big.Rat).SetString(string(x.PositionAmt))
		if !ok {
			return exchange.Snapshot{}, errors.New("signed position 수량 오류")
		}
		if q.Sign() == 0 {
			continue
		}
		out.Positions = append(out.Positions, exchange.Position{Symbol: x.Symbol, Side: x.PositionSide, Qty: string(x.PositionAmt), MarginType: "isolated"})
	}
	sort.Slice(out.Positions, func(i, j int) bool { return out.Positions[i].Side < out.Positions[j].Side })
	return out, nil
}

// NewModelDemoLiveGate is the model-order-specific signed gate used by the
// DemoExecutor. It intentionally blocks any outstanding exchange order/algo;
// protection-aware strategy exits will be enabled only after Demo E2E proves
// the cancel/trigger sequencing. This is conservative but never bypasses the
// durable reservation or wallet allocation.
func NewModelDemoLiveGate(client *exchange.Client, coordinator *Coordinator, budget *walletpkg.Store) (Preflight, error) {
	return NewModelDemoLiveGateForModel(model.TestStrategyID, client, coordinator, budget)
}

// The signed account totals include manual positions. Counting all of them
// against the model cap is conservative when ownership cannot be proved.
func checkAutoEquityCap(port exchange.AccountPortfolio, pending *big.Rat) error {
	if pending == nil || pending.Sign() < 0 {
		return errors.New("모델 예약 증거금 오류")
	}
	equity, ok := new(big.Rat).SetString(string(port.TotalMarginBalance))
	if !ok || equity.Sign() <= 0 {
		return errors.New("signed account Equity 오류")
	}
	initial, ok := new(big.Rat).SetString(string(port.TotalInitialMargin))
	if !ok || initial.Sign() < 0 {
		return errors.New("signed account initial margin 오류")
	}
	position, ok := new(big.Rat).SetString(string(port.TotalPositionInitialMargin))
	if !ok || position.Sign() < 0 {
		return errors.New("signed position initial margin 오류")
	}
	openOrders, ok := new(big.Rat).SetString(string(port.TotalOpenOrderInitialMargin))
	if !ok || openOrders.Sign() != 0 || position.Cmp(initial) != 0 {
		return errors.New("signed account 초기 증거금 구성 불일치")
	}
	if new(big.Rat).Add(initial, pending).Cmp(new(big.Rat).Mul(equity, big.NewRat(3, 10))) > 0 {
		return errors.New("전체 계정 초기 증거금과 모델 예약이 Equity 30% 한도를 초과합니다")
	}
	return nil
}

func NewModelDemoLiveGateForModel(modelID string, client *exchange.Client, coordinator *Coordinator, budget *walletpkg.Store) (Preflight, error) {
	if modelID == "" {
		return nil, errors.New("등록된 model ID가 필요합니다")
	}
	if client == nil || coordinator == nil || budget == nil || !budget.BelongsTo(filepath.Dir(coordinator.root)) {
		return nil, errors.New("모델 Demo 실행 gate 구성요소가 없습니다")
	}
	return func(ctx context.Context, p PlannedOrder, rules exchange.SymbolRules, quote Quote, creds DemoCredentials) error {
		if creds.APIKey == "" || creds.Secret == "" {
			return errors.New("Demo 키가 잠겨 있습니다")
		}
		reservations, err := coordinator.scan()
		if err != nil {
			return err
		}
		var reservation *Reservation
		for idx := range reservations {
			r := &reservations[idx]
			plan, planErr := r.Intent.Plan()
			if planErr != nil {
				return planErr
			}
			if r.ClientOrderID == p.ClientOrderID {
				if plan != p || r.Intent.Source != Model || r.Intent.ModelID != modelID || r.Intent.SignalCandleMS <= 0 {
					return errors.New("모델 reservation과 실행 plan이 일치하지 않습니다")
				}
				reservation = r
				break
			}
		}
		if reservation == nil {
			return errors.New("실행 plan의 durable reservation을 찾을 수 없습니다")
		}
		signed, err := client.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, p.Symbol)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if signed.Environment != "demo" || !signed.Portfolio.CanTrade || signed.FinishedAt.IsZero() || signed.FinishedAt.After(now.Add(time.Second)) || now.Sub(signed.FinishedAt) > 5*time.Second {
			return errors.New("최신 Binance Demo trading capability를 확인할 수 없습니다")
		}
		cfg := signed.Configuration
		if cfg.Environment != "demo" || cfg.Symbol != "BTCUSDT" || !cfg.SettingsMatchV1 || !cfg.HedgeMode || cfg.MarginType != "ISOLATED" || cfg.Leverage != 15 || cfg.AutoAddMargin {
			return errors.New("Demo Hedge/ISOLATED/15x 설정이 실행 계약과 다릅니다")
		}
		if len(signed.OpenOrders) != 0 || len(signed.OpenAlgos) != 0 {
			return errors.New("미체결 normal/algo 주문이 있어 모델 주문을 직렬화할 수 없습니다")
		}
		plannedQty, err := positiveRat(p.Quantity)
		if err != nil {
			return err
		}
		sideQty := new(big.Rat)
		for _, pos := range signed.Portfolio.Positions {
			if pos.Symbol != "BTCUSDT" || pos.PositionSide != string(p.PositionSide) {
				continue
			}
			q, ok := new(big.Rat).SetString(string(pos.PositionAmt))
			if !ok {
				return errors.New("signed side position 수량 오류")
			}
			q.Abs(q)
			sideQty.Add(sideQty, q)
		}
		if reservation.Intent.Action == Enter {
			if sideQty.Sign() != 0 {
				return errors.New("동일 모델 방향에 기존 exchange position이 있어 신규 진입을 중단합니다")
			}
			fingerprint := sha256.Sum256([]byte(creds.APIKey))
			assigned, err := budget.SnapshotFor(hex.EncodeToString(fingerprint[:]))
			if err != nil || !assigned.Verified {
				return errors.New("자동매매 배정 지갑과 현재 Demo 키가 일치하지 않습니다")
			}
			assignedQ, err := decimalRat(assigned.AssignedUSDT, true)
			if err != nil || assignedQ.Sign() <= 0 {
				return errors.New("자동매매 배정 지갑이 비어 있습니다")
			}
			pending := new(big.Rat)
			for _, r := range reservations {
				if coordinator.isDone(r) || r.Intent.Source != Model || r.Intent.Action != Enter {
					continue
				}
				m, err := decMargin(r.MarginUSDT)
				if err != nil || m.Sign() < 0 {
					return errors.New("모델 reservation 증거금이 손상되었습니다")
				}
				pending.Add(pending, m)
			}
			if err := checkAutoEquityCap(signed.Portfolio, pending); err != nil {
				return err
			}
			positionMargin, ok := new(big.Rat).SetString(string(signed.Portfolio.TotalPositionInitialMargin))
			if !ok || positionMargin.Sign() < 0 {
				return errors.New("signed position initial margin 오류")
			}
			if new(big.Rat).Add(positionMargin, pending).Cmp(assignedQ) > 0 {
				return errors.New("현재 포지션+예약 증거금이 자동매매 배정 한도를 초과합니다")
			}
			available, ok := new(big.Rat).SetString(string(signed.Portfolio.AvailableBalance))
			if !ok || available.Sign() < 0 {
				return errors.New("signed available balance 오류")
			}
			reservedMargin, err := decMargin(reservation.MarginUSDT)
			if err != nil || reservedMargin.Sign() <= 0 || reservedMargin.Cmp(available) > 0 {
				return errors.New("현재 가용 USDT가 durable reservation 증거금보다 작습니다")
			}
		} else {
			if sideQty.Cmp(plannedQty) < 0 {
				return errors.New("현재 exchange side position이 모델 청산 수량보다 작습니다")
			}
		}
		if err := ValidatePlannedOrder(p, rules, quote.Price, now); err != nil {
			return err
		}
		return nil
	}, nil
}

type AutoExecutionRuntime struct {
	Path        *AutoExecutionPath
	coordinator *Coordinator
}

func (r *AutoExecutionRuntime) RunOnce(ctx context.Context) (AutoExecutionResult, error) {
	if r == nil || r.Path == nil {
		return AutoExecutionResult{}, errors.New("자동매매 runtime이 초기화되지 않았습니다")
	}
	return r.Path.RunOnce(ctx)
}

func (r *AutoExecutionRuntime) Close() error {
	if r == nil || r.coordinator == nil {
		return nil
	}
	return r.coordinator.Close()
}

func scopedAutoExecutionAuditState(dataDir string, intent Intent) (string, error) {
	audit, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return ExecutionAuditBlocked, err
	}
	if audit.State != ExecutionAuditPending {
		return audit.State, nil
	}
	plan, err := intent.Plan()
	if err != nil {
		return ExecutionAuditBlocked, err
	}
	pending := make([]ExecutionLifecycleAuditItem, 0, audit.Pending)
	for _, item := range audit.Items {
		if item.Stage != ExecutionSettled {
			pending = append(pending, item)
		}
	}
	if len(pending) == 1 && pending[0].ClientOrderID == plan.ClientOrderID &&
		(pending[0].Stage == ExecutionReserved || pending[0].Stage == ExecutionMayHaveSent) {
		// DispatchOnce deliberately calls the same guard once while this order is
		// RESERVED and once after its own MAY_HAVE_SENT fsync. Only that exact
		// lifecycle may be ignored; every other pending lifecycle stays blocking.
		return ExecutionAuditClear, nil
	}
	return audit.State, nil
}

func activeExclusiveDiagnostic(dataDir string) (string, error) {
	for _, item := range []struct {
		rel  string
		name string
	}{
		{filepath.Join("trading", "recovery_sessions", "exclusive.active"), "recovery probe"},
		{filepath.Join("trading", "reconnect_watch_sessions", "exclusive.active"), "reconnect watch"},
	} {
		info, err := os.Lstat(filepath.Join(dataDir, item.rel))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%s lock 형식이 안전하지 않습니다", item.name)
		}
		return item.name, nil
	}
	return "", nil
}

// NewAutoSignedTradingReadinessGuard is the execution-scoped authoritative
// gate. Unlike the read-only server status builder, it understands that the
// currently dispatched order itself is expected to be RESERVED/MAY_HAVE_SENT
// and that this runtime owns coordinator.active. No other exception is made.
func NewAutoSignedTradingReadinessGuard(dataDir string, coordinator *Coordinator, budget *walletpkg.Store,
	reader ExecutionPreflightReader, credentials CredentialsProvider, market AutoMarketProvider) (func(context.Context, Intent) error, error) {
	return NewAutoSignedTradingReadinessGuardForModel(model.TestStrategyID, dataDir, coordinator, budget, reader, credentials, market)
}

func NewAutoSignedTradingReadinessGuardForModel(modelID, dataDir string, coordinator *Coordinator, budget *walletpkg.Store,
	reader ExecutionPreflightReader, credentials CredentialsProvider, market AutoMarketProvider) (func(context.Context, Intent) error, error) {
	if modelID == "" {
		return nil, errors.New("등록된 model ID가 필요합니다")
	}
	if dataDir == "" || coordinator == nil || budget == nil || reader == nil || credentials == nil || market == nil || !budget.BelongsTo(dataDir) {
		return nil, errors.New("자동매매 scoped readiness 필수 구성요소가 없습니다")
	}
	return func(ctx context.Context, intent Intent) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if intent.Source != Model || intent.ModelID != modelID || intent.Symbol != "BTCUSDT" {
			return errors.New("자동매매 scoped readiness에 모델 BTCUSDT intent가 필요합니다")
		}
		if err := coordinator.ownsLock(); err != nil {
			return fmt.Errorf("자동매매 coordinator 소유권 실패: %w", err)
		}
		if diag, err := activeExclusiveDiagnostic(dataDir); err != nil {
			return err
		} else if diag != "" {
			return fmt.Errorf("%s가 실행 중이거나 미해결 상태입니다", diag)
		}

		now := time.Now().UTC()
		m, err := market(ctx)
		if err != nil {
			return fmt.Errorf("자동매매 시장 freshness 조회 실패: %w", err)
		}
		received, err := time.Parse(time.RFC3339Nano, m.LastReceivedAt)
		if err != nil {
			return errors.New("자동매매 시장 수신 시각이 유효하지 않습니다")
		}
		marketReady := m.Ready && m.State == "live" && m.Symbol == "BTCUSDT" && m.Source == "BINANCE_FUTURES_DEMO_WEBSOCKET" && m.Interval == "1m"
		marketAge := now.Sub(received)

		creds, err := credentials(ctx)
		if err != nil {
			return err
		}
		if creds.APIKey == "" || creds.Secret == "" {
			return errors.New("Demo 키가 잠겨 있습니다")
		}
		signed, err := reader.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
		if err != nil {
			return fmt.Errorf("signed execution preflight 실패: %w", err)
		}
		if signed.Environment != "demo" || signed.Configuration.Environment != "demo" || signed.Configuration.Symbol != "BTCUSDT" || !signed.Configuration.SettingsMatchV1 {
			return errors.New("Demo Hedge/격리/15배 실행 설정이 검증되지 않았습니다")
		}
		if intent.Action == Enter {
			reservations, err := coordinator.scan()
			if err != nil {
				return err
			}
			pending := new(big.Rat)
			for _, r := range reservations {
				if coordinator.isDone(r) || r.Intent.Source != Model || r.Intent.Action != Enter {
					continue
				}
				margin, err := decMargin(r.MarginUSDT)
				if err != nil || margin.Sign() < 0 {
					return errors.New("모델 reservation 증거금이 손상되었습니다")
				}
				pending.Add(pending, margin)
			}
			if err := checkAutoEquityCap(signed.Portfolio, pending); err != nil {
				return err
			}
		}

		fingerprint := sha256.Sum256([]byte(creds.APIKey))
		walletState, err := budget.SnapshotFor(hex.EncodeToString(fingerprint[:]))
		if err != nil {
			return err
		}
		held, err := MutationGatePresent(dataDir)
		if err != nil {
			return err
		}
		execState, err := scopedAutoExecutionAuditState(dataDir, intent)
		if err != nil {
			return fmt.Errorf("자동매매 execution lifecycle 검증 실패: %w", err)
		}
		unsettled, arming, err := ReviewProtectionReadiness(dataDir)
		if err != nil {
			return fmt.Errorf("자동매매 protection 검증 실패: %w", err)
		}

		in := TradingReadinessInput{
			Environment:             "demo",
			MarketState:             m.State,
			MarketReady:             marketReady,
			MarketAge:               marketAge,
			MaxMarketAge:            5 * time.Second,
			AccountStatus:           "connected",
			AccountEnvironment:      "demo",
			AccountAsOf:             signed.FinishedAt,
			MaxAccountAge:           5 * time.Second,
			AccountCanTradeKnown:    true,
			AccountCanTrade:         signed.Portfolio.CanTrade,
			WalletVerified:          walletState.Verified,
			WalletAssignedUSDT:      walletState.AssignedUSDT,
			MutationGatePresent:     held,
			CoordinatorLockPresent:  true,
			CoordinatorLockExpected: true,
			ExecutionAuditState:     execState,
			ProtectionUnsettled:     unsettled,
			ProtectionArmingState:   arming,
		}
		r, err := EvaluateTradingReadiness(in, now)
		if err != nil {
			return err
		}
		if !r.Ready {
			return fmt.Errorf("trading readiness 차단 (%s): %s", r.State, strings.Join(r.Blockers, "; "))
		}
		return nil
	}, nil
}

// NewDemoAutoExecutionRuntime composes the entire internal automatic path:
// model signal -> signed evidence -> wallet-aware reservation -> lifecycle ->
// execution-scoped signed readiness -> DemoExecutor. The server deliberately
// does not call this constructor while /trading/auto/start remains HTTP 423.
func NewDemoAutoExecutionRuntime(dataDir, modelID string, client *exchange.Client, credentials CredentialsProvider,
	market AutoMarketProvider, control *AutoRunControl, lease AutoRunLease) (*AutoExecutionRuntime, error) {
	return NewDemoAutoExecutionRuntimeWithRegistry(model.DefaultRegistry(), dataDir, modelID, client, credentials, market, control, lease)
}

// NewDemoAutoExecutionRuntimeWithRegistry supports a selected, registered
// strategy without creating a second LONG/SHORT trading engine. Unknown IDs
// are rejected before opening coordinator or touching durable state.
func NewDemoAutoExecutionRuntimeWithRegistry(registry *model.Registry, dataDir, modelID string, client *exchange.Client, credentials CredentialsProvider,
	market AutoMarketProvider, control *AutoRunControl, lease AutoRunLease) (*AutoExecutionRuntime, error) {
	strategy, err := registry.Resolve(modelID)
	if err != nil {
		return nil, err
	}
	if dataDir == "" || client == nil || credentials == nil || market == nil || control == nil || lease.Generation == 0 || lease.RunID == "" {
		return nil, errors.New("Demo 자동매매 runtime 필수 구성요소가 없습니다")
	}
	coordinator, err := OpenCoordinator(dataDir)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*AutoExecutionRuntime, error) { _ = coordinator.Close(); return nil, e }
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
	guard, err := NewAutoSignedTradingReadinessGuardForModel(modelID, dataDir, coordinator, budget, client, credentials, market)
	if err != nil {
		return fail(err)
	}
	gate, err := NewModelDemoLiveGateForModel(modelID, client, coordinator, budget)
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
	return &AutoExecutionRuntime{Path: path, coordinator: coordinator}, nil
}
