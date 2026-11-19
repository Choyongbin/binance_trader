package trading

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

func twoModelAutoFixture(t *testing.T, firstAction, secondAction string) (*MultiModelAutoRuntime, *AutoRunControl, *Coordinator, *autoPathExecutor, *atomic.Int32) {
	t.Helper()
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	first := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: firstAction, CandleOpenMS: candle}
	p1, dir, coordinator, exec, _ := autoPathFixture(t, first)
	p2, err := NewAutoExecutionPathWithStrategy(model.StrictBuiltinStrategy(), coordinator, p1.ownership, p1.wallet,
		p1.lifecycle, exec, p1.guard, p1.evidence, p1.market)
	if err != nil {
		t.Fatal(err)
	}
	p2.now = p1.now
	calls := &atomic.Int32{}
	p1.evaluate = func(exchange.LiveMarketSnapshot, model.OwnerState, time.Time) (model.Decision, error) {
		calls.Add(1)
		return first, nil
	}
	p2.evaluate = func(exchange.LiveMarketSnapshot, model.OwnerState, time.Time) (model.Decision, error) {
		calls.Add(1)
		return model.Decision{ModelID: model.StrictStrategyID, Status: model.StatusReady, Action: secondAction, CandleOpenMS: candle}, nil
	}
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []*AutoExecutionPath{p1, p2} {
		if err := p.bindRunControl(control, lease); err != nil {
			t.Fatal(err)
		}
	}
	rt, err := newMultiModelAutoRuntime([]*AutoExecutionPath{p1, p2}, control, lease, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return rt, control, coordinator, exec, calls
}

func TestProductMultiModelConstructorBindsSignedEntryRecovery(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	client := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	creds := func(context.Context) (DemoCredentials, error) {
		return DemoCredentials{APIKey: "fake", Secret: "fake"}, nil
	}
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) { return exchange.LiveMarketSnapshot{}, nil }
	runtime, err := NewDemoMultiModelAutoRuntimeWithRegistry(model.DefaultRegistry(), dir,
		[]string{model.TestStrategyID, model.StrictStrategyID}, client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if len(runtime.paths) != 2 || runtime.paths[0].recoverEntry == nil || runtime.paths[1].recoverEntry == nil ||
		runtime.paths[0].prepareProtection == nil || runtime.paths[1].prepareProtection == nil {
		t.Fatal("product multi-model paths lack signed entry recovery or protection review")
	}
}

func TestMultiModelRuntimeBothHoldNoReservations(t *testing.T) {
	rt, control, coordinator, exec, calls := twoModelAutoFixture(t, model.ActionHold, model.ActionHold)
	defer rt.Close()
	for i := 0; i < 2; i++ {
		result, err := rt.RunOnce(context.Background())
		if err != nil || result.Dispatched || result.ClientOrderID != "" || result.Decision.Action != model.ActionHold || len(result.Evaluations) != 2 {
			t.Fatalf("cycle=%d result=%+v err=%v", i, result, err)
		}
		if result.Evaluations[0].ModelID != model.TestStrategyID || result.Evaluations[1].ModelID != model.StrictStrategyID {
			t.Fatalf("unexpected model order: %+v", result.Evaluations)
		}
	}
	if calls.Load() != 4 || exec.calls != 0 {
		t.Fatalf("evaluations=%d sends=%d", calls.Load(), exec.calls)
	}
	reservations, err := coordinator.scan()
	if err != nil || len(reservations) != 0 {
		t.Fatalf("HOLD wrote reservations: %+v %v", reservations, err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiModelRuntimeFirstHoldSecondDispatchStopsRun(t *testing.T) {
	rt, control, coordinator, exec, calls := twoModelAutoFixture(t, model.ActionHold, model.ActionEnterShort)
	defer rt.Close()
	result, err := rt.RunOnce(context.Background())
	if !errors.Is(err, ErrSupervisedSingleOrderComplete) || !result.Dispatched || result.Intent.ModelID != model.StrictStrategyID ||
		result.Intent.PositionSide != Short || result.ClientOrderID == "" || len(result.Evaluations) != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if exec.calls != 1 || calls.Load() != 2 {
		t.Fatalf("expected one send, got sends=%d evals=%d", exec.calls, calls.Load())
	}
	r, err := coordinator.scan()
	if err != nil || len(r) != 1 || r[0].Intent.ModelID != model.StrictStrategyID {
		t.Fatalf("shared durable reservations=%+v %v", r, err)
	}
	if again, err := rt.RunOnce(context.Background()); err == nil || again.Dispatched || exec.calls != 1 || calls.Load() != 2 {
		t.Fatalf("additional attempt allowed: %+v %v", again, err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.RunOnce(context.Background()); err == nil || exec.calls != 1 {
		t.Fatalf("STOP lease allowed new dispatch: %v", err)
	}
}

func TestMultiModelRuntimeFirstBlockedNeverFallsThrough(t *testing.T) {
	rt, control, coordinator, exec, calls := twoModelAutoFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	defer rt.Close()
	rt.paths[0].guard = func(context.Context, Intent) error { return errors.New("signed wallet mismatch") }
	got, err := rt.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "signed wallet mismatch") || len(got.Evaluations) != 1 || got.Dispatched {
		t.Fatalf("expected terminal first-model failure, got %+v %v", got, err)
	}
	if calls.Load() != 1 || exec.calls != 0 {
		t.Fatalf("later model evaluated or dispatched: evals=%d sends=%d", calls.Load(), exec.calls)
	}
	reservations, err := coordinator.scan()
	if err != nil || len(reservations) != 0 {
		t.Fatalf("block produced durable reservations: %+v %v", reservations, err)
	}
	if _, err := rt.RunOnce(context.Background()); err == nil || calls.Load() != 1 {
		t.Fatalf("terminal run retried: %v", err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiModelRuntimeConcurrentCallsNeverSendMoreThanOne(t *testing.T) {
	rt, control, coordinator, exec, _ := twoModelAutoFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	defer rt.Close()
	var wg sync.WaitGroup
	var succeeded atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := rt.RunOnce(context.Background())
			if errors.Is(err, ErrSupervisedSingleOrderComplete) && got.Dispatched {
				succeeded.Add(1)
			}
		}()
	}
	wg.Wait()
	if succeeded.Load() != 1 || exec.calls != 1 {
		t.Fatalf("concurrent one-order fence failed: success=%d send=%d", succeeded.Load(), exec.calls)
	}
	r, err := coordinator.scan()
	if err != nil || len(r) != 1 {
		t.Fatalf("expected one durable reservation, got %+v %v", r, err)
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiModelRuntimeStopLeaseBeforeEvaluation(t *testing.T) {
	rt, control, _, exec, calls := twoModelAutoFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	defer rt.Close()
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
	_, err := rt.RunOnce(context.Background())
	if !errors.Is(err, ErrAutoRunStopped) || calls.Load() != 0 || exec.calls != 0 {
		t.Fatalf("STOP bypass: %v evals=%d sends=%d", err, calls.Load(), exec.calls)
	}
}

func TestMultiModelRuntimeRejectMixedLedgersAndDuplicateID(t *testing.T) {
	rt, control, _, _, _ := twoModelAutoFixture(t, model.ActionHold, model.ActionHold)
	defer rt.Close()
	if _, err := newMultiModelAutoRuntime([]*AutoExecutionPath{rt.paths[0], rt.paths[0]}, control, rt.lease, func() error { return nil }); err == nil {
		t.Fatal("duplicate model accepted")
	}
	fake := *rt.paths[1]
	fake.ownership = &OwnershipStore{}
	if _, err := newMultiModelAutoRuntime([]*AutoExecutionPath{rt.paths[0], &fake}, control, rt.lease, func() error { return nil }); err == nil {
		t.Fatal("mixed ownership store accepted")
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiModelRuntimeFailUnknownModelBeforeCoordinatorMutation(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDemoMultiModelAutoRuntimeWithRegistry(model.DefaultRegistry(), dir,
		[]string{model.TestStrategyID, "unknown_strategy"}, &exchange.Client{},
		func(context.Context) (DemoCredentials, error) { return DemoCredentials{}, nil },
		func(context.Context) (exchange.LiveMarketSnapshot, error) { return exchange.LiveMarketSnapshot{}, nil }, control, lease)
	if err == nil {
		t.Fatal("unknown model accepted")
	}
	if _, err := NewDemoMultiModelAutoRuntimeWithRegistry(model.DefaultRegistry(), dir,
		[]string{model.TestStrategyID, model.TestStrategyID}, &exchange.Client{},
		func(context.Context) (DemoCredentials, error) { return DemoCredentials{}, nil },
		func(context.Context) (exchange.LiveMarketSnapshot, error) { return exchange.LiveMarketSnapshot{}, nil }, control, lease); err == nil {
		t.Fatal("duplicate IDs accepted")
	}
	if err := control.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiModelRuntimeStopBetweenModelEvaluations(t *testing.T) {
	rt, control, coordinator, exec, calls := twoModelAutoFixture(t, model.ActionHold, model.ActionEnterShort)
	defer rt.Close()
	original := rt.paths[0].evaluate
	rt.paths[0].evaluate = func(m exchange.LiveMarketSnapshot, state model.OwnerState, now time.Time) (model.Decision, error) {
		got, err := original(m, state, now)
		if stopErr := control.Stop(); stopErr != nil {
			return model.Decision{}, stopErr
		}
		return got, err
	}
	result, err := rt.RunOnce(context.Background())
	if !errors.Is(err, ErrAutoRunStopped) || result.Dispatched || calls.Load() != 1 || exec.calls != 0 {
		t.Fatalf("stop-between-models bypass: %+v %v evals=%d sends=%d", result, err, calls.Load(), exec.calls)
	}
	reservations, err := coordinator.scan()
	if err != nil || len(reservations) != 0 {
		t.Fatalf("stop-between-models reserved: %+v %v", reservations, err)
	}
}

func TestMultiModelAutoControllerSignedReviewStop(t *testing.T) {
	rt, control, _, exec, _ := twoModelAutoFixture(t, model.ActionHold, model.ActionEnterLong)
	// Constructor from the real controller factory would bind the actual lease;
	// here the runtime already owns a durable lease and is exercised through the
	// same controller loop to prove its stop and review semantics.
	defer rt.Close()
	c := &AutoController{control: control, interval: time.Hour, status: AutoControllerStatus{Running: true}}
	done := make(chan struct{})
	c.done = done
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.loop(ctx, done, rt)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("one order should terminate controller without a second timer tick")
	}
	st := c.Status()
	if st.Running || st.FailClosed || !st.SignedReviewRequired || !st.Dispatched || st.LastOrderID == "" || exec.calls != 1 {
		t.Fatalf("controller review state=%+v exec=%d", st, exec.calls)
	}
	if err := c.Start(); err == nil || !strings.Contains(err.Error(), "START 재허용 금지") {
		t.Fatalf("restart after supervised dispatch permitted: %v", err)
	}
	status, err := control.Status()
	if err != nil || status.State != AutoRunStopped {
		t.Fatalf("durable STOP missing: %+v %v", status, err)
	}
}
