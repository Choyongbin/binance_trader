package trading

import (
	"context"
	"net/http"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

type alternateSignalModel struct {
	id       string
	decision model.Decision
}

func (s alternateSignalModel) ID() string { return s.id }
func (s alternateSignalModel) EvaluateLive(exchange.LiveMarketSnapshot, model.OwnerState, time.Time) (model.Decision, error) {
	return s.decision, nil
}
func TestRegisteredStrategySelectsSharedTradingRuntime(t *testing.T) {
	dir := t.TempDir()
	client := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	creds := func(context.Context) (DemoCredentials, error) { return DemoCredentials{"key", "secret"}, nil }
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) { return exchange.LiveMarketSnapshot{}, nil }
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	reg := model.NewRegistry()
	if err = reg.Register(alternateSignalModel{id: "alternate_model_v2", decision: model.Decision{ModelID: "alternate_model_v2", Status: model.StatusReady, Action: model.ActionHold}}); err != nil {
		t.Fatal(err)
	}
	if _, err = NewDemoAutoExecutionRuntimeWithRegistry(reg, dir, "alternate_model_v2", client, creds, market, control, lease); err == nil {
		t.Fatal("unfrozen registry was accepted")
	}
	if err = reg.Freeze(); err != nil {
		t.Fatal(err)
	}
	if _, err = NewDemoAutoExecutionRuntimeWithRegistry(reg, dir, "not_registered", client, creds, market, control, lease); err == nil {
		t.Fatal("unknown model was accepted")
	}
	rt, err := NewDemoAutoExecutionRuntimeWithRegistry(reg, dir, "alternate_model_v2", client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if rt.Path.modelID != "alternate_model_v2" {
		t.Fatal("wrong model bound")
	}
	if decision, err := rt.Path.evaluate(exchange.LiveMarketSnapshot{}, model.OwnerState{}, time.Now()); err != nil || decision.ModelID != "alternate_model_v2" {
		t.Fatalf("strategy injection failed: %+v, %v", decision, err)
	}
}

func TestModelDecisionMismatchFailsBeforeReservation(t *testing.T) {
	nowCandle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	cases := []struct {
		name string
		dec  model.Decision
	}{
		{"wrong ID HOLD", model.Decision{ModelID: "different_id", Status: model.StatusReady, Action: model.ActionHold}},
		{"wrong ID ENTER", model.Decision{ModelID: "different_id", Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: nowCandle}},
		{"unauthorized flag", model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterShort, CandleOpenMS: nowCandle, TradingAuthorized: true}},
		{"unknown action", model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: "SWAP", CandleOpenMS: nowCandle}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			path, _, _, exec, guards := autoPathFixture(t, tt.dec)
			if _, err := path.RunOnce(context.Background()); err == nil {
				t.Fatal("unsafe decision accepted")
			}
			if exec.calls != 0 || *guards != 0 {
				t.Fatalf("side effects on invalid strategy signal: executor=%d guard=%d", exec.calls, *guards)
			}
		})
	}
}

func TestModelSpecificSignedGateRejectsOtherOwners(t *testing.T) {
	// Validation is performed before any account requests. A distinct model's
	// reservation cannot be substituted for the selected model in the live gate.
	if _, err := NewModelDemoLiveGateForModel("", nil, nil, nil); err == nil {
		t.Fatal("empty model accepted")
	}
	if _, err := NewAutoSignedTradingReadinessGuardForModel("", "", nil, nil, nil, nil, nil); err == nil {
		t.Fatal("empty readiness model accepted")
	}
}

func TestAlternativeModelLongShortShareOrderAndRiskEngine(t *testing.T) {
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	for _, tc := range []struct {
		name, action string
		side         PositionSide
		exchangeSide string
	}{
		{"long", model.ActionEnterLong, Long, "BUY"},
		{"short", model.ActionEnterShort, Short, "SELL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "alternate_model_v2"
			decision := model.Decision{ModelID: id, Status: model.StatusReady, Action: tc.action, CandleOpenMS: candle}
			defaultPath, _, _, executor, guards := autoPathFixture(t, decision)
			r := model.NewRegistry()
			if err := r.Register(alternateSignalModel{id: id, decision: decision}); err != nil {
				t.Fatal(err)
			}
			if err := r.Freeze(); err != nil {
				t.Fatal(err)
			}
			s, err := r.Resolve(id)
			if err != nil {
				t.Fatal(err)
			}
			path, err := NewAutoExecutionPathWithStrategy(s, defaultPath.coordinator, defaultPath.ownership, defaultPath.wallet, defaultPath.lifecycle,
				defaultPath.executor, defaultPath.guard, defaultPath.evidence, defaultPath.market)
			if err != nil {
				t.Fatal(err)
			}
			path.now = defaultPath.now
			got, err := path.RunOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !got.Dispatched || got.Intent.ModelID != id || got.Intent.PositionSide != tc.side || got.Intent.Action != Enter || executor.calls != 1 || *guards == 0 {
				t.Fatalf("unexpected model dispatch: %+v, executor=%d guards=%d", got, executor.calls, *guards)
			}
			order, err := got.Intent.Plan()
			if err != nil {
				t.Fatal(err)
			}
			if order.ExchangeSide != tc.exchangeSide || order.PositionSide != tc.side {
				t.Fatalf("wrong exchange direction: %+v", order)
			}
			if got.Intent.RequestID != autoRequestID(id, candle, tc.action) {
				t.Fatal("model ID was not used in deterministic request ID")
			}
			if got.Intent.RequestID == autoRequestID(model.TestStrategyID, candle, tc.action) {
				t.Fatal("different models share request ID")
			}
			// The same signal must not create a second durable order.
			if again, err := path.RunOnce(context.Background()); err == nil && again.Dispatched {
				t.Fatal("duplicate dispatch for same model/candle")
			}
		})
	}
}

func TestModelSpecificGateCannotUseOtherModelReservation(t *testing.T) {
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	original := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle}
	path, _, coordinator, _, _ := autoPathFixture(t, original)
	result, err := path.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := result.Intent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	client := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	gate, err := NewModelDemoLiveGateForModel("alternate_model_v2", client, coordinator, path.wallet)
	if err != nil {
		t.Fatal(err)
	}
	err = gate(context.Background(), plan, sizingRules(time.Now()), Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: time.Now()}, DemoCredentials{APIKey: "k", Secret: "s"})
	if err == nil {
		t.Fatal("different model reservation was accepted")
	}
}
