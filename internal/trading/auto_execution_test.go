package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	walletpkg "binance_trader/internal/wallet"
)

type autoPathExecutor struct {
	calls int
	final int
}

func (f *autoPathExecutor) durableFinalGuardContract() {}
func (f *autoPathExecutor) SubmitWithFinalCheck(ctx context.Context, p PlannedOrder, check func(context.Context) error) (Submission, error) {
	f.calls++
	if check == nil {
		return Submission{}, errors.New("missing final check")
	}
	f.final++
	if err := check(ctx); err != nil {
		return Submission{}, err
	}
	return Submission{ClientOrderID: p.ClientOrderID, OrderID: 991, Status: "NEW"}, nil
}

func autoPathFixture(t *testing.T, decision model.Decision) (*AutoExecutionPath, string, *Coordinator, *autoPathExecutor, *int) {
	t.Helper()
	dir := t.TempDir()
	own, err := NewOwnershipStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := walletpkg.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("k"))
	fingerprint := hex.EncodeToString(sum[:])
	if _, err := budget.Change("allocate_auto_001", "ALLOCATE", "1000", "1000", fingerprint); err != nil {
		t.Fatal(err)
	}
	coord, err := OpenCoordinator(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coord.Close() })
	life, err := NewExecutionLifecycleStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 30, 12, 0, 30, 0, time.UTC)
	snapshot := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: now.Format(time.RFC3339Nano),
		Balances: []exchange.Balance{{Asset: "USDT", Wallet: "1000", Available: "1000"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	ev := AutoExecutionEvidence{Snapshot: snapshot, Orders: map[string]OrderObservation{}, Rules: sizingRules(now), AccountFingerprint: fingerprint}
	market := exchange.LiveMarketSnapshot{MarketSnapshot: exchange.MarketSnapshot{Symbol: "BTCUSDT", Candles: []exchange.Candle{{OpenTime: now.UnixMilli() / 60000 * 60000, CloseTime: now.UnixMilli()/60000*60000 + 59999, Open: 100000, High: 100100, Low: 99900, Close: 100000, Volume: 1}}}, LiveMarketStatus: exchange.LiveMarketStatus{State: "live", Ready: true, LastReceivedAt: now.Add(-time.Second).Format(time.RFC3339Nano)}}
	exec := &autoPathExecutor{}
	guardCalls := 0
	guard := func(context.Context, Intent) error { guardCalls++; return nil }
	path, err := NewAutoExecutionPath(model.TestStrategyID, coord, own, budget, life, exec, guard,
		func(context.Context) (AutoExecutionEvidence, error) { return ev, nil },
		func(context.Context) (exchange.LiveMarketSnapshot, error) { return market, nil })
	if err != nil {
		t.Fatal(err)
	}
	path.now = func() time.Time { return now }
	path.evaluate = func(exchange.LiveMarketSnapshot, model.OwnerState, time.Time) (model.Decision, error) {
		return decision, nil
	}
	return path, dir, coord, exec, &guardCalls
}

func TestSnapshotFromExecutionPreflightPreservesCompleteFlatCollections(t *testing.T) {
	now := time.Date(2026, 10, 17, 5, 0, 0, 0, time.UTC)
	s := exchange.ExecutionPreflightSnapshot{
		Environment: "demo",
		FinishedAt:  now,
		Portfolio: exchange.AccountPortfolio{
			Assets:    []exchange.PortfolioAsset{},
			Positions: []exchange.PortfolioPosition{},
		},
		OpenOrders: []exchange.Order{},
		OpenAlgos:  []exchange.AlgoOrder{},
	}

	got, err := snapshotFromExecutionPreflight(s)
	if err != nil {
		t.Fatal(err)
	}
	if got.Balances == nil || got.Positions == nil || got.Orders == nil || got.AlgoOrders == nil {
		t.Fatalf("complete empty signed collections collapsed to nil: balances=%v positions=%v orders=%v algos=%v",
			got.Balances == nil, got.Positions == nil, got.Orders == nil, got.AlgoOrders == nil)
	}
	if len(got.Balances) != 0 || len(got.Positions) != 0 || len(got.Orders) != 0 || len(got.AlgoOrders) != 0 {
		t.Fatalf("unexpected non-empty flat conversion: %+v", got)
	}

	store, err := NewOwnershipStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	report, err := store.Audit(got, map[string]OrderObservation{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ReadyForAssessment || len(report.Blockers) != 0 {
		t.Fatalf("complete flat signed snapshot must be ownership-auditable: %+v", report)
	}
}

func TestAutoExecutionPathHoldCreatesNoDurableMutation(t *testing.T) {
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	p, _, c, exec, guards := autoPathFixture(t, model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionHold, CandleOpenMS: candle})
	got, err := p.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Dispatched || got.ClientOrderID != "" || exec.calls != 0 || *guards != 0 {
		t.Fatalf("HOLD mutated execution path: %+v calls=%d guards=%d", got, exec.calls, *guards)
	}
	rs, err := c.scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 0 {
		t.Fatalf("HOLD created reservation: %+v", rs)
	}
}

func TestAutoExecutionPathEnterLongConnectsSignalToDurableDispatch(t *testing.T) {
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, _, c, exec, guards := autoPathFixture(t, d)
	got, err := p.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Dispatched || got.Intent.Source != Model || got.Intent.ModelID != model.TestStrategyID || got.Intent.PositionSide != Long || got.Intent.Action != Enter || got.Intent.OrderType != Market || got.Intent.SignalCandleMS != candle || got.Intent.Quantity == "" {
		t.Fatalf("bad auto dispatch: %+v", got)
	}
	if exec.calls != 1 || exec.final != 1 || *guards != 3 {
		t.Fatalf("expected pre + dispatch + final guards, executor=%+v guards=%d", exec, *guards)
	}
	rs, err := c.scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || rs[0].Intent.SignalCandleMS != candle || rs[0].ClientOrderID != got.ClientOrderID {
		t.Fatalf("durable signal candle missing: %+v", rs)
	}
	life, err := p.lifecycle.Load(rs[0])
	if err != nil {
		t.Fatal(err)
	}
	if life.Stage != ExecutionMayHaveSent {
		t.Fatalf("stage=%s", life.Stage)
	}
}

func TestAutoExecutionPathReadinessFailureBeforeReservation(t *testing.T) {
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, _, c, exec, _ := autoPathFixture(t, d)
	p.guard = func(context.Context, Intent) error { return errors.New("stale account") }
	if _, err := p.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "예약 전 readiness") {
		t.Fatalf("expected pre-reservation block: %v", err)
	}
	rs, err := c.scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 0 || exec.calls != 0 {
		t.Fatalf("readiness failure mutated durable state: %+v calls=%d", rs, exec.calls)
	}
}

func TestAutoRequestIDStablePerModelCandleAction(t *testing.T) {
	a := autoRequestID(model.TestStrategyID, 1710000000000, model.ActionEnterLong)
	b := autoRequestID(model.TestStrategyID, 1710000000000, model.ActionEnterLong)
	c := autoRequestID(model.TestStrategyID, 1710000060000, model.ActionEnterLong)
	if a != b || a == c || len(a) < 8 || len(a) > 80 {
		t.Fatalf("ids %q %q %q", a, b, c)
	}
}

func TestModelSignalCandleIntentLegacyCompatibleAndValidated(t *testing.T) {
	legacy := Intent{RequestID: "model_old_001", Source: Model, ModelID: "legacy", Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.01"}
	if err := legacy.Validate(); err != nil {
		t.Fatalf("legacy zero candle rejected: %v", err)
	}
	bad := legacy
	bad.SignalCandleMS = 123
	if err := bad.Validate(); err == nil {
		t.Fatal("unaligned signal candle accepted")
	}
	manual := Intent{RequestID: "manual_001", Source: Manual, Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.01", SignalCandleMS: 1710000000000}
	if err := manual.Validate(); err == nil {
		t.Fatal("manual signal candle accepted")
	}
}

func TestModelDemoLiveGateRechecksSignedCapabilityAndReservation(t *testing.T) {
	candle := time.Now().UTC().Truncate(time.Minute).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, _, c, _, _ := autoPathFixture(t, d)
	// Build the durable reservation/lifecycle using the fake executor first.
	got, err := p.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := got.Intent.Plan()
	if err != nil {
		t.Fatal(err)
	}

	canTrade := true
	equity := "1000"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/time":
			fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		case "/fapi/v3/account":
			fmt.Fprintf(w, `{"totalWalletBalance":%q,"totalUnrealizedProfit":"0","totalMarginBalance":%q,"totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":%q,"assets":[{"asset":"USDT","walletBalance":%q,"availableBalance":%q,"marginBalance":%q}],"positions":[]}`, equity, equity, equity, equity, equity, equity)
		case "/fapi/v1/accountConfig":
			fmt.Fprintf(w, `{"canTrade":%t,"multiAssetsMargin":false,"dualSidePosition":true}`, canTrade)
		case "/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders":
			w.Write([]byte(`[]`))
		case "/fapi/v1/positionSide/dual":
			w.Write([]byte(`{"dualSidePosition":true}`))
		case "/fapi/v1/symbolConfig":
			w.Write([]byte(`[{"symbol":"BTCUSDT","marginType":"ISOLATED","leverage":15,"isAutoAddMargin":false}]`))
		default:
			t.Fatalf("unexpected %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	client := exchange.NewWithBase(srv.URL, srv.Client())
	gate, err := NewModelDemoLiveGate(client, c, p.wallet)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	rules := sizingRules(now)
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	if err := gate(context.Background(), plan, rules, q, DemoCredentials{"k", "s"}); err != nil {
		t.Fatalf("gate should pass: %v", err)
	}
	canTrade = false
	if err := gate(context.Background(), plan, rules, q, DemoCredentials{"k", "s"}); err == nil || !strings.Contains(err.Error(), "trading capability") {
		t.Fatalf("canTrade false accepted: %v", err)
	}
	canTrade = true
	equity = "10"
	if err := gate(context.Background(), plan, rules, q, DemoCredentials{"k", "s"}); err == nil || !strings.Contains(err.Error(), "Equity 30%") {
		t.Fatalf("account-wide equity cap bypassed: %v", err)
	}
}

func TestAutoEquityCapCountsManualExposureAndExactBoundary(t *testing.T) {
	port := exchange.AccountPortfolio{TotalMarginBalance: "1000", TotalInitialMargin: "250", TotalPositionInitialMargin: "250", TotalOpenOrderInitialMargin: "0"}
	if err := checkAutoEquityCap(port, big.NewRat(50, 1)); err != nil {
		t.Fatalf("exact 30 percent boundary rejected: %v", err)
	}
	if err := checkAutoEquityCap(port, big.NewRat(5001, 100)); err == nil || !strings.Contains(err.Error(), "Equity 30%") {
		t.Fatalf("existing account exposure not counted: %v", err)
	}
	port.TotalPositionInitialMargin = "0"
	if err := checkAutoEquityCap(port, big.NewRat(1, 1)); err == nil || !strings.Contains(err.Error(), "구성 불일치") {
		t.Fatalf("inconsistent signed margin totals accepted: %v", err)
	}
}

func TestDemoAutoExecutionRuntimeCompositionKeepsServerUnlockSeparate(t *testing.T) {
	dir := t.TempDir()
	client := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	creds := func(context.Context) (DemoCredentials, error) { return DemoCredentials{"k", "s"}, nil }
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) { return exchange.LiveMarketSnapshot{}, nil }
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewDemoAutoExecutionRuntime(dir, model.TestStrategyID, client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	if rt.Path == nil || rt.coordinator == nil {
		t.Fatal("runtime composition missing")
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
}

func signedAutoPreflight(now time.Time, canTrade bool) exchange.ExecutionPreflightSnapshot {
	return exchange.ExecutionPreflightSnapshot{
		Environment: "demo",
		StartedAt:   now.Add(-100 * time.Millisecond),
		FinishedAt:  now,
		Portfolio: exchange.AccountPortfolio{
			CanTrade:                    canTrade,
			TotalWalletBalance:          "1000",
			TotalUnrealizedProfit:       "0",
			TotalMarginBalance:          "1000",
			TotalInitialMargin:          "0",
			TotalPositionInitialMargin:  "0",
			TotalOpenOrderInitialMargin: "0",
			AvailableBalance:            "1000",
			Assets:                      []exchange.PortfolioAsset{{Asset: "USDT", WalletBalance: "1000", AvailableBalance: "1000", MarginBalance: "1000"}},
			Positions:                   []exchange.PortfolioPosition{},
		},
		OpenOrders: []exchange.Order{},
		OpenAlgos:  []exchange.AlgoOrder{},
		Configuration: exchange.TradingConfiguration{
			Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "ISOLATED", Leverage: 15,
			AutoAddMargin: false, SettingsMatchV1: true, AsOf: now.Format(time.RFC3339Nano), Warnings: []string{},
		},
	}
}

func liveAutoMarket(now time.Time) exchange.LiveMarketSnapshot {
	return exchange.LiveMarketSnapshot{
		MarketSnapshot: exchange.MarketSnapshot{Symbol: "BTCUSDT", Interval: "1m", Source: "BINANCE_FUTURES_DEMO_WEBSOCKET"},
		LiveMarketStatus: exchange.LiveMarketStatus{
			State: "live", Ready: true, StreamSymbol: "BTCUSDT", StreamInterval: "1m",
			LastReceivedAt: now.Add(-100 * time.Millisecond).Format(time.RFC3339Nano), LastEventMs: now.Add(-100 * time.Millisecond).UnixMilli(),
		},
	}
}

func TestAutoScopedReadinessAllowsOnlyOwnReservedAndMayHaveSent(t *testing.T) {
	candle := time.Now().UTC().Truncate(time.Minute).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, dir, coord, _, _ := autoPathFixture(t, d)
	intent := Intent{RequestID: autoRequestID(model.TestStrategyID, candle, model.ActionEnterLong), Source: Model, ModelID: model.TestStrategyID, Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.001", SignalCandleMS: candle}
	now := time.Now().UTC()
	basis := cleanReservationBasis(now)
	basis.WalletVerified = true
	basis.AutoWalletAssignedUSDT = "1000"
	res, _, err := coord.reservePreview(intent, basis, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.lifecycle.Begin(res, now); err != nil {
		t.Fatal(err)
	}
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) {
		return liveAutoMarket(time.Now().UTC()), nil
	}
	lowEquity := false
	reader := preflightReaderFunc(func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
		signed := signedAutoPreflight(time.Now().UTC(), true)
		if lowEquity {
			signed.Portfolio.TotalWalletBalance = "1"
			signed.Portfolio.TotalMarginBalance = "1"
			signed.Portfolio.AvailableBalance = "1"
			signed.Portfolio.Assets[0].WalletBalance = "1"
			signed.Portfolio.Assets[0].MarginBalance = "1"
			signed.Portfolio.Assets[0].AvailableBalance = "1"
		}
		return signed, nil
	})
	creds := func(context.Context) (DemoCredentials, error) { return DemoCredentials{APIKey: "k", Secret: "s"}, nil }
	guard, err := NewAutoSignedTradingReadinessGuard(dir, coord, p.wallet, reader, creds, market)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard(context.Background(), intent); err != nil {
		t.Fatalf("own RESERVED lifecycle must be allowed: %v", err)
	}
	if _, err := p.lifecycle.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := guard(context.Background(), intent); err != nil {
		t.Fatalf("own MAY_HAVE_SENT lifecycle must be allowed for final guard: %v", err)
	}
	lowEquity = true
	if err := guard(context.Background(), intent); err == nil || !strings.Contains(err.Error(), "Equity 30%") {
		t.Fatalf("final signed reread failed to enforce equity cap: %v", err)
	}
}

func TestAutoScopedReadinessBlocksDifferentPendingLifecycle(t *testing.T) {
	candle := time.Now().UTC().Truncate(time.Minute).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, dir, coord, _, _ := autoPathFixture(t, d)
	// Create one unrelated model reservation/lifecycle. It must remain a global blocker.
	other := Intent{RequestID: "auto_other_pending_01", Source: Model, ModelID: model.TestStrategyID, Symbol: "BTCUSDT", PositionSide: Short, Action: Enter, OrderType: Market, Quantity: "0.001", SignalCandleMS: candle - 60000}
	basis := cleanReservationBasis(time.Now().UTC())
	basis.WalletVerified = true
	basis.AutoWalletAssignedUSDT = "1000"
	res, _, err := coord.reservePreview(other, basis, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.lifecycle.Begin(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	p.market = func(context.Context) (exchange.LiveMarketSnapshot, error) {
		return liveAutoMarket(time.Now().UTC()), nil
	}
	reader := preflightReaderFunc(func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
		return signedAutoPreflight(time.Now().UTC(), true), nil
	})
	creds := func(context.Context) (DemoCredentials, error) { return DemoCredentials{APIKey: "k", Secret: "s"}, nil }
	guard, err := NewAutoSignedTradingReadinessGuard(dir, coord, p.wallet, reader, creds, p.market)
	if err != nil {
		t.Fatal(err)
	}
	intent := Intent{RequestID: autoRequestID(model.TestStrategyID, candle, model.ActionEnterLong), Source: Model, ModelID: model.TestStrategyID, Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.001", SignalCandleMS: candle}
	if err := guard(context.Background(), intent); err == nil || !strings.Contains(err.Error(), "EXECUTION_RECOVERY_REQUIRED") {
		t.Fatalf("different pending lifecycle must block: %v", err)
	}
}

func TestAutoScopedReadinessBlocksRecoveryExclusiveLock(t *testing.T) {
	candle := time.Now().UTC().Truncate(time.Minute).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, dir, coord, _, _ := autoPathFixture(t, d)
	if err := os.MkdirAll(filepath.Join(dir, "trading", "recovery_sessions", "exclusive.active"), 0700); err != nil {
		t.Fatal(err)
	}
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) {
		return liveAutoMarket(time.Now().UTC()), nil
	}
	reader := preflightReaderFunc(func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
		return signedAutoPreflight(time.Now().UTC(), true), nil
	})
	creds := func(context.Context) (DemoCredentials, error) { return DemoCredentials{APIKey: "k", Secret: "s"}, nil }
	guard, err := NewAutoSignedTradingReadinessGuard(dir, coord, p.wallet, reader, creds, market)
	if err != nil {
		t.Fatal(err)
	}
	intent := Intent{RequestID: autoRequestID(model.TestStrategyID, candle, model.ActionEnterLong), Source: Model, ModelID: model.TestStrategyID, Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.001", SignalCandleMS: candle}
	if err := guard(context.Background(), intent); err == nil || !strings.Contains(err.Error(), "recovery probe") {
		t.Fatalf("recovery lock must block: %v", err)
	}
}

func TestAutoScopedReadinessRejectsCredentialRotationBetweenGuards(t *testing.T) {
	candle := time.Now().UTC().Truncate(time.Minute).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, dir, coord, _, _ := autoPathFixture(t, d)
	intent := Intent{RequestID: autoRequestID(model.TestStrategyID, candle, model.ActionEnterLong), Source: Model, ModelID: model.TestStrategyID, Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.001", SignalCandleMS: candle}
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) {
		return liveAutoMarket(time.Now().UTC()), nil
	}
	reader := preflightReaderFunc(func(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
		return signedAutoPreflight(time.Now().UTC(), true), nil
	})
	apiKey := "k"
	creds := func(context.Context) (DemoCredentials, error) {
		return DemoCredentials{APIKey: apiKey, Secret: "s"}, nil
	}
	guard, err := NewAutoSignedTradingReadinessGuard(dir, coord, p.wallet, reader, creds, market)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard(context.Background(), intent); err != nil {
		t.Fatalf("original credential should pass: %v", err)
	}
	apiKey = "rotated-key"
	if err := guard(context.Background(), intent); err == nil || !strings.Contains(err.Error(), "WALLET_UNVERIFIED") {
		t.Fatalf("rotated credential must fail wallet binding before POST: %v", err)
	}
}

func TestAutoExecutionSameSignalNeverDispatchesTwice(t *testing.T) {
	candle := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	d := model.Decision{ModelID: model.TestStrategyID, Status: model.StatusReady, Action: model.ActionEnterLong, CandleOpenMS: candle, LongEntry: true}
	p, _, c, exec, _ := autoPathFixture(t, d)
	first, err := p.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !first.Dispatched || exec.calls != 1 {
		t.Fatalf("first=%+v calls=%d", first, exec.calls)
	}
	_, secondErr := p.RunOnce(context.Background())
	if secondErr == nil {
		t.Fatal("same pending signal unexpectedly dispatched twice")
	}
	if exec.calls != 1 {
		t.Fatalf("duplicate signal reached executor: calls=%d err=%v", exec.calls, secondErr)
	}
	rs, err := c.scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || rs[0].ClientOrderID != first.ClientOrderID {
		t.Fatalf("duplicate signal created additional reservation: %+v", rs)
	}
}
