package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

func liveSignalMarket(now time.Time) exchange.LiveMarketSnapshot {
	minute := now.UnixMilli() / 60000 * 60000
	candles := make([]exchange.Candle, 180)
	for i := range candles {
		start := minute - int64(179-i)*60000
		value := 100.0 + float64(i)*0.1
		candles[i] = exchange.Candle{OpenTime: start, CloseTime: start + 59999, Open: value, High: value + 1, Low: value - 1, Close: value, Volume: 1}
	}
	return exchange.LiveMarketSnapshot{MarketSnapshot: exchange.MarketSnapshot{Symbol: "BTCUSDT", Source: "BINANCE_FUTURES_DEMO_WEBSOCKET", Interval: "1m", Candles: candles}, LiveMarketStatus: exchange.LiveMarketStatus{Ready: true, State: "live", StreamSymbol: "BTCUSDT", StreamInterval: "1m", LastEventMs: now.UnixMilli(), LastReceivedAt: now.UTC().Format(time.RFC3339Nano)}}
}

func readMarketSignals(t *testing.T, s *Server) modelSignalStatus {
	t.Helper()
	reply := request(s.Handler(), "GET", "/api/v1/models/market-signals", "", "", "")
	if reply.Code != 200 {
		t.Fatalf("market signals endpoint: %d %s", reply.Code, reply.Body)
	}
	var status modelSignalStatus
	if err := json.Unmarshal(reply.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestModelSignalObserverReadOnlyEndToEnd(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ids := []string{model.TestStrategyID, model.StrictStrategyID}
	if _, err := s.store.UpdateSelection(ids); err != nil {
		t.Fatal(err)
	}
	var unavailable atomic.Bool
	s.modelSignalMarketProvider = func() (exchange.LiveMarketSnapshot, error) {
		if unavailable.Load() {
			return exchange.LiveMarketSnapshot{}, errors.New("disconnected")
		}
		return liveSignalMarket(time.Now().UTC()), nil
	}
	s.modelObserver.Start(s.sampleModelSignals)
	s.sampleModelSignals()
	status := readMarketSignals(t, s)
	if !status.SchedulerRunning || status.State != "MARKET_SIGNALS_ONLY" || len(status.Signals) != 2 || status.Signals[0].ModelID != ids[0] || status.Signals[1].ModelID != ids[1] || status.TradingAuthorized || status.ExecutionEnabled || status.OwnershipVerified || status.RiskPreviewReady || status.SignedAccountFresh || status.Assumption != "HYPOTHETICAL_FLAT_NO_ACCOUNT_OR_OWNER_ATTESTATION" {
		t.Fatalf("observer unsafe/nonfunctional: %+v", status)
	}
	for _, signal := range status.Signals {
		if signal.State != "INDICATIVE_ONLY" || (signal.Direction != "LONG" && signal.Direction != "SHORT" && signal.Direction != "HOLD") {
			t.Fatal(signal)
		}
	}
	runtime := request(s.Handler(), "GET", "/api/v1/models/runtime", "", "", "")
	var rt struct {
		Running    bool `json:"scheduler_running"`
		Authorized bool `json:"trading_authorized"`
		Enabled    bool `json:"execution_enabled"`
	}
	if err := json.Unmarshal(runtime.Body.Bytes(), &rt); err != nil || !rt.Running || rt.Authorized || rt.Enabled {
		t.Fatalf("runtime: %s %v", runtime.Body, err)
	}
	token := bootstrapToken(t, s.Handler())
	for _, route := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		if r := request(s.Handler(), "POST", route, token, "http://127.0.0.1:8080", `{}`); r.Code != 423 {
			t.Fatalf("trade route unlocked %s: %d", route, r.Code)
		}
	}
	if r := request(s.Handler(), "GET", "/api/v1/models/market-signals?unsafe=1", "", "", ""); r.Code != 400 {
		t.Fatal("unexpected query accepted")
	}
	if r := request(s.Handler(), "POST", "/api/v1/models/market-signals", "", "", `{}`); r.Code != 405 && r.Code != 404 {
		t.Fatalf("read-only endpoint accepted mutation: status %d body %s", r.Code, r.Body.String())
	}

	unavailable.Store(true)
	s.sampleModelSignals()
	blocked := readMarketSignals(t, s)
	if blocked.State != "WAITING_FOR_FRESH_DEMO_WS" || len(blocked.Signals) != 0 || blocked.ExecutionEnabled {
		t.Fatalf("stale model signals leaked: %+v", blocked)
	}

	unavailable.Store(false)
	s.sampleModelSignals()
	late := s.modelSignalStatusAt(time.Now().UTC().Add(70 * time.Second))
	if len(late.Signals) != 0 || late.State == "MARKET_SIGNALS_ONLY" || late.TradingAuthorized {
		t.Fatalf("stale cache replay %+v", late)
	}

	if _, err := s.store.UpdateSelection([]string{model.TestStrategyID}); err != nil {
		t.Fatal(err)
	}
	if changed := readMarketSignals(t, s); len(changed.Signals) != 0 || changed.State != "WAITING_FOR_MULTI_SELECTION" {
		t.Fatalf("old model leaked after selection change: %+v", changed)
	}
	if _, err := s.store.UpdateEnvironment(app.Real); err != nil {
		t.Fatal(err)
	}
	if status := readMarketSignals(t, s); status.State != "BLOCKED_REAL_ENVIRONMENT" || len(status.Signals) != 0 {
		t.Fatalf("Real exposure: %+v", status)
	}
}

func TestModelSignalObserverConcurrentReadAndTick(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.store.UpdateSelection([]string{model.TestStrategyID, model.StrictStrategyID}); err != nil {
		t.Fatal(err)
	}
	s.modelSignalMarketProvider = func() (exchange.LiveMarketSnapshot, error) { return liveSignalMarket(time.Now().UTC()), nil }
	s.modelObserver.Start(s.sampleModelSignals)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				s.sampleModelSignals()
				status := s.modelSignalStatusAt(time.Now().UTC())
				if status.TradingAuthorized || status.ExecutionEnabled || status.OwnershipVerified {
					t.Error("observer authorized trading")
				}
			}
		}()
	}
	wg.Wait()
}

func TestModelSignalEndpointNoServerStart(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if reply := request(s.Handler(), http.MethodGet, "/api/v1/models/market-signals", "", "", ""); reply.Code != 200 {
		t.Fatal(reply.Code)
	}
	if got := readMarketSignals(t, s); got.SchedulerRunning || len(got.Signals) != 0 || got.TradingAuthorized {
		t.Fatal(got)
	}
}

// Regression: the background initial tick and an explicit sample must never
// read the provider concurrently. Otherwise a delayed successful sample can
// overwrite a newer disconnected status and leak stale market indicators.
func TestModelSignalObserverSerializesSamples(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.store.UpdateSelection([]string{model.TestStrategyID, model.StrictStrategyID}); err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	var maxActive atomic.Int32
	s.modelSignalMarketProvider = func() (exchange.LiveMarketSnapshot, error) {
		n := active.Add(1)
		for {
			old := maxActive.Load()
			if n <= old || maxActive.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		time.Sleep(time.Millisecond * 4)
		return liveSignalMarket(time.Now().UTC()), nil
	}
	s.modelObserver.Start(s.sampleModelSignals)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.sampleModelSignals() }()
	}
	wg.Wait()
	if maxActive.Load() != 1 {
		t.Fatalf("overlapping provider samples: %d", maxActive.Load())
	}
}
