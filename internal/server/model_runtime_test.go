package server

import (
	"binance_trader/internal/app"
	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	"binance_trader/internal/trading"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type modelSelectionFakeFetcher struct{ snapshot exchange.Snapshot }

func (f modelSelectionFakeFetcher) Fetch(context.Context, string, string) (exchange.Snapshot, error) {
	s := f.snapshot
	s.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
	return s, nil
}
func freshSignedModelAccount() exchange.Snapshot {
	return exchange.Snapshot{Environment: "demo", Status: "connected", Balances: []exchange.Balance{{Asset: "USDT", Available: "1000"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
}
func TestServerModelSelectionBindsActualControllerFactory(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	origin := "http://127.0.0.1:8080"
	status := request(h, "GET", "/api/v1/models/runtime", "", "", "")
	if status.Code != 200 {
		t.Fatal(status.Code)
	}
	var doc struct {
		RuntimeModelID string `json:"runtime_model_id"`
		Ready          bool   `json:"runtime_selection_ready"`
		Authorized     bool   `json:"trading_authorized"`
	}
	if err := json.Unmarshal(status.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Ready || doc.Authorized || doc.RuntimeModelID != "" {
		t.Fatal("unselected runtime became ready", doc)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1"]}`); r.Code != 423 {
		t.Fatalf("locked credentials accepted selection: %d %s", r.Code, r.Body)
	}
	if err := s.demo.Connect(context.Background(), "local-key", "local-secret"); err != nil {
		t.Fatal(err)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1"]}`); r.Code != 200 {
		t.Fatalf("safe selection denied: %d %s", r.Code, r.Body)
	}
	chosen := s.store.Snapshot().SelectedModelIDs
	modelID, err := selectedRuntimeModel(chosen, s.strategyRegistry)
	if err != nil || modelID != model.TestStrategyID {
		t.Fatalf("factory model mismatch: %s %v", modelID, err)
	}
	status = request(h, "GET", "/api/v1/models/runtime", "", "", "")
	if err := json.Unmarshal(status.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if !doc.Ready || doc.Authorized || doc.RuntimeModelID != model.TestStrategyID {
		t.Fatalf("unexpected runtime: %+v", doc)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1"]}`); r.Code != 200 {
		t.Fatal("same selection rejected", r.Code)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["unknown_model"]}`); r.Code != 400 {
		t.Fatal("unknown model accepted", r.Code)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1","ema_rsi_macd_v1"]}`); r.Code != 400 {
		t.Fatal("multiple strategies accepted", r.Code)
	}
	for _, route := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		if r := request(h, "POST", route, token, origin, `{}`); r.Code != 423 {
			t.Fatalf("order route unlocked %s: %d", route, r.Code)
		}
	}
	// Stopping a session may not rewrite the already stored selection.
	s.demo.Clear()
	if got := s.store.Snapshot().SelectedModelIDs; len(got) != 1 {
		t.Fatal("selection lost", got)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":[]}`); r.Code != 423 {
		t.Fatal("offline model change allowed", r.Code)
	}
	if _, err := selectedRuntimeModel(nil, model.DefaultRegistry()); err == nil {
		t.Fatal("empty selection accepted by controller")
	}
	if _, err := selectedRuntimeModel([]string{"bad"}, model.DefaultRegistry()); err == nil {
		t.Fatal("unregistered model accepted by controller")
	}
	if current := s.store.Snapshot(); current.Environment != app.Demo {
		t.Fatal("fixture environment altered")
	}
}
func TestModelSwitchRejectsPendingLocalEvidence(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	if err := s.demo.Connect(context.Background(), "k", "s"); err != nil {
		t.Fatal(err)
	}
	// An actual durable RUNNING marker must be stopped; its presence alone
	// must not authorize a selection change. Corrupt evidence blocks instead.
	if _, err := s.autoControl.Start(); err != nil {
		t.Fatal(err)
	}
	// Inject unknown lifecycle file in a valid directory to simulate unsafe evidence.
	if err := os.MkdirAll(filepath.Join(dir, "trading", "execution_lifecycle"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "trading", "execution_lifecycle", "unknown.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, "http://127.0.0.1:8080", `{"ids":["ema_rsi_macd_v1"]}`); r.Code != 423 {
		t.Fatalf("pending evidence did not block model change: %d %s", r.Code, r.Body)
	}
	if len(s.store.Snapshot().SelectedModelIDs) != 0 {
		t.Fatal("unsafe selection persisted")
	}
	state, err := s.autoControl.Status()
	if err != nil || state.State != trading.AutoRunStopped {
		t.Fatalf("controller lease not stopped: %+v %v", state, err)
	}
}

func TestModelChangeSignedAccountNotFlat(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update func(*exchange.Snapshot)
	}{
		{"long", func(s *exchange.Snapshot) {
			s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.001"}}
		}},
		{"short", func(s *exchange.Snapshot) {
			s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "SHORT", Qty: "-0.001"}}
		}},
		{"pending order", func(s *exchange.Snapshot) { s.Orders = []exchange.Order{{Symbol: "BTCUSDT"}} }},
		{"pending algo", func(s *exchange.Snapshot) { s.AlgoOrders = []exchange.AlgoOrder{{Symbol: "BTCUSDT"}} }},
		{"missing signed positions", func(s *exchange.Snapshot) { s.Positions = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := freshSignedModelAccount()
			tc.update(&snapshot)
			s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{snapshot})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			token := bootstrapToken(t, s.Handler())
			if err := s.demo.Connect(context.Background(), "local-key", "local-secret"); err != nil {
				t.Fatal(err)
			}
			res := request(s.Handler(), "PUT", "/api/v1/models/selection", token, "http://127.0.0.1:8080", `{"ids":["ema_rsi_macd_v1"]}`)
			if res.Code != 423 {
				t.Fatalf("unsafe signed account accepted: %d %s", res.Code, res.Body)
			}
			if len(s.store.Snapshot().SelectedModelIDs) != 0 {
				t.Fatal("unsafe selection persisted")
			}
		})
	}
}
