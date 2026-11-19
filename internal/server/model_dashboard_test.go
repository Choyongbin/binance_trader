package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"binance_trader/internal/app"
	"binance_trader/internal/model"
)

func TestModelDashboardReadOnlyFailClosedAndNoFabricatedPnL(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := s.Handler()
	token := bootstrapToken(t, handler)
	before := inventoryFiles(t, dir)
	reply := request(handler, "GET", "/api/v1/trading/model-dashboard", "", "", "")
	if reply.Code != http.StatusOK {
		t.Fatalf("status %d: %s", reply.Code, reply.Body.String())
	}
	if reply.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("response could be cached")
	}
	var v modelDashboardView
	if err := json.Unmarshal(reply.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.TradingAuthorized || v.ExecutionEnabled || !v.AutoStartLocked || !v.AutoStopLocked || !v.ManualOrderLocked ||
		v.AutomaticResume || v.RiskPreviewReady || v.OwnershipVerified || v.HistoricalTradesVerified ||
		v.RiskState != "BLOCKED_MANUAL_REVIEW_REQUIRED" || v.PnLAttributionStatus != "MODEL_FINAL_NET_AND_UNREALIZED_UNVERIFIED" || len(v.RiskBlockers) == 0 {
		t.Fatalf("dangerous dashboard status %+v", v)
	}
	if len(v.Models) != len(model.DefaultRegistry().IDs()) {
		t.Fatalf("unexpected model rows: %+v", v.Models)
	}
	for _, row := range v.Models {
		if row.Selected || row.RealizedPnLStatus != "UNVERIFIED" || row.UnrealizedPnLStatus != "UNVERIFIED" ||
			len(row.LocalLongQuantities) != 0 || len(row.LocalShortQuantities) != 0 || row.AttributionStatus != "NO_VERIFIED_MODEL_POSITION" || row.MarketSignal != "UNAVAILABLE" {
			t.Fatalf("fabricated PnL, ownership or signal: %+v", row)
		}
	}
	after := inventoryFiles(t, dir)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("GET created/changed files before=%v after=%v", before, after)
	}
	if reply := request(handler, "GET", "/api/v1/trading/model-dashboard?override=1", "", "", ""); reply.Code != 400 {
		t.Fatal("accepted forged query", reply.Code)
	}
	for _, endpoint := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		if r := request(handler, "POST", endpoint, token, "http://127.0.0.1:8080", `{}`); r.Code != http.StatusLocked {
			t.Fatalf("order unlocked %s: %d", endpoint, r.Code)
		}
	}
	// Signed Demo snapshot itself is not ownership proof or a PnL attestation.
	if err := s.demo.Connect(context.Background(), "fake-key", "fake-secret"); err != nil {
		t.Fatal(err)
	}
	if r := request(handler, "GET", "/api/v1/trading/model-dashboard", "", "", ""); r.Code != http.StatusOK || !strings.Contains(r.Body.String(), `"trading_authorized":false`) {
		t.Fatal("auth incorrectly became permit", r.Code, r.Body.String())
	}
	if _, err := s.store.UpdateEnvironment(app.Real); err != nil {
		t.Fatal(err)
	}
	if r := request(handler, "GET", "/api/v1/trading/model-dashboard", "", "", ""); r.Code != http.StatusLocked {
		t.Fatal("real mode unguarded", r.Code)
	}
}

func TestModelDashboardSelectedModelsRemainNonAuthorizing(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	token := bootstrapToken(t, s.Handler())
	if err := s.demo.Connect(context.Background(), "key", "secret"); err != nil {
		t.Fatal(err)
	}
	ids := []string{model.StrictStrategyID, model.TestStrategyID}
	r := request(s.Handler(), "PUT", "/api/v1/models/selection", token, "http://127.0.0.1:8080", `{"ids":["ema_rsi_macd_strict_v1","ema_rsi_macd_v1"]}`)
	if r.Code != 200 {
		t.Fatalf("selection: %d %s", r.Code, r.Body)
	}
	resp := request(s.Handler(), "GET", "/api/v1/trading/model-dashboard", "", "", "")
	if resp.Code != 200 {
		t.Fatalf("status: %d %s", resp.Code, resp.Body)
	}
	var v modelDashboardView
	if err := json.Unmarshal(resp.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v.SelectedModelIDs, ids) {
		t.Fatalf("selection order lost %+v", v.SelectedModelIDs)
	}
	selected := 0
	for _, m := range v.Models {
		if m.Selected {
			selected++
		}
		if m.MarketSignal != "UNAVAILABLE" || m.RealizedPnLStatus != "UNVERIFIED" {
			t.Fatal("fabricated execution", m)
		}
	}
	if selected != 2 || v.TradingAuthorized || !v.AutoStartLocked {
		t.Fatal("multi selection trading enabled", v)
	}
}

func TestModelDashboardUIEvidenceAndLockedControls(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"/", []string{"modelControlState", "modelLifecycleState", "modelSignedState", "modelAccountingState", "modelStatsBody", "modelRiskState"}},
		{"/app.js", []string{"/api/v1/trading/model-dashboard", "MODEL_FINAL_NET_AND_UNREALIZED_UNVERIFIED", "renderModelDashboard", "auto_start_locked!==true"}},
	} {
		r := request(s.Handler(), "GET", tc.path, "", "", "")
		if r.Code != 200 {
			t.Fatalf("asset %s: %d", tc.path, r.Code)
		}
		for _, want := range tc.want {
			if !strings.Contains(r.Body.String(), want) {
				t.Fatalf("asset %s missing %s", tc.path, want)
			}
		}
	}
}

func inventoryFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		out[rel] = string(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
