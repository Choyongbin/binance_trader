package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"binance_trader/internal/model"
)

func TestMultiSelectionIsPreviewOnlyAndRealOrderLocked(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	origin := "http://127.0.0.1:8080"
	ids := []string{model.TestStrategyID, model.StrictStrategyID}
	body := `{"ids":["` + ids[0] + `","` + ids[1] + `"]}`
	if res := request(h, "PUT", "/api/v1/models/selection", token, origin, body); res.Code != 423 {
		t.Fatalf("unauthenticated multi selection allowed: %d", res.Code)
	}
	if err = s.demo.Connect(context.Background(), "offline-key", "offline-secret"); err != nil {
		t.Fatal(err)
	}
	if res := request(h, "PUT", "/api/v1/models/selection", token, origin, body); res.Code != 200 {
		t.Fatalf("verified multi preference rejected: %d %s", res.Code, res.Body.String())
	}
	if _, err := selectedRuntimeModel(ids, s.strategyRegistry); err == nil {
		t.Fatal("two models bound to live order runtime")
	}
	r := request(h, "GET", "/api/v1/models/runtime", "", "", "")
	var status struct {
		Mode       string   `json:"scheduler_mode"`
		Runtime    string   `json:"runtime_model_id"`
		Ready      bool     `json:"runtime_selection_ready"`
		Wired      bool     `json:"internal_multi_factory_wired"`
		Preview    bool     `json:"preview_configuration_ready"`
		Running    bool     `json:"scheduler_running"`
		Enabled    bool     `json:"execution_enabled"`
		Authorized bool     `json:"trading_authorized"`
		Locked     bool     `json:"auto_start_locked"`
		IDs        []string `json:"scheduler_selected_ids"`
	}
	if err = json.Unmarshal(r.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if r.Code != 200 || status.Mode != "MULTI_READ_ONLY_PREVIEW" || status.Runtime != "" || status.Ready || !status.Preview || !status.Wired || status.Running || status.Enabled || status.Authorized || !status.Locked || len(status.IDs) != 2 {
		t.Fatalf("runtime exposed an unsafe multi-execution capability: %+v", status)
	}
	for _, route := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		if r = request(h, "POST", route, token, origin, `{}`); r.Code != 423 {
			t.Fatalf("order route unlocked: %s %d", route, r.Code)
		}
	}
	if r = request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1","ema_rsi_macd_v1"]}`); r.Code != 400 {
		t.Fatal("duplicate selection accepted")
	}
	if r = request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1","ema_rsi_macd_strict_v1","other"]}`); r.Code != 400 {
		t.Fatal("third model accepted")
	}
	if err = os.MkdirAll(filepath.Join(dir, "trading", "execution_lifecycle"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "trading", "execution_lifecycle", "invalid.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if res := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1"]}`); res.Code != 423 {
		t.Fatal("unsafe pending evidence did not block switching away from multi selection", res.Code)
	}
	if got := s.store.Snapshot().SelectedModelIDs; len(got) != 2 {
		t.Fatal("failed mutation changed saved preference", got)
	}
}
