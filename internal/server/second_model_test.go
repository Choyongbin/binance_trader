package server

import (
	"binance_trader/internal/model"
	"context"
	"encoding/json"
	"testing"
)

func TestSecondModelSignedSelectionAndRuntimeMapping(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	origin := "http://127.0.0.1:8080"
	if err = s.demo.Connect(context.Background(), "offline-test-key", "offline-test-secret"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{model.StrictStrategyID, model.TestStrategyID, model.StrictStrategyID} {
		response := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["`+id+`"]}`)
		if response.Code != 200 {
			t.Fatalf("cannot select %s: HTTP %d %s", id, response.Code, response.Body.String())
		}
		runtime := request(h, "GET", "/api/v1/models/runtime", "", "", "")
		var doc struct {
			RuntimeModelID string   `json:"runtime_model_id"`
			IDs            []string `json:"registered_model_ids"`
			Ready          bool     `json:"runtime_selection_ready"`
			Trading        bool     `json:"trading_authorized"`
			Locked         bool     `json:"auto_start_locked"`
		}
		if err := json.Unmarshal(runtime.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		if runtime.Code != 200 || doc.RuntimeModelID != id || len(doc.IDs) != 2 || !doc.Ready || doc.Trading || !doc.Locked {
			t.Fatalf("runtime=%+v response=%s", doc, runtime.Body.String())
		}
		selected, err := selectedRuntimeModel(s.store.Snapshot().SelectedModelIDs, s.strategyRegistry)
		if err != nil || selected != id {
			t.Fatalf("not bound: %q %v", selected, err)
		}
	}
	for _, route := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		response := request(h, "POST", route, token, origin, `{}`)
		if response.Code != 423 {
			t.Fatalf("unsafe unlock %s HTTP %d", route, response.Code)
		}
	}
}
