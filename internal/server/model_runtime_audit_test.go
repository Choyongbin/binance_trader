package server

import (
	"encoding/json"
	"testing"

	"binance_trader/internal/model"
)

func TestRuntimeStatusReflectsRealRegistryAndSupervisionLimits(t *testing.T) {
	s, err := newWithFetcher(t.TempDir(), modelSelectionFakeFetcher{freshSignedModelAccount()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	getAudit := func() struct {
		PreviewReady bool `json:"preview_configuration_ready"`
		MultiFactory bool `json:"internal_multi_factory_wired"`
		Authorized   bool `json:"trading_authorized"`
		Audit        struct {
			State          string   `json:"state"`
			Count          int      `json:"registered_count"`
			LegacyVerified bool     `json:"legacy_logistic_models_verified"`
			OneOrder       bool     `json:"supervised_one_order_per_run"`
			Models         []string `json:"registered_model_ids"`
		} `json:"model_registry_audit"`
	} {
		t.Helper()
		r := request(s.Handler(), "GET", "/api/v1/models/runtime", "", "", "")
		if r.Code != 200 {
			t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
		}
		var v struct {
			PreviewReady bool `json:"preview_configuration_ready"`
			MultiFactory bool `json:"internal_multi_factory_wired"`
			Authorized   bool `json:"trading_authorized"`
			Audit        struct {
				State          string   `json:"state"`
				Count          int      `json:"registered_count"`
				LegacyVerified bool     `json:"legacy_logistic_models_verified"`
				OneOrder       bool     `json:"supervised_one_order_per_run"`
				Models         []string `json:"registered_model_ids"`
			} `json:"model_registry_audit"`
		}
		if err := json.Unmarshal(r.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	v := getAudit()
	if v.PreviewReady || v.MultiFactory || v.Authorized || v.Audit.Count != 2 || v.Audit.LegacyVerified || !v.Audit.OneOrder {
		t.Fatal(v)
	}
	if _, err := s.store.UpdateSelection([]string{model.TestStrategyID, model.StrictStrategyID}); err != nil {
		t.Fatal(err)
	}
	v = getAudit()
	if !v.PreviewReady || !v.MultiFactory || v.Authorized || v.Audit.Count != 2 || v.Audit.LegacyVerified || v.Audit.State != "CONFIG_CONSISTENT_TRADING_LOCKED" {
		t.Fatal(v)
	}
}
