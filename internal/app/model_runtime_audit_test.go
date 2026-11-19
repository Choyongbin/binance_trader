package app

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"binance_trader/internal/model"
)

func TestModelAuditRealRegistryAndCatalog(t *testing.T) {
	a := AuditModelConfiguration(model.DefaultRegistry(), []string{TestModelID, StrictModelID})
	if a.State != "CONFIG_CONSISTENT_TRADING_LOCKED" || !a.CatalogRegistryConsistent ||
		!a.SelectionResolved || !a.MultiModelConfigured || a.RegisteredCount != 2 || len(a.Blockers) != 0 {
		t.Fatalf("actual registry/catalog mismatch: %+v", a)
	}
	if !reflect.DeepEqual(a.RegisteredModelIDs, []string{TestModelID, StrictModelID}) &&
		!reflect.DeepEqual(a.RegisteredModelIDs, []string{StrictModelID, TestModelID}) {
		t.Fatal(a.RegisteredModelIDs)
	}
	if a.TradingAuthorized || a.LiveMultiOrderTradingEnabled || a.ModelFinalPnLVerified ||
		a.LegacyLogisticModelsVerified || a.IndependentModelPositions || !a.SharedAccountBudget || !a.SupervisedOneOrderPerRun {
		t.Fatalf("unverified capabilities were promoted: %+v", a)
	}
	one := AuditModelConfiguration(model.DefaultRegistry(), []string{TestModelID})
	if !one.SingleModelConfigured || one.MultiModelConfigured || one.TradingAuthorized {
		t.Fatal(one)
	}
	zero := AuditModelConfiguration(model.DefaultRegistry(), nil)
	if zero.State != "NO_MODEL_SELECTED" || zero.SelectionResolved || !zero.CatalogRegistryConsistent {
		t.Fatal(zero)
	}
}

func TestModelAuditRejectsUnknownDuplicateOrDrift(t *testing.T) {
	r := model.DefaultRegistry()
	for _, tc := range [][]string{{TestModelID, TestModelID}, {TestModelID, "logistic_unverified"}, {""}} {
		a := AuditModelConfiguration(r, tc)
		if a.SelectionResolved || len(a.Blockers) == 0 || a.TradingAuthorized || a.MultiModelConfigured {
			t.Fatal(tc, a)
		}
	}
	for _, catalog := range [][]Model{
		{{ID: TestModelID, Name: "test", Description: "test", Implemented: true}},
		{{ID: TestModelID, Name: "test", Description: "test", Implemented: true}, {ID: TestModelID, Name: "test", Description: "test", Implemented: true}},
		{{ID: TestModelID, Name: "test", Description: "test", Implemented: true}, {ID: StrictModelID, Name: "test", Description: "test", Implemented: false}},
	} {
		a := auditModelConfiguration(r, catalog, []string{TestModelID, StrictModelID})
		if a.CatalogRegistryConsistent || a.SelectionResolved || len(a.Blockers) == 0 {
			t.Fatal(a)
		}
	}
	unfrozen := model.NewRegistry()
	a := AuditModelConfiguration(unfrozen, []string{TestModelID})
	if a.SelectionResolved || a.CatalogRegistryConsistent {
		t.Fatal(a)
	}
}

func TestReadOnlySettingsDoesNotWriteAndRejectsUnsafeInputs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	original := []byte(`{"version":1,"environment":"demo","selected_model_ids":["ema_rsi_macd_v1"]}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := ReadModelSettingsOnly(path)
	if err != nil || len(s.SelectedModelIDs) != 1 || s.SelectedModelIDs[0] != TestModelID {
		t.Fatal(s, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(original) {
		t.Fatal("offline inspection modified settings")
	}
	for _, raw := range []string{
		`{"version":1,"environment":"demo","selected_model_ids":[]} {}`,
		`{"version":1,"environment":"demo","selected_model_ids":[],"unknown":1}`,
		`{"version":9,"environment":"demo","selected_model_ids":[]}`,
		strings.Repeat(" ", (32<<10)+1),
	} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadModelSettingsOnly(path); err == nil {
			t.Fatal("accepted invalid settings")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(link, path); err == nil {
		if _, err := ReadModelSettingsOnly(path); err == nil {
			t.Fatal("symlink settings accepted")
		}
	}
}
