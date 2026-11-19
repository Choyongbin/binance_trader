package app

import (
	"binance_trader/internal/model"
	"testing"
)

func TestEveryCatalogModelMatchesFrozenRegistry(t *testing.T) {
	registry := model.DefaultRegistry()
	ids := registry.IDs()
	if len(ModelCatalog) != len(ids) {
		t.Fatalf("catalog and registry sizes differ: %d vs %d", len(ModelCatalog), len(ids))
	}
	for _, item := range ModelCatalog {
		if !item.Implemented || item.Name == "" || item.Description == "" {
			t.Fatalf("missing visible metadata: %+v", item)
		}
		strategy, err := registry.Resolve(item.ID)
		if err != nil || strategy.ID() != item.ID {
			t.Fatalf("catalog/runtime mismatch: %+v %v", item, err)
		}
	}
}
func TestStrictModelSelectionPersists(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpdateSelection([]string{StrictModelID}); err != nil {
		t.Fatal(err)
	}
	if ids := s.Snapshot().SelectedModelIDs; len(ids) != 1 || ids[0] != StrictModelID {
		t.Fatalf("selection=%v", ids)
	}
}
