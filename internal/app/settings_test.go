package app

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSettingsPersistAndValidation(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot(); got.Environment != Demo || len(got.SelectedModelIDs) != 0 {
		t.Fatalf("bad default: %+v", got)
	}
	if _, err := s.UpdateEnvironment(Real); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateSelection([]string{TestModelID}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := reloaded.Snapshot()
	if got.Environment != Real || !reflect.DeepEqual(got.SelectedModelIDs, []string{TestModelID}) {
		t.Fatalf("persist failed: %+v", got)
	}
	if _, err := reloaded.UpdateSelection([]string{TestModelID, TestModelID}); err == nil {
		t.Fatal("duplicate selection accepted")
	}
	if _, err := reloaded.UpdateSelection([]string{"unknown"}); err == nil {
		t.Fatal("unknown model accepted")
	}
	if _, err := reloaded.UpdateEnvironment("other"); err == nil {
		t.Fatal("invalid environment accepted")
	}
	unchanged := reloaded.Snapshot()
	if unchanged.Environment != Real || !reflect.DeepEqual(unchanged.SelectedModelIDs, []string{TestModelID}) {
		t.Fatal("invalid update changed state")
	}
	unchanged.SelectedModelIDs[0] = "injected"
	if reloaded.Snapshot().SelectedModelIDs[0] != TestModelID {
		t.Fatal("snapshot leaked mutable storage")
	}
}
func TestCorruptSettingsFailClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"version":1,"environment":"demo","selected_model_ids":[]} {}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(dir); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}
