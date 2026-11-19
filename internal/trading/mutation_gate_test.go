package trading

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSharedGateProtectsCoordinatorStartupAndWalletHistory(t *testing.T) {
	dir := t.TempDir()
	gate, err := AcquireMutationGate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCoordinator(dir); !errors.Is(err, ErrMutationGateBusy) {
		t.Fatalf("coordinator started inside wallet's audit+commit interval: %v", err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	coord, err := OpenCoordinator(dir)
	if err != nil {
		t.Fatal(err)
	}
	if held, err := MutationGatePresent(dir); held || err != nil {
		t.Fatalf("startup gate not released: %v", err)
	}
	if _, _, err := CheckWalletEditHistory(dir); err == nil {
		t.Fatal("wallet ignored live coordinator")
	}
	if err := coord.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash leaving only an empty gate. It must NOT be auto-expired.
	if err := os.Mkdir(filepath.Join(dir, "trading", "mutation_gate.active"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireMutationGate(dir); !errors.Is(err, ErrMutationGateBusy) {
		t.Fatal("abandoned gate ignored", err)
	}
	if _, err := OpenCoordinator(dir); !errors.Is(err, ErrMutationGateBusy) {
		t.Fatal("abandoned gate did not block coordination", err)
	}
}
