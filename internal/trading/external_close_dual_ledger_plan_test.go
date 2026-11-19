package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func dualLedgerFixture(t *testing.T, side PositionSide) (string, *ExternalCloseDualLedgerPlanStore, string) {
	t.Helper()
	dir, shadow, id := shadowFixture(t, side)
	if _, err := shadow.Prepare(id); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.Replay(id); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalCloseDualLedgerPlanStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.dir); !os.IsNotExist(err) {
		t.Fatalf("constructor wrote data: %v", err)
	}
	return dir, store, id
}

func TestDualLedgerPlanBothSidesCrashRestartAndIdempotence(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, store, id := dualLedgerFixture(t, side)
			before, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			results := make(chan error, 30)
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r, e := store.Prepare(id)
					if e == nil && (r.PostingApplied || r.OwnershipChanged || r.LifecycleChanged || r.Stage != ExternalDualLedgerPrepared) {
						e = errUnsafeShadow
					}
					results <- e
				}()
			}
			wg.Wait()
			close(results)
			for e := range results {
				if e != nil {
					t.Fatal(e)
				}
			}
			restarted, err := NewExternalCloseDualLedgerPlanStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			root, event, err := restarted.Load(id)
			if err != nil || event != nil || root.SimulatedRemainingBTC != "0" || root.ProposedExitQuantityBTC != "0.001" {
				t.Fatalf("bad projection %+v: %v", root, err)
			}
			results2 := make(chan error, 30)
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r, e := restarted.Verify(id)
					if e == nil && (r.PostingApplied || r.SettlementAuthorized || r.TradingAuthorized || r.PreviousChecksum != root.Checksum) {
						e = errUnsafeShadow
					}
					results2 <- e
				}()
			}
			wg.Wait()
			close(results2)
			for e := range results2 {
				if e != nil {
					t.Fatal(e)
				}
			}
			final, err := NewExternalCloseDualLedgerPlanStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, ev, err := final.Load(id)
			if err != nil || ev == nil || ev.Stage != ExternalDualLedgerVerified {
				t.Fatal("restart did not recover verified envelope", err)
			}
			after, err := snapshotExecutionEvidence(dir)
			if err != nil || after != before {
				t.Fatal("original lifecycle or ownership mutated", err)
			}
			ents, err := os.ReadDir(final.dir)
			if err != nil || len(ents) != 2 {
				t.Fatal("unexpected immutable file count", err)
			}
			if _, err := final.Prepare("bt_0000000000000000000000000000"); err == nil {
				t.Fatal("accepted second incident")
			}
		})
	}
}

func TestDualLedgerPlanTamperOrIncompleteFailsClosed(t *testing.T) {
	for _, variant := range []string{"changed_checksum", "pretend_posted", "changed_exit", "torn_prepare", "torn_verified", "orphan", "unknown", "symlink", "upstream", "local_drift", "active_runtime"} {
		t.Run(variant, func(t *testing.T) {
			dir, store, id := dualLedgerFixture(t, Short)
			root, err := store.Prepare(id)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "changed_checksum":
				root.Checksum = "0"
			case "pretend_posted":
				root.PostingApplied = true
				root.Checksum = root.digest()
			case "changed_exit":
				root.ProposedExitQuantityBTC = "900"
				root.Checksum = root.digest()
			case "torn_prepare":
				err = os.WriteFile(store.path(ExternalDualLedgerPrepared), []byte(`{"schema":`), 0600)
			case "torn_verified":
				_, err = store.Verify(id)
				if err == nil {
					err = os.WriteFile(store.path(ExternalDualLedgerVerified), []byte(`{}`), 0600)
				}
			case "orphan":
				err = os.Remove(store.path(ExternalDualLedgerPrepared))
			case "unknown":
				err = os.WriteFile(filepath.Join(store.dir, "other.json"), []byte(`{}`), 0600)
			case "symlink":
				err = os.Remove(store.path(ExternalDualLedgerPrepared))
				if err == nil {
					err = os.Symlink(filepath.Join(dir, "trading", "not-found"), store.path(ExternalDualLedgerPrepared))
				}
				if err != nil {
					t.Skip(err)
				}
			case "upstream":
				err = os.WriteFile(store.shadow.path(ExternalShadowReplayed), []byte(`{}`), 0600)
			case "local_drift":
				files, e := os.ReadDir(filepath.Join(dir, "trading", "fills"))
				if e != nil || len(files) == 0 {
					t.Fatal(e)
				}
				err = os.Remove(filepath.Join(dir, "trading", "fills", files[0].Name()))
			case "active_runtime":
				err = os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if variant == "changed_checksum" || variant == "pretend_posted" || variant == "changed_exit" {
				raw, _ := json.Marshal(root)
				if err := os.WriteFile(store.path(ExternalDualLedgerPrepared), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := store.Load(id); err == nil {
				t.Fatalf("tampered %s accepted", variant)
			}
			if _, err := store.Verify(id); err == nil {
				t.Fatalf("unsafe verify accepted: %s", variant)
			}
		})
	}
}

func TestDualLedgerPlanRequiresUpstreamReplay(t *testing.T) {
	dir, _, id := shadowFixture(t, Short)
	store, err := NewExternalCloseDualLedgerPlanStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Prepare(id); err == nil {
		t.Fatal("unreplayed shadow accepted")
	}
	if _, err = os.Stat(store.dir); !os.IsNotExist(err) {
		t.Fatal("failure wrote unsafe plan directory")
	}
}
