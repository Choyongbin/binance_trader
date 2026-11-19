package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func shadowFixture(t *testing.T, side PositionSide) (string, *ExternalCloseShadowLedger, string) {
	t.Helper()
	dir, intentStore, p := postingIntentFixture(t, side)
	if _, err := intentStore.Reserve(p.ClientOrderID); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalCloseShadowLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.dir); !os.IsNotExist(err) {
		t.Fatalf("constructor must not mutate data: %v", err)
	}
	return dir, store, p.ClientOrderID
}

func TestExternalShadowCrashRestartExactlyOnceOnIsolatedProjection(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, ledger, id := shadowFixture(t, side)
			before, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			const threads = 30
			outcomes := make(chan error, threads)
			var wg sync.WaitGroup
			for i := 0; i < threads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r, err := ledger.Prepare(id)
					if err == nil && (r.Stage != ExternalShadowPrepared || r.SimulatedRemainingQuantity != "0" || r.Applied || r.SettlementAuthorized) {
						err = errUnsafeShadow
					}
					outcomes <- err
				}()
			}
			wg.Wait()
			close(outcomes)
			for err := range outcomes {
				if err != nil {
					t.Fatal(err)
				}
			}
			restarted, err := NewExternalCloseShadowLedger(dir)
			if err != nil {
				t.Fatal(err)
			}
			root, event, err := restarted.Load(id)
			if err != nil || event != nil {
				t.Fatalf("unexpected prepared state: %v", err)
			}
			if root.EntryQuantity != root.ExitQuantity || len(root.EntryTradeIDs) == 0 || len(root.ExitTradeIDs) == 0 {
				t.Fatal("shadow projection has wrong totals")
			}
			outcomes2 := make(chan error, threads)
			for i := 0; i < threads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r, e := restarted.Replay(id)
					if e == nil && (r.Stage != ExternalShadowReplayed || r.PreviousChecksum != root.Checksum || r.Applied || r.TradingAuthorized) {
						e = errUnsafeShadow
					}
					outcomes2 <- e
				}()
			}
			wg.Wait()
			close(outcomes2)
			for err := range outcomes2 {
				if err != nil {
					t.Fatal(err)
				}
			}
			fresh, err := NewExternalCloseShadowLedger(dir)
			if err != nil {
				t.Fatal(err)
			}
			loaded, replay, err := fresh.Load(id)
			if err != nil || replay == nil || loaded.Checksum != root.Checksum || replay.PreviousChecksum != root.Checksum {
				t.Fatal("restart lost sealed replay", err)
			}
			same, err := fresh.Replay(id)
			if err != nil || same.Checksum != replay.Checksum {
				t.Fatal("replay not idempotent", err)
			}
			after, err := snapshotExecutionEvidence(dir)
			if err != nil || after != before {
				t.Fatal("real lifecycle or ownership evidence changed", err)
			}
			entries, err := os.ReadDir(fresh.dir)
			if err != nil || len(entries) != 2 {
				t.Fatal("unexpected write count", err)
			}
			if _, err := fresh.Prepare("bt_0000000000000000000000000000"); err == nil {
				t.Fatal("second incident accepted")
			}
		})
	}
}

var errUnsafeShadow = &shadowTestErr{}

type shadowTestErr struct{}

func (*shadowTestErr) Error() string { return "unsafe shadow posting flag" }

func TestExternalShadowFailClosedTamperAndInterruptedWrite(t *testing.T) {
	for _, variant := range []string{"checksum", "posted", "wrong_net", "torn_prepared", "torn_replay", "orphan", "symlink", "unknown", "upstream", "runtime_active"} {
		t.Run(variant, func(t *testing.T) {
			dir, ledger, id := shadowFixture(t, Short)
			root, err := ledger.Prepare(id)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "checksum":
				root.Checksum = "0"
			case "posted":
				root.Applied = true
				root.Checksum = root.digest()
			case "wrong_net":
				root.NetExcludingFunding = "999999"
				root.Checksum = root.digest()
			case "torn_prepared":
				if err := os.WriteFile(ledger.path(ExternalShadowPrepared), []byte("{\"stage\":"), 0600); err != nil {
					t.Fatal(err)
				}
			case "torn_replay":
				if _, err := ledger.Replay(id); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(ledger.path(ExternalShadowReplayed), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				if err := os.Remove(ledger.path(ExternalShadowPrepared)); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(ledger.path(ExternalShadowPrepared)); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(dir, "VERSION"), ledger.path(ExternalShadowPrepared)); err != nil {
					t.Skip(err)
				}
			case "unknown":
				if err := os.WriteFile(filepath.Join(ledger.dir, "other.txt"), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "upstream":
				if err := os.WriteFile(ledger.intents.intentPath(), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "runtime_active":
				if err := os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if variant == "checksum" || variant == "posted" || variant == "wrong_net" {
				raw, _ := json.Marshal(root)
				if err := os.WriteFile(ledger.path(ExternalShadowPrepared), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := ledger.Load(id); err == nil {
				t.Fatal("tampered or incomplete shadow accepted")
			}
			if _, err := ledger.Replay(id); err == nil {
				t.Fatal("unsafe replay accepted")
			}
		})
	}
}

func TestShadowRequiresSealedPostingIntent(t *testing.T) {
	dir, _, p := postingIntentFixture(t, Long)
	shadow, err := NewExternalCloseShadowLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.Prepare(p.ClientOrderID); err == nil {
		t.Fatal("missing posting intent accepted")
	}
	if _, err := os.Stat(shadow.dir); !os.IsNotExist(err) {
		t.Fatal("failed preflight created ledger directory")
	}
}
