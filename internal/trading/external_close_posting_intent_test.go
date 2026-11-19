package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func postingIntentFixture(t *testing.T, side PositionSide) (string, *ExternalClosePostingIntentStore, ExternalCloseSettlementProposal) {
	t.Helper()
	dir, proposals, fixture := settlementProposalFixture(t, side)
	p, err := proposals.Prepare(fixture.EntryClientOrderID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	replay, err := NewExternalCloseReplayCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	reader := mockCloseReader{v: SignedCloseEvidence{EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID, EntryTradeIDs: p.EntryTradeIDs, ExitTradeIDs: p.ExitTradeIDs, AccountFlat: true, ObservedAt: time.Now().UTC()}}
	if _, err := replay.Stage(context.Background(), p.ClientOrderID, reader, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalClosePostingIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.dir); !os.IsNotExist(err) {
		t.Fatal("constructor created intent dir", err)
	}
	if _, err := store.Reserve(p.ClientOrderID); err == nil {
		t.Fatal("intent reserved without independent recheck")
	}
	if _, err := replay.Recheck(context.Background(), p.ClientOrderID, reader, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return dir, store, p
}

func TestExternalPostingIntentCrashRestartAndConcurrentIdempotence(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, store, proposal := postingIntentFixture(t, side)
			original, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			const n = 30
			errs := make(chan error, n)
			items := make(chan ExternalClosePostingIntent, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); v, e := store.Reserve(proposal.ClientOrderID); items <- v; errs <- e }()
			}
			wg.Wait()
			close(errs)
			close(items)
			var first string
			for e := range errs {
				if e != nil {
					t.Fatalf("concurrent reserved: %v", e)
				}
			}
			for item := range items {
				if err := validatePostingIntent(item); err != nil {
					t.Fatal(err)
				}
				if first == "" {
					first = item.Checksum
				} else if first != item.Checksum {
					t.Fatal("non-idempotent posting identity")
				}
				if item.PostingApplied || item.SettlementAuthorized || item.TradingAuthorized || item.FundingAllocated || item.HistoryCoverageComplete {
					t.Fatal("unsafe posting intent flags")
				}
			}
			restarted, err := NewExternalClosePostingIntentStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err := restarted.Load(proposal.ClientOrderID)
			if err != nil || got.Checksum != first {
				t.Fatal("crash/restart lost intent", err)
			}
			after, err := snapshotExecutionEvidence(dir)
			if err != nil || after != original {
				t.Fatal("mutable trading evidence changed", err)
			}
			entries, err := os.ReadDir(store.dir)
			if err != nil || len(entries) != 1 {
				t.Fatal("multiple intents", err)
			}
			if _, err := restarted.Reserve("bt_0000000000000000000000000000"); err == nil {
				t.Fatal("different incident stole global lock")
			}
		})
	}
}

func TestExternalPostingIntentTamperFailsClosed(t *testing.T) {
	for _, variant := range []string{"checksum", "applied", "trade", "torn", "symlink", "extra", "active", "local-drift", "source-recheck"} {
		t.Run(variant, func(t *testing.T) {
			dir, store, p := postingIntentFixture(t, Short)
			r, err := store.Reserve(p.ClientOrderID)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "checksum":
				r.Checksum = ""
			case "applied":
				r.PostingApplied = true
				r.Checksum = r.digest()
			case "trade":
				r.ExitTradeIDs[0] = 999999
				r.Checksum = r.digest()
			case "torn":
				if err := os.WriteFile(store.intentPath(), []byte(`{"schema":`), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(store.intentPath()); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(dir, "VERSION"), store.intentPath()); err != nil {
					t.Skip(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(store.dir, "unexpected"), []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
			case "active":
				if err := os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700); err != nil {
					t.Fatal(err)
				}
			case "local-drift":
				fills, err := os.ReadDir(filepath.Join(dir, "trading", "fills"))
				if err != nil || len(fills) == 0 {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, "trading", "fills", fills[0].Name())); err != nil {
					t.Fatal(err)
				}
			case "source-recheck":
				event := store.replay.pathEvent()
				if err := os.WriteFile(event, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if variant == "checksum" || variant == "applied" || variant == "trade" {
				raw, _ := json.Marshal(r)
				if err := os.WriteFile(store.intentPath(), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.Load(p.ClientOrderID); err == nil {
				t.Fatal("tamper was accepted", variant)
			}
			if _, err := store.Reserve(p.ClientOrderID); err == nil {
				t.Fatal("unsafe retry was accepted", variant)
			}
		})
	}
}
