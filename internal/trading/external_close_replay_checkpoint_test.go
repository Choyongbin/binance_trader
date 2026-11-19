package trading

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestExternalCloseReplayCheckpointResumesAndNeverPosts(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, proposal, review := settlementProposalFixture(t, side)
			p, err := proposal.Prepare(review.EntryClientOrderID, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			original, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			reader := mockCloseReader{v: SignedCloseEvidence{EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID, EntryTradeIDs: p.EntryTradeIDs, ExitTradeIDs: p.ExitTradeIDs, AccountFlat: true, ObservedAt: time.Now().UTC()}}
			store, err := NewExternalCloseReplayCheckpointStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(store.dir); !os.IsNotExist(err) {
				t.Fatal("constructor modified data", err)
			}
			first, err := store.Stage(context.Background(), p.ClientOrderID, reader, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if first.Stage != ExternalReplayStaged || first.PostingAuthorized || first.TradingAuthorized || first.LifecycleChanged || first.OwnershipChanged {
				t.Fatal("unsafe stage")
			}
			restart, err := NewExternalCloseReplayCheckpointStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			loaded, event, err := restart.Load(p.ClientOrderID)
			if err != nil || event != nil || loaded.Checksum != first.Checksum {
				t.Fatal("not restartable", err)
			}
			var wg sync.WaitGroup
			errorsChan := make(chan error, 20)
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := restart.Recheck(context.Background(), p.ClientOrderID, reader, time.Now().UTC())
					errorsChan <- err
				}()
			}
			wg.Wait()
			close(errorsChan)
			for err := range errorsChan {
				if err != nil {
					t.Errorf("non-idempotent concurrent recheck: %v", err)
				}
			}
			_, ev, err := restart.Load(p.ClientOrderID)
			if err != nil || ev == nil || ev.Stage != ExternalReplayRechecked || ev.PostingAuthorized || ev.TradingAuthorized {
				t.Fatalf("invalid recheck %+v %v", ev, err)
			}
			if got, err := snapshotExecutionEvidence(dir); err != nil || got != original {
				t.Fatal("original trading evidence modified", err)
			}
			reader.v.ExitTradeIDs = []int64{999999}
			if _, err := restart.Stage(context.Background(), p.ClientOrderID, reader, time.Now().UTC()); err == nil {
				t.Fatal("changed signed evidence accepted")
			}
			if _, err := restart.Recheck(context.Background(), p.ClientOrderID, reader, time.Now().UTC()); err == nil {
				t.Fatal("recheck changed evidence accepted")
			}
		})
	}
}
func TestExternalCloseReplayCheckpointTamperAndLocks(t *testing.T) {
	for _, mode := range []string{"root", "event", "symlink", "unknown", "active-lock", "original-drift"} {
		t.Run(mode, func(t *testing.T) {
			dir, proposal, r := settlementProposalFixture(t, Short)
			p, err := proposal.Prepare(r.EntryClientOrderID, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			reader := mockCloseReader{v: SignedCloseEvidence{EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID, EntryTradeIDs: p.EntryTradeIDs, ExitTradeIDs: p.ExitTradeIDs, AccountFlat: true, ObservedAt: time.Now().UTC()}}
			store, err := NewExternalCloseReplayCheckpointStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.Stage(context.Background(), p.ClientOrderID, reader, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if _, err = store.Recheck(context.Background(), p.ClientOrderID, reader, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "root":
				err = os.WriteFile(store.pathRoot(), []byte(`{}`), 0600)
			case "event":
				err = os.WriteFile(store.pathEvent(), []byte(`{}`), 0600)
			case "symlink":
				err = os.Remove(store.pathEvent())
				if err == nil {
					err = os.Symlink(store.pathRoot(), store.pathEvent())
					if err != nil {
						t.Skip(err)
					}
				}
			case "unknown":
				err = os.WriteFile(filepath.Join(store.dir, "surprise"), []byte("x"), 0600)
			case "active-lock":
				err = os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700)
			case "original-drift":
				fills, readErr := os.ReadDir(filepath.Join(dir, "trading", "fills"))
				if readErr != nil || len(fills) == 0 {
					t.Fatal(readErr)
				}
				err = os.Remove(filepath.Join(dir, "trading", "fills", fills[0].Name()))
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Load(p.ClientOrderID); err == nil {
				t.Fatal("unsafe loaded", mode)
			}
		})
	}
}
