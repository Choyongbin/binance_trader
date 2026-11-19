package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func atomicRehearsalFixture(t *testing.T, side PositionSide) (string, *ExternalAtomicRehearsalStore, string, PackageAEntryArchive) {
	t.Helper()
	ctx := context.Background()
	data, journal, id, archive := settlementJournalFixture(t, side)
	if _, err := journal.Prepare(ctx, id, archive); err != nil {
		t.Fatalf("prepare settlement fence: %v", err)
	}
	if _, err := journal.Fence(ctx, id, archive); err != nil {
		t.Fatalf("fence settlement: %v", err)
	}
	store, err := NewExternalAtomicRehearsalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	return data, store, id, archive
}

func assertAtomicNoPosting(t *testing.T, got ExternalAtomicRehearsalInspection) {
	t.Helper()
	if got.PostingApplied || got.LifecycleChanged || got.OwnershipChanged || got.SettlementAuthorized || got.TradingAuthorized {
		t.Fatalf("unsafe financial posting status: %+v", got)
	}
	if !got.AtomicProjectionVisible && (got.LifecycleProposal != "" || got.OwnershipProposal != "" || got.ProjectedExitBTC != "" || got.ProjectedRemainingBTC != "") {
		t.Fatalf("partial projection became visible: %+v", got)
	}
}

func TestAtomicRehearsalLongShortAllCrashBoundaries(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := atomicRehearsalFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			initial, err := store.Inspect(ctx, id, archive)
			if err != nil || initial.Stage != ExternalAtomicRehearsalAbsent {
				t.Fatalf("unexpected initial status: %+v %v", initial, err)
			}
			if _, err := os.Lstat(store.dir); !os.IsNotExist(err) {
				t.Fatalf("inspection created a file: %v", err)
			}
			// A seal is forbidden without every preceding crash boundary.
			if _, err := store.Seal(ctx, id, archive); err == nil {
				t.Fatal("seal bypassed prerequisites")
			}
			steps := []struct {
				stage string
				fn    func(*ExternalAtomicRehearsalStore) (ExternalAtomicRehearsalInspection, error)
			}{
				{ExternalAtomicRehearsalIntent, func(s *ExternalAtomicRehearsalStore) (ExternalAtomicRehearsalInspection, error) {
					return s.Prepare(ctx, id, archive)
				}},
				{ExternalAtomicRehearsalLifecycle, func(s *ExternalAtomicRehearsalStore) (ExternalAtomicRehearsalInspection, error) {
					return s.StageLifecycle(ctx, id, archive)
				}},
				{ExternalAtomicRehearsalOwnership, func(s *ExternalAtomicRehearsalStore) (ExternalAtomicRehearsalInspection, error) {
					return s.StageOwnership(ctx, id, archive)
				}},
				{ExternalAtomicRehearsalSealed, func(s *ExternalAtomicRehearsalStore) (ExternalAtomicRehearsalInspection, error) {
					return s.Seal(ctx, id, archive)
				}},
			}
			for i, step := range steps {
				got, err := step.fn(store)
				if err != nil || got.Stage != step.stage {
					t.Fatalf("step %d: %+v %v", i, got, err)
				}
				assertAtomicNoPosting(t, got)
				if got.AtomicProjectionVisible != (i == 3) {
					t.Fatalf("invalid atomic visibility at step %d", i)
				}
				if i == 3 && (got.LifecycleProposal == "" || got.OwnershipProposal == "" || got.ProjectedExitBTC == "" || got.ProjectedRemainingBTC != "0") {
					t.Fatalf("bad sealed projection: %+v", got)
				}
				// Simulate process restart after each durable stage.
				restart, err := NewExternalAtomicRehearsalStore(data)
				if err != nil {
					t.Fatal(err)
				}
				read, err := restart.Inspect(ctx, id, archive)
				if err != nil || !reflect.DeepEqual(got, read) {
					t.Fatalf("restart state drift: %v %+v %+v", err, got, read)
				}
				replay, err := step.fn(restart)
				if err != nil || !reflect.DeepEqual(read, replay) {
					t.Fatalf("idempotency failure: %v %+v %+v", err, read, replay)
				}
				store = restart
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("original lifecycle/ownership/request/fill mutated: %v", err)
			}
			if _, err := store.Prepare(ctx, "bt_ffffffffffffffffffffffffffff", archive); err == nil {
				t.Fatal("foreign transaction was accepted")
			}
		})
	}
}

func TestAtomicRehearsalFailClosedOrphanTornExtraForgeryAndSourceDrift(t *testing.T) {
	for _, kind := range []string{"orphan", "torn", "unknown", "symlink", "forged_authority", "source_drift", "changed_transaction", "missing_checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := atomicRehearsalFixture(t, Short)
			if _, err := store.Prepare(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			if _, err := store.StageLifecycle(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			if _, err := store.StageOwnership(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "orphan":
				err = os.Remove(filepath.Join(store.dir, atomicRehearsalFiles[1]))
			case "torn":
				err = os.WriteFile(filepath.Join(store.dir, atomicRehearsalFiles[1]), []byte("{"), 0600)
			case "unknown":
				err = os.WriteFile(filepath.Join(store.dir, "old.tmp"), []byte("x"), 0600)
			case "symlink":
				if err = os.Remove(filepath.Join(store.dir, atomicRehearsalFiles[2])); err == nil {
					err = os.Symlink(filepath.Join(store.dir, atomicRehearsalFiles[0]), filepath.Join(store.dir, atomicRehearsalFiles[2]))
					if err != nil {
						t.Skipf("symlink creation unavailable on this host: %v", err)
					}
				}
			case "forged_authority", "changed_transaction":
				path := filepath.Join(store.dir, atomicRehearsalFiles[2])
				var record ExternalAtomicRehearsalRecord
				if err = strictReviewJSON(path, &record); err == nil {
					if kind == "forged_authority" {
						record.SettlementAuthorized = true
					} else {
						record.TransactionKey = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
					}
					record.Checksum = record.digest() // malicious recomputation must still be caught
					var raw []byte
					raw, err = json.Marshal(record)
					if err == nil {
						err = os.WriteFile(path, raw, 0600)
					}
				}
			case "source_drift":
				err = os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json"))
			case "missing_checkpoint":
				err = os.Remove(filepath.Join(store.dir, atomicRehearsalFiles[0]))
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Inspect(ctx, id, archive); err == nil {
				t.Fatal("unsafe or orphaned rehearsal passed inspection")
			}
			if _, err := store.Seal(ctx, id, archive); err == nil {
				t.Fatal("unsafe or orphaned rehearsal was sealed")
			}
		})
	}
}

func TestAtomicRehearsalConcurrentIdempotency(t *testing.T) {
	ctx := context.Background()
	data, store, id, archive := atomicRehearsalFixture(t, Long)
	steps := []func() (ExternalAtomicRehearsalInspection, error){
		func() (ExternalAtomicRehearsalInspection, error) { return store.Prepare(ctx, id, archive) },
		func() (ExternalAtomicRehearsalInspection, error) { return store.StageLifecycle(ctx, id, archive) },
		func() (ExternalAtomicRehearsalInspection, error) { return store.StageOwnership(ctx, id, archive) },
		func() (ExternalAtomicRehearsalInspection, error) { return store.Seal(ctx, id, archive) },
	}
	for index, step := range steps {
		const count = 8
		var wg sync.WaitGroup
		outcomes := make(chan error, count)
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := step()
				if err == nil && result.Stage != atomicRehearsalStages[index] {
					outcomes <- errUnsafeShadow
				} else {
					outcomes <- err
				}
			}()
		}
		wg.Wait()
		close(outcomes)
		for err := range outcomes {
			if err != nil {
				t.Fatalf("concurrent stage %d: %v", index, err)
			}
		}
	}
	restart, err := NewExternalAtomicRehearsalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	read, err := restart.Inspect(ctx, id, archive)
	if err != nil || read.Stage != ExternalAtomicRehearsalSealed || !read.AtomicProjectionVisible {
		t.Fatalf("incorrect concurrent final state: %+v %v", read, err)
	}
	assertAtomicNoPosting(t, read)
}

func TestAtomicRehearsalRequiresFencedEvidence(t *testing.T) {
	ctx := context.Background()
	data, journal, id, archive := settlementJournalFixture(t, Short)
	store, err := NewExternalAtomicRehearsalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Prepare(ctx, id, archive); err == nil {
		t.Fatal("unfenced journal was accepted")
	}
	if _, err := journal.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Prepare(ctx, id, archive); err == nil {
		t.Fatal("PREPARED journal was accepted")
	}
	if _, err := os.Lstat(store.dir); !os.IsNotExist(err) {
		t.Fatalf("unfenced rehearsal created data: %v", err)
	}
}
