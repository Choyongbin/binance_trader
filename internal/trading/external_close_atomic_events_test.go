package trading

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestAtomicEventStoreIsolatedDurableLongShort(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, legacy, id, archive := atomicRehearsalFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewExternalAtomicEventStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(store.eventsRoot); !os.IsNotExist(err) {
				t.Fatalf("constructor created events root: %v", err)
			}
			empty, err := store.Inspect(ctx, id, archive)
			if err != nil || empty.Stage != ExternalAtomicRehearsalAbsent {
				t.Fatalf("initial: %+v %v", empty, err)
			}
			if _, err := os.Lstat(store.eventsRoot); !os.IsNotExist(err) {
				t.Fatalf("read created events root: %v", err)
			}
			// A second, correctly-shaped event directory must not be modified
			// while the selected event runs; no cross-event global seal.
			foreignID := "bt_ffffffffffffffffffffffffffff"
			if foreignID == id {
				foreignID = "bt_eeeeeeeeeeeeeeeeeeeeeeeeeeee"
			}
			sibling := filepath.Join(store.eventsRoot, foreignID)
			if err := os.Mkdir(store.eventsRoot, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(sibling, 0700); err != nil {
				t.Fatal(err)
			}
			for index, step := range []func() (ExternalAtomicRehearsalInspection, error){
				func() (ExternalAtomicRehearsalInspection, error) { return store.Prepare(ctx, id, archive) },
				func() (ExternalAtomicRehearsalInspection, error) { return store.StageLifecycle(ctx, id, archive) },
				func() (ExternalAtomicRehearsalInspection, error) { return store.StageOwnership(ctx, id, archive) },
				func() (ExternalAtomicRehearsalInspection, error) { return store.Seal(ctx, id, archive) },
			} {
				state, err := step()
				if err != nil || state.Stage != atomicRehearsalStages[index] {
					t.Fatalf("stage %d: %+v %v", index, state, err)
				}
				assertAtomicNoPosting(t, state)
				restarted, err := NewExternalAtomicEventStore(data, id)
				if err != nil {
					t.Fatal(err)
				}
				got, err := restarted.Inspect(ctx, id, archive)
				if err != nil || !reflect.DeepEqual(got, state) {
					t.Fatalf("restart %d: %+v %v", index, got, err)
				}
				store = restarted
			}
			sealed, err := store.Inspect(ctx, id, archive)
			if err != nil {
				t.Fatal(err)
			}
			pair, err := store.ReadLedgerPair(ctx, id, archive, sealed.CommitSHA256)
			if err != nil || pair.ClientOrderID != id || pair.Lifecycle.CommitSHA256 != pair.Ownership.CommitSHA256 || pair.PostingApplied {
				t.Fatalf("pair %+v %v", pair, err)
			}
			if _, err := store.ReadLedgerPair(ctx, id, archive, strings.Repeat("f", 64)); err == nil {
				t.Fatal("foreign seal accepted")
			}
			if _, err := store.Inspect(ctx, foreignID, archive); err == nil {
				t.Fatal("foreign client ID accepted")
			}
			if _, err := store.Prepare(ctx, foreignID, archive); err == nil {
				t.Fatal("foreign event write accepted")
			}
			if entries, err := os.ReadDir(sibling); err != nil || len(entries) != 0 {
				t.Fatalf("foreign event changed: %v %v", entries, err)
			}
			if _, err := os.Lstat(legacy.dir); !os.IsNotExist(err) {
				t.Fatalf("legacy mutated: %v", err)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("source evidence mutated %v", err)
			}
		})
	}
}

func TestAtomicEventStoreRejectsLegacySameEventAndUnsafeRoot(t *testing.T) {
	ctx := context.Background()
	data, legacy, id, archive := atomicRehearsalFixture(t, Short)
	if _, err := legacy.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	scoped, err := NewExternalAtomicEventStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.Inspect(ctx, id, archive); err == nil {
		t.Fatal("legacy duplicate exposed as fresh event")
	}
	if _, err := scoped.Prepare(ctx, id, archive); err == nil {
		t.Fatal("legacy duplicate wrote new event")
	}
	if _, err := os.Lstat(scoped.eventsRoot); !os.IsNotExist(err) {
		t.Fatalf("blocked migration created events: %v", err)
	}
	if historical, err := legacy.Inspect(ctx, id, archive); err != nil || historical.Stage != ExternalAtomicRehearsalIntent {
		t.Fatalf("legacy unreadable: %+v %v", historical, err)
	}
	// A legacy record for an unrelated entry does not block a new event.
	var record ExternalAtomicRehearsalRecord
	if err := strictReviewJSON(filepath.Join(legacy.dir, atomicRehearsalFiles[0]), &record); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(legacy.dir, atomicRehearsalFiles[0])); err != nil {
		t.Fatal(err)
	}
	record.ClientOrderID = "bt_ffffffffffffffffffffffffffff"
	record.Checksum = record.digest()
	if err := exclusiveReviewWrite(filepath.Join(legacy.dir, atomicRehearsalFiles[0]), record); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.Prepare(ctx, id, archive); err != nil {
		t.Fatalf("unrelated legacy entry blocked: %v", err)
	}
	// Once a root is replaced by a symlink, even inspection is denied.
	if err := os.Rename(scoped.eventsRoot, scoped.eventsRoot+".hold"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(scoped.eventsRoot+".hold", scoped.eventsRoot); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if _, err := scoped.Inspect(ctx, id, archive); err == nil {
		t.Fatal("symlink events root accepted")
	}
}

func TestAtomicEventStoreRejectsExtraAndTornRecords(t *testing.T) {
	for _, kind := range []string{"unknown-root", "unknown-event", "torn-record", "missing-stage", "new-candidate-key"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			data, _, id, archive := atomicRehearsalFixture(t, Long)
			store, err := NewExternalAtomicEventStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.Prepare(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown-root":
				err = os.WriteFile(filepath.Join(store.eventsRoot, "stray"), []byte("x"), 0600)
			case "unknown-event":
				err = os.WriteFile(filepath.Join(store.dir, "stray"), []byte("x"), 0600)
			case "torn-record":
				err = os.WriteFile(filepath.Join(store.dir, atomicRehearsalFiles[0]), []byte("{"), 0600)
			case "missing-stage":
				if _, err = store.StageLifecycle(ctx, id, archive); err != nil {
					t.Fatal(err)
				}
				err = os.Remove(filepath.Join(store.dir, atomicRehearsalFiles[0]))
			case "new-candidate-key":
				path := filepath.Join(store.dir, atomicRehearsalFiles[0])
				var record ExternalAtomicRehearsalRecord
				if err = strictReviewJSON(path, &record); err == nil {
					record.TransactionKey = strings.Repeat("f", 64)
					record.Checksum = record.digest()
					var b []byte
					b, err = json.Marshal(record)
					if err == nil {
						err = os.WriteFile(path, b, 0600)
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.Inspect(ctx, id, archive); err == nil {
				t.Fatal("unsafe event passed inspection")
			}
			if _, err = store.StageLifecycle(ctx, id, archive); err == nil {
				t.Fatal("unsafe event advanced")
			}
		})
	}
}

// Separate store instances emulate independent workers. O_EXCL means one may
// lose the race; after the race the *only* accepted result is one valid record.
func TestAtomicEventConcurrentWritersFailClosed(t *testing.T) {
	ctx := context.Background()
	data, _, id, archive := atomicRehearsalFixture(t, Short)
	const workers = 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, e := NewExternalAtomicEventStore(data, id)
			if e == nil {
				_, e = s.Prepare(ctx, id, archive)
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for e := range errs {
		if e == nil {
			wins++
		}
	}
	if wins == 0 {
		t.Fatal("no worker could create immutable event")
	}
	reopened, err := NewExternalAtomicEventStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Inspect(ctx, id, archive)
	if err != nil || got.Stage != ExternalAtomicRehearsalIntent {
		t.Fatalf("corrupted collision state: %+v %v", got, err)
	}
	entries, err := os.ReadDir(reopened.dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("collision created duplicate records: %v %v", entries, err)
	}
}

func TestAtomicEventSubprocessWriter(t *testing.T) {
	if os.Getenv("ATOMIC_EVENT_SUBPROCESS") != "1" {
		return
	}
	data := os.Getenv("ATOMIC_EVENT_DATA")
	id := os.Getenv("ATOMIC_EVENT_ID")
	path := os.Getenv("ATOMIC_EVENT_ARCHIVE")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var archive PackageAEntryArchive
	if err = json.Unmarshal(b, &archive); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalAtomicEventStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Prepare(context.Background(), id, archive); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicEventCrossProcessCollision(t *testing.T) {
	ctx := context.Background()
	data, _, id, archive := atomicRehearsalFixture(t, Short)
	path := filepath.Join(t.TempDir(), "archive.json")
	b, err := json.Marshal(archive)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	const workers = 3
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestAtomicEventSubprocessWriter$", "-test.v=false")
			cmd.Env = append(os.Environ(), "ATOMIC_EVENT_SUBPROCESS=1", "ATOMIC_EVENT_DATA="+data, "ATOMIC_EVENT_ID="+id, "ATOMIC_EVENT_ARCHIVE="+path)
			_, e := cmd.CombinedOutput()
			results <- e
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for e := range results {
		if e == nil {
			successes++
		}
	}
	if successes == 0 {
		t.Fatal("no subprocess could write")
	}
	store, err := NewExternalAtomicEventStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Inspect(ctx, id, archive)
	if err != nil || got.Stage != ExternalAtomicRehearsalIntent {
		t.Fatalf("cross-process collision corrupted checkpoint: %+v %v", got, err)
	}
}
