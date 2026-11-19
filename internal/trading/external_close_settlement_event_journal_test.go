package trading

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestSettlementEventJournalScopedRestartLongShort(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, legacy, id, archive := settlementJournalFixture(t, side)
			baseline, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewExternalSettlementEventJournalStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = os.Lstat(s.eventsRoot); !os.IsNotExist(err) {
				t.Fatalf("constructor wrote root: %v", err)
			}
			status, err := s.InspectRecovery(ctx, id, archive)
			if err != nil || status.Stage != ExternalRecoveryPrerequisitesOnly {
				t.Fatalf("inspect absent: %+v %v", status, err)
			}
			if _, err = os.Lstat(s.eventsRoot); !os.IsNotExist(err) {
				t.Fatal("read-only inspection wrote root")
			}
			foreignID := "bt_ffffffffffffffffffffffffffff"
			if foreignID == id {
				foreignID = "bt_eeeeeeeeeeeeeeeeeeeeeeeeeeee"
			}
			foreign := filepath.Join(s.eventsRoot, foreignID)
			if err := os.Mkdir(s.eventsRoot, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(foreign, 0700); err != nil {
				t.Fatal(err)
			}
			root, err := s.Prepare(ctx, id, archive)
			if err != nil || root.Stage != ExternalSettlementJournalPrepared {
				t.Fatalf("prepare: %+v %v", root, err)
			}
			if root.PostingApplied || root.TradingAuthorized || root.SettlementAuthorized {
				t.Fatal("authorization leakage")
			}
			restart, err := NewExternalSettlementEventJournalStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			replay, fenced, err := restart.Load(ctx, id, archive)
			if err != nil || fenced != nil || replay.Checksum != root.Checksum {
				t.Fatalf("restart: %+v %+v %v", replay, fenced, err)
			}
			fence, err := restart.Fence(ctx, id, archive)
			if err != nil || fence.Stage != ExternalSettlementJournalFenced || fence.PreviousChecksum != root.Checksum {
				t.Fatalf("fence: %+v %v", fence, err)
			}
			next, err := NewExternalSettlementEventJournalStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			rec, err := next.InspectRecovery(ctx, id, archive)
			if err != nil || rec.Stage != ExternalRecoveryFencedOnly || rec.FencedChecksum != fence.Checksum {
				t.Fatalf("fenced recovery: %+v %v", rec, err)
			}
			if again, err := next.Prepare(ctx, id, archive); err != nil || again.Checksum != root.Checksum {
				t.Fatalf("idempotent prepare: %v", err)
			}
			if again, err := next.Fence(ctx, id, archive); err != nil || again.Checksum != fence.Checksum {
				t.Fatalf("idempotent fence: %v", err)
			}
			if _, err := next.Prepare(ctx, foreignID, archive); err == nil {
				t.Fatal("foreign ID accepted")
			}
			if _, _, err := next.Load(ctx, foreignID, archive); err == nil {
				t.Fatal("foreign ID read accepted")
			}
			entries, err := os.ReadDir(foreign)
			if err != nil || len(entries) != 0 {
				t.Fatalf("foreign event changed: %v %v", entries, err)
			}
			if _, err := os.Lstat(legacy.dir); !os.IsNotExist(err) {
				t.Fatalf("legacy mutated: %v", err)
			}
			if _, _, err := legacy.Load(ctx, id, archive); err == nil {
				t.Fatal("old journal unexpectedly reads scoped entry")
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || baseline != after {
				t.Fatalf("trading ledger altered: %v", err)
			}
		})
	}
}

func TestSettlementEventJournalRejectsLegacyDuplicateAndCorruption(t *testing.T) {
	ctx := context.Background()
	data, legacy, id, archive := settlementJournalFixture(t, Short)
	if _, err := legacy.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	scoped, err := NewExternalSettlementEventJournalStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.Prepare(ctx, id, archive); err == nil {
		t.Fatal("duplicate legacy event staged")
	}
	if _, err := os.Lstat(scoped.eventsRoot); !os.IsNotExist(err) {
		t.Fatal("duplicate attempt wrote root")
	}
	// Historical record is readable by explicit legacy loader only.
	if old, _, err := legacy.Load(ctx, id, archive); err != nil || old.ClientOrderID != id {
		t.Fatalf("legacy lost: %v", err)
	}
	// Unlike legacy, an event-scoped store may coexist with an unrelated
	// historical event, but may not modify its bytes.
	raw, err := os.ReadFile(legacy.path(ExternalSettlementJournalPrepared))
	if err != nil {
		t.Fatal(err)
	}
	var old ExternalSettlementJournalRecord
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatal(err)
	}
	old.ClientOrderID = "bt_eeeeeeeeeeeeeeeeeeeeeeeeeeee"
	old.Checksum = old.digest()
	b, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy.path(ExternalSettlementJournalPrepared), b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.Prepare(ctx, id, archive); err != nil {
		t.Fatalf("unrelated legacy event blocks new event: %v", err)
	}
	frozen, err := os.ReadFile(legacy.path(ExternalSettlementJournalPrepared))
	if err != nil || string(frozen) != string(b) {
		t.Fatal("legacy bytes changed")
	}
}

func TestSettlementEventJournalFailClosedUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"foreign-root", "root-symlink", "event-symlink", "unknown-child", "torn", "orphan", "drift", "legacy-torn"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			data, legacy, id, archive := settlementJournalFixture(t, Long)
			s, err := NewExternalSettlementEventJournalStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "legacy-torn" {
				if err := os.Mkdir(legacy.dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(legacy.path(ExternalSettlementJournalPrepared), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if kind == "root-symlink" {
				dest := filepath.Join(t.TempDir(), "dest")
				if err := os.Mkdir(dest, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(dest, s.eventsRoot); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			} else {
				if err := os.Mkdir(s.eventsRoot, 0700); err != nil {
					t.Fatal(err)
				}
				if kind == "foreign-root" {
					if err := os.WriteFile(filepath.Join(s.eventsRoot, "stray"), []byte("x"), 0600); err != nil {
						t.Fatal(err)
					}
				} else if kind == "event-symlink" {
					target := t.TempDir()
					if err := os.Symlink(target, s.dir); err != nil {
						t.Skipf("symlink unavailable: %v", err)
					}
				} else {
					if err := os.Mkdir(s.dir, 0700); err != nil {
						t.Fatal(err)
					}
					if kind == "unknown-child" {
						err = os.WriteFile(filepath.Join(s.dir, "extra.json"), []byte("{}"), 0600)
					} else if kind == "torn" || kind == "orphan" {
						if kind == "torn" {
							err = os.WriteFile(s.path(ExternalSettlementJournalPrepared), []byte("{"), 0600)
						} else {
							err = os.WriteFile(s.path(ExternalSettlementJournalFenced), []byte("{}"), 0600)
						}
					} else if kind == "drift" {
						// The original signed archive checksum is a required invariant.
						archive.Checksum = strings.Repeat("f", 64)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := s.Prepare(ctx, id, archive); err == nil {
				t.Fatal("unsafe event accepted")
			}
		})
	}
}

func TestSettlementEventJournalConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	data, _, id, archive := settlementJournalFixture(t, Short)
	const n = 8
	var wg sync.WaitGroup
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, e := NewExternalSettlementEventJournalStore(data, id)
			if e == nil {
				_, e = s.Prepare(ctx, id, archive)
			}
			results <- e
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		}
	}
	if winners == 0 {
		t.Fatal("no worker wrote a valid immutable journal")
	}
	restarted, err := NewExternalSettlementEventJournalStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := restarted.Load(ctx, id, archive)
	if err != nil || rec.Stage != ExternalSettlementJournalPrepared {
		t.Fatalf("concurrent writer corrupted journal: %+v %v", rec, err)
	}
}

func TestSettlementEventJournalSubprocess(t *testing.T) {
	if os.Getenv("SCOPED_SETTLEMENT_JOURNAL_SUBPROCESS") != "1" {
		return
	}
	data, id, archivePath := os.Getenv("SCOPED_SETTLEMENT_DATA"), os.Getenv("SCOPED_SETTLEMENT_ID"), os.Getenv("SCOPED_SETTLEMENT_ARCHIVE")
	raw, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	var archive PackageAEntryArchive
	if err = json.Unmarshal(raw, &archive); err != nil {
		t.Fatal(err)
	}
	s, err := NewExternalSettlementEventJournalStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Prepare(context.Background(), id, archive); err != nil {
		t.Fatal(err)
	}
}

func TestSettlementEventJournalCrossProcessCollision(t *testing.T) {
	ctx := context.Background()
	data, _, id, archive := settlementJournalFixture(t, Long)
	raw, err := json.Marshal(archive)
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(t.TempDir(), "archive.json")
	if err = os.WriteFile(archivePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	const n = 3
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestSettlementEventJournalSubprocess$", "-test.v=false")
			cmd.Env = append(os.Environ(), "SCOPED_SETTLEMENT_JOURNAL_SUBPROCESS=1", "SCOPED_SETTLEMENT_DATA="+data, "SCOPED_SETTLEMENT_ID="+id, "SCOPED_SETTLEMENT_ARCHIVE="+archivePath)
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
		t.Fatal("all subprocess writes failed")
	}
	s, err := NewExternalSettlementEventJournalStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	root, ev, err := s.Load(ctx, id, archive)
	if err != nil || ev != nil || root.Stage != ExternalSettlementJournalPrepared {
		t.Fatalf("corrupt after cross process race: %+v %v", root, err)
	}
}
