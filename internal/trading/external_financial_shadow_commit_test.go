package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func financialShadowFixture(t *testing.T, side PositionSide) (string, string, PackageAEntryArchive, string, *ExternalShadowCommitStore) {
	t.Helper()
	data, id, archive, linked := linkedScopedFixture(t, side)
	seal := sealLinkedScoped(t, linked, id, archive)
	shadow, err := NewExternalShadowCommitStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	return data, id, archive, seal, shadow
}

func TestFinancialShadowPublicationCrashBoundariesLongShort(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, archive, seal, store := financialShadowFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			initial, err := store.Inspect(ctx, archive, seal)
			if err != nil || initial.Stage != ExternalShadowCommitAbsent || initial.ShadowPublicationVisible {
				t.Fatalf("initial: %+v %v", initial, err)
			}
			if _, err := store.ReadShadowPair(ctx, archive, seal); err == nil {
				t.Fatal("unpublished read accepted")
			}
			if _, err := store.PublishShadow(ctx, archive, seal); err == nil {
				t.Fatal("publish without prepare accepted")
			}
			prep, err := store.PrepareShadow(ctx, archive, seal)
			if err != nil || prep.Stage != ExternalShadowCommitPrepared || prep.ShadowPublicationVisible {
				t.Fatalf("prepare: %+v %v", prep, err)
			}
			restart, err := NewExternalShadowCommitStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			got, err := restart.Inspect(ctx, archive, seal)
			if err != nil || !reflect.DeepEqual(prep, got) {
				t.Fatalf("restart before commit: %+v %v", got, err)
			}
			if _, err := restart.ReadShadowPair(ctx, archive, seal); err == nil {
				t.Fatal("prepared-only pair exposed")
			}
			final, err := restart.PublishShadow(ctx, archive, seal)
			if err != nil || final.Stage != ExternalShadowCommitPublished || !final.ShadowPublicationVisible || !externalReviewHash.MatchString(final.ShadowPublishSHA256) {
				t.Fatalf("published: %+v %v", final, err)
			}
			pair, err := restart.ReadShadowPair(ctx, archive, seal)
			if err != nil || pair.CommitSHA256 != seal || pair.Lifecycle.CommitSHA256 != seal || pair.Ownership.CommitSHA256 != seal || pair.Lifecycle.Proposal == "" || pair.Ownership.Proposal == "" || pair.PostingApplied || pair.Lifecycle.Applied || pair.Ownership.Applied || pair.TradingAuthorized {
				t.Fatalf("bad shadow pair: %+v %v", pair, err)
			}
			fresh, err := NewExternalShadowCommitStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := fresh.PublishShadow(ctx, archive, seal)
			if err != nil || !reflect.DeepEqual(final, replay) {
				t.Fatalf("replay drift: %+v %v", replay, err)
			}
			replay, err = fresh.PrepareShadow(ctx, archive, seal)
			if err != nil || !reflect.DeepEqual(final, replay) {
				t.Fatalf("prepare replay drift: %+v %v", replay, err)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("original financial records modified: %v", err)
			}
			if _, err := fresh.Inspect(ctx, archive, strings.Repeat("f", 64)); err == nil {
				t.Fatal("foreign pinned seal accepted")
			}
			found, err := os.ReadDir(filepath.Dir(fresh.dir))
			if err != nil || len(found) != 1 {
				t.Fatalf("unexpected shadow files: %v %v", found, err)
			}
		})
	}
}

func TestFinancialShadowRejectsCorruptionAndStaleEvidence(t *testing.T) {
	for _, kind := range []string{"prepared_torn", "published_torn", "forged", "orphan", "unknown", "symlink", "source_drift", "wrong_id", "foreign_seal"} {
		t.Run(kind, func(t *testing.T) {
			data, _, archive, seal, s := financialShadowFixture(t, Long)
			ctx := context.Background()
			if _, err := s.PrepareShadow(ctx, archive, seal); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PublishShadow(ctx, archive, seal); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "prepared_torn":
				if err := os.WriteFile(filepath.Join(s.dir, "prepared.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "published_torn":
				if err := os.WriteFile(filepath.Join(s.dir, "published.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "forged":
				path := filepath.Join(s.dir, "published.json")
				var rec ExternalShadowCommitRecord
				if err := strictReviewJSON(path, &rec); err != nil {
					t.Fatal(err)
				}
				rec.PostingApplied = true
				rec.Checksum = rec.digest()
				b, err := json.Marshal(rec)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				if err := os.Remove(filepath.Join(s.dir, "prepared.json")); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				if err := os.WriteFile(filepath.Join(s.dir, "unsolicited.tmp"), []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(filepath.Join(s.dir, "published.json")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(s.dir, "prepared.json"), filepath.Join(s.dir, "published.json")); err != nil {
					t.Skipf("symlink not permitted: %v", err)
				}
			case "source_drift":
				if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
					t.Fatal(err)
				}
			case "wrong_id":
				s.id = "bt_ffffffffffffffffffffffffffff"
			case "foreign_seal":
				seal = strings.Repeat("f", 64)
			}
			if _, err := s.Inspect(ctx, archive, seal); err == nil {
				t.Fatal("unsafe or stale shadow publication accepted")
			}
			if _, err := s.ReadShadowPair(ctx, archive, seal); err == nil {
				t.Fatal("unsafe shadow pair exposed")
			}
		})
	}
}

func TestFinancialShadowTwoEventsAndConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	dataA, idA, archiveA, sealA, first := financialShadowFixture(t, Long)
	dataB, idB, archiveB, sealB, second := financialShadowFixture(t, Short)
	// Separate stores are bound to separate sandbox roots in this fixture.
	if dataA == dataB || idA == "" || idB == "" {
		t.Fatal("invalid independent fixture")
	}
	for _, tc := range []struct {
		s    *ExternalShadowCommitStore
		a    PackageAEntryArchive
		seal string
	}{
		{first, archiveA, sealA}, {second, archiveB, sealB},
	} {
		if _, err := tc.s.PrepareShadow(ctx, tc.a, tc.seal); err != nil {
			t.Fatal(err)
		}
		if _, err := tc.s.PublishShadow(ctx, tc.a, tc.seal); err != nil {
			t.Fatal(err)
		}
	}
	restarted, err := NewExternalShadowCommitStore(dataA, idA)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := restarted.PublishShadow(ctx, archiveA, sealA)
			if err == nil && (!r.ShadowPublicationVisible || r.PostingApplied || r.TradingAuthorized) {
				errs <- os.ErrPermission
			} else if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// Two independent store instances simulate separate processes that have no
// shared Go mutex. An O_EXCL collision is not silently retried as a second
// publication; a fresh reader must still see precisely one valid pair.
func TestFinancialShadowIndependentWritersOnePublishMarker(t *testing.T) {
	ctx := context.Background()
	data, id, archive, seal, first := financialShadowFixture(t, Long)
	if _, err := first.PrepareShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	second, err := NewExternalShadowCommitStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		got ExternalShadowCommitInspection
		err error
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, store := range []*ExternalShadowCommitStore{first, second} {
		wg.Add(1)
		go func(s *ExternalShadowCommitStore) {
			defer wg.Done()
			got, err := s.PublishShadow(ctx, archive, seal)
			outcomes <- outcome{got, err}
		}(store)
	}
	wg.Wait()
	close(outcomes)
	success := 0
	for o := range outcomes {
		if o.err == nil {
			if !o.got.ShadowPublicationVisible {
				t.Fatal("success without publication")
			}
			success++
		}
	}
	if success < 1 || success > 2 {
		t.Fatalf("invalid publisher count: %d", success)
	}
	restarted, err := NewExternalShadowCommitStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Inspect(ctx, archive, seal)
	if err != nil || !got.ShadowPublicationVisible {
		t.Fatalf("lost publish: %+v %v", got, err)
	}
	if _, err := restarted.ReadShadowPair(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(restarted.dir)
	if err != nil || len(files) != 2 {
		t.Fatalf("unexpected checkpoint count %d: %v", len(files), err)
	}
}
