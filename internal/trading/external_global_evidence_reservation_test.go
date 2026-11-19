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

func TestGlobalEvidenceReservationsAppendRestartIdempotentAndOriginalUnmodified(t *testing.T) {
	ctx := context.Background()
	a := crossFinancialFixture(t, Long, "global-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "global-b", 1000, 80*time.Minute, 0)
	c := crossFinancialFixture(t, Long, "global-c", 2000, 20*time.Minute, 0)
	d := crossFinancialFixture(t, Short, "global-d", 3000, 20*time.Minute, 0)
	first := []ExternalFinancialEventSource{a, b}
	second := []ExternalFinancialEventSource{c, d}
	root := t.TempDir()
	s, err := NewExternalGlobalEvidenceReservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	hashes := make([]string, 4)
	for i, e := range []ExternalFinancialEventSource{a, b, c, d} {
		hashes[i], err = snapshotExecutionEvidence(e.DataDir)
		if err != nil {
			t.Fatal(err)
		}
	}
	state, err := s.Inspect(ctx)
	if err != nil || len(state.Reservations) != 0 {
		t.Fatalf("initial state: %+v %v", state, err)
	}
	state, err = s.ReserveBatch(ctx, first)
	if err != nil || len(state.Reservations) != 1 || state.TotalClaims != 2 || state.PostingApplied || state.TradingAuthorized || state.SettlementAuthorized {
		t.Fatalf("first: %+v %v", state, err)
	}
	state, err = s.ReserveBatch(ctx, []ExternalFinancialEventSource{b, a})
	if err != nil || len(state.Reservations) != 1 {
		t.Fatalf("idempotency: %+v %v", state, err)
	}
	state, err = s.ReserveBatch(ctx, second)
	if err != nil || len(state.Reservations) != 2 || state.TotalClaims != 4 {
		t.Fatalf("second: %+v %v", state, err)
	}
	restarted, _ := NewExternalGlobalEvidenceReservationStore(root)
	read, err := restarted.Inspect(ctx)
	if err != nil || read.LastChecksum != state.LastChecksum || read.TotalClaims != 4 {
		t.Fatalf("restart: %+v %v", read, err)
	}
	for i, e := range []ExternalFinancialEventSource{a, b, c, d} {
		h, err := snapshotExecutionEvidence(e.DataDir)
		if err != nil || h != hashes[i] {
			t.Fatal("original execution evidence changed", err)
		}
	}
}

func TestGlobalEvidenceReservationsRejectCrossBatchDuplicateAndOverlap(t *testing.T) {
	ctx := context.Background()
	a := crossFinancialFixture(t, Long, "global-first-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "global-first-b", 1000, 80*time.Minute, 0)
	c := crossFinancialFixture(t, Long, "global-overlap-c", 2000, 80*time.Minute, 0) // same-side overlap
	d := crossFinancialFixture(t, Short, "global-overlap-d", 3000, 20*time.Minute, 0)
	s, _ := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	if _, err := s.ReserveBatch(ctx, []ExternalFinancialEventSource{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveBatch(ctx, []ExternalFinancialEventSource{c, d}); err == nil {
		t.Fatal("pooled side overlap accepted")
	}
	if _, err := s.ReserveBatch(ctx, []ExternalFinancialEventSource{a, d}); err == nil {
		t.Fatal("same client already reserved")
	}
	state, err := s.Inspect(ctx)
	if err != nil || len(state.Reservations) != 1 {
		t.Fatalf("conflicts wrote into registry: %+v %v", state, err)
	}
}

func TestGlobalEvidenceReservationsConcurrentSameBatchExactlyOnce(t *testing.T) {
	a := crossFinancialFixture(t, Long, "concurrent-a", 0, 60*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "concurrent-b", 1000, 60*time.Minute, 0)
	s, _ := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.ReserveBatch(context.Background(), []ExternalFinancialEventSource{a, b})
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	state, err := s.Inspect(context.Background())
	if err != nil || successes == 0 || len(state.Reservations) != 1 || state.TotalClaims != 2 {
		t.Fatalf("race: successes=%d %+v %v", successes, state, err)
	}
}

func TestGlobalEvidenceReservationsTornMissingUnknownAndForgedApprovalFailClosed(t *testing.T) {
	a := crossFinancialFixture(t, Long, "forensic-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "forensic-b", 1000, 80*time.Minute, 0)
	events := []ExternalFinancialEventSource{a, b}
	for _, mode := range []string{"torn", "unknown", "missing", "forged", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			s, _ := NewExternalGlobalEvidenceReservationStore(root)
			if mode == "torn" {
				os.Mkdir(s.dir, 0700)
				os.WriteFile(filepath.Join(s.dir, reservationSlot(1)), []byte("{\"partial\":"), 0600)
			} else {
				if _, err := s.ReserveBatch(context.Background(), events); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "unknown":
					os.WriteFile(filepath.Join(s.dir, "rogue.txt"), []byte("x"), 0600)
				case "missing":
					os.Rename(filepath.Join(s.dir, reservationSlot(1)), filepath.Join(s.dir, reservationSlot(2)))
				case "forged":
					state, _ := s.Inspect(context.Background())
					r := state.Reservations[0]
					r.SettlementAuthorized = true
					r.Checksum = r.digest()
					raw, _ := json.Marshal(r)
					os.WriteFile(filepath.Join(s.dir, reservationSlot(1)), raw, 0600)
				case "symlink":
					os.Remove(filepath.Join(s.dir, reservationSlot(1)))
					if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(s.dir, reservationSlot(1))); err != nil {
						t.Skipf("symlink creation unavailable on this host: %v", err)
					}
				}
			}
			restart, _ := NewExternalGlobalEvidenceReservationStore(root)
			if _, err := restart.Inspect(context.Background()); err == nil {
				t.Fatal("corruption accepted")
			}
			if _, err := restart.ReserveBatch(context.Background(), events); err == nil {
				t.Fatal("corrupt registry auto-repaired")
			}
		})
	}
}

func TestGlobalEvidenceReservationDoesNotWriteIntoSource(t *testing.T) {
	a := crossFinancialFixture(t, Long, "source-a", 0, 60*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "source-b", 1000, 60*time.Minute, 0)
	s, _ := NewExternalGlobalEvidenceReservationStore(a.DataDir)
	if _, err := s.ReserveBatch(context.Background(), []ExternalFinancialEventSource{a, b}); err == nil {
		t.Fatal("global registry created inside original data")
	}
}

func TestGlobalEvidenceReservationsRejectCrossBatchOrderTradeFundingReuses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		makeSecond func(*testing.T) ExternalFinancialEventSource
	}{
		{"order_reuse", func(t *testing.T) ExternalFinancialEventSource {
			return crossFinancialFixture(t, Long, "global-reuse-order", 0, 20*time.Minute, 0, 2000)
		}},
		{"trade_reuse", func(t *testing.T) ExternalFinancialEventSource {
			return crossFinancialFixture(t, Long, "global-reuse-trade", 2000, 20*time.Minute, 0, 0)
		}},
		{"funding_reuse", func(t *testing.T) ExternalFinancialEventSource {
			return crossFinancialFixture(t, Long, "global-reuse-funding", 2000, 20*time.Minute, 77)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := crossFinancialFixture(t, Long, "global-original", 0, 80*time.Minute, 77)
			b := crossFinancialFixture(t, Short, "global-original-other", 1000, 80*time.Minute, 0)
			c := tc.makeSecond(t)
			d := crossFinancialFixture(t, Short, "global-other", 3000, 20*time.Minute, 0)
			s, _ := NewExternalGlobalEvidenceReservationStore(t.TempDir())
			if _, err := s.ReserveBatch(context.Background(), []ExternalFinancialEventSource{a, b}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ReserveBatch(context.Background(), []ExternalFinancialEventSource{c, d}); err == nil {
				t.Fatal("cross-batch duplicate accepted")
			}
			state, err := s.Inspect(context.Background())
			if err != nil || len(state.Reservations) != 1 {
				t.Fatalf("conflicting batch recorded: %+v %v", state, err)
			}
		})
	}
}

func TestGlobalEvidenceReservationsConcurrentDistinctBatchesNeverAllocateTwice(t *testing.T) {
	a := crossFinancialFixture(t, Long, "distinct-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "distinct-b", 1000, 80*time.Minute, 0)
	c := crossFinancialFixture(t, Long, "distinct-c", 2000, 20*time.Minute, 0)
	d := crossFinancialFixture(t, Short, "distinct-d", 3000, 20*time.Minute, 0)
	batches := [][]ExternalFinancialEventSource{{a, b}, {c, d}}
	s, _ := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	var wg sync.WaitGroup
	for _, batch := range batches {
		wg.Add(1)
		go func(e []ExternalFinancialEventSource) {
			defer wg.Done()
			_, _ = s.ReserveBatch(context.Background(), e)
		}(batch)
	}
	wg.Wait()
	// A peer can read another writer's incomplete O_EXCL slot and fail
	// closed. Once it is durable, retry is safe and deterministically heals
	// the *request*, never the potentially torn on-disk slot.
	for _, batch := range batches {
		if _, err := s.ReserveBatch(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	result, err := s.Inspect(context.Background())
	if err != nil || len(result.Reservations) != 2 || result.TotalClaims != 4 {
		t.Fatalf("lost or duplicated reservation: %+v %v", result, err)
	}
}
