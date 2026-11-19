package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func settlementProposalFixture(t *testing.T, side PositionSide) (string, *ExternalCloseSettlementProposalStore, ExternalCloseRehearsal) {
	t.Helper()
	dir, rehearsal := externalReviewFixture(t, side)
	ledger, err := NewExternalCloseReviewLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Stage(rehearsal, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Decide(rehearsal.EntryClientOrderID, ExternalReviewAcknowledged, "operator_01", "Reviewed only; no settlement authorization", time.Now()); err != nil {
		t.Fatal(err)
	}
	store, err := NewExternalCloseSettlementProposalStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir, store, rehearsal
}

func TestExternalCloseProposalLongShortRestartIdempotentNeverPosts(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, s, r := settlementProposalFixture(t, side)
			before, err := snapshotExecutionEvidence(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(s.dir); !os.IsNotExist(err) {
				t.Fatalf("constructor wrote file: %v", err)
			}
			p, err := s.Prepare(r.EntryClientOrderID, time.Now().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if p.Stage != ExternalSettlementUnposted || p.ModelID != r.ModelID || p.ProposedPositionDelta != "-0.001" ||
				p.FundingStatus != "UNVERIFIED" || p.HistoricalScope != "BOUNDED_NOT_PROVEN_COMPLETE" ||
				p.SettlementAuthorized || p.TradingAuthorized || p.PostingApplied || p.LifecycleChanged || p.OwnershipChanged ||
				p.EntryOrderID != r.EntryOrderID || p.ExitOrderID != r.ExitOrderID {
				t.Fatalf("unsafe proposal %+v", p)
			}
			if err := ValidateExternalCloseSettlementProposal(p); err != nil {
				t.Fatal(err)
			}
			again, err := s.Prepare(r.EntryClientOrderID, time.Now().Add(time.Hour))
			if err != nil || again.Checksum != p.Checksum {
				t.Fatalf("idempotence failed: %v", err)
			}
			restarted, err := NewExternalCloseSettlementProposalStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := restarted.Load(r.EntryClientOrderID)
			if err != nil || loaded.Checksum != p.Checksum {
				t.Fatalf("restart failed: %v", err)
			}
			after, err := snapshotExecutionEvidence(dir)
			if err != nil || before != after {
				t.Fatalf("original execution evidence changed: %v", err)
			}
		})
	}
}

func TestExternalCloseProposalRejectsUnacknowledgedAndRejected(t *testing.T) {
	for _, decision := range []string{"staged", "rejected"} {
		t.Run(decision, func(t *testing.T) {
			dir, r := externalReviewFixture(t, Short)
			l, err := NewExternalCloseReviewLedger(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.Stage(r, time.Now().Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
			if decision == "rejected" {
				if _, err := l.Decide(r.EntryClientOrderID, ExternalReviewRejected, "operator_01", "Not acceptable", time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			s, err := NewExternalCloseSettlementProposalStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Prepare(r.EntryClientOrderID, time.Now()); err == nil {
				t.Fatal("unauthorized review accepted")
			}
		})
	}
}

func TestExternalCloseProposalFailClosedDriftTamperingLocks(t *testing.T) {
	for _, scen := range []string{"original-drift", "root-event-drift", "tamper", "tamper-even-rehashed", "unknown-file", "active-lock", "symlink"} {
		t.Run(scen, func(t *testing.T) {
			dir, s, r := settlementProposalFixture(t, Long)
			p, err := s.Prepare(r.EntryClientOrderID, time.Now().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			switch scen {
			case "original-drift":
				files, err := os.ReadDir(filepath.Join(dir, "trading", "fills"))
				if err != nil || len(files) == 0 {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, "trading", "fills", files[0].Name())); err != nil {
					t.Fatal(err)
				}
			case "root-event-drift":
				l, err := NewExternalCloseReviewLedger(dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(l.eventPath(r.EntryClientOrderID), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "tamper", "tamper-even-rehashed":
				p.SignedRealizedPnL = "999"
				if scen == "tamper-even-rehashed" {
					p.Checksum = p.digest()
				}
				if err := os.WriteFile(s.path(), mustProposalJSON(t, p), 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown-file":
				if err := os.WriteFile(filepath.Join(s.dir, "extra.json"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "active-lock":
				if err := os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(s.path()); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(dir, "VERSION"), s.path()); err != nil {
					t.Skip(err)
				}
			}
			if _, err := s.Load(r.EntryClientOrderID); err == nil {
				t.Fatal("unsafe proposal loaded: " + scen)
			}
		})
	}
}

func mustProposalJSON(t *testing.T, p ExternalCloseSettlementProposal) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestExternalCloseProposalMismatchedFinancialEquationCannotRehash(t *testing.T) {
	_, s, r := settlementProposalFixture(t, Short)
	p, err := s.Prepare(r.EntryClientOrderID, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	p.ProvisionalNetExcludingFunding = "999"
	p.Checksum = p.digest()
	if err := ValidateExternalCloseSettlementProposal(p); err == nil {
		t.Fatal("forged cash equation accepted")
	}
	p.ProvisionalNetExcludingFunding = "0"
	p.PostingApplied = true
	p.Checksum = p.digest()
	if err := ValidateExternalCloseSettlementProposal(p); err == nil {
		t.Fatal("posting authorization accepted")
	}
}

func TestExternalCloseProposalConcurrentSingleGlobalIncident(t *testing.T) {
	_, s, r := settlementProposalFixture(t, Short)
	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Prepare(r.EntryClientOrderID, time.Now().Add(time.Second))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	// During the moment between O_EXCL and fsync, concurrent callers may
	// fail closed on an incomplete record. No caller may overwrite it.
	for err := range errs {
		if err != nil && !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "unexpected end") {
			t.Log("concurrent request safely rejected", err)
		}
	}
	if _, err := s.Load(r.EntryClientOrderID); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil || len(entries) != 1 {
		t.Fatal(err, entries)
	}
}
