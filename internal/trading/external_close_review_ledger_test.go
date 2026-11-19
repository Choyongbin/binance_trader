package trading

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func externalReviewFixture(t *testing.T, side PositionSide) (string, ExternalCloseRehearsal) {
	t.Helper()
	dir, mock, _ := externalCloseFixture(t, side)
	report := reviewFixture(dir, mock)
	if len(report.ExternalCloseRehearsals) != 1 {
		t.Fatalf("missing review: %+v", report)
	}
	p := report.ExternalCloseRehearsals[0]
	if p.Status != ExternalCloseRehearsed {
		t.Fatalf("invalid review fixture: %+v", p)
	}
	return dir, p
}

func TestExternalReviewLedgerAppendOnlyAndRestartLongShort(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			dir, p := externalReviewFixture(t, side)
			before, e := snapshotExecutionEvidence(dir)
			if e != nil {
				t.Fatal(e)
			}
			ledger, e := NewExternalCloseReviewLedger(dir)
			if e != nil {
				t.Fatal(e)
			}
			if _, e := os.Lstat(filepath.Join(dir, "trading", "external_close_reviews")); !os.IsNotExist(e) {
				t.Fatal("constructor mutated disk", e)
			}
			staged, e := ledger.Stage(p, time.Now().UTC())
			if e != nil {
				t.Fatal(e)
			}
			if staged.Stage != ExternalReviewStaged || !staged.Root.ReviewOnly || staged.TradingAuthorized || staged.SettlementAuthorized || len(staged.Events) != 0 {
				t.Fatal(staged)
			}
			again, e := ledger.Stage(p, time.Now().Add(time.Hour))
			if e != nil || again.Root.Checksum != staged.Root.Checksum {
				t.Fatalf("duplicate stage %v", e)
			}
			reopened, e := NewExternalCloseReviewLedger(dir)
			if e != nil {
				t.Fatal(e)
			}
			ack, e := reopened.Decide(p.EntryClientOrderID, ExternalReviewAcknowledged, "reviewer-1", "Checked candidate only; funding still unverified", time.Now().Add(time.Minute))
			if e != nil {
				t.Fatal(e)
			}
			if ack.Stage != ExternalReviewAcknowledged || len(ack.Events) != 1 || ack.TradingAuthorized || ack.SettlementAuthorized || ack.Events[0].PreviousChecksum != staged.Root.Checksum {
				t.Fatal(ack)
			}
			retried, e := reopened.Decide(p.EntryClientOrderID, ExternalReviewAcknowledged, "reviewer-1", "Checked candidate only; funding still unverified", time.Now().Add(time.Hour))
			if e != nil || retried.Events[0].Checksum != ack.Events[0].Checksum {
				t.Fatalf("idempotent decide %v", e)
			}
			if _, e := reopened.Decide(p.EntryClientOrderID, ExternalReviewRejected, "reviewer-2", "No", time.Now().Add(time.Hour)); e == nil {
				t.Fatal("conflicting review decision accepted")
			}
			after, e := snapshotExecutionEvidence(dir)
			if e != nil || before != after {
				t.Fatalf("original evidence modified: %v", e)
			}
			check, e := reopened.Load(p.EntryClientOrderID)
			if e != nil || check.Stage != ExternalReviewAcknowledged {
				t.Fatalf("restart: %+v %v", check, e)
			}
		})
	}
}
func TestExternalReviewLedgerRejectAndNoTransitionToSettled(t *testing.T) {
	dir, p := externalReviewFixture(t, Short)
	ledger, e := NewExternalCloseReviewLedger(dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ledger.Stage(p, time.Now()); e != nil {
		t.Fatal(e)
	}
	rejected, e := ledger.Decide(p.EntryClientOrderID, ExternalReviewRejected, "operator_1", "Need more evidence", time.Now().Add(time.Second))
	if e != nil || rejected.Stage != ExternalReviewRejected {
		t.Fatal(e, rejected)
	}
	if _, e = ledger.Decide(p.EntryClientOrderID, "SETTLED", "operator_1", "attempt", time.Now().Add(time.Minute)); e == nil {
		t.Fatal("settlement mutation accepted")
	}
	if _, e = ledger.Decide(p.EntryClientOrderID, ExternalReviewAcknowledged, "operator_1", "attempt", time.Now().Add(time.Minute)); e == nil {
		t.Fatal("reversal accepted")
	}
}
func TestExternalReviewLedgerFailClosedInvalidAndDrift(t *testing.T) {
	for _, scenario := range []string{"unsealed", "fake-trading", "missing-fills", "active-lock", "diff-preview", "symlink-ledger"} {
		t.Run(scenario, func(t *testing.T) {
			dir, p := externalReviewFixture(t, Short)
			ledger, e := NewExternalCloseReviewLedger(dir)
			if e != nil {
				t.Fatal(e)
			}
			switch scenario {
			case "unsealed":
				p.SignedReadDigestSHA256 = "garbage"
			case "fake-trading":
				p.TradingAuthorized = true
				p.PreviewSHA256 = fingerprintRehearsal(p)
			case "missing-fills":
				fills, e := os.ReadDir(filepath.Join(dir, "trading", "fills"))
				if e != nil || len(fills) != 1 {
					t.Fatal(e, fills)
				}
				if e = os.Remove(filepath.Join(dir, "trading", "fills", fills[0].Name())); e != nil {
					t.Fatal(e)
				}
			case "active-lock":
				if e = os.Mkdir(filepath.Join(dir, "trading", "coordinator.active"), 0700); e != nil {
					t.Fatal(e)
				}
			case "symlink-ledger":
				if e = os.Symlink(t.TempDir(), ledger.dir); e != nil {
					t.Skip(e)
				}
			case "diff-preview":
				if _, e = ledger.Stage(p, time.Now()); e != nil {
					t.Fatal(e)
				}
				p.SignedRealizedPnL = "999"
				p.PreviewSHA256 = fingerprintRehearsal(p)
			}
			if _, e = ledger.Stage(p, time.Now()); e == nil {
				t.Fatal("unsafe stage accepted", scenario)
			}
		})
	}
}
func TestExternalReviewLedgerChecksumTamperAndUnexpectedFiles(t *testing.T) {
	for _, scenario := range []string{"root-tamper", "event-tamper", "unknown", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			dir, p := externalReviewFixture(t, Long)
			ledger, e := NewExternalCloseReviewLedger(dir)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = ledger.Stage(p, time.Now()); e != nil {
				t.Fatal(e)
			}
			if scenario == "event-tamper" {
				if _, e = ledger.Decide(p.EntryClientOrderID, ExternalReviewAcknowledged, "reviewer-1", "Confirm only", time.Now().Add(time.Minute)); e != nil {
					t.Fatal(e)
				}
			}
			path := ledger.rootPath(p.EntryClientOrderID)
			switch scenario {
			case "event-tamper":
				path = ledger.eventPath(p.EntryClientOrderID)
			case "unknown":
				if e = os.WriteFile(filepath.Join(ledger.dir, "evil.json"), []byte("{}"), 0600); e != nil {
					t.Fatal(e)
				}
			case "truncated":
				if e = os.WriteFile(path, []byte(`{"schema":1`), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if strings.Contains(scenario, "tamper") {
				b, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				b = []byte(strings.Replace(string(b), `"review_only": true`, `"review_only": false`, 1))
				if e = os.WriteFile(path, b, 0600); e != nil {
					t.Fatal(e)
				}
			}
			if _, e = ledger.Load(p.EntryClientOrderID); e == nil {
				t.Fatal("tamper accepted", scenario)
			}
		})
	}
}
func TestExternalReviewLedgerConcurrentSameCaseExactlyOne(t *testing.T) {
	dir, p := externalReviewFixture(t, Short)
	ledger, e := NewExternalCloseReviewLedger(dir)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = ledger.Stage(p, time.Now()) }()
	}
	wg.Wait()
	state, e := ledger.Load(p.EntryClientOrderID)
	if e != nil || state.Stage != ExternalReviewStaged {
		t.Fatalf("concurrent stage: %v %+v", e, state)
	}
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = ledger.Decide(p.EntryClientOrderID, ExternalReviewAcknowledged, "reviewer-1", "I reviewed this candidate", time.Now().Add(time.Minute))
		}()
	}
	wg.Wait()
	state, e = ledger.Load(p.EntryClientOrderID)
	if e != nil || len(state.Events) != 1 || state.Stage != ExternalReviewAcknowledged {
		t.Fatalf("concurrent decision: %v %+v", e, state)
	}
}

// Even a previously valid operator acknowledgement must not be represented as
// current evidence after a change to the immutable entry/fill journal.
func TestExternalReviewLedgerLoadRejectsEvidenceDrift(t *testing.T) {
	dir, p := externalReviewFixture(t, Short)
	ledger, err := NewExternalCloseReviewLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Stage(p, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Decide(p.EntryClientOrderID, ExternalReviewAcknowledged, "operator_1", "Review only", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	fills, err := os.ReadDir(filepath.Join(dir, "trading", "fills"))
	if err != nil || len(fills) == 0 {
		t.Fatal(err, fills)
	}
	if err := os.Remove(filepath.Join(dir, "trading", "fills", fills[0].Name())); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Load(p.EntryClientOrderID); err == nil {
		t.Fatal("stale acknowledgement accepted after fill deletion")
	}
}
