package protectionfence

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newAuditFixture(t *testing.T) (*Store, Intent, RecoveryReview, ExposureSnapshot) {
	t.Helper()
	s, in, o := recoveryFixture(t, SubmitTP)
	r, err := s.InspectRecoveryOnce(context.Background(), &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}})
	if err != nil || r.State != RecoverySubmitObserved {
		t.Fatalf("read %+v %v", r, err)
	}
	exposure := ExposureSnapshot{AccountFingerprintSHA256: in.AccountFingerprintSHA256, ObservedAt: time.Now().UTC(), LongQtyBTC: in.QuantityBTC, ShortQtyBTC: "0",
		Owners: []OwnerShare{{GroupID: in.GroupID, Source: "model", ModelID: "m1", Symbol: "BTCUSDT", Side: in.PositionSide, QuantityBTC: in.QuantityBTC}}}
	return s, in, r, exposure
}
func TestRecoveryAuditAppendReopenAndNoReplay(t *testing.T) {
	s, in, r, e := newAuditFixture(t)
	when := time.Now().UTC()
	before, err := s.ReadRecoveryAudit()
	if err != nil || len(before) != 0 {
		t.Fatalf("empty trail %+v %v", before, err)
	}
	saved, err := s.AppendRecoveryAudit(r, e, when)
	if err != nil || saved.Sequence != 1 || saved.ExposureReview.State != ExposureOneOwner || saved.Checksum == "" {
		t.Fatalf("save %+v %v", saved, err)
	}
	s2, _ := Open(filepath.Dir(filepath.Dir(s.dir)))
	after, err := s2.ReadRecoveryAudit()
	if err != nil || len(after) != 1 || after[0].Checksum != saved.Checksum {
		t.Fatalf("reload %+v %v", after, err)
	}
	// Journal is a sibling of the exclusive fence slot, never an orphan file within it.
	if fs, err := s2.InspectAll(); err != nil || len(fs) != 1 {
		t.Fatalf("fence scan %+v %v", fs, err)
	}
	if _, err := s2.SealBeforePossibleSend(in, time.Now()); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("mutation unlocked: %v", err)
	}
	if _, err := s2.AppendRecoveryAudit(r, e, when); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("duplicate audit permitted %v", err)
	}
}
func TestRecoveryAuditTwoOrderedEntriesChained(t *testing.T) {
	s, _, r, e := newAuditFixture(t)
	now := time.Now().UTC()
	a, err := s.AppendRecoveryAudit(r, e, now)
	if err != nil {
		t.Fatal(err)
	}
	r.FirstObservedAt = now.Add(100 * time.Millisecond)
	r.SecondObservedAt = now.Add(100 * time.Millisecond)
	e.ObservedAt = now.Add(100 * time.Millisecond)
	later := now.Add(200 * time.Millisecond)
	b, err := s.AppendRecoveryAudit(r, e, later)
	if err != nil {
		t.Fatal(err)
	}
	if b.Sequence != 2 || b.PreviousSHA256 != a.Checksum {
		t.Fatalf("chain broken: %+v", b)
	}
	arr, err := s.ReadRecoveryAudit()
	if err != nil || len(arr) != 2 {
		t.Fatalf("read %+v %v", arr, err)
	}
	if _, err := s.SealBeforePossibleSend(testIntent(SubmitSL), time.Now()); err == nil {
		t.Fatal("fence accidentally released")
	}
}
func TestRecoveryAuditRejectsUnsafeApprovalAndMismatchedIdentity(t *testing.T) {
	cases := []struct {
		name   string
		change func(*RecoveryReview)
	}{
		{"trading", func(r *RecoveryReview) { r.TradingAuthorized = true }},
		{"submit", func(r *RecoveryReview) { r.SubmitAuthorized = true }},
		{"cancel", func(r *RecoveryReview) { r.CancelAuthorized = true }},
		{"retry", func(r *RecoveryReview) { r.RetryAuthorized = true }},
		{"settlement", func(r *RecoveryReview) { r.SettlementAuthorized = true }},
		{"group mismatch", func(r *RecoveryReview) { r.GroupID = "other" }},
		{"unknown state", func(r *RecoveryReview) { r.State = "APPROVED" }},
		{"manual marker false", func(r *RecoveryReview) { r.ManualReviewRequired = false }},
		{"first observation stale", func(r *RecoveryReview) { r.FirstObservedAt = time.Now().Add(-time.Minute) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _, r, e := newAuditFixture(t)
			tc.change(&r)
			if _, err := s.AppendRecoveryAudit(r, e, time.Now()); !errors.Is(err, ErrAuditIntegrity) {
				t.Fatalf("unsafe accepted: %v", err)
			}
			if files, _ := os.ReadDir(s.auditDir()); len(files) != 0 {
				t.Fatalf("wrote unsafe audit: %v", files)
			}
		})
	}
}
func TestRecoveryAuditTamperedOrOrphanFailClosed(t *testing.T) {
	cases := []string{"checksum", "missing-index", "extra", "symlink", "changed-authority", "recorded-exposure"}
	for _, kind := range cases {
		t.Run(kind, func(t *testing.T) {
			s, _, r, e := newAuditFixture(t)
			_, err := s.AppendRecoveryAudit(r, e, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(s.auditDir(), auditName(1))
			switch kind {
			case "checksum":
				b, _ := os.ReadFile(p)
				b[len(b)-2] = 'x'
				_ = os.WriteFile(p, b, 0600)
			case "missing-index":
				_ = os.Rename(p, filepath.Join(s.auditDir(), auditName(2)))
			case "extra":
				_ = os.WriteFile(filepath.Join(s.auditDir(), "unknown"), []byte("{}"), 0600)
			case "symlink":
				_ = os.Remove(p)
				if err = os.Symlink("elsewhere", p); err != nil {
					t.Skip(err)
				}
			case "changed-authority":
				b, _ := os.ReadFile(p)
				var obj RecoveryAuditRecord
				_ = json.Unmarshal(b, &obj)
				obj.Recovery.SubmitAuthorized = true
				obj.Checksum = obj.digest()
				b, _ = json.Marshal(obj)
				_ = os.WriteFile(p, b, 0600)
			case "recorded-exposure":
				b, _ := os.ReadFile(p)
				var obj RecoveryAuditRecord
				_ = json.Unmarshal(b, &obj)
				obj.ExposureReview.State = ExposurePooled
				obj.Checksum = obj.digest()
				b, _ = json.Marshal(obj)
				_ = os.WriteFile(p, b, 0600)
			}
			if _, err := s.ReadRecoveryAudit(); !errors.Is(err, ErrAuditIntegrity) {
				t.Fatalf("bad audit accepted: %v", err)
			}
			if _, err := s.AppendRecoveryAudit(r, e, time.Now()); !errors.Is(err, ErrAuditIntegrity) {
				t.Fatalf("append after corrupt allowed: %v", err)
			}
		})
	}
}
func TestRecoveryAuditPooledExposurePersistedNonAuthorizing(t *testing.T) {
	s, _, r, e := newAuditFixture(t)
	e.Owners = append(e.Owners, OwnerShare{GroupID: "other", Source: "model", ModelID: "m2", Symbol: "BTCUSDT", Side: "LONG", QuantityBTC: "0.02"})
	e.LongQtyBTC = "0.03"
	saved, err := s.AppendRecoveryAudit(r, e, time.Now())
	if err != nil || saved.ExposureReview.State != ExposurePooled || saved.ExposureReview.TradingAuthorized {
		t.Fatalf("pooled %+v %v", saved, err)
	}
}
func TestRecoveryAuditConcurrentAppendHasSingleSequenceWinner(t *testing.T) {
	s, _, r, e := newAuditFixture(t)
	when := time.Now().UTC()
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.AppendRecoveryAudit(r, e, when); err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 1 {
		t.Fatalf("multiple writers succeeded: %d", success.Load())
	}
	audits, err := s.ReadRecoveryAudit()
	if err != nil || len(audits) != 1 {
		t.Fatalf("journal %+v %v", audits, err)
	}
}
