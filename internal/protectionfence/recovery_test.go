package protectionfence

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

type scriptedSignedReader struct {
	observations []SignedAlgoObservation
	count        int
	failAt       int
	after        func(int)
}

func (r *scriptedSignedReader) ReadSignedAlgo(ctx context.Context, _ Intent) (SignedAlgoObservation, error) {
	r.count++
	if r.after != nil {
		r.after(r.count)
	}
	if err := ctx.Err(); err != nil {
		return SignedAlgoObservation{}, err
	}
	if r.failAt == r.count {
		return SignedAlgoObservation{}, errors.New("signed GET unavailable")
	}
	if r.count > len(r.observations) {
		return SignedAlgoObservation{}, errors.New("too many requests")
	}
	return r.observations[r.count-1], nil
}
func recoveryFixture(t *testing.T, op Operation) (*Store, Intent, SignedAlgoObservation) {
	t.Helper()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	in := testIntent(op)
	if op == CancelSibling {
		in.TargetAlgoID = 5001
	}
	if _, err := store.SealBeforePossibleSend(in, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	found := SignedAlgoObservation{ObservedAt: time.Now().UTC(), AccountFingerprintSHA256: in.AccountFingerprintSHA256, Found: true, ClientAlgoID: in.TargetClientAlgoID, AlgoID: 5001, Symbol: in.Symbol, PositionSide: in.PositionSide, QuantityBTC: in.QuantityBTC, Status: "WORKING"}
	return store, in, found
}
func assertOnlyReview(t *testing.T, o RecoveryReview) {
	t.Helper()
	if o.TradingAuthorized || o.SubmitAuthorized || o.CancelAuthorized || o.RetryAuthorized || o.SettlementAuthorized || !o.ManualReviewRequired {
		t.Fatalf("unsafe authority: %+v", o)
	}
}
func TestSignedRecoverySubmitObservesOneLegButNeverReplays(t *testing.T) {
	s, in, o := recoveryFixture(t, SubmitTP)
	reader := &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}}
	got, err := s.InspectRecoveryOnce(context.Background(), reader)
	if err != nil || got.State != RecoverySubmitObserved || reader.count != 2 || got.GroupID != in.GroupID {
		t.Fatalf("got=%+v err=%v reads=%d", got, err, reader.count)
	}
	assertOnlyReview(t, got)
	copyStore := &Store{dir: s.dir}
	if _, err := copyStore.Inspect(in); err != nil {
		t.Fatal(err)
	}
	if _, err := copyStore.SealBeforePossibleSend(in, time.Now()); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("retry permitted: %v", err)
	}
}
func TestSignedRecoveryCancelObservedButNeverSettles(t *testing.T) {
	s, _, o := recoveryFixture(t, CancelSibling)
	o.Status = "CANCELED"
	reader := &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}}
	got, err := s.InspectRecoveryOnce(context.Background(), reader)
	if err != nil || got.State != RecoveryCancelObserved {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	assertOnlyReview(t, got)
	existing, err := s.InspectAll()
	if err != nil || len(existing) != 1 {
		t.Fatalf("fence lost: %+v %v", existing, err)
	}
}
func TestSignedRecoveryNoRecordIsNotNoSendProof(t *testing.T) {
	for _, op := range []Operation{SubmitSL, CancelSibling} {
		t.Run(string(op), func(t *testing.T) {
			s, in, _ := recoveryFixture(t, op)
			missing := SignedAlgoObservation{ObservedAt: time.Now().UTC(), AccountFingerprintSHA256: in.AccountFingerprintSHA256}
			reader := &scriptedSignedReader{observations: []SignedAlgoObservation{missing, missing}}
			got, err := s.InspectRecoveryOnce(context.Background(), reader)
			if err != nil || got.State != RecoveryAbsentUnproven {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			assertOnlyReview(t, got)
		})
	}
}
func TestSignedRecoveryTerminalRequiresIndependentFillAudit(t *testing.T) {
	for _, status := range []string{"TRIGGERING", "TRIGGERED", "FINISHED", "REJECTED", "EXPIRED", "CANCELED"} {
		t.Run(status, func(t *testing.T) {
			s, _, o := recoveryFixture(t, SubmitTP)
			o.Status = status
			got, err := s.InspectRecoveryOnce(context.Background(), &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}})
			if err != nil || got.State != RecoveryTerminalNeedsAudit {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			assertOnlyReview(t, got)
		})
	}
}
func TestSignedRecoveryBlocksDriftIdentityAndStaleEvidence(t *testing.T) {
	cases := []struct {
		name       string
		change     func(*SignedAlgoObservation)
		secondOnly bool
	}{
		{"wrong account", func(o *SignedAlgoObservation) { o.AccountFingerprintSHA256 = strings.Repeat("d", 64) }, false},
		{"wrong clientAlgoId", func(o *SignedAlgoObservation) { o.ClientAlgoID = "foreign" }, false},
		{"wrong exchange Algo id", func(o *SignedAlgoObservation) { o.AlgoID = 6001 }, true},
		{"bad qty", func(o *SignedAlgoObservation) { o.QuantityBTC = "0.02000000" }, false},
		{"future observation", func(o *SignedAlgoObservation) { o.ObservedAt = time.Now().Add(4 * time.Second) }, false},
		{"stale observation", func(o *SignedAlgoObservation) { o.ObservedAt = time.Now().Add(-10 * time.Second) }, false},
		{"status drift", func(o *SignedAlgoObservation) { o.Status = "FINISHED" }, true},
		{"unknown status", func(o *SignedAlgoObservation) { o.Status = "NEW_FUTURE_STATUS" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _, o := recoveryFixture(t, SubmitTP)
			first, second := o, o
			if tc.secondOnly {
				tc.change(&second)
			} else {
				tc.change(&first)
				tc.change(&second)
			}
			got, err := s.InspectRecoveryOnce(context.Background(), &scriptedSignedReader{observations: []SignedAlgoObservation{first, second}})
			if err == nil || got.State != RecoveryBlocked {
				t.Fatalf("should block: %+v %v", got, err)
			}
			assertOnlyReview(t, got)
		})
	}
	t.Run("cancel wrong algo ID", func(t *testing.T) {
		s, _, o := recoveryFixture(t, CancelSibling)
		o.AlgoID = 6001
		got, err := s.InspectRecoveryOnce(context.Background(), &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}})
		if err == nil || got.State != RecoveryBlocked {
			t.Fatalf("unsafe cancel: %+v %v", got, err)
		}
	})
}
func TestSignedRecoveryRejectsChangedOrCorruptFenceDuringQuery(t *testing.T) {
	s, in, o := recoveryFixture(t, SubmitSL)
	reader := &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}}
	reader.after = func(n int) {
		if n == 1 {
			_ = os.WriteFile(s.name(in), []byte("{"), 0600)
		}
	}
	got, err := s.InspectRecoveryOnce(context.Background(), reader)
	if err == nil || got.State != RecoveryBlocked {
		t.Fatalf("corrupt fence got=%+v err=%v", got, err)
	}
	assertOnlyReview(t, got)
}
func TestSignedRecoveryHandlesFailuresAndCancellation(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			s, _, o := recoveryFixture(t, SubmitTP)
			reader := &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}, failAt: failAt}
			got, err := s.InspectRecoveryOnce(context.Background(), reader)
			if err == nil || got.State != RecoveryBlocked {
				t.Fatalf("failed signed query accepted: %+v %v", got, err)
			}
			assertOnlyReview(t, got)
		})
	}
	s, _, o := recoveryFixture(t, SubmitTP)
	ctx, cancel := context.WithCancel(context.Background())
	reader := &scriptedSignedReader{observations: []SignedAlgoObservation{o, o}, after: func(n int) {
		if n == 1 {
			cancel()
		}
	}}
	got, err := s.InspectRecoveryOnce(ctx, reader)
	if !errors.Is(err, context.Canceled) || got.State != RecoveryBlocked || reader.count != 1 {
		t.Fatalf("cancellation: %+v %v count=%d", got, err, reader.count)
	}
	assertOnlyReview(t, got)
}
func TestSignedRecoveryDoesNotApproveEmptyOrOrphanedLedger(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.InspectRecoveryOnce(context.Background(), &scriptedSignedReader{})
	if err == nil || got.State != RecoveryBlocked {
		t.Fatalf("empty ledger: %+v %v", got, err)
	}
	assertOnlyReview(t, got)
}
