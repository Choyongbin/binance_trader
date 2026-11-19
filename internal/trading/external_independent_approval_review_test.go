package trading

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type independentReviewFixture struct {
	registry    *ExternalGlobalEvidenceReservationStore
	receipts    *ExternalIndependentReviewReceiptStore
	events      []ExternalFinancialEventSource
	request     ExternalIndependentApprovalRequest
	attestation ExternalIndependentReviewAttestation
	key         ed25519.PublicKey
	pin         string
	now         int64
}

func independentApprovalFixture(t *testing.T) independentReviewFixture {
	t.Helper()
	ctx := context.Background()
	a := crossFinancialFixture(t, Long, "approval-a", 0, 70*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "approval-b", 1000, 70*time.Minute, 0)
	events := []ExternalFinancialEventSource{a, b}
	registry, err := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.ReserveBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		prepareGlobalAtomicFixture(t, e, true)
	}
	state, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	now := int64(1800000000)
	request, err := PrepareExternalIndependentApprovalRequest(state, strings.Repeat("a", 64), now-100, now+100)
	if err != nil {
		t.Fatal(err)
	}
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ph := sha256.Sum256(pub)
	pin := hex.EncodeToString(ph[:])
	att := ExternalIndependentReviewAttestation{
		Schema: 1, Purpose: ExternalApprovalAttestationPurpose, ReviewDecision: ExternalApprovalReviewDecision,
		RequestSHA256: request.DigestSHA256, ReviewerPublicKeySHA256: pin,
		NonceHex: request.NonceHex, SignedAtUnix: now - 5,
	}
	bSig, err := att.SigningBytes()
	if err != nil {
		t.Fatal(err)
	}
	att.SignatureHex = hex.EncodeToString(ed25519.Sign(private, bSig))
	receipts, err := NewExternalIndependentReviewReceiptStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return independentReviewFixture{registry, receipts, events, request, att, pub, pin, now}
}

func TestIndependentReviewerSignedOfflineReceiptNeverPostsAndRejectsReplay(t *testing.T) {
	x := independentApprovalFixture(t)
	ctx := context.Background()
	before := make([]string, len(x.events))
	for i, e := range x.events {
		var err error
		before[i], err = snapshotExecutionEvidence(e.DataDir)
		if err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := x.receipts.RecordVerifiedExternalIndependentReview(ctx, x.registry, x.events, x.request, x.attestation, x.key, x.pin, x.now)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Stage != ExternalApprovalReviewRecorded || !receipt.ReviewerSignatureVerified || receipt.IndependentAccountAttested || receipt.IndependentSettlementApproval || receipt.PostingApplied || receipt.SettlementAuthorized || receipt.TradingAuthorized || receipt.DigestSHA256 != receipt.digest() {
		t.Fatalf("unsafe receipt %+v", receipt)
	}
	copyStore, err := NewExternalIndependentReviewReceiptStore(x.receipts.root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = copyStore.RecordVerifiedExternalIndependentReview(ctx, x.registry, x.events, x.request, x.attestation, x.key, x.pin, x.now); err == nil || !strings.Contains(err.Error(), "REPLAY_BLOCKED") {
		t.Fatalf("restart replay not blocked: %v", err)
	}
	for i, e := range x.events {
		after, err := snapshotExecutionEvidence(e.DataDir)
		if err != nil || before[i] != after {
			t.Fatalf("original changed: %v", err)
		}
	}
	receiptBytes, err := os.ReadFile(filepath.Join(x.receipts.dir, "nonce-"+x.request.NonceHex+".json"))
	if err != nil || !strings.Contains(string(receiptBytes), ExternalApprovalReviewRecorded) {
		t.Fatalf("missing receipt %v", err)
	}
}

func TestIndependentReviewerForgeriesAndStaleRequestsRejected(t *testing.T) {
	x := independentApprovalFixture(t)
	ctx := context.Background()
	cases := []struct {
		name   string
		mutate func(*ExternalIndependentApprovalRequest, *ExternalIndependentReviewAttestation, *string, *int64)
	}{
		{"rehashed_batch", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			r.BatchKey = strings.Repeat("b", 64)
			r.DigestSHA256 = r.digest()
		}},
		{"rehashed_candidate", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			r.Candidates[0].AtomicCommitSHA256 = strings.Repeat("b", 64)
			r.DigestSHA256 = r.digest()
		}},
		{"rehashed_authority", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			r.SettlementAuthorized = true
			r.DigestSHA256 = r.digest()
		}},
		{"bad_signature", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			a.SignatureHex = strings.Repeat("a", 128)
		}},
		{"wrong_pin", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			*p = strings.Repeat("1", 64)
		}},
		{"claim_approval", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			a.ReviewDecision = "APPROVE_SETTLEMENT"
		}},
		{"reissued_nonce", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			r.NonceHex = strings.Repeat("c", 64)
			r.DigestSHA256 = r.digest()
		}},
		{"expired", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			*n = r.ExpiresAtUnix
		}},
		{"not_yet_issued", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			*n = r.IssuedAtUnix - 1
		}},
		{"future_signed", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			a.SignedAtUnix = *n + 1
		}},
		{"wrong_subject", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			a.RequestSHA256 = strings.Repeat("b", 64)
		}},
		{"wrong_key_claim", func(r *ExternalIndependentApprovalRequest, a *ExternalIndependentReviewAttestation, p *string, n *int64) {
			a.ReviewerPublicKeySHA256 = strings.Repeat("b", 64)
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := x.request
			r.Candidates = append([]ExternalApprovalCandidateBinding(nil), r.Candidates...)
			a := x.attestation
			p := x.pin
			n := x.now
			tt.mutate(&r, &a, &p, &n)
			if _, err := x.receipts.RecordVerifiedExternalIndependentReview(ctx, x.registry, x.events, r, a, x.key, p, n); err == nil {
				t.Fatal("forged/stale attestation recorded")
			}
		})
	}
	if _, err := os.Stat(x.receipts.dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed verifications should not create nonce receipts: %v", err)
	}
	// The constructor must never let receipts be kept alongside original evidence.
	for _, root := range []string{x.events[0].DataDir, x.registry.root} {
		bad, err := NewExternalIndependentReviewReceiptStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = bad.RecordVerifiedExternalIndependentReview(ctx, x.registry, x.events, x.request, x.attestation, x.key, x.pin, x.now); err == nil {
			t.Fatal("overlapping receipt store accepted")
		}
	}
}

func TestIndependentReviewerConcurrentNonceRace(t *testing.T) {
	x := independentApprovalFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := x.receipts.RecordVerifiedExternalIndependentReview(ctx, x.registry, x.events, x.request, x.attestation, x.key, x.pin, x.now)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	passed := 0
	blocked := 0
	for err := range results {
		if err == nil {
			passed++
		} else {
			blocked++
		}
	}
	if passed != 1 || blocked != 5 {
		t.Fatalf("duplicate nonce: %d successes %d blocked", passed, blocked)
	}
}

func TestIndependentReviewerRejectsIncompleteSeals(t *testing.T) {
	ctx := context.Background()
	a := crossFinancialFixture(t, Long, "no-seal-a", 0, 70*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "no-seal-b", 1000, 70*time.Minute, 0)
	events := []ExternalFinancialEventSource{a, b}
	registry, err := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.ReserveBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	prepareGlobalAtomicFixture(t, a, true)
	prepareGlobalAtomicFixture(t, b, false)
	partial, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PrepareExternalIndependentApprovalRequest(partial, strings.Repeat("f", 64), 100, 200); err == nil {
		t.Fatal("partial sealed review allowed")
	}
}

func TestIndependentReviewerValidRequestDeterministic(t *testing.T) {
	x := independentApprovalFixture(t)
	state, err := InspectExternalGlobalAtomicConsistency(context.Background(), x.registry, []ExternalFinancialEventSource{x.events[1], x.events[0]})
	if err != nil {
		t.Fatal(err)
	}
	r, err := PrepareExternalIndependentApprovalRequest(state, x.request.NonceHex, x.request.IssuedAtUnix, x.request.ExpiresAtUnix)
	if err != nil || !reflect.DeepEqual(r, x.request) {
		t.Fatalf("unstable signed subject %v", err)
	}
	if _, err = VerifyExternalIndependentReview(context.Background(), x.registry, x.events, x.request, x.attestation, x.key, x.pin, x.now); err != nil {
		t.Fatal(err)
	}
}
