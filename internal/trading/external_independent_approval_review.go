package trading

// Offline independent reviewer signature contract, dev.3.130.
// IMPORTANT: a valid detached signature proves only possession of the pinned
// key over a specific NON-POSTED simulated batch. It is NOT an exchange account
// attestation, independent financial settlement approval, or permission to
// post a ledger. Do not import this from production trading execution paths.

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

const (
	ExternalApprovalRequestPurpose     = "BINANCE_TRADER_OFFLINE_SETTLEMENT_CANDIDATE_REVIEW_V1"
	ExternalApprovalAttestationPurpose = "BINANCE_TRADER_INDEPENDENT_REVIEW_ACKNOWLEDGMENT_V1"
	ExternalApprovalReviewDecision     = "ACKNOWLEDGE_SIMULATION_ONLY_NOT_SETTLEMENT"
	ExternalApprovalReviewRecorded     = "SIGNED_OFFLINE_REVIEW_RECORDED_NOT_SETTLED"
	externalApprovalSigningDomain      = "BINANCE_TRADER_EXTERNAL_REVIEW_ACK_V1\x00"
)

type ExternalApprovalCandidateBinding struct {
	ClientOrderID            string       `json:"client_order_id"`
	ModelID                  string       `json:"model_id"`
	PositionSide             PositionSide `json:"position_side"`
	AtomicCommitSHA256       string       `json:"atomic_commit_sha256"`
	FinancialPreflightSHA256 string       `json:"financial_preflight_sha256"`
}

// Request: unsigned immutable view of exactly one *simulated* sealed batch.
// The caller's digest is not proof: verification reopens the original sources.
// Nonce is 32 random bytes encoded as lowercase hex; expiration <= 24 hours.
type ExternalIndependentApprovalRequest struct {
	Schema                  int                                `json:"schema"`
	Purpose                 string                             `json:"purpose"`
	BatchKey                string                             `json:"batch_key"`
	GlobalConsistencySHA256 string                             `json:"global_consistency_sha256"`
	RegistryChecksum        string                             `json:"registry_checksum"`
	ReservationSHA256       string                             `json:"reservation_sha256"`
	BoundedAuditSHA256      string                             `json:"bounded_audit_sha256"`
	Candidates              []ExternalApprovalCandidateBinding `json:"candidates"`
	NonceHex                string                             `json:"nonce_hex"`
	IssuedAtUnix            int64                              `json:"issued_at_unix"`
	ExpiresAtUnix           int64                              `json:"expires_at_unix"`
	PostingRequested        bool                               `json:"posting_requested"`
	SettlementAuthorized    bool                               `json:"settlement_authorized"`
	TradingAuthorized       bool                               `json:"trading_authorized"`
	DigestSHA256            string                             `json:"digest_sha256"`
}

func (r ExternalIndependentApprovalRequest) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

func PrepareExternalIndependentApprovalRequest(
	state ExternalGlobalAtomicConsistency, nonceHex string, issued, expires int64,
) (ExternalIndependentApprovalRequest, error) {
	var zero ExternalIndependentApprovalRequest
	if state.Schema != 1 || state.Stage != ExternalGlobalAtomicConsistent ||
		!state.AllSimulatedPairsVisible || len(state.Candidates) < 2 || len(state.Candidates) > 64 ||
		state.DigestSHA256 != state.digest() || state.SharedAccountProven || state.CompleteHistoryProven ||
		state.FundingAllocated || state.PostingApplied || state.SettlementAuthorized || state.TradingAuthorized ||
		!externalReviewHash.MatchString(state.BatchKey) || !externalReviewHash.MatchString(state.RegistryChecksum) ||
		!externalReviewHash.MatchString(state.ReservationSHA256) || !externalReviewHash.MatchString(state.BoundedAuditSHA256) {
		return zero, errors.New("cannot request review of incomplete, unsealed, or authorizing batch")
	}
	if !externalReviewHash.MatchString(nonceHex) || issued <= 0 || expires <= issued || expires-issued > 86400 {
		return zero, errors.New("invalid 32-byte nonce or review interval (max 24h)")
	}
	result := ExternalIndependentApprovalRequest{
		Schema: 1, Purpose: ExternalApprovalRequestPurpose,
		BatchKey: state.BatchKey, GlobalConsistencySHA256: state.DigestSHA256,
		RegistryChecksum: state.RegistryChecksum, ReservationSHA256: state.ReservationSHA256,
		BoundedAuditSHA256: state.BoundedAuditSHA256,
		NonceHex:           nonceHex, IssuedAtUnix: issued, ExpiresAtUnix: expires,
		Candidates: make([]ExternalApprovalCandidateBinding, 0, len(state.Candidates)),
	}
	prevID := ""
	for _, c := range state.Candidates {
		if !c.SimulatedPairVisible || c.PostingApplied || c.SettlementAuthorized || c.TradingAuthorized ||
			c.ClientOrderID <= prevID || c.ClientOrderID == "" || c.ModelID == "" ||
			(c.PositionSide != Long && c.PositionSide != Short) ||
			!externalReviewHash.MatchString(c.AtomicCommitSHA256) ||
			!externalReviewHash.MatchString(c.FinancialPreflightSHA256) ||
			c.ReservationSHA256 != state.ReservationSHA256 {
			return zero, errors.New("inconsistent or unordered simulated approval candidate")
		}
		result.Candidates = append(result.Candidates, ExternalApprovalCandidateBinding{
			ClientOrderID: c.ClientOrderID, ModelID: c.ModelID, PositionSide: c.PositionSide,
			AtomicCommitSHA256:       c.AtomicCommitSHA256,
			FinancialPreflightSHA256: c.FinancialPreflightSHA256,
		})
		prevID = c.ClientOrderID
	}
	result.DigestSHA256 = result.digest()
	return result, nil
}

// The detached attestation has no authority flags by design. The signer must
// possess the private key corresponding to a public key pinned OUTSIDE both
// request and attestation, through a separately trusted process.
type ExternalIndependentReviewAttestation struct {
	Schema                  int    `json:"schema"`
	Purpose                 string `json:"purpose"`
	ReviewDecision          string `json:"review_decision"`
	RequestSHA256           string `json:"request_sha256"`
	ReviewerPublicKeySHA256 string `json:"reviewer_public_key_sha256"`
	NonceHex                string `json:"nonce_hex"`
	SignedAtUnix            int64  `json:"signed_at_unix"`
	SignatureHex            string `json:"signature_hex"`
}

// SigningBytes exposes the canonical domain-separated bytes to an OUTSIDE
// reviewer signing implementation; it never signs or approves a settlement.
func (a ExternalIndependentReviewAttestation) SigningBytes() ([]byte, error) {
	a.SignatureHex = ""
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return append([]byte(externalApprovalSigningDomain), b...), nil
}

// VerifyExternalIndependentReview checks cryptographic evidence and the exact
// current candidate snapshot. This cannot prove reviewer independence merely
// because the caller supplied a matching key+pin, and never authorizes payment.
func VerifyExternalIndependentReview(
	ctx context.Context, registry *ExternalGlobalEvidenceReservationStore,
	events []ExternalFinancialEventSource,
	request ExternalIndependentApprovalRequest,
	attestation ExternalIndependentReviewAttestation,
	externallyPinnedPublicKey ed25519.PublicKey, externallyPinnedSHA256 string,
	nowUnix int64,
) (ExternalGlobalAtomicConsistency, error) {
	var zero ExternalGlobalAtomicConsistency
	if ctx == nil || len(externallyPinnedPublicKey) != ed25519.PublicKeySize ||
		!externalReviewHash.MatchString(externallyPinnedSHA256) ||
		request.IssuedAtUnix <= 0 || nowUnix < request.IssuedAtUnix || nowUnix >= request.ExpiresAtUnix ||
		request.ExpiresAtUnix-request.IssuedAtUnix > 86400 {
		return zero, errors.New("missing externally pinned reviewer key or stale review window")
	}
	pubSum := sha256.Sum256(externallyPinnedPublicKey)
	if hex.EncodeToString(pubSum[:]) != externallyPinnedSHA256 {
		return zero, errors.New("review public key does not match externally pinned fingerprint")
	}
	actual, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		return zero, fmt.Errorf("independent review requires fresh atomic evidence: %w", err)
	}
	expected, err := PrepareExternalIndependentApprovalRequest(actual, request.NonceHex, request.IssuedAtUnix, request.ExpiresAtUnix)
	if err != nil || !reflect.DeepEqual(request, expected) || request.DigestSHA256 != request.digest() {
		return zero, errors.New("request differs from freshly validated exact batch, or attempts authority escalation")
	}
	if attestation.Schema != 1 || attestation.Purpose != ExternalApprovalAttestationPurpose ||
		attestation.ReviewDecision != ExternalApprovalReviewDecision ||
		attestation.RequestSHA256 != request.DigestSHA256 ||
		attestation.ReviewerPublicKeySHA256 != externallyPinnedSHA256 ||
		attestation.NonceHex != request.NonceHex ||
		attestation.SignedAtUnix < request.IssuedAtUnix ||
		attestation.SignedAtUnix > nowUnix || attestation.SignedAtUnix >= request.ExpiresAtUnix ||
		len(attestation.SignatureHex) != ed25519.SignatureSize*2 {
		return zero, errors.New("review signature metadata, decision, or expiration mismatch")
	}
	sig, err := hex.DecodeString(attestation.SignatureHex)
	if err != nil || hex.EncodeToString(sig) != attestation.SignatureHex {
		return zero, errors.New("invalid canonical lowercase signature")
	}
	payload, err := attestation.SigningBytes()
	if err != nil {
		return zero, err
	}
	if !ed25519.Verify(externallyPinnedPublicKey, payload, sig) {
		return zero, errors.New("invalid detached independent reviewer signature")
	}
	return actual, nil
}
