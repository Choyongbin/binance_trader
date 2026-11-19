package trading

// One-time OFFLINE signature-observation receipt, not a real permission token.
// O_EXCL is the same-root replay fence. A writable filesystem and a different
// root cannot establish globally immutable history; the receipt is not a
// substitute for independent WORM audit storage or a real financial COMMIT.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// A report of a verified signed REVIEW ACK, not an approval to post.
type ExternalIndependentReviewReceipt struct {
	Schema                        int      `json:"schema"`
	Stage                         string   `json:"stage"`
	RequestSHA256                 string   `json:"request_sha256"`
	BatchKey                      string   `json:"batch_key"`
	GlobalConsistencySHA256       string   `json:"global_consistency_sha256"`
	NonceHex                      string   `json:"nonce_hex"`
	ReviewerPublicKeySHA256       string   `json:"reviewer_public_key_sha256"`
	SignedAtUnix                  int64    `json:"signed_at_unix"`
	RecordedAtUnix                int64    `json:"recorded_at_unix"`
	ReviewerSignatureVerified     bool     `json:"reviewer_signature_verified"`
	IndependentAccountAttested    bool     `json:"independent_account_attested"`
	IndependentSettlementApproval bool     `json:"independent_settlement_approval"`
	PostingApplied                bool     `json:"posting_applied"`
	SettlementAuthorized          bool     `json:"settlement_authorized"`
	TradingAuthorized             bool     `json:"trading_authorized"`
	BlockerCodes                  []string `json:"blocker_codes"`
	DigestSHA256                  string   `json:"digest_sha256"`
}

func (r ExternalIndependentReviewReceipt) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

// Dedicated pre-existing directory, separate from the registry and originals.
type ExternalIndependentReviewReceiptStore struct{ dir, root string }

func NewExternalIndependentReviewReceiptStore(root string) (*ExternalIndependentReviewReceiptStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("explicit separate review receipt root required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe approval receipt root")
	}
	canon, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	return &ExternalIndependentReviewReceiptStore{root: canon, dir: filepath.Join(canon, "offline_independent_review_receipts")}, nil
}

// Validate external sources are not inside our writable receipt directory or
// parent root. Use canonical paths, not user-supplied relative path strings.
func (s *ExternalIndependentReviewReceiptStore) checkSeparation(registry *ExternalGlobalEvidenceReservationStore, events []ExternalFinancialEventSource) error {
	if s == nil || registry == nil {
		return errors.New("receipt and global registry required")
	}
	for _, x := range append([]string{registry.root}, func() []string {
		out := make([]string, 0, len(events))
		for _, e := range events {
			out = append(out, e.DataDir)
		}
		return out
	}()...) {
		p, err := filepath.Abs(x)
		if err != nil {
			return err
		}
		p, err = filepath.EvalSymlinks(p)
		if err != nil {
			return err
		}
		// Disallow ancestor, descendant, or identical roots. Otherwise a local
		// tampering incident could erase the nonce fence along with evidence.
		rel1, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		rel2, err := filepath.Rel(p, s.root)
		if err != nil {
			return err
		}
		contained := func(rel string) bool {
			return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
		}
		if contained(rel1) || contained(rel2) {
			return errors.New("review receipt root overlaps input evidence or registry")
		}
	}
	return nil
}

func (s *ExternalIndependentReviewReceiptStore) checkDir() error {
	if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	st, err := os.Lstat(s.dir)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe receipt directory")
	}
	return nil
}

// RecordVerifiedExternalIndependentReview only consumes an offline one-time
// nonce after full signed verification. It always returns unauthorized.
// A duplicate nonce MUST fail even if all other fields are identical.
func (s *ExternalIndependentReviewReceiptStore) RecordVerifiedExternalIndependentReview(
	ctx context.Context, registry *ExternalGlobalEvidenceReservationStore,
	events []ExternalFinancialEventSource, request ExternalIndependentApprovalRequest,
	attestation ExternalIndependentReviewAttestation,
	key ed25519.PublicKey, pinnedKeySHA256 string, nowUnix int64,
) (ExternalIndependentReviewReceipt, error) {
	var zero ExternalIndependentReviewReceipt
	if s == nil || ctx == nil {
		return zero, errors.New("review receipt store and context required")
	}
	if err := s.checkSeparation(registry, events); err != nil {
		return zero, err
	}
	snapshot, err := VerifyExternalIndependentReview(ctx, registry, events, request, attestation, key, pinnedKeySHA256, nowUnix)
	if err != nil {
		return zero, err
	}
	// Re-evaluate the batch before recording so no changed candidate is
	// recorded between signature verification and the write barrier.
	fresh, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil || fresh.DigestSHA256 != snapshot.DigestSHA256 {
		return zero, errors.New("simulated batch changed before review receipt")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := s.checkDir(); err != nil {
		return zero, err
	}
	receipt := ExternalIndependentReviewReceipt{
		Schema: 1, Stage: ExternalApprovalReviewRecorded, RequestSHA256: request.DigestSHA256,
		BatchKey: request.BatchKey, GlobalConsistencySHA256: request.GlobalConsistencySHA256,
		NonceHex: request.NonceHex, ReviewerPublicKeySHA256: pinnedKeySHA256,
		SignedAtUnix: attestation.SignedAtUnix, RecordedAtUnix: nowUnix,
		ReviewerSignatureVerified: true,
		BlockerCodes:              []string{"SIGNED_ACK_ONLY_NOT_FINANCIAL_APPROVAL", "ACCOUNT_LIFETIME_AND_ALL_SYMBOLS_NOT_ATTESTED", "MODEL_FUNDING_AND_OWNERSHIP_UNPROVEN", "ATOMIC_FINANCIAL_POSTING_NOT_IMPLEMENTED", "HTTP_423_ORDER_LOCK_REQUIRED"},
	}
	receipt.DigestSHA256 = receipt.digest()
	b, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return zero, err
	}
	// The nonce is fixed lowercase hex and never interpreted as a path.
	path := filepath.Join(s.dir, "nonce-"+request.NonceHex+".json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return zero, errors.New("REPLAY_BLOCKED: independent review nonce already consumed")
	}
	if err != nil {
		return zero, err
	}
	n, writeErr := f.Write(append(b, '\n'))
	if writeErr == nil && n != len(b)+1 {
		writeErr = errors.New("short receipt write")
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return zero, fmt.Errorf("receipt may be torn and nonce is blocked: write=%v close=%v", writeErr, closeErr)
	}
	// Do not erase the receipt even if the source changed afterwards; fail
	// closed and require a NEW signed nonce for a separately verified attempt.
	after, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil || after.DigestSHA256 != snapshot.DigestSHA256 {
		return zero, errors.New("batch changed after consuming offline reviewer nonce; manual audit required")
	}
	return receipt, nil
}
