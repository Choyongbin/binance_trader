package trading

// PC-only, signed-GET read-only inspection of previously sealed entry evidence.
// Never construct a Reservation or account fingerprint from CLI inputs. The
// reservation and wallet binding are loaded and verified from durable files.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

type countedSignedGET struct {
	original WindowDemoReader
	requests int
}

func (c *countedSignedGET) FetchDemoRecoveryBaseline(ctx context.Context, key, secret string, fromMS int64) (exchange.RecoveryBaseline, error) {
	c.requests++
	return c.original.FetchDemoRecoveryBaseline(ctx, key, secret, fromMS)
}

type ProtectionSignedPCReview struct {
	Schema                  int                               `json:"schema"`
	CheckedAt               time.Time                         `json:"checked_at"`
	ClientOrderID           string                            `json:"client_order_id"`
	WalletBindingRevision   uint64                            `json:"wallet_binding_revision,omitempty"`
	WalletBindingTailSHA256 string                            `json:"wallet_binding_tail_sha256,omitempty"`
	SignedGETAttempted      bool                              `json:"signed_get_attempted"`
	Gate                    ProtectionExecutionApprovalReview `json:"gate"`
	ErrorCode               string                            `json:"error_code,omitempty"`
	TradingAuthorized       bool                              `json:"trading_authorized"`
	ManualReviewRequired    bool                              `json:"manual_review_required"`
}

func ReviewPreparedProtectionSignedPC(ctx context.Context, dataDir, clientID string, reader WindowDemoReader, creds DemoCredentials, fromMS int64) (ProtectionSignedPCReview, error) {
	result := ProtectionSignedPCReview{
		Schema: 1, CheckedAt: time.Now().UTC(), ClientOrderID: clientID,
		ManualReviewRequired: true,
		Gate: ProtectionExecutionApprovalReview{Schema: 1, State: ProtectionGateBlocked, ClientOrderID: clientID,
			HistoryCoverage: "BOUNDED_SIGNED_REST_NOT_CONTINUOUS", ManualReviewRequired: true, Reasons: []string{}},
	}
	block := func(code string, e error) (ProtectionSignedPCReview, error) {
		result.ErrorCode = code
		result.Gate.State = ProtectionGateBlocked
		result.Gate.Reasons = append(result.Gate.Reasons, code)
		return result, e
	}
	if ctx == nil || reader == nil || dataDir == "" || !reservationName.MatchString(clientID+".json") || creds.APIKey == "" || creds.Secret == "" {
		return block("INVALID_INPUT", errors.New("invalid signed protection review input"))
	}
	if err := ctx.Err(); err != nil {
		return block("CANCELED", err)
	}
	// No mkdir, no coordinator acquisition, no reservation creation, no POST.
	reservationPath := filepath.Join(dataDir, "trading", "reservations", clientID+".json")
	for _, parent := range []string{dataDir, filepath.Join(dataDir, "trading"), filepath.Join(dataDir, "trading", "reservations")} {
		info, statErr := os.Lstat(parent)
		if statErr != nil {
			return block("LOCAL_RESERVATION_UNAVAILABLE", statErr)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return block("LOCAL_RESERVATION_UNSAFE", errors.New("reservation parent is not a regular directory"))
		}
	}
	info, err := os.Lstat(reservationPath)
	if err != nil {
		return block("LOCAL_RESERVATION_UNAVAILABLE", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return block("LOCAL_RESERVATION_UNSAFE", errors.New("reservation is not a regular file"))
	}
	reservation, err := decodeReservation(reservationPath)
	if err != nil {
		return block("LOCAL_RESERVATION_INVALID", err)
	}
	bound, revision, tail, err := wallet.ReadBoundAccountFingerprint(dataDir)
	if err != nil {
		return block("ACCOUNT_NOT_BOUND", err)
	}
	keyHash := sha256.Sum256([]byte(creds.APIKey))
	if bound != hex.EncodeToString(keyHash[:]) {
		return block("ACCOUNT_BINDING_MISMATCH", errors.New("stored Demo account binding differs from supplied Demo API key"))
	}
	result.WalletBindingRevision, result.WalletBindingTailSHA256 = revision, tail
	counter := &countedSignedGET{original: reader}
	gate, gateErr := InspectProtectionExecutionApprovalGate(ctx, dataDir, reservation, counter, creds, bound, fromMS)
	result.SignedGETAttempted = counter.requests > 0
	result.Gate = gate
	// Even a failed signed read must not suppress the second local binding
	// check: it could have raced an operator wallet change or data corruption.
	after, afterRevision, afterTail, bindingErr := wallet.ReadBoundAccountFingerprint(dataDir)
	if bindingErr != nil || after != bound || afterRevision != revision || afterTail != tail {
		return block("ACCOUNT_BINDING_CHANGED", errors.New("wallet binding changed during signed review"))
	}
	// Also pin the reservation itself, which is not a substitute for the full
	// local execution evidence pin already performed by the core gate.
	afterInfo, statErr := os.Lstat(reservationPath)
	afterReservation, resErr := decodeReservation(reservationPath)
	if statErr != nil || !afterInfo.Mode().IsRegular() || afterInfo.Mode()&os.ModeSymlink != 0 || resErr != nil || afterReservation != reservation {
		return block("LOCAL_RESERVATION_CHANGED", errors.New("sealed entry reservation changed during signed review"))
	}
	if gateErr != nil {
		return block("SIGNED_OR_LOCAL_GATE_BLOCKED", gateErr)
	}
	if gate.State != ProtectionGateBoundedReview || gate.TradingAuthorized || gate.ProtectionSubmitAuthorized || gate.SiblingCancelAuthorized || gate.FinanciallySettled || gate.ContinuousCoverageProven || !gate.ManualReviewRequired {
		return block("UNEXPECTED_APPROVAL_STATE", errors.New("unsafe or unexpected gate state"))
	}
	return result, nil
}
