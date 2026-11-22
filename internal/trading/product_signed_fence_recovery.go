package trading

// Product read-only adapter connecting the durable v11/v12 mutation fence to
// Binance Demo's authenticated GET /fapi/v1/algoOrder?clientAlgoId=... .
// Not exposed by HTTP, AutoExecutionRuntime, or the order dispatcher.
// The existing product Arm/CancelTriggeredSibling/HTTP 423 remain locked.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/protectionfence"
)

type productFenceSignedAlgoReader struct {
	client             *exchange.Client
	creds              DemoCredentials
	accountFingerprint string
	expected           ProtectionInstruction
}

func (r productFenceSignedAlgoReader) ReadSignedAlgo(ctx context.Context, intent protectionfence.Intent) (protectionfence.SignedAlgoObservation, error) {
	missing := protectionfence.SignedAlgoObservation{}
	if ctx == nil || r.client == nil || intent.Validate() != nil ||
		r.accountFingerprint != intent.AccountFingerprintSHA256 ||
		r.expected.ClientAlgoID != intent.TargetClientAlgoID ||
		r.expected.Symbol != intent.Symbol ||
		string(r.expected.PositionSide) != intent.PositionSide ||
		!sameSignedDecimal(r.expected.Quantity, intent.QuantityBTC, true) {
		return missing, errors.New("signed protection reader identity mismatch")
	}
	result, err := r.client.QueryDemoAlgoByClientID(ctx, r.creds.APIKey, r.creds.Secret, intent.TargetClientAlgoID)
	observedAt := time.Now().UTC()
	if errors.Is(err, exchange.ErrOrderNotFound) {
		// -2013 is *not* proof of failed POST/DELETE. The v12 recovery
		// classifier will return NOT_FOUND_NOT_PROOF_NO_REPLAY and keep the
		// immutable fence forever, pending independent manual reconciliation.
		return protectionfence.SignedAlgoObservation{
			ObservedAt: observedAt, AccountFingerprintSHA256: r.accountFingerprint,
			Found: false,
		}, nil
	}
	if err != nil {
		return missing, err
	}
	if result.ClientAlgoID != intent.TargetClientAlgoID || result.Symbol != r.expected.Symbol ||
		result.Side != r.expected.Side || result.PositionSide != string(r.expected.PositionSide) ||
		result.AlgoType != r.expected.AlgoType || result.Type != r.expected.OrderType ||
		result.WorkingType != r.expected.WorkingType || result.ClosePosition || result.ReduceOnly ||
		!sameSignedDecimal(string(result.Quantity), r.expected.Quantity, true) ||
		!sameSignedDecimal(string(result.TriggerPrice), r.expected.TriggerPrice, true) ||
		(intent.Operation == protectionfence.CancelSibling && result.AlgoID != intent.TargetAlgoID) {
		return missing, errors.New("signed Algo does not match immutable TP/SL instruction or cancel target")
	}
	return protectionfence.SignedAlgoObservation{
		ObservedAt: observedAt, AccountFingerprintSHA256: r.accountFingerprint,
		Found: true, ClientAlgoID: result.ClientAlgoID, AlgoID: result.AlgoID,
		Symbol: result.Symbol, PositionSide: result.PositionSide,
		QuantityBTC: string(result.Quantity), Status: result.Status,
	}, nil
}

// InspectProductMutationFenceSignedOnce is the first actual product read-only
// attachment for v12's signed recovery observer. A coordinator lease is
// mandatory; the immutable local protection plan and API-key fingerprint are
// reverified before any network request. NO trading/cancel/retry/settlement
// authorization or write to the fence follows from its result.
func (r *DemoProtectionRuntimeCoordinator) InspectProductMutationFenceSignedOnce(ctx context.Context) (protectionfence.RecoveryReview, error) {
	blocked := protectionfence.RecoveryReview{State: protectionfence.RecoveryBlocked,
		Reasons: []string{"product signed mutation recovery not verified"}, ManualReviewRequired: true}
	if r == nil || ctx == nil || r.coordinator == nil || r.client == nil ||
		r.credentials == nil || r.submissions == nil || r.dataDir == "" {
		return blocked, errors.New("product signed mutation recovery dependencies missing")
	}
	if err := ctx.Err(); err != nil {
		return blocked, err
	}
	if err := r.coordinator.ownsLock(); err != nil {
		return blocked, err
	}
	// Do not create an empty fence directory during what is advertised as
	// a read-only investigation. Missing state is not proof of no submission.
	fenceDir := filepath.Join(r.dataDir, "trading", "product_mutation_fences")
	st, err := os.Lstat(fenceDir)
	if err != nil {
		return blocked, fmt.Errorf("unavailable mutation fence inventory: %w", err)
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return blocked, errors.New("mutation fence inventory is not a real directory")
	}
	store, err := protectionfence.Open(r.dataDir)
	if err != nil {
		return blocked, err
	}
	fences, err := store.InspectAll()
	if err != nil || len(fences) != 1 {
		return blocked, fmt.Errorf("expected one intact mutation fence, count %d: %w", len(fences), errOrUnverified(err))
	}
	intent := fences[0].Intent
	plan, err := r.submissions.read(intent.GroupID)
	if err != nil || plan.Digest != intent.PlanSHA256 ||
		plan.TP.Quantity != intent.QuantityBTC || plan.SL.Quantity != intent.QuantityBTC ||
		string(plan.PositionSide) != intent.PositionSide || plan.TP.Symbol != intent.Symbol {
		return blocked, fmt.Errorf("durable protection plan/fence mismatch: %w", errOrUnverified(err))
	}
	var expected ProtectionInstruction
	switch intent.Operation {
	case protectionfence.SubmitTP:
		expected = plan.TP
		if intent.TargetAlgoID != 0 || intent.TargetClientAlgoID != expected.ClientAlgoID {
			return blocked, errors.New("fenced TP target and immutable plan mismatch")
		}
	case protectionfence.SubmitSL:
		expected = plan.SL
		if intent.TargetAlgoID != 0 || intent.TargetClientAlgoID != expected.ClientAlgoID {
			return blocked, errors.New("fenced SL target and immutable plan mismatch")
		}
	case protectionfence.CancelSibling:
		if intent.TargetAlgoID <= 0 {
			return blocked, errors.New("fenced cancel target lacks exchange Algo ID")
		}
		switch intent.TargetClientAlgoID {
		case plan.TP.ClientAlgoID:
			expected = plan.TP
		case plan.SL.ClientAlgoID:
			expected = plan.SL
		default:
			return blocked, errors.New("fenced cancel target not in immutable plan")
		}
	default:
		return blocked, errors.New("unknown fenced protection mutation")
	}
	creds, err := r.credentials(ctx)
	if err != nil || creds.APIKey == "" || creds.Secret == "" {
		return blocked, fmt.Errorf("Demo signed credentials unavailable: %w", errOrUnverified(err))
	}
	hash := sha256.Sum256([]byte(creds.APIKey))
	fingerprint := hex.EncodeToString(hash[:])
	if fingerprint != intent.AccountFingerprintSHA256 {
		return blocked, errors.New("fenced account fingerprint differs from current signed key")
	}
	reader := productFenceSignedAlgoReader{client: r.client, creds: creds,
		accountFingerprint: fingerprint, expected: expected}
	result, err := store.InspectRecoveryOnce(ctx, reader)
	if err != nil {
		return blocked, err
	}
	// The independent local plan could have changed while two signed GETs
	// were in flight. Even an otherwise matched read must then be rejected.
	after, err := r.submissions.read(intent.GroupID)
	if err != nil || after.Digest != plan.Digest || after.TP != plan.TP ||
		after.SL != plan.SL || after.PositionSide != plan.PositionSide ||
		after.ModelID != plan.ModelID || after.Source != plan.Source {
		return blocked, fmt.Errorf("protection plan changed during recovery: %w", errOrUnverified(err))
	}
	return result, nil
}

func errOrUnverified(err error) error {
	if err != nil {
		return err
	}
	return errors.New("incomplete evidence")
}
