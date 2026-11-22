package trading

// The v15 product boundary makes actual SIGNED Demo REST baseline calls and
// locally reconstructs owner lots. It is intentionally GET-only relative to
// Binance: never a mutation capability, HTTP route or background permission.
// This method requires the pre-existing coordinator lease. A matching pair
// of bounded account snapshots is not continuous ownership or OCO proof.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/protectionfence"
)

func signedAccountFrameV15(b exchange.RecoveryBaseline, fingerprint string) (protectionfence.SignedAccountFrameV15, error) {
	f := protectionfence.SignedAccountFrameV15{Environment: b.Environment, Coverage: b.Coverage,
		AccountFingerprintSHA256: fingerprint, StartedAt: b.StartedAt, FinishedAt: b.FinishedAt,
		WindowStartMS: b.WindowStartMS, WindowEndMS: b.WindowEndMS,
		OpenNormalOrders: len(b.OpenOrders), OpenAlgoOrders: len(b.OpenAlgos),
		CanTrade: b.Portfolio.CanTrade, MultiAssetsMargin: b.Portfolio.MultiAssetsMargin}
	if b.OpenOrders == nil || b.OpenAlgos == nil || b.RecentBTCOrders == nil ||
		b.RecentBTCAlgos == nil || b.RecentBTCTrades == nil || b.RecentIncome == nil {
		return f, errors.New("missing complete signed account history or open order lists")
	}
	if err := exchange.ValidatePortfolioSnapshot(b.Portfolio); err != nil {
		return f, err
	}
	// Only a fresh signed account-wide endpoint can supply this information.
	// Duplicate Hedge rows, BOTH exposures and unknown asset exposure always
	// fail closed; a caller's numeric summary is never trusted.
	foundLong, foundShort, foundUSDT := false, false, false
	f.LongQtyBTC, f.ShortQtyBTC = "0", "0"
	for _, p := range b.Portfolio.Positions {
		q, err := signedRat(string(p.PositionAmt))
		if err != nil {
			return f, err
		}
		if p.Symbol != "BTCUSDT" || p.PositionSide == "BOTH" {
			if q.Sign() != 0 {
				f.ForeignExposure = true
			}
			continue
		}
		if p.PositionSide == "LONG" {
			if foundLong || q.Sign() < 0 {
				return f, errors.New("duplicate or wrong-signed long position")
			}
			foundLong = true
			f.LongQtyBTC = formatOwnershipDecimal(q)
		} else if p.PositionSide == "SHORT" {
			if foundShort || q.Sign() > 0 {
				return f, errors.New("duplicate or wrong-signed short position")
			}
			foundShort = true
			f.ShortQtyBTC = formatOwnershipDecimal(new(big.Rat).Abs(q))
		} else if q.Sign() != 0 {
			f.ForeignExposure = true
		}
	}
	for _, a := range b.Portfolio.Assets {
		if a.Asset == "USDT" {
			foundUSDT = true
			continue
		}
		q, err := signedRat(string(a.MarginBalance))
		if err != nil {
			return f, err
		}
		if q.Sign() != 0 {
			f.UnverifiedCollateral = true
		}
	}
	if !foundUSDT {
		return f, errors.New("signed USDT collateral asset missing")
	}
	// Hash normalized full evidence, not merely Hedge quantity. Changing
	// funding, unrelated orders or trade/income pages must invalidate a match.
	// The bracket timestamps and the query endTime legitimately differ.
	stable := canonicalRecoveryReviewBaseline(b)
	stable.StartedAt, stable.FinishedAt = time.Time{}, time.Time{}
	stable.WindowEndMS = 0
	raw, err := json.Marshal(stable)
	if err != nil {
		return f, err
	}
	sum := sha256.Sum256(raw)
	f.AccountEvidenceSHA256 = hex.EncodeToString(sum[:])
	return f, nil
}

func localReviewOwnersV15(review RecoveryReview) []OwnerPosition {
	if review.Ownership == nil {
		return nil
	}
	return review.Ownership.Owned
}

func sameBTCQtyV15(a, b string, allowZero bool) bool {
	x, err := decimalRat(a, allowZero)
	if err != nil {
		return false
	}
	y, err := decimalRat(b, allowZero)
	return err == nil && x.Cmp(y) == 0
}

func signedOwnerTotalsV15(owned []OwnerPosition) (string, string, error) {
	totals := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	seen := map[string]bool{}
	for _, o := range owned {
		if o.Symbol != "BTCUSDT" || (o.Side != Long && o.Side != Short) ||
			(o.Source != Model && o.Source != Manual) ||
			(o.Source == Model && !modelIDPattern.MatchString(o.ModelID)) ||
			(o.Source == Manual && o.ModelID != "") {
			return "", "", errors.New("invalid local model/manual owner")
		}
		key := string(o.Source) + "|" + o.ModelID + "|" + string(o.Side)
		if seen[key] {
			return "", "", errors.New("duplicate local owner key")
		}
		seen[key] = true
		q, err := decimalRat(o.Quantity, true)
		if err != nil || q.Sign() < 0 {
            return "", "", errors.New("invalid signed local owner quantity")
		}
		totals[o.Side].Add(totals[o.Side], q)
	}
	return formatOwnershipDecimal(totals[Long]), formatOwnershipDecimal(totals[Short]), nil
}

// InspectProductSignedAccountExposureV15 uses the actual client implementing
// signed GET /fapi/v3/account, /fapi/v1/accountConfig, /fapi/v1/openOrders,
// /fapi/v1/openAlgoOrders, trades and income via FetchDemoRecoveryBaseline.
// It also independently reads local request/fill/protection journals with
// ReviewRecovery; no browser-provided owner list, approved flag or qty accepted.
// This is deliberately NOT called from an automated trade path.
func (r *DemoProtectionRuntimeCoordinator) InspectProductSignedAccountExposureV15(ctx context.Context, fromMS int64) (protectionfence.SignedAccountExposureReviewV15, error) {
	blocked := protectionfence.SignedAccountExposureReviewV15{State: "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION",
		ManualReviewRequired: true, Reasons: []string{"independent signed account inspection incomplete"},
		EvidenceLimit: "BOUNDED_ACCOUNT_GET_ONLY_NOT_CONTINUOUS_OR_AUTHORIZING"}
	if ctx == nil || r == nil || r.coordinator == nil || r.submissions == nil || r.owners == nil ||
		r.client == nil || r.credentials == nil || r.dataDir == "" {
		return blocked, errors.New("signed account inspection dependencies missing")
	}
	if err := ctx.Err(); err != nil {
		return blocked, err
	}
	if err := r.coordinator.ownsLock(); err != nil {
		return blocked, err
	}
	dir := filepath.Join(r.dataDir, "trading", "product_mutation_fences")
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return blocked, errors.New("existing immutable mutation fence required")
	}
	fences, err := protectionfence.Open(r.dataDir)
	if err != nil {
		return blocked, err
	}
	before, err := fences.InspectAll()
	if err != nil || len(before) != 1 {
		return blocked, errors.New("exactly one intact mutation fence required")
	}
	f := before[0]
	blocked.GroupID = f.Intent.GroupID
	plan, err := r.submissions.read(f.Intent.GroupID)
	if err != nil || plan.Digest != f.Intent.PlanSHA256 || string(plan.PositionSide) != f.Intent.PositionSide ||
		plan.TP.Quantity != f.Intent.QuantityBTC || plan.SL.Quantity != f.Intent.QuantityBTC ||
		(f.Intent.TargetClientAlgoID != plan.TP.ClientAlgoID && f.Intent.TargetClientAlgoID != plan.SL.ClientAlgoID) {
		return blocked, errors.New("immutable protection plan does not match fenced identity")
	}
	creds, err := r.credentials(ctx)
	if err != nil || creds.APIKey == "" || creds.Secret == "" {
		return blocked, errors.New("Demo credentials not available")
	}
	keyhash := sha256.Sum256([]byte(creds.APIKey))
	fingerprint := hex.EncodeToString(keyhash[:])
	if fingerprint != f.Intent.AccountFingerprintSHA256 {
		return blocked, errors.New("Demo account does not own existing immutable fence")
	}
	now := time.Now().UTC()
	if fromMS <= 0 || fromMS >= now.UnixMilli() || now.Sub(time.UnixMilli(fromMS)) > 6*24*time.Hour {
		return blocked, errors.New("invalid bounded signed account window")
	}
	// Limit the entire two-window read-only probe: a timed-out probe is never
    // allowed to reuse stale evidence as a subsequent execution permit.
    ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
    defer cancel()
	first, err := r.client.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return blocked, fmt.Errorf("first signed full-account baseline: %w", err)
	}
	firstReview, err := ReviewRecovery(r.dataDir, r.owners, first, time.Now().UTC())
	if err != nil {
		return blocked, fmt.Errorf("first local signed ledger assessment: %w", err)
	}
	second, err := r.client.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return blocked, fmt.Errorf("second signed full-account baseline: %w", err)
	}
	secondReview, err := ReviewRecovery(r.dataDir, r.owners, second, time.Now().UTC())
	if err != nil {
		return blocked, fmt.Errorf("second local signed ledger assessment: %w", err)
	}
	a, err := signedAccountFrameV15(first, fingerprint)
	if err != nil {
		return blocked, err
	}
	b, err := signedAccountFrameV15(second, fingerprint)
	if err != nil {
		return blocked, err
	}
	firstOwned, secondOwned := localReviewOwnersV15(firstReview), localReviewOwnersV15(secondReview)
	// Do not trust a per-model aggregate as an immutable per-entry group.
	// If multiple owner lots exist, refuse to invent their protection GroupID.
	long1, short1, err := signedOwnerTotalsV15(firstOwned)
	if err != nil {
		return blocked, err
	}
	long2, short2, err := signedOwnerTotalsV15(secondOwned)
	if err != nil {
		return blocked, err
	}
	if long1 != long2 || short1 != short2 || !sameBTCQtyV15(long1, a.LongQtyBTC, true) ||
		!sameBTCQtyV15(short1, a.ShortQtyBTC, true) || !sameBTCQtyV15(long2, b.LongQtyBTC, true) ||
		!sameBTCQtyV15(short2, b.ShortQtyBTC, true) {
		return blocked, errors.New("local model ownership differs from signed account side quantity")
	}
	// Even if the aggregate quantities agree, multiple model/manual owners on
	// the same side cannot prove which one received an external TP/SL fill.
	targetCount := 0
	for _, o := range secondOwned {
		if o.Side == plan.PositionSide && o.Quantity != "0" {
			targetCount++
		}
	}
	pair := protectionfence.InspectSignedAccountFramePairV15(f, a, b, time.Now().UTC())
	if pair.State != "SIGNED_BOUNDED_ACCOUNT_STABLE_OWNER_UNPROVEN" {
		return pair, nil
	}
	if targetCount != 1 || len(secondOwned) != 1 {
		blocked.State = "SIGNED_POOLED_OR_OPPOSITE_SIDE_OWNER_MANUAL_RECONCILIATION"
		blocked.SameSideOwnerCount = targetCount
		blocked.Reasons = []string{"multiple or ambiguous aggregate owner lots cannot be mapped to immutable per-entry protection groups"}
		after, inspectErr := fences.InspectAll()
		latest, planErr := r.submissions.read(f.Intent.GroupID)
		if inspectErr != nil || len(after) != 1 || after[0] != f || planErr != nil || latest.Digest != plan.Digest ||
			latest.TP != plan.TP || latest.SL != plan.SL || r.coordinator.ownsLock() != nil {
			return protectionfence.SignedAccountExposureReviewV15{State: "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION", ManualReviewRequired: true},
				errors.New("local fence, plan, or product lease changed during pooled owner review")
		}
		return blocked, nil
	}
	o := secondOwned[0]
	if o.Source != plan.Source || o.ModelID != plan.ModelID || o.Symbol != "BTCUSDT" ||
		o.Side != plan.PositionSide || !sameBTCQtyV15(o.Quantity, plan.TP.Quantity, false) ||
		!sameBTCQtyV15(firstOwned[0].Quantity, o.Quantity, false) {
		return blocked, errors.New("immutable protected owner does not match both local signed reviews")
	}
	owners := []protectionfence.OwnerShare{{GroupID: f.Intent.GroupID, Source: string(o.Source), ModelID: o.ModelID,
		Symbol: o.Symbol, Side: string(o.Side), QuantityBTC: o.Quantity}}
	out := protectionfence.ReviewSignedAccountFramesV15(f, a, b, owners, time.Now().UTC())
	// Any incomplete order/fill/stream or allocation reconciliation is still
	// an explicit blocker, even when the bounded position totals happen to match.
	if len(firstReview.Issues) != 0 || len(secondReview.Issues) != 0 || firstReview.Ownership == nil ||
		secondReview.Ownership == nil || !firstReview.Ownership.ReadyForAssessment ||
		!secondReview.Ownership.ReadyForAssessment {
		out.State = "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION"
		out.Reasons = append(out.Reasons, "local ledger, funding, allocation or private-stream history remains unreconciled")
	}
	// Local planning and mutation fence must still be unchanged after all GETs.
	after, err := fences.InspectAll()
	if err != nil || len(after) != 1 || after[0] != f {
		return blocked, errors.New("immutable fence changed during signed account audit")
	}
	latest, err := r.submissions.read(f.Intent.GroupID)
	if err != nil || latest.Digest != plan.Digest || latest.TP != plan.TP || latest.SL != plan.SL ||
		latest.Source != plan.Source || latest.ModelID != plan.ModelID {
		return blocked, errors.New("protection plan changed during signed account audit")
	}
	if err := r.coordinator.ownsLock(); err != nil {
		return blocked, err
	}
	return out, nil
}
