package trading

// An independently fetched, account-wide signed REST + local-ledger continuity
// gate for *reviewing* whether a prepared TP/SL plan is even consistent with
// the known account. This gate NEVER returns a capability/approval to execute.
// Binance's bounded history cannot prove lifetime ownership or stream continuity,
// and two separate conditional orders are not an atomic OCO pair. No HTTP,
// production execution or simulated POST/DELETE path consumes its output.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

const (
	ProtectionGateBlocked       = "BLOCKED"
	ProtectionGateBoundedReview = "SIGNED_BOUNDED_OWNER_MATCH_NOT_APPROVED"
)

type ProtectionExecutionApprovalReview struct {
	Schema                     int          `json:"schema"`
	State                      string       `json:"state"`
	GroupID                    string       `json:"group_id"`
	ClientOrderID              string       `json:"client_order_id"`
	FirstObservedAt            time.Time    `json:"first_observed_at"`
	SecondObservedAt           time.Time    `json:"second_observed_at"`
	FirstAccountSHA256         string       `json:"first_account_sha256,omitempty"`
	SecondAccountSHA256        string       `json:"second_account_sha256,omitempty"`
	LocalEvidenceSHA256        string       `json:"local_evidence_sha256,omitempty"`
	LocalOwnerCount            int          `json:"local_owner_count"`
	ProtectedSide              PositionSide `json:"protected_side,omitempty"`
	ProtectedQtyBTC            string       `json:"protected_qty_btc,omitempty"`
	HistoryCoverage            string       `json:"history_coverage"`
	Reasons                    []string     `json:"reasons"`
	ProtectionSubmitAuthorized bool         `json:"protection_submit_authorized"` // ALWAYS false
	SiblingCancelAuthorized    bool         `json:"sibling_cancel_authorized"`    // ALWAYS false
	TradingAuthorized          bool         `json:"trading_authorized"`           // ALWAYS false
	ContinuousCoverageProven   bool         `json:"continuous_coverage_proven"`   // ALWAYS false
	FinanciallySettled         bool         `json:"financially_settled"`          // ALWAYS false
	ManualReviewRequired       bool         `json:"manual_review_required"`       // ALWAYS true
}

// Validate the entire local protection directory, not just the selected plan.
// An orphaned attempt/observation or a second plan can never be ignored.
func inspectPreparedProtectionInventory(dir, group string) (ProtectionDispatchPlan, error) {
	s := &ProtectionSubmissionStore{dir: filepath.Join(dir, "trading", "protection_submissions")}
	info, err := os.Lstat(s.dir)
	if err != nil {
		return ProtectionDispatchPlan{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ProtectionDispatchPlan{}, errors.New("protection directory is not a real directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return ProtectionDispatchPlan{}, err
	}
	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 || e.Name() != group+".json" {
			return ProtectionDispatchPlan{}, fmt.Errorf("other or malformed protection state in %s", s.dir)
		}
	}
	if len(entries) != 1 {
		return ProtectionDispatchPlan{}, errors.New("expected exactly one immutable protection plan")
	}
	plan, err := s.read(group)
	if err != nil {
		return ProtectionDispatchPlan{}, err
	}
	checkpoint, err := InspectProtectionArmingCheckpoint(s, group)
	if err != nil || checkpoint.State != "PREPARED_NEVER_DISPATCHED" {
		return ProtectionDispatchPlan{}, errors.New("protection plan has an attempt, observation, or corrupt checkpoint")
	}
	// A sibling cancel ledger can exist without a submitted protection leg;
	// that is a corrupt local state, not an authorization bypass.
	cancelDir := filepath.Join(dir, "trading", "algo_cancel_intents")
	if info, statErr := os.Lstat(cancelDir); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ProtectionDispatchPlan{}, errors.New("cancel directory is not a real directory")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return ProtectionDispatchPlan{}, statErr
	}
	cancels, err := os.ReadDir(cancelDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ProtectionDispatchPlan{}, err
	}
	if len(cancels) != 0 {
		return ProtectionDispatchPlan{}, errors.New("cancel intent exists before protection submission")
	}
	return plan, nil
}

// Strict account-wide scope: no unrelated asset/collateral, other symbol,
// normal order, open conditional order, pooled same-side multi-owner, or stale
// unknown local order can be treated as belonging to the selected plan.
func checkPreparedAccountAndOwner(b exchange.RecoveryBaseline, owned []OwnerPosition, plan ProtectionDispatchPlan) error {
	if err := exchange.ValidatePortfolioSnapshot(b.Portfolio); err != nil {
		return err
	}
	if !b.Portfolio.CanTrade || b.Portfolio.MultiAssetsMargin || len(b.OpenOrders) != 0 || len(b.OpenAlgos) != 0 {
		return errors.New("account not eligible: other orders, algos, or multi-asset margin")
	}
	for _, a := range b.Portfolio.Assets {
		if a.Asset == "USDT" {
			continue
		}
		q, err := signedRat(string(a.MarginBalance))
		if err != nil || q.Sign() != 0 {
			return errors.New("foreign collateral present")
		}
	}
	var found bool
	bySide := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	counts := map[PositionSide]int{}
	for _, o := range owned {
		q, err := positiveRat(o.Quantity)
		if err != nil {
			return errors.New("local owner quantity invalid")
		}
		if o.Symbol != "BTCUSDT" || (o.Side != Long && o.Side != Short) {
			return errors.New("foreign owned position")
		}
		bySide[o.Side].Add(bySide[o.Side], q)
		counts[o.Side]++
		if o.Side == plan.PositionSide && o.Source == plan.Source && o.ModelID == plan.ModelID {
			if found || !sameSignedDecimal(o.Quantity, plan.TP.Quantity, true) {
				return errors.New("ambiguous or mismatched plan owner")
			}
			found = true
		}
	}
	if !found || counts[plan.PositionSide] != 1 || len(owned) != 1 {
		return errors.New("account does not have exactly one uniquely proven protected owner")
	}
	actual := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, p := range b.Portfolio.Positions {
		q, err := signedRat(string(p.PositionAmt))
		if err != nil {
			return err
		}
		if p.Symbol != "BTCUSDT" || (p.PositionSide != string(Long) && p.PositionSide != string(Short)) {
			if q.Sign() != 0 {
				return errors.New("foreign or BOTH account exposure")
			}
			continue
		}
		if q.Sign() != 0 && !p.Isolated {
			return errors.New("non-isolated position")
		}
		if (p.PositionSide == string(Long) && q.Sign() < 0) || (p.PositionSide == string(Short) && q.Sign() > 0) {
			return errors.New("Hedge side sign inconsistent")
		}
		actual[PositionSide(p.PositionSide)].Add(actual[PositionSide(p.PositionSide)], new(big.Rat).Abs(q))
	}
	for _, side := range []PositionSide{Long, Short} {
		if bySide[side].Cmp(actual[side]) != 0 {
			return errors.New("account-wide signed quantity disagrees with local owner ledger")
		}
	}
	return nil
}

// InspectProtectionExecutionApprovalGate never accepts an ownershipVerified
// boolean or caller-provided owner list. It reconstructs the owned positions
// from the persisted journal/fills via ReviewRecovery, bracketed by TWO fresh
// independent authenticated full-account queries and a local evidence pin.
// fromMS must be within six days, but even a successful bounded match cannot
// prove older history, funding attribution, a private stream epoch or safety
// against an intervention immediately after the final signed read.
func InspectProtectionExecutionApprovalGate(ctx context.Context, dataDir string, r Reservation,
	reader WindowDemoReader, creds DemoCredentials, expectedAccountFingerprint string, fromMS int64,
) (ProtectionExecutionApprovalReview, error) {
	out := ProtectionExecutionApprovalReview{Schema: 1, State: ProtectionGateBlocked,
		GroupID: entryProtectionGroupID(r), ClientOrderID: r.ClientOrderID,
		HistoryCoverage: "BOUNDED_SIGNED_REST_NOT_CONTINUOUS", ManualReviewRequired: true,
		Reasons: []string{},
	}
	block := func(reason string, err error) (ProtectionExecutionApprovalReview, error) {
		out.State = ProtectionGateBlocked
		out.Reasons = append(out.Reasons, reason)
		return out, err
	}
	if ctx == nil || reader == nil || dataDir == "" || creds.APIKey == "" || creds.Secret == "" ||
		r.ClientOrderID == "" || r.Intent.Action != Enter || r.Intent.Symbol != "BTCUSDT" ||
		!requestIDPattern.MatchString(out.GroupID) || fromMS <= 0 {
		return block("invalid or missing signed account review input", errors.New("protection gate input invalid"))
	}
	if err := ctx.Err(); err != nil {
		return block("context canceled", err)
	}
	// The fingerprint must come from an independently verified account-bound
	// configuration, never a browser-supplied approval checkbox. Matching a
	// fingerprint alone is not an authorization or proof of historic account
	// activity; no secrets or the raw API key are persisted in this report.
	keyHash := sha256.Sum256([]byte(creds.APIKey))
	if len(expectedAccountFingerprint) != 64 || hex.EncodeToString(keyHash[:]) != expectedAccountFingerprint {
		return block("Demo account fingerprint mismatch", errors.New("expected account fingerprint not verified"))
	}
	start := time.Now().UTC()
	if fromMS >= start.UnixMilli() || start.Sub(time.UnixMilli(fromMS)) > 6*24*time.Hour {
		return block("bounded signed account window invalid", errors.New("signed history window invalid"))
	}
	pin, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return block("local execution files unavailable", err)
	}
	out.LocalEvidenceSHA256 = pin
	plan, err := inspectPreparedProtectionInventory(dataDir, out.GroupID)
	if err != nil {
		return block("prepared local protection inventory invalid", err)
	}
	if plan.Source != r.Intent.Source || plan.ModelID != r.Intent.ModelID || plan.PositionSide != r.Intent.PositionSide ||
		!sameSignedDecimal(plan.TP.Quantity, r.Intent.Quantity, true) || !sameSignedDecimal(plan.SL.Quantity, r.Intent.Quantity, true) {
		return block("entry and durable protection owner mismatch", errors.New("protection plan ownership mismatch"))
	}
	// Actual local journal must contain this entry; caller's reservation alone
	// never establishes ownership. Never create missing directories on review.
	store := &OwnershipStore{journalDir: filepath.Join(dataDir, "trading", "requests"), fillDir: filepath.Join(dataDir, "trading", "fills")}
	records, err := store.ScanJournal()
	if err != nil {
		return block("local order journal invalid", err)
	}
	found := false
	for _, rec := range records {
		if rec.Plan.ClientOrderID == r.ClientOrderID && rec.Intent.Action == Enter && reflect.DeepEqual(rec.Intent, r.Intent) {
			found = true
		}
	}
	if !found {
		return block("entry is not independently owned by journal", errors.New("missing exact durable entry journal"))
	}
	// A caller-supplied Reservation can never substitute for the durable on-disk
	// reservation, nor may an imported-fill count substitute for the sealed
	// execution lifecycle's signed-observation -> FILLS_IMPORTED transition.
	// In particular, reject legacy or incomplete entries before any signed GET.
	reserved, err := decodeReservation(filepath.Join(dataDir, "trading", "reservations", r.ClientOrderID+".json"))
	if err != nil || !reflect.DeepEqual(reserved, r) {
		return block("immutable reservation missing or mismatched", errors.New("entry reservation is not independently verified"))
	}
	stage, present, err := executionLifecycleStatusForReservation(dataDir, r)
	if err != nil || !present || stage != ExecutionFillsImported {
		return block("entry signed execution lifecycle is not at FILLS_IMPORTED", errors.New("entry signed lifecycle is incomplete or invalid"))
	}
	// Look up signed data from the GET-only reader. All unknown recent trades,
	// open orders, income and prior stream gaps are diagnosed by ReviewRecovery.
	first, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return block("first independent signed full-account GET failed", err)
	}
	if err := validBoundedDriftInput(first, fromMS, time.Now().UTC()); err != nil {
		return block("first signed full-account baseline invalid", err)
	}
	out.FirstObservedAt = first.FinishedAt
	firstHash, err := boundedExposureSignature(first)
	if err != nil {
		return block("first signed exposure invalid", err)
	}
	out.FirstAccountSHA256 = firstHash
	recovery, err := ReviewRecovery(dataDir, store, first, time.Now().UTC())
	if err != nil || recovery.Ownership == nil || !recovery.Ownership.ReadyForAssessment {
		if err == nil {
			err = errors.New("ownership replay is not verified")
		}
		return block("locally replayed owner and signed order/fill audit failed", err)
	}
	// ReviewRecovery carries two deliberate *policy* blockers in every
	// result. Everything else indicates an unexplained exchange/local change.
	for _, reason := range recovery.Issues {
		if strings.HasPrefix(reason, "계정 자산 위험: 자동매매 전용 배정 지갑과 실제 Demo 계정 일치 검증이 필요합니다") ||
			strings.HasPrefix(reason, "과거 bounded review만으로는 live continuity 승인 체크포인트를 대체할 수 없음") {
			continue
		}
		return block("signed account/journal audit: "+reason, errors.New("account history is not fully reconciled"))
	}
	if err := checkPreparedAccountAndOwner(first, recovery.Ownership.Owned, plan); err != nil {
		return block("full-account owner/side/conditional inventory mismatch", err)
	}
	out.LocalOwnerCount, out.ProtectedSide, out.ProtectedQtyBTC = len(recovery.Ownership.Owned), plan.PositionSide, plan.TP.Quantity
	if err := ctx.Err(); err != nil {
		return block("context canceled", err)
	}
	second, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return block("second independent signed full-account GET failed", err)
	}
	if err := validBoundedDriftInput(second, fromMS, time.Now().UTC()); err != nil {
		return block("second signed full-account baseline invalid", err)
	}
	out.SecondObservedAt = second.FinishedAt
	secondHash, err := boundedExposureSignature(second)
	if err != nil {
		return block("second signed exposure invalid", err)
	}
	out.SecondAccountSHA256 = secondHash
	if second.StartedAt.Before(first.FinishedAt) || firstHash != secondHash {
		return block("signed account exposure changed or independent GET intervals overlap", errors.New("signed account drift"))
	}
	if err := checkPreparedAccountAndOwner(second, recovery.Ownership.Owned, plan); err != nil {
		return block("second independent signed account no longer matches local owners", err)
	}
	if refreshed, err := snapshotExecutionEvidence(dataDir); err != nil || refreshed != pin {
		return block("original local ownership/execution evidence changed during signed reads", errors.New("local execution drift"))
	}
	laterPlan, err := inspectPreparedProtectionInventory(dataDir, out.GroupID)
	if err != nil || !reflect.DeepEqual(plan, laterPlan) {
		return block("protection/cancel inventory changed during signed reads", errors.New("protection inventory drift"))
	}
	if err := ctx.Err(); err != nil {
		return block("context canceled", err)
	}
	out.State = ProtectionGateBoundedReview
	out.Reasons = append(out.Reasons,
		"Signed account and local ownership agree within the checked bounded REST window only",
		"Incomplete lifetime history and private stream continuity; independent TP/SL are not atomic OCO",
		"No submit/cancel/auto-trading permission or operator override is issued")
	return out, nil
}
