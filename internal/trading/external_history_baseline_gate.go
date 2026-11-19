package trading

// dev.3.129: an offline read-only baseline boundary for bounded captured
// history. This does NOT attest an exchange source, account lifetime, funding
// ownership or financial settlement, even if a local file claims otherwise.
import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

const (
	ExternalHistoryBaselineMatched    = "BOUNDED_CAPTURE_BASELINE_MATCHED_NOT_ACCOUNT_ATTESTED"
	ExternalHistoryBaselineIncomplete = "BOUNDED_CAPTURE_BASELINE_INCOMPLETE_NOT_ACCOUNT_ATTESTED"
)

// ExternalHistoryBaselineClaim is an explicit, operator-supplied *expectation*,
// not proof. The scope cannot be broadened by recomputing its local checksum.
// The five authority assertions MUST be false, including for empty funding.
type ExternalHistoryBaselineClaim struct {
	Schema                           int      `json:"schema"`
	ClientOrderID                    string   `json:"client_order_id"`
	IncidentArchiveSHA256            string   `json:"incident_archive_sha256"`
	OfflineSourceSHA256              string   `json:"offline_source_sha256"`
	Scope                            string   `json:"scope"`
	FromMS                           int64    `json:"from_ms"`
	ToMS                             int64    `json:"to_ms"`
	DataSets                         []string `json:"data_sets"`
	WholeAccountHistoryClaimed       bool     `json:"whole_account_history_claimed"`
	AllSymbolsClaimed                bool     `json:"all_symbols_claimed"`
	PrivateStreamContinuityClaimed   bool     `json:"private_stream_continuity_claimed"`
	ModelFundingAllocatedClaimed     bool     `json:"model_funding_allocated_claimed"`
	ModelSettlementAuthorizedClaimed bool     `json:"model_settlement_authorized_claimed"`
	Checksum                         string   `json:"checksum"`
}

func (v ExternalHistoryBaselineClaim) digest() string { v.Checksum = ""; return externalCloseHash(v) }

// PrepareExternalHistoryBaselineClaim supplies the only local baseline this
// offline capture supports. It cannot create a lifetime history assertion.
func PrepareExternalHistoryBaselineClaim(id string, incident PackageAIncidentArchive, sourceHash string) (ExternalHistoryBaselineClaim, error) {
	h, err := captureHeader(id, incident, sourceHash)
	if err != nil {
		return ExternalHistoryBaselineClaim{}, err
	}
	c := ExternalHistoryBaselineClaim{Schema: 1, ClientOrderID: id, IncidentArchiveSHA256: incident.Checksum,
		OfflineSourceSHA256: sourceHash, Scope: ExternalSignedHistoryScope, FromMS: h.FromMS, ToMS: h.ToMS,
		DataSets: []string{"BTCUSDT_USER_TRADES", "BTCUSDT_FUNDING_FEE"}}
	c.Checksum = c.digest()
	return c, nil
}

// All authorization-related flags are intentionally immutable false values.
// Observed completeness means only that every millisecond of the requested
// event interval has a sealed *local mock* checkpoint, not a Binance proof.
type ExternalHistoryBaselineReview struct {
	Schema                        int      `json:"schema"`
	Stage                         string   `json:"stage"`
	ClientOrderID                 string   `json:"client_order_id"`
	ModelID                       string   `json:"model_id"`
	Scope                         string   `json:"scope"`
	FromMS                        int64    `json:"from_ms"`
	ToMS                          int64    `json:"to_ms"`
	CapturedWindowCount           int      `json:"captured_window_count"`
	CoveredUntilMS                int64    `json:"covered_until_ms"`
	ClaimSHA256                   string   `json:"claim_sha256"`
	IncidentArchiveSHA256         string   `json:"incident_archive_sha256"`
	OfflineSourceSHA256           string   `json:"offline_source_sha256"`
	ModelEvidenceSHA256           string   `json:"model_evidence_sha256"`
	ContinuitySHA256              string   `json:"continuity_sha256,omitempty"`
	FinalCheckpointSHA256         string   `json:"final_checkpoint_sha256,omitempty"`
	BoundedCaptureComplete        bool     `json:"bounded_capture_complete"`
	WholeAccountHistoryProven     bool     `json:"whole_account_history_proven"`
	AllSymbolsCovered             bool     `json:"all_symbols_covered"`
	PrivateStreamContinuityProven bool     `json:"private_stream_continuity_proven"`
	ExchangeSourceAttested        bool     `json:"exchange_source_attested"`
	ModelOwnershipProven          bool     `json:"model_ownership_proven"`
	ModelFundingAllocated         bool     `json:"model_funding_allocated"`
	ModelNetPnLFinal              bool     `json:"model_net_pnl_final"`
	ModelSettlementAuthorized     bool     `json:"model_settlement_authorized"`
	PostingApplied                bool     `json:"posting_applied"`
	TradingAuthorized             bool     `json:"trading_authorized"`
	BlockerCodes                  []string `json:"blocker_codes"`
	DigestSHA256                  string   `json:"digest_sha256"`
}

func (r ExternalHistoryBaselineReview) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

func ValidateExternalHistoryBaselineReview(r ExternalHistoryBaselineReview) error {
	expected := []string{"ACCOUNT_LIFETIME_BASELINE_UNKNOWN", "OTHER_SYMBOL_AND_ORDER_HISTORY_NOT_CAPTURED",
		"PRIVATE_STREAM_CONTINUITY_NOT_ATTESTED", "OFFLINE_CAPTURE_NOT_EXCHANGE_SIGNED_EVIDENCE",
		"MODEL_FUNDING_AND_OWNERSHIP_UNPROVEN", "INDEPENDENT_FINANCIAL_APPROVAL_AND_ATOMIC_POSTING_MISSING",
		"HTTP_423_ORDER_LOCK_REQUIRED"}
	if !r.BoundedCaptureComplete {
		expected = append(expected, "INCOMPLETE_CHECKPOINT_AND_SEAL")
	}
	if r.Schema != 1 || r.ClientOrderID == "" || r.ModelID == "" || r.Scope != ExternalSignedHistoryScope ||
		r.FromMS <= 0 || r.ToMS < r.FromMS || r.CapturedWindowCount < 0 ||
		r.CoveredUntilMS < r.FromMS-1 || r.CoveredUntilMS > r.ToMS ||
		!externalReviewHash.MatchString(r.ClaimSHA256) || !externalReviewHash.MatchString(r.IncidentArchiveSHA256) ||
		!externalReviewHash.MatchString(r.OfflineSourceSHA256) || !externalReviewHash.MatchString(r.ModelEvidenceSHA256) ||
		r.WholeAccountHistoryProven || r.AllSymbolsCovered || r.PrivateStreamContinuityProven ||
		r.ExchangeSourceAttested || r.ModelOwnershipProven || r.ModelFundingAllocated || r.ModelNetPnLFinal ||
		r.ModelSettlementAuthorized || r.PostingApplied || r.TradingAuthorized ||
		!reflect.DeepEqual(r.BlockerCodes, expected) ||
		(r.BoundedCaptureComplete && (r.Stage != ExternalHistoryBaselineMatched || r.CoveredUntilMS != r.ToMS ||
			r.CapturedWindowCount == 0 || !externalReviewHash.MatchString(r.ContinuitySHA256) ||
			!externalReviewHash.MatchString(r.FinalCheckpointSHA256))) ||
		(!r.BoundedCaptureComplete && (r.Stage != ExternalHistoryBaselineIncomplete || r.ContinuitySHA256 != "" ||
			(r.CapturedWindowCount == 0 && r.FinalCheckpointSHA256 != ""))) ||
		r.DigestSHA256 != r.digest() {
		return errors.New("invalid or authorizing bounded-history baseline report")
	}
	return nil
}

func InspectExternalHistoryBaseline(ctx context.Context, s *ExternalHistoryCaptureStore, dataDir, clientID string,
	incident PackageAIncidentArchive, sourceHash string, claim ExternalHistoryBaselineClaim) (ExternalHistoryBaselineReview, error) {
	var zero ExternalHistoryBaselineReview
	if ctx == nil || s == nil {
		return zero, errors.New("context and offline capture store required")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	want, err := PrepareExternalHistoryBaselineClaim(clientID, incident, sourceHash)
	if err != nil {
		return zero, err
	}
	if !reflect.DeepEqual(claim, want) || claim.Checksum != claim.digest() {
		return zero, errors.New("baseline claim exceeded or mismatched the bounded event identity/scope")
	}
	model, err := InspectExternalModelFinancialEvidence(ctx, dataDir, clientID, incident)
	if err != nil {
		return zero, fmt.Errorf("original model evidence: %w", err)
	}
	if err := ValidateExternalModelFinancialEvidence(model); err != nil {
		return zero, err
	}
	if model.IncidentArchiveSHA256 != incident.Checksum || model.CompleteHistoryProven || model.FundingAllocated ||
		model.ModelOwnershipProven || model.ModelFundingUSDT != nil || model.ModelFinalNetUSDT != nil ||
		model.SettlementAuthorized || model.PostingApplied || model.TradingAuthorized {
		return zero, errors.New("model evidence attempts a financial or history authority escalation")
	}
	state, err := s.Inspect(ctx, dataDir, clientID, incident, sourceHash)
	if err != nil {
		return zero, fmt.Errorf("sealed checkpoint inspection: %w", err)
	}
	if state.ClientOrderID != clientID || state.Schema != 1 || state.ToMS != claim.ToMS ||
		state.WholeAccountHistoryProven || state.ExchangeSourceAttested || state.PostingApplied ||
		state.SettlementAuthorized || state.TradingAuthorized {
		return zero, errors.New("unsafe capture state")
	}
	r := ExternalHistoryBaselineReview{Schema: 1, ClientOrderID: clientID, ModelID: model.ModelID,
		Scope: claim.Scope, FromMS: claim.FromMS, ToMS: claim.ToMS,
		CapturedWindowCount: state.CompletedWindows, CoveredUntilMS: state.NextFromMS - 1,
		ClaimSHA256: claim.Checksum, IncidentArchiveSHA256: incident.Checksum,
		OfflineSourceSHA256: sourceHash, ModelEvidenceSHA256: model.DigestSHA256,
		BlockerCodes: []string{"ACCOUNT_LIFETIME_BASELINE_UNKNOWN", "OTHER_SYMBOL_AND_ORDER_HISTORY_NOT_CAPTURED",
			"PRIVATE_STREAM_CONTINUITY_NOT_ATTESTED", "OFFLINE_CAPTURE_NOT_EXCHANGE_SIGNED_EVIDENCE",
			"MODEL_FUNDING_AND_OWNERSHIP_UNPROVEN", "INDEPENDENT_FINANCIAL_APPROVAL_AND_ATOMIC_POSTING_MISSING",
			"HTTP_423_ORDER_LOCK_REQUIRED"}}
	if state.Complete {
		if state.BoundedContinuity == nil || state.Stage != ExternalHistoryCaptureComplete ||
			!state.BoundedContinuity.ObservedWindowSequenceContiguous || !state.BoundedContinuity.CapturedRowsMatchIncident ||
			state.BoundedContinuity.ModelEvidenceSHA256 != model.DigestSHA256 ||
			state.BoundedContinuity.FromMS != claim.FromMS || state.BoundedContinuity.ToMS != claim.ToMS ||
			state.BoundedContinuity.WholeAccountHistoryProven || state.BoundedContinuity.SettlementAuthorized {
			return zero, errors.New("complete capture diverged from the incident model baseline")
		}
		r.Stage = ExternalHistoryBaselineMatched
		r.BoundedCaptureComplete = true
		r.ContinuitySHA256 = state.BoundedContinuity.DigestSHA256
		r.FinalCheckpointSHA256 = state.LastChecksum
	} else {
		if state.Stage != ExternalHistoryCapturePartial || state.BoundedContinuity != nil {
			return zero, errors.New("unsafe partial capture state")
		}
		r.Stage = ExternalHistoryBaselineIncomplete
		r.BlockerCodes = append(r.BlockerCodes, "INCOMPLETE_CHECKPOINT_AND_SEAL")
		r.FinalCheckpointSHA256 = state.LastChecksum
	}
	// Recheck both independent inputs. A racing checkpoint/source mutation
	// cannot turn an earlier locally matched window into an approval signal.
	again, err := s.Inspect(ctx, dataDir, clientID, incident, sourceHash)
	if err != nil || !reflect.DeepEqual(state, again) {
		return zero, errors.New("capture changed while verifying baseline")
	}
	fresh, err := InspectExternalModelFinancialEvidence(ctx, dataDir, clientID, incident)
	if err != nil || fresh.DigestSHA256 != model.DigestSHA256 {
		return zero, errors.New("model evidence changed while verifying baseline")
	}
	r.DigestSHA256 = r.digest()
	if err := ValidateExternalHistoryBaselineReview(r); err != nil {
		return zero, err
	}
	return r, nil
}
