package protectionfence

// SignedAccountFrameV15 is an envelope derived from an AUTHENTICATED full-portfolio
// Binance Demo recovery baseline by the trusted product adapter. A struct value
// is not itself signed, and cannot be supplied by HTTP/strategy callers as an
// approval token. Equal successive observations do not prove stream continuity.
import (
	"crypto/subtle"
	"math/big"
	"time"
)

type SignedAccountFrameV15 struct {
	Environment              string    `json:"environment"`
	Coverage                 string    `json:"coverage"`
	AccountFingerprintSHA256 string    `json:"account_fingerprint_sha256"`
	StartedAt                time.Time `json:"started_at"`
	FinishedAt               time.Time `json:"finished_at"`
	WindowStartMS            int64     `json:"window_start_ms"`
	WindowEndMS              int64     `json:"window_end_ms"`
	AccountEvidenceSHA256    string    `json:"account_evidence_sha256"`
	LongQtyBTC               string    `json:"long_qty_btc"`
	ShortQtyBTC              string    `json:"short_qty_btc"`
	OpenNormalOrders         int       `json:"open_normal_orders"`
	OpenAlgoOrders           int       `json:"open_algo_orders"`
	ForeignExposure          bool      `json:"foreign_exposure"`
	UnverifiedCollateral     bool      `json:"unverified_collateral"`
	CanTrade                 bool      `json:"can_trade"`
	MultiAssetsMargin        bool      `json:"multi_assets_margin"`
}

type SignedAccountExposureReviewV15 struct {
	State                string         `json:"state"`
	GroupID              string         `json:"group_id"`
	FirstObservedAt      time.Time      `json:"first_observed_at,omitempty"`
	SecondObservedAt     time.Time      `json:"second_observed_at,omitempty"`
	SameSideOwnerCount   int            `json:"same_side_owner_count"`
	Reasons              []string       `json:"reasons"`
	Exposure             ExposureReview `json:"exposure"`
	EvidenceLimit        string         `json:"evidence_limit"`
	TradingAuthorized    bool           `json:"trading_authorized"`
	SubmitAuthorized     bool           `json:"submit_authorized"`
	CancelAuthorized     bool           `json:"cancel_authorized"`
	RetryAuthorized      bool           `json:"retry_authorized"`
	SettlementAuthorized bool           `json:"settlement_authorized"`
	ManualReviewRequired bool           `json:"manual_review_required"`
}

func blockedSignedAccountV15(f Fence, reason string) SignedAccountExposureReviewV15 {
	return SignedAccountExposureReviewV15{
		State: "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION", GroupID: f.Intent.GroupID,
		Reasons: []string{reason}, ManualReviewRequired: true,
		EvidenceLimit: "TWO_BOUNDED_SIGNED_REST_WINDOWS_NOT_CONTINUOUS_STREAM_OR_OWNER_PROOF",
	}
}

func validateSignedAccountFrameV15(f Fence, s SignedAccountFrameV15, now time.Time) bool {
	if s.Environment != "demo" || s.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		!isSHA256(s.AccountFingerprintSHA256) || subtle.ConstantTimeCompare([]byte(s.AccountFingerprintSHA256), []byte(f.Intent.AccountFingerprintSHA256)) != 1 ||
		!isSHA256(s.AccountEvidenceSHA256) ||
		s.StartedAt.IsZero() || s.FinishedAt.Before(s.StartedAt) || s.FinishedAt.After(now.Add(time.Second)) ||
		now.Sub(s.FinishedAt) > 5*time.Second || s.FinishedAt.Sub(s.StartedAt) > 30*time.Second ||
		s.WindowStartMS <= 0 || s.WindowEndMS != s.StartedAt.UnixMilli() || s.WindowStartMS >= s.WindowEndMS ||
		s.OpenNormalOrders < 0 || s.OpenAlgoOrders < 0 {
		return false
	}
	_, errLong := ownerQty(s.LongQtyBTC, true)
	_, errShort := ownerQty(s.ShortQtyBTC, true)
	return errLong == nil && errShort == nil
}

// InspectSignedAccountFramePairV15 checks independent account evidence before
// accepting even a descriptive pooled-owner classification. No mutation grant.
func InspectSignedAccountFramePairV15(f Fence, first, second SignedAccountFrameV15, now time.Time) SignedAccountExposureReviewV15 {
	out := blockedSignedAccountV15(f, "signed full account evidence missing or invalid")
	if f.validate() != nil || now.IsZero() || !validateSignedAccountFrameV15(f, first, now) || !validateSignedAccountFrameV15(f, second, now) {
		return out
	}
	out.FirstObservedAt, out.SecondObservedAt = first.FinishedAt, second.FinishedAt
	if second.StartedAt.Before(first.FinishedAt) || first.WindowStartMS != second.WindowStartMS ||
		first.AccountEvidenceSHA256 != second.AccountEvidenceSHA256 ||
		first.OpenNormalOrders != second.OpenNormalOrders || first.OpenAlgoOrders != second.OpenAlgoOrders ||
		first.ForeignExposure != second.ForeignExposure || first.UnverifiedCollateral != second.UnverifiedCollateral ||
		first.CanTrade != second.CanTrade || first.MultiAssetsMargin != second.MultiAssetsMargin {
		out.Reasons = []string{"signed full account data drifted or signed reads overlap"}
		return out
	}
	aLong, _ := ownerQty(first.LongQtyBTC, true)
	aShort, _ := ownerQty(first.ShortQtyBTC, true)
	bLong, _ := ownerQty(second.LongQtyBTC, true)
	bShort, _ := ownerQty(second.ShortQtyBTC, true)
	if aLong.Cmp(bLong) != 0 || aShort.Cmp(bShort) != 0 {
		out.Reasons = []string{"signed Hedge quantities changed between independent windows"}
		return out
	}
	if !first.CanTrade || first.MultiAssetsMargin || first.ForeignExposure || first.UnverifiedCollateral ||
		first.OpenNormalOrders != 0 || first.OpenAlgoOrders != 0 {
		out.Reasons = []string{"foreign exposure or collateral, open orders/Algos or invalid account capabilities"}
		return out
	}
	out.State = "SIGNED_BOUNDED_ACCOUNT_STABLE_OWNER_UNPROVEN"
	out.Reasons = []string{"bounded signed account data matches but does not prove ownership, intervening fills, private WS continuity or OCO"}
	return out
}

// ReviewSignedAccountFramesV15 checks *independent bounded observations* and
// then calls v14's conservative Hedge ownership assessor. The only allowed
// result is a descriptive, non-authorizing review. For a pooled Hedge side,
// even a precise quantity match remains unable to attribute exchange fills.
func ReviewSignedAccountFramesV15(f Fence, first, second SignedAccountFrameV15, owners []OwnerShare, now time.Time) SignedAccountExposureReviewV15 {
	out := InspectSignedAccountFramePairV15(f, first, second, now)
	if out.State != "SIGNED_BOUNDED_ACCOUNT_STABLE_OWNER_UNPROVEN" {
		return out
	}
	// Prevent a structurally valid, but stale or untrustworthy target from
	// becoming a pseudo-authorization through a forged struct or decimal text.
	exp := ExposureSnapshot{AccountFingerprintSHA256: first.AccountFingerprintSHA256,
		ObservedAt: second.FinishedAt, LongQtyBTC: second.LongQtyBTC, ShortQtyBTC: second.ShortQtyBTC,
		Owners: append([]OwnerShare(nil), owners...)}
	out.Exposure = AssessExposure(f, exp, now)
	out.SameSideOwnerCount = out.Exposure.SameSideOwnerCount
	switch out.Exposure.State {
	case ExposurePooled:
		out.State = "SIGNED_BOUNDED_POOLED_SIDE_MANUAL_RECONCILIATION"
		out.Reasons = []string{"multiple owners share an exchange Hedge side; no independent fill attribution or OCO"}
	case ExposureOneOwner:
		out.State = "SIGNED_BOUNDED_SINGLE_OWNER_NOT_AUTHORIZED"
		out.Reasons = []string{"two bounded account snapshots and local quantity agree; lifetime trades, private WS continuity and finance remain unproven"}
	default:
		out.State = "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION"
		out.Reasons = append([]string{"local owner shares cannot be reconciled to independently observed account"}, out.Exposure.Reasons...)
	}
	return out
}

// SignedTotalBTCV15 validates a signed quantity's sign and converts it to a
// nonnegative BTC quantity without accepting scientific notation or rounding.
func SignedTotalBTCV15(s string, short bool) (string, bool) {
	if s == "" || len(s) > 48 {
		return "", false
	}
	neg := len(s) > 0 && s[0] == '-'
	if neg {
		s = s[1:]
	}
	r, err := ownerQty(s, true)
	if err != nil {
		return "", false
	}
	if short && r.Sign() != 0 && !neg {
		return "", false
	}
	if !short && neg {
		return "", false
	}
	if r.Cmp(new(big.Rat).SetInt64(1000000000)) > 0 {
		return "", false
	}
	return s, true
}
