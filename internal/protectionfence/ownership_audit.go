package protectionfence

// This is a deliberately NON-AUTHORIZING read-only assessment of an account
// exposure observation supplied by a caller. Callers MUST independently prove
// its signed REST provenance, full-account scope and journal continuity.
// Equal quantities do not establish execution ownership on a pooled Hedge side.
import (
	"errors"
	"math/big"
	"regexp"
	"time"
)

type ExposureState string

const (
	ExposureBlocked  ExposureState = "BLOCKED_UNVERIFIED_ACCOUNT_EXPOSURE"
	ExposurePooled   ExposureState = "POOLED_HEDGE_SIDE_MANUAL_RECONCILIATION"
	ExposureOneOwner ExposureState = "SINGLE_OWNER_BOUNDED_MATCH_NOT_AUTHORIZED"
)

// OwnerShare is NOT proof of fills or of the identity of a Binance order.
// GroupID is the immutable local entry-protection group (not model ID).
type OwnerShare struct {
	GroupID     string `json:"group_id"`
	Source      string `json:"source"` // "model" or "manual"
	ModelID     string `json:"model_id,omitempty"`
	Symbol      string `json:"symbol"`
	Side        string `json:"side"` // Binance position side LONG/SHORT
	QuantityBTC string `json:"quantity_btc"`
}

// ExposureSnapshot is caller-provided. Its fields are NOT signed by this
// structure; do not use it as a trading or settlement authorization token.
type ExposureSnapshot struct {
	AccountFingerprintSHA256 string       `json:"account_fingerprint_sha256"`
	ObservedAt               time.Time    `json:"observed_at"`
	LongQtyBTC               string       `json:"long_qty_btc"`
	ShortQtyBTC              string       `json:"short_qty_btc"`
	Owners                   []OwnerShare `json:"owners"`
}

type ExposureReview struct {
	State                ExposureState `json:"state"`
	GroupID              string        `json:"group_id"`
	TargetSide           string        `json:"target_side"`
	SameSideOwnerCount   int           `json:"same_side_owner_count"`
	Reasons              []string      `json:"reasons"`
	EvidenceLimit        string        `json:"evidence_limit"`
	ManualReviewRequired bool          `json:"manual_review_required"`
	TradingAuthorized    bool          `json:"trading_authorized"`
	SubmitAuthorized     bool          `json:"submit_authorized"`
	CancelAuthorized     bool          `json:"cancel_authorized"`
	RetryAuthorized      bool          `json:"retry_authorized"`
	SettlementAuthorized bool          `json:"settlement_authorized"`
}

var ownerModelID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)
var btcFixedDecimal = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,8})?$`)

func ownerQty(s string, allowZero bool) (*big.Rat, error) {
	if !btcFixedDecimal.MatchString(s) {
		return nil, errors.New("invalid BTC decimal")
	}
	n, ok := new(big.Rat).SetString(s)
	if !ok || n.Sign() < 0 || (!allowZero && n.Sign() == 0) {
		return nil, errors.New("invalid BTC quantity")
	}
	return n, nil
}

func assessExposure(f Fence, snapshot ExposureSnapshot, now time.Time) ExposureReview {
	out := ExposureReview{
		State: ExposureBlocked, GroupID: f.Intent.GroupID, TargetSide: f.Intent.PositionSide,
		ManualReviewRequired: true, EvidenceLimit: "CALLER_SUPPLIED_BOUNDED_OBSERVATION_NOT_ACCOUNT_HISTORY",
		Reasons: []string{},
	}
	block := func(reason string) ExposureReview { out.Reasons = append(out.Reasons, reason); return out }
	if f.validate() != nil || now.IsZero() {
		return block("immutable protection fence invalid")
	}
	if snapshot.AccountFingerprintSHA256 != f.Intent.AccountFingerprintSHA256 ||
		snapshot.ObservedAt.IsZero() || snapshot.ObservedAt.After(now.Add(time.Second)) ||
		now.Sub(snapshot.ObservedAt) > 5*time.Second {
		return block("account identity or bounded observation freshness invalid")
	}
	longQty, err := ownerQty(snapshot.LongQtyBTC, true)
	if err != nil {
		return block("invalid signed long quantity")
	}
	shortQty, err := ownerQty(snapshot.ShortQtyBTC, true)
	if err != nil {
		return block("invalid signed short quantity")
	}
	if len(snapshot.Owners) == 0 || len(snapshot.Owners) > 128 {
		return block("incomplete or oversized owner inventory")
	}
	totals := map[string]*big.Rat{"LONG": new(big.Rat), "SHORT": new(big.Rat)}
	seenGroup := make(map[string]bool, len(snapshot.Owners))
	targetFound := false
	for _, o := range snapshot.Owners {
		if !validID(o.GroupID) || seenGroup[o.GroupID] || o.Symbol != "BTCUSDT" ||
			(o.Side != "LONG" && o.Side != "SHORT") ||
			(o.Source != "model" && o.Source != "manual") ||
			(o.Source == "model" && !ownerModelID.MatchString(o.ModelID)) ||
			(o.Source == "manual" && o.ModelID != "") {
			return block("unknown or duplicate local position owner")
		}
		seenGroup[o.GroupID] = true
		qty, err := ownerQty(o.QuantityBTC, false)
		if err != nil {
			return block("invalid owner quantity")
		}
		totals[o.Side].Add(totals[o.Side], qty)
		if o.Side == f.Intent.PositionSide {
			out.SameSideOwnerCount++
		}
		if o.GroupID == f.Intent.GroupID {
			if o.Side != f.Intent.PositionSide || qty.Cmp(mustFenceQty(f.Intent.QuantityBTC)) != 0 {
				return block("fenced target quantity or side differs from local owner")
			}
			targetFound = true
		}
	}
	if !targetFound {
		return block("fenced target is missing from owner inventory")
	}
	if totals["LONG"].Cmp(longQty) != 0 || totals["SHORT"].Cmp(shortQty) != 0 {
		return block("account Hedge-side quantity differs from sum of local owners")
	}
	if out.SameSideOwnerCount != 1 {
		out.State = ExposurePooled
		out.Reasons = []string{"same-side Hedge owners share one exchange position; fills cannot be attributed to a single model"}
		return out
	}
	out.State = ExposureOneOwner
	out.Reasons = []string{"one bounded quantity match only; full history, independent signed provenance, OCO and funding/fee ownership unproven"}
	return out
}

func mustFenceQty(s string) *big.Rat {
	q, ok := new(big.Rat).SetString(s) // fence already validated
	if !ok {
		return new(big.Rat)
	}
	return q
}

// AssessExposure does not verify signatures or perform network reads. A
// successful value is always NON-AUTHORIZING, including the single-owner case.
func AssessExposure(f Fence, snapshot ExposureSnapshot, now time.Time) ExposureReview {
	return assessExposure(f, snapshot, now)
}
