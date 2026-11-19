package trading

// External-close revalidation defines a fail-closed evidence boundary and a
// deterministic replay key. It is READ ONLY: no settlement writer exists.
// A caller MUST implement SignedCloseEvidenceReader using independently
// authenticated, fresh Binance signed REST responses, never a UI assertion.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"binance_trader/internal/exchange"
)

const ExternalCloseRevalidationBlocked = "REVALIDATED_EVIDENCE_POSTING_STILL_BLOCKED"

// SignedCloseEvidenceReader is deliberately separate from local review data.
// Production wiring must enforce signed request authentication and pagination.
type SignedCloseEvidenceReader interface {
	RefreshExternalCloseEvidence(context.Context, int64, int64) (SignedCloseEvidence, error)
}

type SignedCloseEvidence struct {
	EntryOrder                 exchange.OrderResult     `json:"entry_order_snapshot"`
	ExitOrder                  exchange.OrderResult     `json:"exit_order_snapshot"`
	HistoricalTrades           []exchange.AccountTrade  `json:"bounded_historical_trades,omitempty"`
	EntryFills                 []exchange.AccountTrade  `json:"entry_fills,omitempty"`
	ExitFills                  []exchange.AccountTrade  `json:"exit_fills,omitempty"`
	FundingRows                []exchange.FundingIncome `json:"funding_rows,omitempty"`
	EntryOrderID               int64                    `json:"entry_order_id"`
	ExitOrderID                int64                    `json:"exit_order_id"`
	EntryTradeIDs              []int64                  `json:"entry_trade_ids"`
	ExitTradeIDs               []int64                  `json:"exit_trade_ids"`
	AccountFlat                bool                     `json:"account_flat"`
	HistoricalCoverageComplete bool                     `json:"historical_coverage_complete"`
	FundingVerified            bool                     `json:"funding_verified"`
	ObservedAt                 time.Time                `json:"observed_at"`
	SignedDetailSHA256         string                   `json:"signed_detail_sha256,omitempty"`
	BoundedFundingRows         int                      `json:"bounded_funding_rows,omitempty"`
	BoundedFundingNetUSDT      string                   `json:"bounded_funding_net_usdt,omitempty"`
	HistoryFromMS              int64                    `json:"history_from_ms,omitempty"`
	HistoryToMS                int64                    `json:"history_to_ms,omitempty"`
	BoundedTradeRows           int                      `json:"bounded_trade_rows,omitempty"`
	TradeWindowsRead           int                      `json:"trade_windows_read,omitempty"`
	FundingWindowsRead         int                      `json:"funding_windows_read,omitempty"`
	HistoryScope               string                   `json:"history_scope,omitempty"`
}

type ExternalCloseRevalidation struct {
	Schema              int      `json:"schema"`
	Stage               string   `json:"stage"`
	ClientOrderID       string   `json:"client_order_id"`
	ProposalSHA256      string   `json:"proposal_sha256"`
	EvidenceFingerprint string   `json:"evidence_fingerprint"`
	ReplayKey           string   `json:"replay_key"`
	EvidenceMatched     bool     `json:"evidence_matched"`
	Blockers            []string `json:"blockers"`
	PostingAuthorized   bool     `json:"posting_authorized"`
	LifecycleChanged    bool     `json:"lifecycle_changed"`
	OwnershipChanged    bool     `json:"ownership_changed"`
	TradingAuthorized   bool     `json:"trading_authorized"`
	DigestSHA256        string   `json:"digest_sha256"`
}

func externalCloseHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (r ExternalCloseRevalidation) digest() string { r.DigestSHA256 = ""; return externalCloseHash(r) }
func ValidateExternalCloseRevalidation(r ExternalCloseRevalidation) error {
	if r.Schema != 1 || r.Stage != ExternalCloseRevalidationBlocked || !externalReviewID.MatchString(r.ClientOrderID) || !externalReviewHash.MatchString(r.ProposalSHA256) || !externalReviewHash.MatchString(r.EvidenceFingerprint) || !externalReviewHash.MatchString(r.ReplayKey) || len(r.Blockers) == 0 || r.PostingAuthorized || r.LifecycleChanged || r.OwnershipChanged || r.TradingAuthorized || r.DigestSHA256 != r.digest() {
		return errors.New("invalid or authorizing revalidation artifact")
	}
	return nil
}
func equalSignedIDs(a, b []int64) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	x := append([]int64(nil), a...)
	y := append([]int64(nil), b...)
	sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
	sort.Slice(y, func(i, j int) bool { return y[i] < y[j] })
	for i := range x {
		if x[i] <= 0 || (i > 0 && x[i] == x[i-1]) || x[i] != y[i] {
			return false
		}
	}
	return true
}

// RevalidateExternalClose retrieves evidence, compares trade identities and
// deliberately refuses posting even when the evidence agrees. Completeness
// and funding require further independent audit; this interface cannot prove
// either by itself. No funds or local journals are mutated.
func RevalidateExternalClose(ctx context.Context, p ExternalCloseSettlementProposal, reader SignedCloseEvidenceReader, now time.Time) (ExternalCloseRevalidation, error) {
	if err := ValidateExternalCloseSettlementProposal(p); err != nil {
		return ExternalCloseRevalidation{}, err
	}
	if reader == nil || now.IsZero() {
		return ExternalCloseRevalidation{}, errors.New("missing signed reader or reference time")
	}
	ev, err := reader.RefreshExternalCloseEvidence(ctx, p.EntryOrderID, p.ExitOrderID)
	if err != nil {
		return ExternalCloseRevalidation{}, fmt.Errorf("signed revalidation failed: %w", err)
	}
	if ev.EntryOrderID != p.EntryOrderID || ev.ExitOrderID != p.ExitOrderID || !equalSignedIDs(ev.EntryTradeIDs, p.EntryTradeIDs) || !equalSignedIDs(ev.ExitTradeIDs, p.ExitTradeIDs) || !ev.AccountFlat || ev.ObservedAt.IsZero() || ev.ObservedAt.After(now.Add(5*time.Second)) || now.Sub(ev.ObservedAt) > 2*time.Minute {
		return ExternalCloseRevalidation{}, errors.New("signed evidence identity, account flatness or freshness mismatch")
	}
	if ev.HistoryScope != "" && (ev.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" || ev.HistoryFromMS <= 0 || ev.HistoryToMS < ev.HistoryFromMS || ev.BoundedTradeRows < len(ev.EntryTradeIDs)+len(ev.ExitTradeIDs) || ev.TradeWindowsRead <= 0 || ev.FundingWindowsRead != ev.TradeWindowsRead || ev.HistoricalCoverageComplete || ev.FundingVerified || ev.SignedDetailSHA256 == "") {
		return ExternalCloseRevalidation{}, errors.New("invalid bounded history provenance")
	}
	if ev.SignedDetailSHA256 != "" && !externalReviewHash.MatchString(ev.SignedDetailSHA256) {
		return ExternalCloseRevalidation{}, errors.New("invalid signed evidence detail digest")
	}
	// Make fingerprint independent of observed time to obtain a stable replay
	// identity on fresh rereads; local financial values stay bound to proposal.
	fingerprint := externalCloseHash(struct {
		Entry, Exit int64
		In, Out     []int64
		Detail      string
	}{p.EntryOrderID, p.ExitOrderID, p.EntryTradeIDs, p.ExitTradeIDs, ev.SignedDetailSHA256})
	replayKey := externalCloseHash(struct{ Proposal, Evidence string }{p.Checksum, fingerprint})
	blockers := []string{"no crash-safe atomic posting implementation", "original lifecycle and pooled Hedge ownership not reconciled", "independent order status, quantities, fees and realized PnL not revalidated", "explicit operator settlement authorization absent"}
	if !ev.HistoricalCoverageComplete {
		blockers = append(blockers, "full historical trade coverage unverified")
	}
	if !ev.FundingVerified {
		blockers = append(blockers, "funding allocation unverified")
	}
	// Even an external reader reporting true is not by itself a proof.
	blockers = append(blockers, "external reader coverage/funding assertions require independent attestation")
	out := ExternalCloseRevalidation{Schema: 1, Stage: ExternalCloseRevalidationBlocked, ClientOrderID: p.ClientOrderID, ProposalSHA256: p.Checksum, EvidenceFingerprint: fingerprint, ReplayKey: replayKey, EvidenceMatched: true, Blockers: blockers}
	out.DigestSHA256 = out.digest()
	return out, ValidateExternalCloseRevalidation(out)
}
