package trading

// External-close posting gate is deliberately READ ONLY. It describes the
// prerequisites for a future crash-recoverable posting protocol; it cannot
// settle execution, clear MAY_HAVE_SENT, allocate ownership, or unlock orders.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const ExternalClosePostingBlocked = "POSTING_BLOCKED_REQUIRES_INDEPENDENT_SIGNED_EVIDENCE"

type ExternalClosePostingGate struct {
	Schema            int                         `json:"schema"`
	Stage             string                      `json:"stage"`
	ClientOrderID     string                      `json:"client_order_id"`
	ModelID           string                      `json:"model_id"`
	PositionSide      PositionSide                `json:"position_side"`
	ProposalSHA256    string                      `json:"proposal_sha256"`
	EvidenceChecks    []ExternalClosePostingCheck `json:"evidence_checks"`
	Blockers          []string                    `json:"blockers"`
	PostingAuthorized bool                        `json:"posting_authorized"`
	LifecycleChanged  bool                        `json:"lifecycle_changed"`
	OwnershipChanged  bool                        `json:"ownership_changed"`
	TradingAuthorized bool                        `json:"trading_authorized"`
	DigestSHA256      string                      `json:"digest_sha256"`
}

type ExternalClosePostingCheck struct {
	Name        string `json:"name"`
	Verified    bool   `json:"verified"`
	Explanation string `json:"explanation"`
}

func (g ExternalClosePostingGate) digest() string {
	g.DigestSHA256 = ""
	raw, _ := json.Marshal(g)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// PreviewExternalClosePosting never accepts user-supplied booleans asserting
// verified funding, complete history, or safe replay. These must eventually be
// derived independently from signed Binance records, durable journal state and
// an atomic/recoverable posting implementation. The current protocol is a
// strictly non-authorizing specification/diagnostic artifact.
func PreviewExternalClosePosting(p ExternalCloseSettlementProposal) (ExternalClosePostingGate, error) {
	if err := ValidateExternalCloseSettlementProposal(p); err != nil {
		return ExternalClosePostingGate{}, err
	}
	checks := []ExternalClosePostingCheck{
		{"independent_signed_order_refresh", false, "Fresh signed GET order and fills must be retrieved and double checked at posting time"},
		{"complete_trade_history", false, "Bounded BTCUSDT history is not proof of complete account trading history"},
		{"funding_allocation", false, "Funding, commission currency and full realized net allocation need independent signed verification"},
		{"original_entry_lifecycle", false, "MAY_HAVE_SENT and missing signed entry fills must first be recovered via owned-order lifecycle without replay"},
		{"pooled_hedge_side_ownership", false, "Pooled LONG/SHORT quantities, other models and external orders require fresh independent reconciliation"},
		{"crash_safe_idempotent_posting", false, "No durable write-ahead, atomic application and restart roll-forward exists for external close posting"},
		{"operator_settlement_authorization", false, "REVIEW_ONLY acknowledgment is not settlement authorization"},
	}
	g := ExternalClosePostingGate{Schema: 1, Stage: ExternalClosePostingBlocked, ClientOrderID: p.ClientOrderID, ModelID: p.ModelID, PositionSide: p.PositionSide, ProposalSHA256: p.Checksum, EvidenceChecks: checks, Blockers: make([]string, 0, len(checks))}
	for _, c := range checks {
		g.Blockers = append(g.Blockers, fmt.Sprintf("%s: %s", c.Name, c.Explanation))
	}
	g.DigestSHA256 = g.digest()
	return g, nil
}

func ValidateExternalClosePostingGate(g ExternalClosePostingGate) error {
	if g.Schema != 1 || g.Stage != ExternalClosePostingBlocked || !externalReviewHash.MatchString(g.ProposalSHA256) ||
		!externalReviewID.MatchString(g.ClientOrderID) || g.ModelID == "" || (g.PositionSide != Long && g.PositionSide != Short) ||
		len(g.EvidenceChecks) != 7 || len(g.Blockers) != 7 || g.PostingAuthorized || g.LifecycleChanged || g.OwnershipChanged || g.TradingAuthorized || g.DigestSHA256 != g.digest() {
		return errors.New("unsafe or corrupted posting gate")
	}
	reference := []string{"independent_signed_order_refresh", "complete_trade_history", "funding_allocation", "original_entry_lifecycle", "pooled_hedge_side_ownership", "crash_safe_idempotent_posting", "operator_settlement_authorization"}
	for i, c := range g.EvidenceChecks {
		if c.Name != reference[i] || c.Verified || c.Explanation == "" || g.Blockers[i] != fmt.Sprintf("%s: %s", c.Name, c.Explanation) {
			return errors.New("posting prerequisites were changed or bypassed")
		}
	}
	return nil
}

// LoadExternalClosePostingGate consults only the already-sealed local proposal
// and verified operator-review artifacts. It performs no writes or network IO.
func (s *ExternalCloseSettlementProposalStore) LoadExternalClosePostingGate(clientID string) (ExternalClosePostingGate, error) {
	if s == nil {
		return ExternalClosePostingGate{}, errors.New("nil store")
	}
	p, err := s.Load(clientID)
	if err != nil {
		return ExternalClosePostingGate{}, err
	}
	return PreviewExternalClosePosting(p)
}
