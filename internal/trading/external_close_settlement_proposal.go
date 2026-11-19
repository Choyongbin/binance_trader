package trading

// ExternalCloseSettlementProposal is an immutable, NON-POSTED accounting
// adjustment design record. It is not an exchange-authenticated settlement.
// Its sole purpose is to make proposed allocations auditable without modifying
// original execution/ownership/reservation journals. In particular, a human
// ACKNOWLEDGED_REVIEW_ONLY receipt is NOT authorization to settle.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const ExternalSettlementUnposted = "EXTERNAL_CLOSE_ADJUSTMENT_PROPOSED_NOT_POSTED"

// The single global file is intentional: the same Binance pooled-side fills
// must never be proposed as allocations to two different model owners. A
// multi-incident writer requires a separately proven global serial allocator.
type ExternalCloseSettlementProposal struct {
	Schema        int          `json:"schema"`
	Stage         string       `json:"stage"`
	ClientOrderID string       `json:"client_order_id"`
	ModelID       string       `json:"model_id"`
	PositionSide  PositionSide `json:"position_side"`
	EntryOrderID  int64        `json:"signed_entry_order_id"`
	ExitOrderID   int64        `json:"signed_external_exit_order_id"`
	EntryTradeIDs []int64      `json:"signed_entry_trade_ids"`
	ExitTradeIDs  []int64      `json:"signed_exit_trade_ids"`
	// Candidate journal deltas only; no quantity or cash is actually posted.
	ProposedPositionDelta          string   `json:"proposed_position_delta_btc_not_posted"`
	SignedRealizedPnL              string   `json:"signed_exit_realized_pnl_usdt"`
	EntryCommission                string   `json:"signed_entry_commission_usdt"`
	ExitCommission                 string   `json:"signed_exit_commission_usdt"`
	ProvisionalNetExcludingFunding string   `json:"provisional_net_excluding_funding_usdt"`
	FundingStatus                  string   `json:"funding_status"`
	HistoricalScope                string   `json:"historical_scope"`
	LocalEvidenceSHA256            string   `json:"local_evidence_sha256"`
	SignedReadDigestSHA256         string   `json:"signed_read_digest_sha256"`
	RehearsalSHA256                string   `json:"rehearsal_sha256"`
	ReviewRootSHA256               string   `json:"review_root_sha256"`
	ReviewAcknowledgmentSHA256     string   `json:"review_acknowledgment_sha256"`
	CreatedAtUTC                   string   `json:"created_at_utc"`
	Blockers                       []string `json:"blockers"`
	PostingApplied                 bool     `json:"posting_applied"`
	OwnershipChanged               bool     `json:"ownership_changed"`
	LifecycleChanged               bool     `json:"lifecycle_changed"`
	SettlementAuthorized           bool     `json:"settlement_authorized"`
	TradingAuthorized              bool     `json:"trading_authorized"`
	Checksum                       string   `json:"checksum"`
}

func (p ExternalCloseSettlementProposal) digest() string {
	p.Checksum = ""
	b, _ := json.Marshal(p)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// This check is about internal record consistency only; SHA-256 is not an
// exchange signature, a MAC, or a proof of a complete funding/trade history.
func ValidateExternalCloseSettlementProposal(p ExternalCloseSettlementProposal) error {
	if p.Schema != 1 || p.Stage != ExternalSettlementUnposted || !externalReviewID.MatchString(p.ClientOrderID) || p.ModelID == "" ||
		(p.PositionSide != Long && p.PositionSide != Short) || p.EntryOrderID <= 0 || p.ExitOrderID <= 0 || p.EntryOrderID == p.ExitOrderID ||
		len(p.EntryTradeIDs) == 0 || len(p.ExitTradeIDs) == 0 || p.FundingStatus != "UNVERIFIED" ||
		p.HistoricalScope != "BOUNDED_NOT_PROVEN_COMPLETE" || !externalReviewHash.MatchString(p.LocalEvidenceSHA256) ||
		!externalReviewHash.MatchString(p.SignedReadDigestSHA256) || !externalReviewHash.MatchString(p.RehearsalSHA256) ||
		!externalReviewHash.MatchString(p.ReviewRootSHA256) || !externalReviewHash.MatchString(p.ReviewAcknowledgmentSHA256) ||
		!validReviewTime(p.CreatedAtUTC) || len(p.Blockers) < 3 || p.PostingApplied || p.OwnershipChanged ||
		p.LifecycleChanged || p.SettlementAuthorized || p.TradingAuthorized || p.Checksum != p.digest() {
		return errors.New("invalid or applied external close proposal")
	}
	seen := map[int64]struct{}{}
	for _, ids := range [][]int64{p.EntryTradeIDs, p.ExitTradeIDs} {
		last := int64(0)
		for _, id := range ids {
			if id <= 0 || id <= last {
				return errors.New("non-unique or unordered exchange trade IDs")
			}
			if _, ok := seen[id]; ok {
				return errors.New("exchange trade ID reused across entry and exit")
			}
			seen[id] = struct{}{}
			last = id
		}
	}
	delta, err := signedRat(p.ProposedPositionDelta)
	if err != nil || delta.Sign() >= 0 {
		return errors.New("invalid non-posted position delta")
	}
	pnl, err := signedRat(p.SignedRealizedPnL)
	if err != nil {
		return err
	}
	entryFee, err := signedRat(p.EntryCommission)
	if err != nil || entryFee.Sign() < 0 {
		return errors.New("invalid entry fee")
	}
	exitFee, err := signedRat(p.ExitCommission)
	if err != nil || exitFee.Sign() < 0 {
		return errors.New("invalid exit fee")
	}
	actual, err := signedRat(p.ProvisionalNetExcludingFunding)
	if err != nil {
		return err
	}
	expected := new(big.Rat).Sub(pnl, entryFee)
	expected.Sub(expected, exitFee)
	if expected.Cmp(actual) != 0 || formatOwnershipDecimal(delta) != p.ProposedPositionDelta ||
		formatOwnershipDecimal(pnl) != p.SignedRealizedPnL || formatOwnershipDecimal(entryFee) != p.EntryCommission ||
		formatOwnershipDecimal(exitFee) != p.ExitCommission || formatOwnershipDecimal(actual) != p.ProvisionalNetExcludingFunding {
		return errors.New("proposed quantities/cash amounts are not canonical or reconciled")
	}
	return nil
}

type ExternalCloseSettlementProposalStore struct {
	dataDir, dir string
	review       *ExternalCloseReviewLedger
}

func NewExternalCloseSettlementProposalStore(dataDir string) (*ExternalCloseSettlementProposalStore, error) {
	r, err := NewExternalCloseReviewLedger(dataDir)
	if err != nil {
		return nil, err
	}
	d := filepath.Join(r.dataDir, "trading", "external_close_settlement_proposals")
	if f, err := os.Lstat(d); err == nil {
		if !f.IsDir() || f.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe settlement proposal directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &ExternalCloseSettlementProposalStore{dataDir: r.dataDir, dir: d, review: r}, nil
}

func (s *ExternalCloseSettlementProposalStore) path() string {
	return filepath.Join(s.dir, "proposal.json")
}

func (s *ExternalCloseSettlementProposalStore) checkDir() error {
	fi, err := os.Lstat(s.dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe proposal directory")
	}
	children, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	if len(children) != 1 || children[0].Name() != "proposal.json" || children[0].IsDir() || children[0].Type()&os.ModeSymlink != 0 {
		return errors.New("missing/unknown proposal records: fail closed")
	}
	return nil
}

func (s *ExternalCloseSettlementProposalStore) expectedReview(clientID string) (ExternalCloseReviewState, error) {
	review, err := s.review.Load(clientID)
	if err != nil {
		return ExternalCloseReviewState{}, err
	}
	if review.Stage != ExternalReviewAcknowledged || len(review.Events) != 1 {
		return ExternalCloseReviewState{}, errors.New("explicit acknowledged operator review required; NOT settlement authorization")
	}
	return review, nil
}

// Load refuses stale/tampered review receipts, altered original entry/fill
// journals and unrecognized files. It never contacts Binance or posts entries.
func (s *ExternalCloseSettlementProposalStore) Load(clientID string) (ExternalCloseSettlementProposal, error) {
	if s == nil || !externalReviewID.MatchString(clientID) {
		return ExternalCloseSettlementProposal{}, errors.New("invalid proposal ID")
	}
	if err := s.review.ensureOffline(); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	if err := s.checkDir(); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	var p ExternalCloseSettlementProposal
	if err := strictReviewJSON(s.path(), &p); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	if err := ValidateExternalCloseSettlementProposal(p); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	review, err := s.expectedReview(clientID)
	if err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	r := review.Root.Rehearsal
	if p.ClientOrderID != clientID || p.ModelID != r.ModelID || p.PositionSide != r.PositionSide ||
		p.EntryOrderID != r.EntryOrderID || p.ExitOrderID != r.ExitOrderID ||
		p.LocalEvidenceSHA256 != r.LocalEvidenceSHA256 || p.SignedReadDigestSHA256 != r.SignedReadDigestSHA256 ||
		p.RehearsalSHA256 != r.PreviewSHA256 || p.ReviewRootSHA256 != review.Root.Checksum ||
		p.ReviewAcknowledgmentSHA256 != review.Events[0].Checksum ||
		p.ProposedPositionDelta != "-"+r.EntryQuantity || p.SignedRealizedPnL != r.SignedRealizedPnL ||
		p.EntryCommission != r.SignedEntryCommission || p.ExitCommission != r.SignedExitCommission ||
		p.ProvisionalNetExcludingFunding != r.ProvisionalNetExcludingFunding ||
		!equalExternalTradeIDs(p.EntryTradeIDs, r.EntryTradeIDs) || !equalExternalTradeIDs(p.ExitTradeIDs, r.ExitTradeIDs) {
		return ExternalCloseSettlementProposal{}, errors.New("proposal differs from verified local review receipt")
	}
	return p, nil
}

func equalExternalTradeIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Prepare writes one immutable global NOT-POSTED proposal. Existing proposals
// are returned idempotently. A different incident is denied to avoid pooled-
// side attribution ambiguity, including under concurrent creation (O_EXCL).
func (s *ExternalCloseSettlementProposalStore) Prepare(clientID string, now time.Time) (ExternalCloseSettlementProposal, error) {
	if s == nil || now.IsZero() {
		return ExternalCloseSettlementProposal{}, errors.New("missing proposal store/time")
	}
	review, err := s.expectedReview(clientID)
	if err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	r := review.Root.Rehearsal
	if err := validateExternalReviewPreview(r); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	if err := s.review.ensureUnchanged(r); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	idsIn, idsOut := append([]int64{}, r.EntryTradeIDs...), append([]int64{}, r.ExitTradeIDs...)
	sort.Slice(idsIn, func(i, j int) bool { return idsIn[i] < idsIn[j] })
	sort.Slice(idsOut, func(i, j int) bool { return idsOut[i] < idsOut[j] })
	p := ExternalCloseSettlementProposal{
		Schema: 1, Stage: ExternalSettlementUnposted, ClientOrderID: clientID, ModelID: r.ModelID, PositionSide: r.PositionSide,
		EntryOrderID: r.EntryOrderID, ExitOrderID: r.ExitOrderID, EntryTradeIDs: idsIn, ExitTradeIDs: idsOut,
		ProposedPositionDelta: "-" + r.EntryQuantity, SignedRealizedPnL: r.SignedRealizedPnL,
		EntryCommission: r.SignedEntryCommission, ExitCommission: r.SignedExitCommission,
		ProvisionalNetExcludingFunding: r.ProvisionalNetExcludingFunding, FundingStatus: "UNVERIFIED",
		HistoricalScope: "BOUNDED_NOT_PROVEN_COMPLETE", LocalEvidenceSHA256: r.LocalEvidenceSHA256,
		SignedReadDigestSHA256: r.SignedReadDigestSHA256, RehearsalSHA256: r.PreviewSHA256,
		ReviewRootSHA256: review.Root.Checksum, ReviewAcknowledgmentSHA256: review.Events[0].Checksum,
		CreatedAtUTC: now.UTC().Format(time.RFC3339Nano),
		Blockers: []string{
			"Operator review is a label, not authenticated independent Binance signed proof",
			"Complete signed history, funding allocation, commission assets and net profit are not verified",
			"Immutable original fills, ownership, reservations and MAY_HAVE_SENT must remain unchanged; no trading permission",
		},
	}
	p.Checksum = p.digest()
	if err := ValidateExternalCloseSettlementProposal(p); err != nil {
		return ExternalCloseSettlementProposal{}, err
	}
	if err := os.Mkdir(s.dir, 0700); err == nil {
		if err := syncAutoControlDirectory(filepath.Join(s.dataDir, "trading")); err != nil {
			return ExternalCloseSettlementProposal{}, err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return ExternalCloseSettlementProposal{}, err
	}
	fi, err := os.Lstat(s.dir)
	if err != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return ExternalCloseSettlementProposal{}, errors.New("unsafe proposal directory")
	}
	if _, err := os.Lstat(s.path()); err == nil {
		saved, err := s.Load(clientID)
		if err != nil {
			return ExternalCloseSettlementProposal{}, err
		}
		if saved.RehearsalSHA256 != r.PreviewSHA256 {
			return ExternalCloseSettlementProposal{}, errors.New("proposal changed; no overwrite")
		}
		return saved, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ExternalCloseSettlementProposal{}, err
	}
	if err := exclusiveReviewWrite(s.path(), p); err != nil {
		if errors.Is(err, os.ErrExist) {
			return s.Load(clientID)
		}
		return ExternalCloseSettlementProposal{}, err
	}
	if err := s.review.ensureUnchanged(r); err != nil {
		return ExternalCloseSettlementProposal{}, fmt.Errorf("staged but source drifted: %w", err)
	}
	return s.Load(clientID)
}
