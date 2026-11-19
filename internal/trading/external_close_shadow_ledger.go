package trading

// ExternalCloseShadowLedger is a *non-posting* crash/restart rehearsal for an
// externally closed position. It replays immutable fill identities into an
// isolated projection and persists two exclusive sidecar records. It NEVER
// modifies the execution lifecycle, reservation, fills, ownership or PnL.
// These records are NOT authority to settle and cannot resolve pooled Hedge
// ownership, full historical coverage or funding allocation.
import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
)

const (
	ExternalShadowPrepared = "EXTERNAL_CLOSE_SHADOW_PREPARED_NOT_POSTED"
	ExternalShadowReplayed = "EXTERNAL_CLOSE_SHADOW_REPLAYED_NOT_POSTED"
)

type ExternalCloseShadowRecord struct {
	Schema                     int          `json:"schema"`
	Stage                      string       `json:"stage"`
	ClientOrderID              string       `json:"client_order_id"`
	ModelID                    string       `json:"model_id"`
	PositionSide               PositionSide `json:"position_side"`
	PostingIntentSHA256        string       `json:"posting_intent_sha256"`
	TransactionKey             string       `json:"transaction_key"`
	ReplayKey                  string       `json:"replay_key"`
	EntryTradeIDs              []int64      `json:"entry_trade_ids"`
	ExitTradeIDs               []int64      `json:"exit_trade_ids"`
	EntryQuantity              string       `json:"entry_quantity_btc"`
	ExitQuantity               string       `json:"exit_quantity_btc"`
	SimulatedRemainingQuantity string       `json:"simulated_remaining_quantity_btc"`
	SignedExitRealizedPnL      string       `json:"signed_exit_realized_pnl_usdt"`
	EntryCommission            string       `json:"entry_commission_usdt"`
	ExitCommission             string       `json:"exit_commission_usdt"`
	NetExcludingFunding        string       `json:"provisional_net_excluding_funding_usdt"`
	PreviousChecksum           string       `json:"previous_checksum,omitempty"`
	Checksum                   string       `json:"checksum"`
	FundingVerified            bool         `json:"funding_verified"`
	HistoryComplete            bool         `json:"history_complete"`
	Applied                    bool         `json:"applied"`
	OwnershipChanged           bool         `json:"ownership_changed"`
	LifecycleChanged           bool         `json:"lifecycle_changed"`
	SettlementAuthorized       bool         `json:"settlement_authorized"`
	TradingAuthorized          bool         `json:"trading_authorized"`
}

func (r ExternalCloseShadowRecord) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

// projectShadow operates solely on signed ID sets that are already sealed in
// the parent records. This is NOT a signed Binance GET; it cannot authorize
// posting. The exact net equation is recomputed, not copied without checking.
func projectShadow(intent ExternalClosePostingIntent, p ExternalCloseSettlementProposal) (ExternalCloseShadowRecord, error) {
	if err := validatePostingIntent(intent); err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	if err := ValidateExternalCloseSettlementProposal(p); err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	if p.Checksum != intent.ProposalSHA256 || p.ClientOrderID != intent.ClientOrderID ||
		p.ModelID != intent.ModelID || p.PositionSide != intent.PositionSide ||
		len(p.EntryTradeIDs) != len(intent.EntryTradeIDs) || len(p.ExitTradeIDs) != len(intent.ExitTradeIDs) {
		return ExternalCloseShadowRecord{}, errors.New("posting intent does not bind the proposal")
	}
	ids := make(map[int64]struct{})
	for i, id := range intent.EntryTradeIDs {
		if id != p.EntryTradeIDs[i] || id <= 0 {
			return ExternalCloseShadowRecord{}, errors.New("entry trade identity changed")
		}
		ids[id] = struct{}{}
	}
	for i, id := range intent.ExitTradeIDs {
		if id != p.ExitTradeIDs[i] || id <= 0 {
			return ExternalCloseShadowRecord{}, errors.New("exit trade identity changed")
		}
		if _, exists := ids[id]; exists {
			return ExternalCloseShadowRecord{}, errors.New("trade applied twice in shadow replay")
		}
		ids[id] = struct{}{}
	}
	delta, err := signedRat(p.ProposedPositionDelta)
	if err != nil || delta.Sign() >= 0 {
		return ExternalCloseShadowRecord{}, errors.New("invalid proposed exit delta")
	}
	entry := new(big.Rat).Neg(delta)
	exit := new(big.Rat).Set(entry)
	remaining := new(big.Rat).Sub(entry, exit)
	realized, e1 := signedRat(p.SignedRealizedPnL)
	entryFee, e2 := signedRat(p.EntryCommission)
	exitFee, e3 := signedRat(p.ExitCommission)
	if e1 != nil || e2 != nil || e3 != nil || entryFee.Sign() < 0 || exitFee.Sign() < 0 || remaining.Sign() != 0 {
		return ExternalCloseShadowRecord{}, errors.New("invalid shadow financial inputs")
	}
	net := new(big.Rat).Sub(realized, entryFee)
	net.Sub(net, exitFee)
	if formatOwnershipDecimal(net) != p.ProvisionalNetExcludingFunding {
		return ExternalCloseShadowRecord{}, errors.New("provisional PnL mismatch")
	}
	r := ExternalCloseShadowRecord{
		Schema: 1, Stage: ExternalShadowPrepared, ClientOrderID: intent.ClientOrderID,
		ModelID: intent.ModelID, PositionSide: intent.PositionSide,
		PostingIntentSHA256: intent.Checksum, TransactionKey: intent.TransactionKey,
		ReplayKey: intent.ReplayKey, EntryTradeIDs: append([]int64(nil), intent.EntryTradeIDs...),
		ExitTradeIDs:  append([]int64(nil), intent.ExitTradeIDs...),
		EntryQuantity: formatOwnershipDecimal(entry), ExitQuantity: formatOwnershipDecimal(exit),
		SimulatedRemainingQuantity: formatOwnershipDecimal(remaining),
		SignedExitRealizedPnL:      formatOwnershipDecimal(realized),
		EntryCommission:            formatOwnershipDecimal(entryFee), ExitCommission: formatOwnershipDecimal(exitFee),
		NetExcludingFunding: formatOwnershipDecimal(net),
	}
	r.Checksum = r.digest()
	return r, nil
}

func validateShadowRecord(r ExternalCloseShadowRecord, expected ExternalCloseShadowRecord) error {
	if r.Schema != 1 || (r.Stage != ExternalShadowPrepared && r.Stage != ExternalShadowReplayed) ||
		r.Applied || r.FundingVerified || r.HistoryComplete || r.OwnershipChanged ||
		r.LifecycleChanged || r.SettlementAuthorized || r.TradingAuthorized ||
		!externalReviewID.MatchString(r.ClientOrderID) || !externalReviewHash.MatchString(r.Checksum) ||
		r.Checksum != r.digest() || r.Checksum != expected.Checksum {
		return errors.New("unsafe, altered or posting-authorizing shadow record")
	}
	return nil
}

type ExternalCloseShadowLedger struct {
	mu      sync.Mutex
	intents *ExternalClosePostingIntentStore
	dir     string
}

func NewExternalCloseShadowLedger(dataDir string) (*ExternalCloseShadowLedger, error) {
	intents, err := NewExternalClosePostingIntentStore(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(intents.replay.proposal.dataDir, "trading", "external_close_shadow_replay")
	if fi, err := os.Lstat(dir); err == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe shadow directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &ExternalCloseShadowLedger{intents: intents, dir: dir}, nil
}

func (s *ExternalCloseShadowLedger) path(stage string) string {
	if stage == ExternalShadowPrepared {
		return filepath.Join(s.dir, "prepared.json")
	}
	return filepath.Join(s.dir, "replayed.json")
}
func (s *ExternalCloseShadowLedger) checkDir(create bool) error {
	if s == nil || s.intents == nil {
		return errors.New("nil shadow ledger")
	}
	if create {
		if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	fi, err := os.Lstat(s.dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe shadow ledger directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if (e.Name() != "prepared.json" && e.Name() != "replayed.json") || e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe or unexpected shadow record: %s", e.Name())
		}
	}
	return nil
}
func (s *ExternalCloseShadowLedger) expected(id string) (ExternalCloseShadowRecord, error) {
	// Load verifies the entire sealed dependency chain and the offline locks.
	intent, err := s.intents.Load(id)
	if err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	proposal, err := s.intents.replay.proposal.Load(id)
	if err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	return projectShadow(intent, proposal)
}
func expectedShadowReplayed(root ExternalCloseShadowRecord) ExternalCloseShadowRecord {
	event := root
	event.Stage = ExternalShadowReplayed
	event.PreviousChecksum = root.Checksum
	event.Checksum = event.digest()
	return event
}

// Load validates upstream evidence on EVERY call. Missing replayed.json means
// PREPARED, not posted; an orphan/torn record is never automatically repaired.
func (s *ExternalCloseShadowLedger) Load(id string) (ExternalCloseShadowRecord, *ExternalCloseShadowRecord, error) {
	expected, err := s.expected(id)
	if err != nil {
		return ExternalCloseShadowRecord{}, nil, err
	}
	if err := s.checkDir(false); err != nil {
		return ExternalCloseShadowRecord{}, nil, err
	}
	var root ExternalCloseShadowRecord
	if err := strictReviewJSON(s.path(ExternalShadowPrepared), &root); err != nil {
		return ExternalCloseShadowRecord{}, nil, err
	}
	if err := validateShadowRecord(root, expected); err != nil {
		return ExternalCloseShadowRecord{}, nil, err
	}
	var event ExternalCloseShadowRecord
	err = strictReviewJSON(s.path(ExternalShadowReplayed), &event)
	if errors.Is(err, os.ErrNotExist) {
		return root, nil, nil
	}
	if err != nil {
		return ExternalCloseShadowRecord{}, nil, err
	}
	if err := validateShadowRecord(event, expectedShadowReplayed(root)); err != nil {
		return ExternalCloseShadowRecord{}, nil, err
	}
	return root, &event, nil
}

func (s *ExternalCloseShadowLedger) Prepare(id string) (ExternalCloseShadowRecord, error) {
	if s == nil {
		return ExternalCloseShadowRecord{}, errors.New("nil shadow ledger")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, err := s.expected(id)
	if err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	if err := s.checkDir(true); err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	if _, err := os.Lstat(s.path(ExternalShadowPrepared)); err == nil {
		got, _, readErr := s.Load(id)
		return got, readErr
	} else if !errors.Is(err, os.ErrNotExist) {
		return ExternalCloseShadowRecord{}, err
	}
	// Never repair orphan replay events or reuse the global Hedge fence.
	entries, err := os.ReadDir(s.dir)
	if err != nil || len(entries) != 0 {
		return ExternalCloseShadowRecord{}, errors.New("orphan or unknown shadow records")
	}
	if err := exclusiveReviewWrite(s.path(ExternalShadowPrepared), expected); err != nil {
		if got, _, loadErr := s.Load(id); loadErr == nil && got.Checksum == expected.Checksum {
			return got, nil
		}
		return ExternalCloseShadowRecord{}, err
	}
	got, _, err := s.Load(id)
	return got, err
}

// Replay is an append-only simulated transition. A crash after Prepare is
// resumed by recomputing from immutable upstream evidence. It never imports
// exchange fills into the real OwnershipStore or settles the LifecycleStore.
func (s *ExternalCloseShadowLedger) Replay(id string) (ExternalCloseShadowRecord, error) {
	if s == nil {
		return ExternalCloseShadowRecord{}, errors.New("nil shadow ledger")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	root, previous, err := s.Load(id)
	if err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	expected := expectedShadowReplayed(root)
	if previous != nil {
		return *previous, nil
	}
	if err := exclusiveReviewWrite(s.path(ExternalShadowReplayed), expected); err != nil {
		_, got, loadErr := s.Load(id)
		if loadErr == nil && got != nil && got.Checksum == expected.Checksum {
			return *got, nil
		}
		return ExternalCloseShadowRecord{}, err
	}
	if err := syncReplayDir(s.dir); err != nil {
		return ExternalCloseShadowRecord{}, err
	}
	_, got, err := s.Load(id)
	if err != nil || got == nil {
		return ExternalCloseShadowRecord{}, errors.New("replayed record not durable or not valid")
	}
	return *got, nil
}
