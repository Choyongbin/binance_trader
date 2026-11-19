package trading

// The posting intent is an append-only *reservation of evidence identities*,
// NOT a settlement entry. It creates a durable, global collision fence before
// any future external-close posting implementation can be considered. This
// deliberately has no operation capable of changing an execution lifecycle,
// fills, ownership, balances or the HTTP order lock.
//
// The original incident remains MAY_HAVE_SENT. An operator REVIEW_ONLY receipt
// and two signed observations are prerequisites, not financial authorization.
import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const ExternalPostingIntentReserved = "EXTERNAL_CLOSE_INTENT_RESERVED_NOT_POSTED"

type ExternalClosePostingIntent struct {
	Schema                     int          `json:"schema"`
	Stage                      string       `json:"stage"`
	ClientOrderID              string       `json:"client_order_id"`
	ModelID                    string       `json:"model_id"`
	PositionSide               PositionSide `json:"position_side"`
	EntryOrderID               int64        `json:"entry_order_id"`
	ExitOrderID                int64        `json:"exit_order_id"`
	EntryTradeIDs              []int64      `json:"entry_trade_ids"`
	ExitTradeIDs               []int64      `json:"exit_trade_ids"`
	ProposalSHA256             string       `json:"proposal_sha256"`
	ReplayRootSHA256           string       `json:"replay_root_sha256"`
	ReplayRecheckSHA256        string       `json:"replay_recheck_sha256"`
	ReplayKey                  string       `json:"replay_key"`
	SignedEvidenceDigestSHA256 string       `json:"signed_evidence_digest_sha256"`
	LocalEvidenceSHA256        string       `json:"local_evidence_sha256"`
	TransactionKey             string       `json:"transaction_key"`
	HistoryCoverageComplete    bool         `json:"history_coverage_complete"`
	FundingAllocated           bool         `json:"funding_allocated"`
	PostingApplied             bool         `json:"posting_applied"`
	LifecycleChanged           bool         `json:"lifecycle_changed"`
	OwnershipChanged           bool         `json:"ownership_changed"`
	SettlementAuthorized       bool         `json:"settlement_authorized"`
	TradingAuthorized          bool         `json:"trading_authorized"`
	Checksum                   string       `json:"checksum"`
}

func (r ExternalClosePostingIntent) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

func validatePostingIntent(r ExternalClosePostingIntent) error {
	if r.Schema != 1 || r.Stage != ExternalPostingIntentReserved || !externalReviewID.MatchString(r.ClientOrderID) ||
		r.ModelID == "" || (r.PositionSide != Long && r.PositionSide != Short) ||
		r.EntryOrderID <= 0 || r.ExitOrderID <= 0 || r.EntryOrderID == r.ExitOrderID ||
		!externalReviewHash.MatchString(r.ProposalSHA256) || !externalReviewHash.MatchString(r.ReplayRootSHA256) ||
		!externalReviewHash.MatchString(r.ReplayRecheckSHA256) || !externalReviewHash.MatchString(r.ReplayKey) ||
		!externalReviewHash.MatchString(r.SignedEvidenceDigestSHA256) || !externalReviewHash.MatchString(r.LocalEvidenceSHA256) ||
		!externalReviewHash.MatchString(r.TransactionKey) ||
		r.HistoryCoverageComplete || r.FundingAllocated || r.PostingApplied || r.LifecycleChanged ||
		r.OwnershipChanged || r.SettlementAuthorized || r.TradingAuthorized || r.Checksum != r.digest() ||
		len(r.EntryTradeIDs) == 0 || len(r.ExitTradeIDs) == 0 {
		return errors.New("invalid or authorizing external close posting intent")
	}
	seen := make(map[int64]bool)
	for _, ids := range [][]int64{r.EntryTradeIDs, r.ExitTradeIDs} {
		last := int64(0)
		for _, id := range ids {
			if id <= last || seen[id] {
				return errors.New("invalid or duplicated signed trade identity in intent")
			}
			seen[id] = true
			last = id
		}
	}
	expected := externalCloseHash(struct {
		Proposal, ReplayRoot, Recheck, Evidence string
		Entry, Exit                             []int64
	}{r.ProposalSHA256, r.ReplayRootSHA256, r.ReplayRecheckSHA256, r.SignedEvidenceDigestSHA256, r.EntryTradeIDs, r.ExitTradeIDs})
	if r.TransactionKey != expected {
		return errors.New("external close intent transaction identity changed")
	}
	return nil
}

type ExternalClosePostingIntentStore struct {
	mu     sync.Mutex
	replay *ExternalCloseReplayCheckpointStore
	dir    string
}

// No directories are created during construction or during Load.
func NewExternalClosePostingIntentStore(dataDir string) (*ExternalClosePostingIntentStore, error) {
	replay, err := NewExternalCloseReplayCheckpointStore(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(replay.proposal.dataDir, "trading", "external_close_posting_intent")
	if fi, err := os.Lstat(dir); err == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe external posting intent directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &ExternalClosePostingIntentStore{replay: replay, dir: dir}, nil
}

func (s *ExternalClosePostingIntentStore) intentPath() string {
	return filepath.Join(s.dir, "intent.json")
}

func (s *ExternalClosePostingIntentStore) checkDirectory(create bool) error {
	if s == nil || s.replay == nil {
		return errors.New("nil posting intent store")
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
		return errors.New("unsafe posting intent directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() != "intent.json" || e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe or unknown posting intent file: %s", e.Name())
		}
	}
	return nil
}

func (s *ExternalClosePostingIntentStore) expected(id string) (ExternalClosePostingIntent, error) {
	if s == nil || !externalReviewID.MatchString(id) {
		return ExternalClosePostingIntent{}, errors.New("invalid intent client order ID")
	}
	if err := s.replay.proposal.review.ensureOffline(); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	p, err := s.replay.proposal.Load(id)
	if err != nil {
		return ExternalClosePostingIntent{}, err
	}
	root, rechecked, err := s.replay.Load(id)
	if err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if rechecked == nil {
		return ExternalClosePostingIntent{}, errors.New("independent signed recheck required before reserving posting intent")
	}
	r := ExternalClosePostingIntent{
		Schema: 1, Stage: ExternalPostingIntentReserved, ClientOrderID: id, ModelID: p.ModelID,
		PositionSide: p.PositionSide, EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID,
		EntryTradeIDs: append([]int64(nil), p.EntryTradeIDs...), ExitTradeIDs: append([]int64(nil), p.ExitTradeIDs...),
		ProposalSHA256: p.Checksum, ReplayRootSHA256: root.Checksum, ReplayRecheckSHA256: rechecked.Checksum,
		ReplayKey: root.ReplayKey, SignedEvidenceDigestSHA256: root.EvidenceDigestSHA256,
		LocalEvidenceSHA256: p.LocalEvidenceSHA256,
	}
	r.TransactionKey = externalCloseHash(struct {
		Proposal, ReplayRoot, Recheck, Evidence string
		Entry, Exit                             []int64
	}{r.ProposalSHA256, r.ReplayRootSHA256, r.ReplayRecheckSHA256, r.SignedEvidenceDigestSHA256, r.EntryTradeIDs, r.ExitTradeIDs})
	r.Checksum = r.digest()
	return r, validatePostingIntent(r)
}

// Load revalidates the ENTIRE local dependency chain on every read. A global
// single-intent file is intentional: it denies a second pooled Hedge incident
// even when a different model ID claims the same Binance side or trade ID.
func (s *ExternalClosePostingIntentStore) Load(id string) (ExternalClosePostingIntent, error) {
	expected, err := s.expected(id)
	if err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if err := s.checkDirectory(false); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	var got ExternalClosePostingIntent
	if err := strictReviewJSON(s.intentPath(), &got); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if err := validatePostingIntent(got); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if got.Checksum != expected.Checksum {
		return ExternalClosePostingIntent{}, errors.New("posting intent differs from original signed/review evidence")
	}
	return got, nil
}

// Reserve creates an immutable, non-posted global transaction-identity fence.
// A crash during O_EXCL creation leaves an incomplete file: it is NOT removed
// or silently recreated. Same evidence replays idempotently after a crash.
func (s *ExternalClosePostingIntentStore) Reserve(id string) (ExternalClosePostingIntent, error) {
	if s == nil {
		return ExternalClosePostingIntent{}, errors.New("nil posting intent store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, err := s.expected(id)
	if err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if err := s.checkDirectory(true); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if len(entries) != 0 {
		return s.Load(id)
	}
	// Recheck the original sealed proposal immediately before the only write.
	if _, err := s.expected(id); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	raw, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		return ExternalClosePostingIntent{}, err
	}
	raw = append(raw, '\n')
	f, err := os.OpenFile(s.intentPath(), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		// Concurrent writers are only idempotent when the entire record and
		// its supporting original ledger are unchanged and fully valid.
		if existing, loadErr := s.Load(id); loadErr == nil && existing.Checksum == expected.Checksum {
			return existing, nil
		}
		return ExternalClosePostingIntent{}, err
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		return ExternalClosePostingIntent{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return ExternalClosePostingIntent{}, err
	}
	if err := f.Close(); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	if err := syncReplayDir(s.dir); err != nil {
		return ExternalClosePostingIntent{}, err
	}
	return s.Load(id)
}
