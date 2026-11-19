package trading

// Package A phase 2. Immutable, crash-visible cross-ledger settlement BARRIER.
// This journal binds a verified dual-ledger preview to the fresh local comparison
// of an archived signed entry, but cannot post a financial or execution event.
// It intentionally has no COMMITTED/SETTLED state. An archived signed HTTP GET
// is not a fresh authenticated exchange attestation.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
)

const (
	ExternalSettlementJournalPrepared = "SETTLEMENT_JOURNAL_PREPARED_NOT_POSTED"
	ExternalSettlementJournalFenced   = "SETTLEMENT_JOURNAL_FENCED_NOT_POSTED"
)

type ExternalSettlementJournalRecord struct {
	Schema                   int          `json:"schema"`
	Stage                    string       `json:"stage"`
	ClientOrderID            string       `json:"client_order_id"`
	ModelID                  string       `json:"model_id"`
	PositionSide             PositionSide `json:"position_side"`
	TransactionKey           string       `json:"transaction_key"`
	DualLedgerVerifiedSHA256 string       `json:"dual_ledger_verified_sha256"`
	EntryArchiveSHA256       string       `json:"entry_archive_sha256"`
	EntryRecoveryPlanSHA256  string       `json:"entry_recovery_plan_sha256"`
	LocalEvidenceSHA256      string       `json:"local_evidence_sha256"`
	LifecycleStage           string       `json:"lifecycle_stage"`
	EntryTradeIDs            []int64      `json:"entry_trade_ids"`
	ExitTradeIDs             []int64      `json:"exit_trade_ids"`
	MissingEntryTradeIDs     []int64      `json:"missing_entry_trade_ids"`
	BlockerCodes             []string     `json:"blocker_codes"`
	PreviousChecksum         string       `json:"previous_checksum,omitempty"`
	Checksum                 string       `json:"checksum"`
	EntryRecoveryApplied     bool         `json:"entry_recovery_applied"`
	LifecycleChanged         bool         `json:"lifecycle_changed"`
	OwnershipChanged         bool         `json:"ownership_changed"`
	PostingApplied           bool         `json:"posting_applied"`
	SettlementAuthorized     bool         `json:"settlement_authorized"`
	TradingAuthorized        bool         `json:"trading_authorized"`
}

func (r ExternalSettlementJournalRecord) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

type ExternalSettlementJournalStore struct {
	mu   sync.Mutex
	dual *ExternalCloseDualLedgerPlanStore
	dir  string
	// Empty means the historical single-event journal. Event journals bind
	// one immutable clientOrderID to an isolated, append-only directory.
	scopedClientID string
	eventsRoot     string
	legacyDir      string
}

func NewExternalSettlementJournalStore(dataDir string) (*ExternalSettlementJournalStore, error) {
	dual, err := NewExternalCloseDualLedgerPlanStore(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(dual.shadow.intents.replay.proposal.dataDir, "trading", "external_close_settlement_journal")
	if fi, e := os.Lstat(dir); e == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe settlement journal directory")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	return &ExternalSettlementJournalStore{dual: dual, dir: dir}, nil
}

// NewExternalSettlementEventJournalStore isolates NEW non-posted barriers by
// original entry clientOrderID. It does not migrate or modify historical
// single-directory journals, and refuses to duplicate a legacy incident.
// Construction is read-only; neither journal nor trading files are written.
func NewExternalSettlementEventJournalStore(dataDir, clientOrderID string) (*ExternalSettlementJournalStore, error) {
	if !externalReviewID.MatchString(clientOrderID) {
		return nil, errors.New("invalid event-scoped settlement journal clientOrderID")
	}
	s, err := NewExternalSettlementJournalStore(dataDir)
	if err != nil {
		return nil, err
	}
	s.legacyDir = s.dir
	s.eventsRoot = filepath.Join(filepath.Dir(s.dir), "external_close_settlement_journal_events")
	s.dir = filepath.Join(s.eventsRoot, clientOrderID)
	s.scopedClientID = clientOrderID
	if err := s.checkEventsRoot(false); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(s.dir); err == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe scoped settlement journal directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return s, nil
}

// Only valid event IDs may appear beneath the event root. Reject unusual
// files/symlinks rather than silently ignoring evidence from another event.
func (s *ExternalSettlementJournalStore) checkEventsRoot(create bool) error {
	if s == nil || s.eventsRoot == "" {
		return nil
	}
	if create {
		if err := os.Mkdir(s.eventsRoot, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	fi, err := os.Lstat(s.eventsRoot)
	if errors.Is(err, os.ErrNotExist) && !create {
		return nil
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe settlement event journal root")
	}
	entries, err := os.ReadDir(s.eventsRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !externalReviewID.MatchString(entry.Name()) || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe settlement event journal root entry: %s", entry.Name())
		}
	}
	return nil
}

// Prevent the same transaction being staged once in the old single-event
// journal and again in the new event-scoped journal. A corrupt legacy sidecar
// is a blocker, not an invitation to overwrite or auto-migrate it.
func (s *ExternalSettlementJournalStore) checkLegacyConflict() error {
	if s.scopedClientID == "" {
		return nil
	}
	legacy := &ExternalSettlementJournalStore{dual: s.dual, dir: s.legacyDir}
	if err := legacy.checkDir(false); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("unsafe legacy settlement journal: %w", err)
	}
	entries, err := os.ReadDir(s.legacyDir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	var record ExternalSettlementJournalRecord
	if err := strictReviewJSON(legacy.path(ExternalSettlementJournalPrepared), &record); err != nil {
		return fmt.Errorf("incomplete legacy settlement journal: %w", err)
	}
	if record.Schema != 1 || record.Stage != ExternalSettlementJournalPrepared ||
		!externalReviewID.MatchString(record.ClientOrderID) || record.Checksum != record.digest() ||
		record.PostingApplied || record.EntryRecoveryApplied || record.LifecycleChanged ||
		record.OwnershipChanged || record.SettlementAuthorized || record.TradingAuthorized {
		return errors.New("invalid legacy settlement journal checkpoint")
	}
	// Even an unrelated historical case may not hide a torn or counterfeit
	// fenced checkpoint; reject it before allowing a new event to proceed.
	var fence ExternalSettlementJournalRecord
	if err := strictReviewJSON(legacy.path(ExternalSettlementJournalFenced), &fence); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("corrupt legacy fenced settlement journal: %w", err)
		}
	} else if fence.Schema != 1 || fence.Stage != ExternalSettlementJournalFenced ||
		fence.ClientOrderID != record.ClientOrderID || fence.PreviousChecksum != record.Checksum ||
		fence.Checksum != fence.digest() || fence.PostingApplied || fence.EntryRecoveryApplied ||
		fence.LifecycleChanged || fence.OwnershipChanged || fence.SettlementAuthorized || fence.TradingAuthorized {
		return errors.New("invalid legacy fenced settlement journal")
	}
	if record.ClientOrderID == s.scopedClientID {
		return errors.New("legacy settlement journal already records this clientOrderID; inspect legacy explicitly, no migration")
	}
	return nil
}

func (s *ExternalSettlementJournalStore) path(stage string) string {
	if stage == ExternalSettlementJournalPrepared {
		return filepath.Join(s.dir, "prepared.json")
	}
	return filepath.Join(s.dir, "fenced.json")
}
func (s *ExternalSettlementJournalStore) checkDir(create bool) error {
	if s == nil || s.dual == nil {
		return errors.New("nil settlement journal")
	}
	if err := s.checkEventsRoot(create); err != nil {
		return err
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
		return errors.New("unsafe settlement journal directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if (e.Name() != "prepared.json" && e.Name() != "fenced.json") || e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unknown or unsafe settlement journal entry: %s", e.Name())
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular settlement journal entry: %s", e.Name())
		}
	}
	return nil
}
func (s *ExternalSettlementJournalStore) expected(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalSettlementJournalRecord, error) {
	if s == nil || !externalReviewID.MatchString(id) || archive.Gap.ClientOrderID != id ||
		(s.scopedClientID != "" && s.scopedClientID != id) {
		return ExternalSettlementJournalRecord{}, errors.New("entry archive must match settlement transaction client ID")
	}
	if err := s.checkEventsRoot(false); err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if err := s.checkLegacyConflict(); err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if err := s.dual.shadow.intents.replay.proposal.review.ensureOffline(); err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	dual, verified, err := s.dual.Load(id)
	if err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if verified == nil || verified.Stage != ExternalDualLedgerVerified {
		return ExternalSettlementJournalRecord{}, errors.New("verified sealed dual-ledger prerequisite missing")
	}
	entry, err := PlanPackageAEntryRecovery(ctx, s.dual.shadow.intents.replay.proposal.dataDir, archive)
	if err != nil {
		return ExternalSettlementJournalRecord{}, fmt.Errorf("entry recovery evidence: %w", err)
	}
	proposal, err := s.dual.shadow.intents.replay.proposal.Load(id)
	if err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if proposal.EntryOrderID != entry.OrderID || proposal.ModelID != entry.ModelID || proposal.PositionSide != entry.PositionSide ||
		dual.ModelID != entry.ModelID || dual.PositionSide != entry.PositionSide || dual.LocalEvidenceSHA256 != entry.LocalEvidenceSHA256 ||
		len(proposal.EntryTradeIDs) != len(archive.Fills) {
		return ExternalSettlementJournalRecord{}, errors.New("entry recovery evidence disagrees with verified dual ledger")
	}
	entryIDs := make([]int64, 0, len(archive.Fills))
	for _, trade := range archive.Fills {
		entryIDs = append(entryIDs, trade.TradeID)
	}
	// Compare sets rather than relying on the capture order of signed GET results.
	sortedEntryIDs := append([]int64(nil), entryIDs...)
	sort.Slice(sortedEntryIDs, func(i, j int) bool { return sortedEntryIDs[i] < sortedEntryIDs[j] })
	if !reflect.DeepEqual(sortedEntryIDs, proposal.EntryTradeIDs) {
		return ExternalSettlementJournalRecord{}, errors.New("archived entry trade identities disagree with close proposal")
	}
	missing := make([]int64, 0, len(entry.Candidates))
	for _, c := range entry.Candidates {
		missing = append(missing, c.TradeID)
	}
	blockers := []string{"FRESH_INDEPENDENT_SIGNED_RECHECK_REQUIRED", "COMPLETE_ACCOUNT_HISTORY_NOT_ATTESTED", "MODEL_FUNDING_ATTRIBUTION_UNVERIFIED", "CROSS_LEDGER_ATOMIC_POSTING_NOT_IMPLEMENTED", "HTTP_423_ORDER_LOCK_REQUIRED"}
	if len(missing) > 0 {
		blockers = append(blockers, "ENTRY_FILLS_NOT_IMPORTED")
	}
	if entry.LifecycleStage == "MISSING_LIFECYCLE_BLOCKED" {
		blockers = append(blockers, "ENTRY_LIFECYCLE_MISSING")
	}
	if entry.LifecycleStage != string(ExecutionWholeReconciled) && entry.LifecycleStage != string(ExecutionSettled) {
		blockers = append(blockers, "ENTRY_LIFECYCLE_NOT_RECONCILED")
	}
	rec := ExternalSettlementJournalRecord{Schema: 1, Stage: ExternalSettlementJournalPrepared, ClientOrderID: id,
		ModelID: dual.ModelID, PositionSide: dual.PositionSide, TransactionKey: dual.TransactionKey,
		DualLedgerVerifiedSHA256: verified.Checksum, EntryArchiveSHA256: archive.Checksum, EntryRecoveryPlanSHA256: entry.Checksum,
		LocalEvidenceSHA256: entry.LocalEvidenceSHA256, LifecycleStage: entry.LifecycleStage,
		EntryTradeIDs: sortedEntryIDs, ExitTradeIDs: append([]int64(nil), proposal.ExitTradeIDs...),
		MissingEntryTradeIDs: missing, BlockerCodes: blockers}
	rec.Checksum = rec.digest()
	return rec, validateExternalSettlementJournalRecord(rec, rec)
}

func expectedSettlementJournalFenced(r ExternalSettlementJournalRecord) ExternalSettlementJournalRecord {
	next := r
	next.Stage = ExternalSettlementJournalFenced
	next.PreviousChecksum = r.Checksum
	next.Checksum = next.digest()
	return next
}
func validateExternalSettlementJournalRecord(got, expected ExternalSettlementJournalRecord) error {
	if got.Schema != 1 || got.Stage != expected.Stage || got.Checksum != got.digest() || got.Checksum != expected.Checksum ||
		!externalReviewHash.MatchString(got.Checksum) || !externalReviewHash.MatchString(got.EntryArchiveSHA256) ||
		!externalReviewHash.MatchString(got.EntryRecoveryPlanSHA256) || !externalReviewHash.MatchString(got.DualLedgerVerifiedSHA256) ||
		got.PostingApplied || got.LifecycleChanged || got.OwnershipChanged || got.EntryRecoveryApplied || got.SettlementAuthorized || got.TradingAuthorized ||
		len(got.BlockerCodes) < 5 || !includesJournalBlocker(got.BlockerCodes, "HTTP_423_ORDER_LOCK_REQUIRED") ||
		!includesJournalBlocker(got.BlockerCodes, "CROSS_LEDGER_ATOMIC_POSTING_NOT_IMPLEMENTED") {
		return errors.New("inconsistent or authorizing settlement journal")
	}
	return nil
}
func includesJournalBlocker(v []string, want string) bool {
	for _, x := range v {
		if x == want {
			return true
		}
	}
	return false
}
func (s *ExternalSettlementJournalStore) Load(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalSettlementJournalRecord, *ExternalSettlementJournalRecord, error) {
	expected, err := s.expected(ctx, id, archive)
	if err != nil {
		return ExternalSettlementJournalRecord{}, nil, err
	}
	if err := s.checkDir(false); err != nil {
		return ExternalSettlementJournalRecord{}, nil, err
	}
	var root ExternalSettlementJournalRecord
	if err := strictReviewJSON(s.path(ExternalSettlementJournalPrepared), &root); err != nil {
		return ExternalSettlementJournalRecord{}, nil, err
	}
	if err := validateExternalSettlementJournalRecord(root, expected); err != nil {
		return ExternalSettlementJournalRecord{}, nil, err
	}
	var ev ExternalSettlementJournalRecord
	err = strictReviewJSON(s.path(ExternalSettlementJournalFenced), &ev)
	if errors.Is(err, os.ErrNotExist) {
		return root, nil, nil
	}
	if err != nil {
		return ExternalSettlementJournalRecord{}, nil, err
	}
	if err := validateExternalSettlementJournalRecord(ev, expectedSettlementJournalFenced(root)); err != nil {
		return ExternalSettlementJournalRecord{}, nil, err
	}
	return root, &ev, nil
}
func (s *ExternalSettlementJournalStore) Prepare(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalSettlementJournalRecord, error) {
	if s == nil {
		return ExternalSettlementJournalRecord{}, errors.New("nil settlement journal")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, err := s.expected(ctx, id, archive)
	if err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if err := s.checkDir(true); err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if entries, err := os.ReadDir(s.dir); err != nil {
		return ExternalSettlementJournalRecord{}, err
	} else if len(entries) > 0 {
		root, _, err := s.Load(ctx, id, archive)
		return root, err
	}
	if _, err := s.expected(ctx, id, archive); err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if err := exclusiveReviewWrite(s.path(ExternalSettlementJournalPrepared), exp); err != nil {
		if got, _, e := s.Load(ctx, id, archive); e == nil && got.Checksum == exp.Checksum {
			return got, nil
		}
		return ExternalSettlementJournalRecord{}, err
	}
	root, _, err := s.Load(ctx, id, archive)
	return root, err
}
func (s *ExternalSettlementJournalStore) Fence(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalSettlementJournalRecord, error) {
	if s == nil {
		return ExternalSettlementJournalRecord{}, errors.New("nil settlement journal")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	root, ev, err := s.Load(ctx, id, archive)
	if err != nil {
		return ExternalSettlementJournalRecord{}, err
	}
	if ev != nil {
		return *ev, nil
	}
	next := expectedSettlementJournalFenced(root)
	if err := exclusiveReviewWrite(s.path(ExternalSettlementJournalFenced), next); err != nil {
		_, got, e := s.Load(ctx, id, archive)
		if e == nil && got != nil && got.Checksum == next.Checksum {
			return *got, nil
		}
		return ExternalSettlementJournalRecord{}, err
	}
	_, got, err := s.Load(ctx, id, archive)
	if err != nil || got == nil {
		return ExternalSettlementJournalRecord{}, errors.New("fenced settlement journal cannot be verified")
	}
	return *got, nil
}
