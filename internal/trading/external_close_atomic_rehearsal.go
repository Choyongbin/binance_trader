package trading

// External-close cross-ledger ATOMICITY REHEARSAL, NEVER A FINANCIAL POST.
//
// The existing trading Lifecycle and Ownership readers use distinct journals.
// Updating those files separately is not atomic, so this rehearsal deliberately
// writes only to a separate directory. A reader may see BOTH proposed views
// only after a single immutable seal has been durably written and revalidated.
// Pre-seal crashes can be resumed without any application of original journals.
// A future financial protocol MUST make all real readers honor a commit boundary,
// independently attest the complete signed history, funding and ownership, and
// have separate operator authorization before lifting the HTTP 423 lock.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	ExternalAtomicRehearsalAbsent    = "ATOMIC_REHEARSAL_ABSENT_NOT_POSTED"
	ExternalAtomicRehearsalIntent    = "ATOMIC_REHEARSAL_INTENT_NOT_POSTED"
	ExternalAtomicRehearsalLifecycle = "ATOMIC_REHEARSAL_LIFECYCLE_STAGED_NOT_POSTED"
	ExternalAtomicRehearsalOwnership = "ATOMIC_REHEARSAL_BOTH_STAGED_NOT_POSTED"
	ExternalAtomicRehearsalSealed    = "ATOMIC_REHEARSAL_BUNDLE_SEALED_NOT_POSTED"
)

// One deterministic record per stage. Only the final, checksum-bound SEAL
// presents the two proposed changes as an indivisible read model.
type ExternalAtomicRehearsalRecord struct {
	Schema                int          `json:"schema"`
	Stage                 string       `json:"stage"`
	ClientOrderID         string       `json:"client_order_id"`
	ModelID               string       `json:"model_id"`
	PositionSide          PositionSide `json:"position_side"`
	TransactionKey        string       `json:"transaction_key"`
	FencedJournalSHA256   string       `json:"fenced_journal_sha256"`
	DualLedgerSHA256      string       `json:"dual_ledger_sha256"`
	LocalEvidenceSHA256   string       `json:"local_evidence_sha256"`
	PreviousChecksum      string       `json:"previous_checksum,omitempty"`
	LifecycleProposal     string       `json:"lifecycle_proposal,omitempty"`
	OwnershipProposal     string       `json:"ownership_proposal,omitempty"`
	ProjectedExitBTC      string       `json:"projected_exit_btc,omitempty"`
	ProjectedRemainingBTC string       `json:"projected_remaining_btc,omitempty"`
	LifecycleRecordSHA256 string       `json:"lifecycle_record_sha256,omitempty"`
	OwnershipRecordSHA256 string       `json:"ownership_record_sha256,omitempty"`
	Checksum              string       `json:"checksum"`
	PostingApplied        bool         `json:"posting_applied"`
	LifecycleChanged      bool         `json:"lifecycle_changed"`
	OwnershipChanged      bool         `json:"ownership_changed"`
	SettlementAuthorized  bool         `json:"settlement_authorized"`
	TradingAuthorized     bool         `json:"trading_authorized"`
}

func (r ExternalAtomicRehearsalRecord) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

// AtomicProjectionVisible refers exclusively to the NON-POSTED simulation.
// There is deliberately no API here to apply financial or lifecycle changes.
type ExternalAtomicRehearsalInspection struct {
	Schema                  int    `json:"schema"`
	Stage                   string `json:"stage"`
	ClientOrderID           string `json:"client_order_id"`
	TransactionKey          string `json:"transaction_key"`
	AtomicProjectionVisible bool   `json:"atomic_projection_visible"`
	CommitSHA256            string `json:"commit_sha256,omitempty"`
	LifecycleProposal       string `json:"lifecycle_proposal,omitempty"`
	OwnershipProposal       string `json:"ownership_proposal,omitempty"`
	ProjectedExitBTC        string `json:"projected_exit_btc,omitempty"`
	ProjectedRemainingBTC   string `json:"projected_remaining_btc,omitempty"`
	PostingApplied          bool   `json:"posting_applied"`
	LifecycleChanged        bool   `json:"lifecycle_changed"`
	OwnershipChanged        bool   `json:"ownership_changed"`
	SettlementAuthorized    bool   `json:"settlement_authorized"`
	TradingAuthorized       bool   `json:"trading_authorized"`
}

type ExternalAtomicRehearsalStore struct {
	mu      sync.Mutex
	journal *ExternalSettlementJournalStore
	dir     string
	// Empty for the historical single-directory rehearsal. Scoped stores bind
	// a single entry clientOrderID to a separate, append-only event directory.
	scopedClientID string
	eventsRoot     string
	legacyDir      string
}

var atomicRehearsalFiles = [4]string{"intent.json", "lifecycle.json", "ownership.json", "seal.json"}
var atomicRehearsalStages = [4]string{
	ExternalAtomicRehearsalIntent, ExternalAtomicRehearsalLifecycle,
	ExternalAtomicRehearsalOwnership, ExternalAtomicRehearsalSealed,
}

// Construction is read-only. No original trading file is opened for mutation.
func NewExternalAtomicRehearsalStore(dataDir string) (*ExternalAtomicRehearsalStore, error) {
	journal, err := NewExternalSettlementJournalStore(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(journal.dual.shadow.intents.replay.proposal.dataDir, "trading", "external_close_atomic_rehearsal")
	if fi, e := os.Lstat(dir); e == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe atomic rehearsal directory")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	return &ExternalAtomicRehearsalStore{journal: journal, dir: dir}, nil
}

// NewExternalAtomicEventStore binds one settlement candidate to one immutable
// per-entry directory. The *existing* legacy constructor remains available for
// reading historical single-directory checkpoints; it is never auto-migrated.
// In particular, the new scoped store will refuse to create a second copy of a
// historical transaction with the same clientOrderID.
// This constructor performs no writes and grants no posting authorization.
func NewExternalAtomicEventStore(dataDir, clientOrderID string) (*ExternalAtomicRehearsalStore, error) {
	if !externalReviewID.MatchString(clientOrderID) {
		return nil, errors.New("invalid scoped atomic event clientOrderID")
	}
	s, err := NewExternalAtomicRehearsalStore(dataDir)
	if err != nil {
		return nil, err
	}
	s.legacyDir = s.dir
	s.eventsRoot = filepath.Join(filepath.Dir(s.dir), "external_close_atomic_events")
	s.dir = filepath.Join(s.eventsRoot, clientOrderID)
	s.scopedClientID = clientOrderID
	if err := s.checkEventsRoot(false); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(s.dir); err == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe scoped atomic event directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return s, nil
}

// NewExternalAtomicLinkedEventStore is the opt-in v3.110 cross-event path.
// Unlike the historical NewExternalAtomicEventStore (kept unchanged to read
// pre-3.110 checkpoints), this store derives its checkpoints exclusively from
// the event-scoped PREPARED/FENCED journal for the *same* clientOrderID.
// Constructor has no disk side effects. Existing legacy/single-event evidence
// is never migrated, and incomplete inputs fail closed.
func NewExternalAtomicLinkedEventStore(dataDir, clientOrderID string) (*ExternalAtomicRehearsalStore, error) {
	store, err := NewExternalAtomicEventStore(dataDir, clientOrderID)
	if err != nil {
		return nil, err
	}
	scopedJournal, err := NewExternalSettlementEventJournalStore(dataDir, clientOrderID)
	if err != nil {
		return nil, err
	}
	store.journal = scopedJournal
	return store, nil
}

// No symlink or unexpected root entry is accepted. Unlike MkdirAll, this
// creates only the one intended directory, and only on explicit write steps.
func (s *ExternalAtomicRehearsalStore) checkEventsRoot(create bool) error {
	if s.eventsRoot == "" {
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
		return errors.New("unsafe atomic events root")
	}
	entries, err := os.ReadDir(s.eventsRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !externalReviewID.MatchString(entry.Name()) || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe atomic events root entry: %s", entry.Name())
		}
	}
	return nil
}

// Explicit legacy read-only compatibility is kept by the old constructor.
// New scoped writes refuse a duplicate of the same historical transaction.
func (s *ExternalAtomicRehearsalStore) checkLegacyConflict() error {
	if s.scopedClientID == "" {
		return nil
	}
	fi, err := os.Lstat(s.legacyDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe legacy atomic rehearsal directory")
	}
	legacy := &ExternalAtomicRehearsalStore{journal: s.journal, dir: s.legacyDir}
	present, err := legacy.checkDir(false)
	if err != nil || !present {
		return err
	}
	path := filepath.Join(s.legacyDir, atomicRehearsalFiles[0])
	var historical ExternalAtomicRehearsalRecord
	if err := strictReviewJSON(path, &historical); errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(s.legacyDir)
		if readErr != nil {
			return readErr
		}
		if len(entries) > 0 {
			return errors.New("legacy atomic rehearsal contains orphan checkpoints")
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("legacy atomic rehearsal unreadable: %w", err)
	}
	if historical.Schema != 1 || historical.Stage != ExternalAtomicRehearsalIntent ||
		historical.Checksum != historical.digest() || !externalReviewID.MatchString(historical.ClientOrderID) ||
		historical.PostingApplied || historical.LifecycleChanged || historical.OwnershipChanged ||
		historical.SettlementAuthorized || historical.TradingAuthorized {
		return errors.New("invalid legacy atomic rehearsal intent")
	}
	if historical.ClientOrderID == s.scopedClientID {
		return errors.New("legacy atomic rehearsal already records this clientOrderID; read legacy explicitly, no automatic migration")
	}
	return nil
}

// Every stage is derived from the already sealed immutable review chain; no
// caller-supplied claim of authorization or signed history completeness exists.
func (s *ExternalAtomicRehearsalStore) expected(ctx context.Context, id string, archive PackageAEntryArchive) ([4]ExternalAtomicRehearsalRecord, error) {
	var out [4]ExternalAtomicRehearsalRecord
	if s == nil || s.journal == nil || !externalReviewID.MatchString(id) ||
		(s.scopedClientID != "" && s.scopedClientID != id) {
		return out, errors.New("invalid atomic rehearsal store or scoped order ID")
	}
	if err := s.checkEventsRoot(false); err != nil {
		return out, err
	}
	if err := s.checkLegacyConflict(); err != nil {
		return out, err
	}
	matrix, err := s.journal.InspectEvidenceMatrix(ctx, id, archive)
	if err != nil {
		return out, err
	}
	if matrix.JournalStage != ExternalRecoveryFencedOnly || matrix.PostingAuthorized || matrix.SettlementAuthorized || matrix.TradingAuthorized {
		return out, errors.New("fully fenced NON-POSTED settlement journal required")
	}
	_, fence, err := s.journal.Load(ctx, id, archive)
	if err != nil || fence == nil {
		return out, errors.New("fenced settlement journal cannot be verified")
	}
	_, dual, err := s.journal.dual.Load(id)
	if err != nil || dual == nil {
		return out, errors.New("verified dual-ledger plan required")
	}
	if fence.ClientOrderID != id || dual.ClientOrderID != id || fence.TransactionKey != matrix.TransactionKey ||
		dual.TransactionKey != fence.TransactionKey || dual.LocalEvidenceSHA256 != fence.LocalEvidenceSHA256 {
		return out, errors.New("settlement journal and dual-ledger evidence disagree")
	}
	base := ExternalAtomicRehearsalRecord{
		Schema: 1, ClientOrderID: id, ModelID: dual.ModelID, PositionSide: dual.PositionSide,
		TransactionKey: fence.TransactionKey, FencedJournalSHA256: fence.Checksum,
		DualLedgerSHA256: dual.Checksum, LocalEvidenceSHA256: fence.LocalEvidenceSHA256,
	}
	out[0] = base
	out[0].Stage = ExternalAtomicRehearsalIntent
	out[0].Checksum = out[0].digest()
	out[1] = base
	out[1].Stage = ExternalAtomicRehearsalLifecycle
	out[1].PreviousChecksum = out[0].Checksum
	out[1].LifecycleProposal = dual.LifecycleProposal
	out[1].Checksum = out[1].digest()
	out[2] = base
	out[2].Stage = ExternalAtomicRehearsalOwnership
	out[2].PreviousChecksum = out[1].Checksum
	out[2].OwnershipProposal = dual.OwnershipProposal
	out[2].ProjectedExitBTC = dual.ProposedExitQuantityBTC
	out[2].ProjectedRemainingBTC = dual.SimulatedRemainingBTC
	out[2].Checksum = out[2].digest()
	out[3] = base
	out[3].Stage = ExternalAtomicRehearsalSealed
	out[3].PreviousChecksum = out[2].Checksum
	out[3].LifecycleRecordSHA256 = out[1].Checksum
	out[3].OwnershipRecordSHA256 = out[2].Checksum
	out[3].Checksum = out[3].digest()
	return out, nil
}

// checkDir rejects unexpected files, non-regular files and symlinks. Partial
// valid prefixes are recoverable, but incomplete/torn files fail closed.
func (s *ExternalAtomicRehearsalStore) checkDir(create bool) (bool, error) {
	if s == nil || s.journal == nil {
		return false, errors.New("nil atomic rehearsal store")
	}
	if err := s.checkEventsRoot(create); err != nil {
		return false, err
	}
	if create {
		if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return false, err
		}
	}
	fi, err := os.Lstat(s.dir)
	if errors.Is(err, os.ErrNotExist) && !create {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("unsafe atomic rehearsal directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return false, err
	}
	allowed := make(map[string]bool)
	for _, name := range atomicRehearsalFiles {
		allowed[name] = true
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] || entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("unknown or unsafe atomic rehearsal entry: %s", entry.Name())
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return false, fmt.Errorf("nonregular atomic rehearsal entry: %s", entry.Name())
		}
	}
	return true, nil
}

func (s *ExternalAtomicRehearsalStore) inspectExpected(records [4]ExternalAtomicRehearsalRecord) (ExternalAtomicRehearsalInspection, int, error) {
	result := ExternalAtomicRehearsalInspection{Schema: 1, Stage: ExternalAtomicRehearsalAbsent,
		ClientOrderID: records[0].ClientOrderID, TransactionKey: records[0].TransactionKey}
	exists, err := s.checkDir(false)
	if err != nil || !exists {
		return result, -1, err
	}
	count := 0
	missingFound := false
	for i, file := range atomicRehearsalFiles {
		path := filepath.Join(s.dir, file)
		_, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			missingFound = true
			continue
		}
		if err != nil {
			return result, -1, err
		}
		if missingFound {
			return result, -1, errors.New("orphan or out-of-order atomic rehearsal checkpoint")
		}
		var got ExternalAtomicRehearsalRecord
		if err := strictReviewJSON(path, &got); err != nil {
			return result, -1, fmt.Errorf("corrupted atomic rehearsal %s: %w", file, err)
		}
		if got.Schema != 1 || got.Stage != atomicRehearsalStages[i] || got.Checksum != got.digest() ||
			got.Checksum != records[i].Checksum || got.PostingApplied || got.LifecycleChanged || got.OwnershipChanged ||
			got.SettlementAuthorized || got.TradingAuthorized {
			return result, -1, fmt.Errorf("tampered or authorizing atomic rehearsal %s", file)
		}
		count++
	}
	if count > 0 {
		result.Stage = atomicRehearsalStages[count-1]
	}
	if count == len(atomicRehearsalFiles) {
		// The seal is the only visibility boundary for the JOINT projected view.
		result.AtomicProjectionVisible = true
		result.CommitSHA256 = records[3].Checksum
		result.LifecycleProposal = records[1].LifecycleProposal
		result.OwnershipProposal = records[2].OwnershipProposal
		result.ProjectedExitBTC = records[2].ProjectedExitBTC
		result.ProjectedRemainingBTC = records[2].ProjectedRemainingBTC
	}
	return result, count - 1, nil
}

// Inspect revalidates the complete upstream journal and local source digest.
// Absence is a BLOCKED state, never permission to create financial entries.
func (s *ExternalAtomicRehearsalStore) Inspect(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalAtomicRehearsalInspection, error) {
	records, err := s.expected(ctx, id, archive)
	if err != nil {
		return ExternalAtomicRehearsalInspection{}, err
	}
	inspection, _, err := s.inspectExpected(records)
	if err != nil {
		return ExternalAtomicRehearsalInspection{}, err
	}
	last, err := s.expected(ctx, id, archive)
	if err != nil || last != records {
		return ExternalAtomicRehearsalInspection{}, errors.New("atomic rehearsal source changed during inspection")
	}
	return inspection, nil
}

// progress is append-only and never repairs damaged evidence automatically.
// Each step has a durable O_EXCL + fsync record. The seal is one final write,
// avoiding a partially visible dual-ledger read model across restarts.
func (s *ExternalAtomicRehearsalStore) progress(ctx context.Context, id string, archive PackageAEntryArchive, step int) (ExternalAtomicRehearsalInspection, error) {
	if s == nil || step < 0 || step >= len(atomicRehearsalFiles) {
		return ExternalAtomicRehearsalInspection{}, errors.New("invalid atomic rehearsal step")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.expected(ctx, id, archive)
	if err != nil {
		return ExternalAtomicRehearsalInspection{}, err
	}
	if _, err := s.checkDir(true); err != nil {
		return ExternalAtomicRehearsalInspection{}, err
	}
	previous, index, err := s.inspectExpected(records)
	if err != nil {
		return ExternalAtomicRehearsalInspection{}, err
	}
	if index >= step {
		last, err := s.expected(ctx, id, archive)
		if err != nil || last != records {
			return ExternalAtomicRehearsalInspection{}, errors.New("atomic rehearsal source drift during replay")
		}
		return previous, nil // idempotent replay; no overwrite, no second side effect
	}
	if index != step-1 {
		return ExternalAtomicRehearsalInspection{}, errors.New("atomic rehearsal stages must be sequential")
	}
	// Last-moment source revalidation before creating any new checkpoint.
	fresh, err := s.expected(ctx, id, archive)
	if err != nil || records[step].Checksum != fresh[step].Checksum {
		return ExternalAtomicRehearsalInspection{}, errors.New("atomic rehearsal source drift before checkpoint")
	}
	if err := exclusiveReviewWrite(filepath.Join(s.dir, atomicRehearsalFiles[step]), records[step]); err != nil {
		// An OS error (even after bytes were written) cannot silently authorize
		// a stage. The next run must independently revalidate all checkpoints.
		return ExternalAtomicRehearsalInspection{}, fmt.Errorf("atomic rehearsal incomplete; inspect before retry: %w", err)
	}
	result, got, err := s.inspectExpected(records)
	if err != nil || got != step {
		return ExternalAtomicRehearsalInspection{}, errors.New("atomic rehearsal post-write verification failed")
	}
	last, err := s.expected(ctx, id, archive)
	if err != nil || last != records {
		return ExternalAtomicRehearsalInspection{}, errors.New("atomic rehearsal source changed after checkpoint write")
	}
	return result, nil
}

func (s *ExternalAtomicRehearsalStore) Prepare(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalAtomicRehearsalInspection, error) {
	return s.progress(ctx, id, archive, 0)
}
func (s *ExternalAtomicRehearsalStore) StageLifecycle(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalAtomicRehearsalInspection, error) {
	return s.progress(ctx, id, archive, 1)
}
func (s *ExternalAtomicRehearsalStore) StageOwnership(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalAtomicRehearsalInspection, error) {
	return s.progress(ctx, id, archive, 2)
}
func (s *ExternalAtomicRehearsalStore) Seal(ctx context.Context, id string, archive PackageAEntryArchive) (ExternalAtomicRehearsalInspection, error) {
	return s.progress(ctx, id, archive, 3)
}
