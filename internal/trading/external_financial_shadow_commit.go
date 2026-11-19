package trading

// Durable, two-phase SHADOW publication of a financial ledger pair.
// This store is deliberately disjoint from the live lifecycle, ownership, PnL,
// wallet, and settlement journals. It proves a crash/restart and cross-reader
// publication boundary in isolated storage; it DOES NOT post a financial trade
// or authorize automatic orders. There is no live-ledger COMMIT entry point.
//
// Both readers validate the same immutable publish marker before returning
// either proposed ledger. Never reinterpret SHADOW_PUBLISHED as SETTLED.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	ExternalShadowCommitAbsent    = "FINANCIAL_SHADOW_COMMIT_ABSENT_NOT_POSTED"
	ExternalShadowCommitPrepared  = "FINANCIAL_SHADOW_COMMIT_PREPARED_NOT_POSTED"
	ExternalShadowCommitPublished = "FINANCIAL_SHADOW_PUBLISHED_NOT_POSTED"
)

const shadowCommitRoot = "external_financial_shadow_commit_events"

// ExternalShadowCommitRecord is a durable, append-only SHADOW checkpoint.
// A caller cannot supply any posting/authorization bit; every persisted record
// keeps these flags false. SHA-256 detects accidental/cross-event mixing, not
// deliberate evidence forgery by an attacker with filesystem write access.
type ExternalShadowCommitRecord struct {
	Schema                int    `json:"schema"`
	Stage                 string `json:"stage"`
	ClientOrderID         string `json:"client_order_id"`
	TransactionKey        string `json:"transaction_key"`
	PinnedSealSHA256      string `json:"pinned_seal_sha256"`
	PreflightDigestSHA256 string `json:"preflight_digest_sha256"`
	LifecycleProposal     string `json:"lifecycle_proposal,omitempty"`
	OwnershipProposal     string `json:"ownership_proposal,omitempty"`
	PreparedChecksum      string `json:"prepared_checksum,omitempty"`
	Checksum              string `json:"checksum"`
	PostingApplied        bool   `json:"posting_applied"`
	LifecycleChanged      bool   `json:"lifecycle_changed"`
	OwnershipChanged      bool   `json:"ownership_changed"`
	SettlementAuthorized  bool   `json:"settlement_authorized"`
	TradingAuthorized     bool   `json:"trading_authorized"`
}

func (r ExternalShadowCommitRecord) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

type ExternalShadowCommitInspection struct {
	Stage                    string `json:"stage"`
	ClientOrderID            string `json:"client_order_id"`
	PinnedSealSHA256         string `json:"pinned_seal_sha256,omitempty"`
	ShadowPublicationVisible bool   `json:"shadow_publication_visible"`
	ShadowPublishSHA256      string `json:"shadow_publish_sha256,omitempty"`
	PostingApplied           bool   `json:"posting_applied"`
	SettlementAuthorized     bool   `json:"settlement_authorized"`
	TradingAuthorized        bool   `json:"trading_authorized"`
}

type ExternalShadowCommitStore struct {
	mu      sync.Mutex
	source  *ExternalAtomicRehearsalStore
	dataDir string
	root    string
	dir     string
	id      string
}

func NewExternalShadowCommitStore(dataDir, id string) (*ExternalShadowCommitStore, error) {
	if !externalReviewID.MatchString(id) {
		return nil, errors.New("invalid shadow commit event ID")
	}
	source, err := NewExternalAtomicLinkedEventStore(dataDir, id)
	if err != nil {
		return nil, err
	}
	tradingDir := filepath.Dir(filepath.Dir(source.dir))
	root := filepath.Join(tradingDir, shadowCommitRoot)
	s := &ExternalShadowCommitStore{source: source, dataDir: dataDir, root: root, dir: filepath.Join(root, id), id: id}
	if err := s.checkDir(false); err != nil {
		return nil, err
	}
	return s, nil
}

// checkDir refuses symlinks, foreign event names, and unrecognized files.
// A power cut between mkdir and the first checkpoint is an empty, safe state.
func (s *ExternalShadowCommitStore) checkDir(create bool) error {
	if s == nil || !externalReviewID.MatchString(s.id) {
		return errors.New("invalid shadow store")
	}
	if create {
		if err := os.Mkdir(s.root, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	root, err := os.Lstat(s.root)
	if errors.Is(err, os.ErrNotExist) && !create {
		return nil
	}
	if err != nil {
		return err
	}
	if !root.IsDir() || root.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe shadow commit root")
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !externalReviewID.MatchString(e.Name()) || !e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("foreign entry in shadow commit root: %s", e.Name())
		}
	}
	if create {
		if err := os.Mkdir(s.dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	dir, err := os.Lstat(s.dir)
	if errors.Is(err, os.ErrNotExist) && !create {
		return nil
	}
	if err != nil {
		return err
	}
	if !dir.IsDir() || dir.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe shadow commit event directory")
	}
	records, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, entry := range records {
		if entry.Name() != "prepared.json" && entry.Name() != "published.json" {
			return fmt.Errorf("foreign shadow commit checkpoint: %s", entry.Name())
		}
		if !entry.Type().IsRegular() || entry.Type()&os.ModeSymlink != 0 {
			return errors.New("unsafe shadow commit checkpoint type")
		}
	}
	return nil
}

func (s *ExternalShadowCommitStore) expected(ctx context.Context, archive PackageAEntryArchive, seal string) ([2]ExternalShadowCommitRecord, error) {
	var empty [2]ExternalShadowCommitRecord
	if s == nil || !externalReviewHash.MatchString(seal) {
		return empty, errors.New("pinned shadow source seal is required")
	}
	preflight, err := s.source.InspectFinancialCommitPreflight(ctx, s.id, archive, seal)
	if err != nil {
		return empty, fmt.Errorf("shadow commit source preflight: %w", err)
	}
	if preflight.Stage != ExternalFinancialCommitBlocked || preflight.ClientOrderID != s.id ||
		preflight.DigestSHA256 != preflight.digest() || preflight.PinnedCommitSHA256 != seal ||
		preflight.PostingApplied || preflight.LifecycleChanged || preflight.OwnershipChanged ||
		preflight.TradingAuthorized || preflight.SettlementAuthorized ||
		preflight.TransactionKey == "" || preflight.LifecycleCandidate == "" || preflight.OwnershipCandidate == "" ||
		len(preflight.OutstandingRequirements) == 0 {
		return empty, errors.New("not a fully blocked, complete shadow source")
	}
	prepare := ExternalShadowCommitRecord{
		Schema: 1, Stage: ExternalShadowCommitPrepared, ClientOrderID: s.id,
		TransactionKey: preflight.TransactionKey, PinnedSealSHA256: seal,
		PreflightDigestSHA256: preflight.DigestSHA256,
		LifecycleProposal:     preflight.LifecycleCandidate, OwnershipProposal: preflight.OwnershipCandidate,
	}
	prepare.Checksum = prepare.digest()
	publish := ExternalShadowCommitRecord{
		Schema: 1, Stage: ExternalShadowCommitPublished, ClientOrderID: s.id,
		TransactionKey: prepare.TransactionKey, PinnedSealSHA256: seal,
		PreflightDigestSHA256: preflight.DigestSHA256, PreparedChecksum: prepare.Checksum,
	}
	publish.Checksum = publish.digest()
	return [2]ExternalShadowCommitRecord{prepare, publish}, nil
}

func (s *ExternalShadowCommitStore) inspect(expected [2]ExternalShadowCommitRecord) (ExternalShadowCommitInspection, int, error) {
	out := ExternalShadowCommitInspection{Stage: ExternalShadowCommitAbsent, ClientOrderID: s.id}
	if err := s.checkDir(false); err != nil {
		return out, -1, err
	}
	count := 0
	for i, name := range []string{"prepared.json", "published.json"} {
		var got ExternalShadowCommitRecord
		err := strictReviewJSON(filepath.Join(s.dir, name), &got)
		if errors.Is(err, os.ErrNotExist) && count == i { // absent suffix
			if i == 0 {
				// If publish exists without prepare it must be rejected.
				if _, err = os.Lstat(filepath.Join(s.dir, "published.json")); err == nil || !errors.Is(err, os.ErrNotExist) {
					return out, -1, errors.New("orphan shadow publish marker")
				}
			}
			break
		}
		if err != nil {
			return out, -1, fmt.Errorf("shadow checkpoint %s: %w", name, err)
		}
		if got != expected[i] || got.Checksum != got.digest() || got.PostingApplied ||
			got.LifecycleChanged || got.OwnershipChanged || got.SettlementAuthorized || got.TradingAuthorized {
			return out, -1, fmt.Errorf("shadow checkpoint mismatch: %s", name)
		}
		count++
	}
	if count > 0 {
		out.Stage = expected[count-1].Stage
		out.PinnedSealSHA256 = expected[0].PinnedSealSHA256
	}
	if count == 2 {
		out.ShadowPublicationVisible = true
		out.ShadowPublishSHA256 = expected[1].Checksum
	}
	return out, count - 1, nil
}

func (s *ExternalShadowCommitStore) Inspect(ctx context.Context, archive PackageAEntryArchive, seal string) (ExternalShadowCommitInspection, error) {
	expected, err := s.expected(ctx, archive, seal)
	if err != nil {
		return ExternalShadowCommitInspection{}, err
	}
	got, _, err := s.inspect(expected)
	if err != nil {
		return ExternalShadowCommitInspection{}, err
	}
	fresh, err := s.expected(ctx, archive, seal)
	if err != nil || expected != fresh {
		return ExternalShadowCommitInspection{}, errors.New("shadow source changed during read")
	}
	return got, nil
}

func (s *ExternalShadowCommitStore) advance(ctx context.Context, archive PackageAEntryArchive, seal string, step int) (ExternalShadowCommitInspection, error) {
	if s == nil || step < 0 || step > 1 {
		return ExternalShadowCommitInspection{}, errors.New("invalid shadow stage")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, err := s.expected(ctx, archive, seal)
	if err != nil {
		return ExternalShadowCommitInspection{}, err
	}
	if err := s.checkDir(true); err != nil {
		return ExternalShadowCommitInspection{}, err
	}
	current, index, err := s.inspect(expected)
	if err != nil {
		return ExternalShadowCommitInspection{}, err
	}
	if index >= step {
		return current, nil
	} // verified idempotent replay
	if index != step-1 {
		return ExternalShadowCommitInspection{}, errors.New("shadow publication requires a prepared pair")
	}
	fresh, err := s.expected(ctx, archive, seal)
	if err != nil || expected != fresh {
		return ExternalShadowCommitInspection{}, errors.New("shadow source changed before checkpoint")
	}
	file := "prepared.json"
	if step == 1 {
		file = "published.json"
	}
	if err := exclusiveReviewWrite(filepath.Join(s.dir, file), expected[step]); err != nil {
		return ExternalShadowCommitInspection{}, fmt.Errorf("shadow write incomplete; restart inspection required: %w", err)
	}
	result, idx, err := s.inspect(expected)
	if err != nil || idx != step {
		return ExternalShadowCommitInspection{}, errors.New("shadow post-write verification failed")
	}
	fresh, err = s.expected(ctx, archive, seal)
	if err != nil || expected != fresh {
		return ExternalShadowCommitInspection{}, errors.New("shadow source changed after checkpoint")
	}
	return result, nil
}

func (s *ExternalShadowCommitStore) PrepareShadow(ctx context.Context, archive PackageAEntryArchive, seal string) (ExternalShadowCommitInspection, error) {
	return s.advance(ctx, archive, seal, 0)
}

// PublishShadow is intentionally NOT a financial commit. It merely publishes
// the already immutable pair for shadow readers with one durable file write.
func (s *ExternalShadowCommitStore) PublishShadow(ctx context.Context, archive PackageAEntryArchive, seal string) (ExternalShadowCommitInspection, error) {
	return s.advance(ctx, archive, seal, 1)
}

// ReadShadowPair returns BOTH proposed ledgers or neither. There are no
// individual ledger readers on this store, preventing mixed visible revisions.
func (s *ExternalShadowCommitStore) ReadShadowPair(ctx context.Context, archive PackageAEntryArchive, seal string) (ExternalAtomicLedgerPair, error) {
	result, err := s.Inspect(ctx, archive, seal)
	if err != nil {
		return ExternalAtomicLedgerPair{}, err
	}
	if !result.ShadowPublicationVisible {
		return ExternalAtomicLedgerPair{}, errors.New("shadow ledger pair not published")
	}
	pair, err := s.source.ReadLedgerPair(ctx, s.id, archive, seal)
	if err != nil {
		return ExternalAtomicLedgerPair{}, err
	}
	again, err := s.Inspect(ctx, archive, seal)
	if err != nil || again != result {
		return ExternalAtomicLedgerPair{}, errors.New("shadow publication changed during read")
	}
	if pair.PostingApplied || pair.SettlementAuthorized || pair.TradingAuthorized || pair.Lifecycle.Applied || pair.Ownership.Applied {
		return ExternalAtomicLedgerPair{}, errors.New("shadow source cannot be financially applied")
	}
	return pair, nil
}
