package trading

// A crash-recoverable, append-only PRE-POSTING checkpoint. This stores only
// signed-evidence revalidation receipts, NOT financial entries. It never writes
// execution, ownership, order, reservation, PnL or authorization state.
// An incident is globally exclusive while Hedge-side allocation is unproven.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const (
	ExternalReplayStaged    = "SIGNED_REPLAY_STAGED_NOT_POSTED"
	ExternalReplayRechecked = "SIGNED_REPLAY_RECHECKED_NOT_POSTED"
)

type ExternalCloseReplayRecord struct {
	Schema               int    `json:"schema"`
	Stage                string `json:"stage"`
	ClientOrderID        string `json:"client_order_id"`
	ProposalSHA256       string `json:"proposal_sha256"`
	ReplayKey            string `json:"replay_key"`
	EvidenceDigestSHA256 string `json:"evidence_digest_sha256"`
	PreviousChecksum     string `json:"previous_checksum,omitempty"`
	Checksum             string `json:"checksum"`
	PostingAuthorized    bool   `json:"posting_authorized"`
	TradingAuthorized    bool   `json:"trading_authorized"`
	LifecycleChanged     bool   `json:"lifecycle_changed"`
	OwnershipChanged     bool   `json:"ownership_changed"`
}

func (r ExternalCloseReplayRecord) digest() string { r.Checksum = ""; return externalCloseHash(r) }
func validReplayRecord(r ExternalCloseReplayRecord, stage, id, proposal, prev string) error {
	if r.Schema != 1 || r.Stage != stage || r.ClientOrderID != id || r.ProposalSHA256 != proposal ||
		!externalReviewID.MatchString(r.ClientOrderID) || !externalReviewHash.MatchString(r.ProposalSHA256) ||
		!externalReviewHash.MatchString(r.ReplayKey) || !externalReviewHash.MatchString(r.EvidenceDigestSHA256) ||
		r.PreviousChecksum != prev || r.Checksum != r.digest() || r.PostingAuthorized || r.TradingAuthorized || r.LifecycleChanged || r.OwnershipChanged {
		return errors.New("unsafe or tampered external-close replay checkpoint")
	}
	return nil
}

type ExternalCloseReplayCheckpointStore struct {
	mu       sync.Mutex // serialize read/write of partially created append-only records within this process
	proposal *ExternalCloseSettlementProposalStore
	dir      string
}

func NewExternalCloseReplayCheckpointStore(dataDir string) (*ExternalCloseReplayCheckpointStore, error) {
	p, err := NewExternalCloseSettlementProposalStore(dataDir)
	if err != nil {
		return nil, err
	}
	d := filepath.Join(p.dataDir, "trading", "external_close_replay_checkpoint")
	if fi, err := os.Lstat(d); err == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe replay checkpoint directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &ExternalCloseReplayCheckpointStore{proposal: p, dir: d}, nil
}
func syncReplayDir(dir string) error {
	// Windows cannot reliably open a directory for fsync. The only durable
	// artifact is a non-posting, fail-closed checkpoint; file fsync remains.
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func (s *ExternalCloseReplayCheckpointStore) pathRoot() string {
	return filepath.Join(s.dir, "checkpoint.json")
}
func (s *ExternalCloseReplayCheckpointStore) pathEvent() string {
	return filepath.Join(s.dir, "recheck.000001.json")
}
func (s *ExternalCloseReplayCheckpointStore) ensureDir(create bool) error {
	if s == nil {
		return errors.New("missing replay checkpoint store")
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
		return errors.New("unsafe replay checkpoint directory")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() != "checkpoint.json" && e.Name() != "recheck.000001.json" || e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unknown or unsafe replay checkpoint: %s", e.Name())
		}
	}
	return nil
}
func writeReplayExclusive(path string, r ExternalCloseReplayRecord) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
func (s *ExternalCloseReplayCheckpointStore) readStage(clientID string) (ExternalCloseReplayRecord, *ExternalCloseReplayRecord, error) {
	p, err := s.proposal.Load(clientID)
	if err != nil {
		return ExternalCloseReplayRecord{}, nil, err
	}
	if err = s.ensureDir(false); err != nil {
		return ExternalCloseReplayRecord{}, nil, err
	}
	var root ExternalCloseReplayRecord
	if err = strictReviewJSON(s.pathRoot(), &root); err != nil {
		return root, nil, err
	}
	if err = validReplayRecord(root, ExternalReplayStaged, clientID, p.Checksum, ""); err != nil {
		return root, nil, err
	}
	var event ExternalCloseReplayRecord
	err = strictReviewJSON(s.pathEvent(), &event)
	if errors.Is(err, os.ErrNotExist) {
		return root, nil, nil
	}
	if err != nil {
		return root, nil, err
	}
	if err = validReplayRecord(event, ExternalReplayRechecked, clientID, p.Checksum, root.Checksum); err != nil {
		return root, nil, err
	}
	if event.ReplayKey != root.ReplayKey || event.EvidenceDigestSHA256 != root.EvidenceDigestSHA256 {
		return root, nil, errors.New("rechecked evidence drifted from checkpoint")
	}
	return root, &event, nil
}

// Stage requires fresh independently fetched evidence. A root-only checkpoint
// after a crash can be resumed via Recheck. An incomplete root file fails
// closed and demands manual forensic recovery; it is never overwritten.
func (s *ExternalCloseReplayCheckpointStore) Stage(ctx context.Context, clientID string, reader SignedCloseEvidenceReader, now time.Time) (ExternalCloseReplayRecord, error) {
	if s == nil {
		return ExternalCloseReplayRecord{}, errors.New("nil replay store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.proposal.Load(clientID)
	if err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	evidence, err := RevalidateExternalClose(ctx, p, reader, now)
	if err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	if err = s.proposal.review.ensureOffline(); err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	record := ExternalCloseReplayRecord{Schema: 1, Stage: ExternalReplayStaged, ClientOrderID: clientID, ProposalSHA256: p.Checksum, ReplayKey: evidence.ReplayKey, EvidenceDigestSHA256: evidence.DigestSHA256}
	record.Checksum = record.digest()
	if err = s.ensureDir(true); err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	old, _, err := s.readStage(clientID)
	if err == nil {
		if old.Checksum == record.Checksum {
			return old, nil
		}
		return ExternalCloseReplayRecord{}, errors.New("replay evidence changed since prior checkpoint")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return ExternalCloseReplayRecord{}, err
	}
	entries, e := os.ReadDir(s.dir)
	if e != nil || len(entries) != 0 {
		return ExternalCloseReplayRecord{}, errors.New("orphan replay checkpoint event or unsafe directory")
	}
	if err = writeReplayExclusive(s.pathRoot(), record); err != nil {
		// A racing identical writer can win; only accept fully verified data.
		prior, _, re := s.readStage(clientID)
		if re == nil && prior.Checksum == record.Checksum {
			return prior, nil
		}
		return ExternalCloseReplayRecord{}, err
	}
	if err := syncReplayDir(s.dir); err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	return record, nil
}

// Recheck is a durable second signed check with an append-only event, not a
// settlement transition. A crash after Stage does not require posting/replay;
// the second read is performed again. A changed signed result is rejected.
func (s *ExternalCloseReplayCheckpointStore) Recheck(ctx context.Context, clientID string, reader SignedCloseEvidenceReader, now time.Time) (ExternalCloseReplayRecord, error) {
	if s == nil {
		return ExternalCloseReplayRecord{}, errors.New("nil replay store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	root, old, err := s.readStage(clientID)
	if err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	p, err := s.proposal.Load(clientID)
	if err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	observed, err := RevalidateExternalClose(ctx, p, reader, now)
	if err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	if observed.ReplayKey != root.ReplayKey || observed.DigestSHA256 != root.EvidenceDigestSHA256 {
		return ExternalCloseReplayRecord{}, errors.New("signed read differs from sealed checkpoint")
	}
	if err = s.proposal.review.ensureOffline(); err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	r := ExternalCloseReplayRecord{Schema: 1, Stage: ExternalReplayRechecked, ClientOrderID: clientID, ProposalSHA256: p.Checksum, ReplayKey: root.ReplayKey, EvidenceDigestSHA256: root.EvidenceDigestSHA256, PreviousChecksum: root.Checksum}
	r.Checksum = r.digest()
	if old != nil {
		if old.Checksum == r.Checksum {
			return *old, nil
		}
		return ExternalCloseReplayRecord{}, errors.New("recheck event mismatch")
	}
	if err = writeReplayExclusive(s.pathEvent(), r); err != nil {
		_, again, re := s.readStage(clientID)
		if re == nil && again != nil && again.Checksum == r.Checksum {
			return *again, nil
		}
		return ExternalCloseReplayRecord{}, err
	}
	if err := syncReplayDir(s.dir); err != nil {
		return ExternalCloseReplayRecord{}, err
	}
	return r, nil
}

// Load never treats the checkpoint as sufficient to mutate owner quantities.
func (s *ExternalCloseReplayCheckpointStore) Load(clientID string) (ExternalCloseReplayRecord, *ExternalCloseReplayRecord, error) {
	if s == nil {
		return ExternalCloseReplayRecord{}, nil, errors.New("nil replay store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readStage(clientID)
}
