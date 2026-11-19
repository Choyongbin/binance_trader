package trading

// Operator review receipts are append-only SIDE-CAR evidence, not restoration of
// the original fills, execution lifecycle, reservations, model ownership, PnL,
// or protection orders. In particular, acknowledging a review NEVER changes
// MAY_HAVE_SENT or grants permission to trade. A saved JSON preview is not an
// authenticated signed exchange response.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	ExternalReviewStaged       = "REVIEW_STAGED"
	ExternalReviewAcknowledged = "OPERATOR_ACKNOWLEDGED_REVIEW_ONLY"
	ExternalReviewRejected     = "OPERATOR_REJECTED_REVIEW_ONLY"
)

var externalReviewID = regexp.MustCompile(`^bt_[0-9a-f]{28}$`)
var externalReviewHash = regexp.MustCompile(`^[0-9a-f]{64}$`)
var externalReviewEventName = regexp.MustCompile(`^bt_[0-9a-f]{28}\.review\.000001\.json$`)
var externalReviewActor = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{1,63}$`)

type ExternalCloseReviewRoot struct {
	Schema               int                    `json:"schema"`
	ClientOrderID        string                 `json:"client_order_id"`
	Rehearsal            ExternalCloseRehearsal `json:"rehearsal"`
	CreatedAtUTC         string                 `json:"created_at_utc"`
	ReviewOnly           bool                   `json:"review_only"`
	SettlementAuthorized bool                   `json:"settlement_authorized"`
	TradingAuthorized    bool                   `json:"trading_authorized"`
	Checksum             string                 `json:"checksum"`
}

func (r ExternalCloseReviewRoot) digest() string {
	r.Checksum = ""
	raw, _ := json.Marshal(r)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

type ExternalCloseReviewEvent struct {
	Schema               int    `json:"schema"`
	ClientOrderID        string `json:"client_order_id"`
	Sequence             int    `json:"sequence"`
	Stage                string `json:"stage"`
	AtUTC                string `json:"at_utc"`
	PreviousChecksum     string `json:"previous_checksum"`
	Reviewer             string `json:"reviewer"`
	Note                 string `json:"note"`
	ReviewOnly           bool   `json:"review_only"`
	SettlementAuthorized bool   `json:"settlement_authorized"`
	TradingAuthorized    bool   `json:"trading_authorized"`
	Checksum             string `json:"checksum"`
}

func (e ExternalCloseReviewEvent) digest() string {
	e.Checksum = ""
	raw, _ := json.Marshal(e)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

type ExternalCloseReviewState struct {
	Root   ExternalCloseReviewRoot    `json:"root"`
	Events []ExternalCloseReviewEvent `json:"events"`
	Stage  string                     `json:"stage"`
	// These flags are hardwired false. The review ledger is not a settlement API.
	SettlementAuthorized bool `json:"settlement_authorized"`
	TradingAuthorized    bool `json:"trading_authorized"`
}

type ExternalCloseReviewLedger struct{ dataDir, dir string }

// The constructor does not create directories or alter evidence.
func NewExternalCloseReviewLedger(dataDir string) (*ExternalCloseReviewLedger, error) {
	if dataDir == "" {
		return nil, errors.New("review data directory is required")
	}
	d := filepath.Clean(dataDir)
	for _, p := range []string{d, filepath.Join(d, "trading")} {
		f, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}
		if !f.IsDir() || f.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe review root directory")
		}
	}
	dir := filepath.Join(d, "trading", "external_close_reviews")
	if f, err := os.Lstat(dir); err == nil {
		if !f.IsDir() || f.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe review ledger directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &ExternalCloseReviewLedger{dataDir: d, dir: dir}, nil
}

func (s *ExternalCloseReviewLedger) ensureOffline() error {
	if s == nil {
		return errors.New("missing review ledger")
	}
	for _, name := range []string{"coordinator.active", "mutation_gate.active"} {
		_, err := os.Lstat(filepath.Join(s.dataDir, "trading", name))
		if err == nil {
			return fmt.Errorf("active trading lock %s; stop runtime first", name)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *ExternalCloseReviewLedger) ensureUnchanged(p ExternalCloseRehearsal) error {
	if err := s.ensureOffline(); err != nil {
		return err
	}
	current, err := snapshotExecutionEvidence(s.dataDir)
	if err != nil {
		return err
	}
	if current != p.LocalEvidenceSHA256 {
		return errors.New("local immutable evidence changed since signed review")
	}
	return nil
}

func validateExternalReviewPreview(p ExternalCloseRehearsal) error {
	if !VerifyExternalCloseRehearsalChecksum(p) || p.Status != ExternalCloseRehearsed ||
		!externalReviewID.MatchString(p.EntryClientOrderID) || p.ModelID == "" ||
		(p.PositionSide != Long && p.PositionSide != Short) || p.EntryOrderID <= 0 || p.ExitOrderID <= 0 ||
		p.EntryOrderID == p.ExitOrderID || p.SimulatedRemainingQty != "0" || p.CommissionAsset != "USDT" ||
		!externalReviewHash.MatchString(p.LocalEvidenceSHA256) || !externalReviewHash.MatchString(p.SignedReadDigestSHA256) ||
		len(p.EntryTradeIDs) == 0 || len(p.ExitTradeIDs) == 0 || len(p.Blockers) == 0 || len(p.RequiredBeforeMutation) == 0 {
		return errors.New("only complete, blocked signed-review rehearsals may be staged for operator review")
	}
	entry, err := positiveRat(p.EntryQuantity)
	if err != nil {
		return err
	}
	exit, err := positiveRat(p.ExitQuantity)
	if err != nil || entry.Cmp(exit) != 0 {
		return errors.New("external close quantities do not match")
	}
	ids := map[int64]bool{}
	for _, id := range append(append([]int64{}, p.EntryTradeIDs...), p.ExitTradeIDs...) {
		if id <= 0 || ids[id] {
			return errors.New("invalid/duplicate signed trade ID")
		}
		ids[id] = true
	}
	return nil
}

func strictReviewJSON(path string, dest any) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Size() > 1<<20 {
		return errors.New("unsafe review record")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing data in review record")
	}
	return nil
}

func (s *ExternalCloseReviewLedger) checkDir() error {
	fi, err := os.Lstat(s.dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe review directory")
	}
	return nil
}

// Sync the newly created record before returning. An interrupted write leaves
// an invalid immutable file and must fail closed; it is never overwritten.
func exclusiveReviewWrite(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("incomplete append-only review record; manual inspection required: %w", err)
	}
	if err := syncAutoControlDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	return nil
}

func (s *ExternalCloseReviewLedger) rootPath(id string) string {
	return filepath.Join(s.dir, id+".json")
}
func (s *ExternalCloseReviewLedger) eventPath(id string) string {
	return filepath.Join(s.dir, id+".review.000001.json")
}

func (s *ExternalCloseReviewLedger) Load(clientID string) (ExternalCloseReviewState, error) {
	if s == nil || !externalReviewID.MatchString(clientID) {
		return ExternalCloseReviewState{}, errors.New("invalid review clientOrderID")
	}
	if err := s.checkDir(); err != nil {
		return ExternalCloseReviewState{}, err
	}
	var root ExternalCloseReviewRoot
	if err := strictReviewJSON(s.rootPath(clientID), &root); err != nil {
		return ExternalCloseReviewState{}, err
	}
	if root.Schema != 1 || root.ClientOrderID != clientID || !root.ReviewOnly || root.SettlementAuthorized || root.TradingAuthorized ||
		root.Checksum != root.digest() || !validReviewTime(root.CreatedAtUTC) || validateExternalReviewPreview(root.Rehearsal) != nil ||
		root.Rehearsal.EntryClientOrderID != clientID {
		return ExternalCloseReviewState{}, errors.New("review root validation failed")
	}
	// A checksum-valid sidecar is NOT trustworthy when the original execution
	// evidence has since changed. Revalidate even for read-only `show` calls:
	// callers must not mistake a stale operator receipt for current evidence.
	if err := s.ensureUnchanged(root.Rehearsal); err != nil {
		return ExternalCloseReviewState{}, fmt.Errorf("review evidence is no longer current: %w", err)
	}
	// Reject unknown files, alternate sequence numbers and leftover temporary
	// records: a partially recovered directory is NEVER interpreted as clean.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return ExternalCloseReviewState{}, err
	}
	for _, e := range entries {
		name := e.Name()
		if !externalReviewID.MatchString(strings.TrimSuffix(name, ".json")) &&
			!externalReviewEventName.MatchString(name) {
			return ExternalCloseReviewState{}, fmt.Errorf("unexpected review ledger file: %s", name)
		}
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return ExternalCloseReviewState{}, errors.New("unsafe review ledger entry")
		}
	}
	state := ExternalCloseReviewState{Root: root, Events: []ExternalCloseReviewEvent{}, Stage: ExternalReviewStaged}
	var evt ExternalCloseReviewEvent
	err = strictReviewJSON(s.eventPath(clientID), &evt)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return ExternalCloseReviewState{}, err
	}
	if evt.Schema != 1 || evt.ClientOrderID != clientID || evt.Sequence != 1 || evt.PreviousChecksum != root.Checksum ||
		!evt.ReviewOnly || evt.TradingAuthorized || evt.SettlementAuthorized || evt.Checksum != evt.digest() ||
		(evt.Stage != ExternalReviewAcknowledged && evt.Stage != ExternalReviewRejected) ||
		!validReviewTime(evt.AtUTC) || !externalReviewActor.MatchString(evt.Reviewer) || len(evt.Note) > 512 || strings.TrimSpace(evt.Note) == "" {
		return ExternalCloseReviewState{}, errors.New("review event validation failed")
	}
	started, _ := time.Parse(time.RFC3339Nano, root.CreatedAtUTC)
	ended, _ := time.Parse(time.RFC3339Nano, evt.AtUTC)
	if ended.Before(started) {
		return ExternalCloseReviewState{}, errors.New("review event precedes root")
	}
	state.Events = append(state.Events, evt)
	state.Stage = evt.Stage
	return state, nil
}

func validReviewTime(v string) bool { _, err := time.Parse(time.RFC3339Nano, v); return err == nil }

func (s *ExternalCloseReviewLedger) Stage(p ExternalCloseRehearsal, now time.Time) (ExternalCloseReviewState, error) {
	if s == nil || now.IsZero() {
		return ExternalCloseReviewState{}, errors.New("missing ledger/time")
	}
	if err := validateExternalReviewPreview(p); err != nil {
		return ExternalCloseReviewState{}, err
	}
	if err := s.ensureUnchanged(p); err != nil {
		return ExternalCloseReviewState{}, err
	}
	if err := os.Mkdir(s.dir, 0700); err == nil {
		if err := syncAutoControlDirectory(filepath.Join(s.dataDir, "trading")); err != nil {
			return ExternalCloseReviewState{}, err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return ExternalCloseReviewState{}, err
	}
	if err := s.checkDir(); err != nil {
		return ExternalCloseReviewState{}, err
	}
	path := s.rootPath(p.EntryClientOrderID)
	if _, err := os.Lstat(path); err == nil {
		existing, loadErr := s.Load(p.EntryClientOrderID)
		if loadErr != nil {
			return ExternalCloseReviewState{}, loadErr
		}
		if existing.Root.Rehearsal.PreviewSHA256 != p.PreviewSHA256 {
			return ExternalCloseReviewState{}, errors.New("different evidence for existing review; no overwrite")
		}
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ExternalCloseReviewState{}, err
	}
	r := ExternalCloseReviewRoot{Schema: 1, ClientOrderID: p.EntryClientOrderID, Rehearsal: p,
		CreatedAtUTC: now.UTC().Format(time.RFC3339Nano), ReviewOnly: true}
	r.Checksum = r.digest()
	if err := exclusiveReviewWrite(path, r); err != nil {
		// A concurrent same-evidence creation is idempotent; a partially
		// created file never passes Load and remains blocked.
		if errors.Is(err, os.ErrExist) {
			existing, loadErr := s.Load(p.EntryClientOrderID)
			if loadErr == nil && existing.Root.Rehearsal.PreviewSHA256 == p.PreviewSHA256 {
				return existing, nil
			}
		}
		return ExternalCloseReviewState{}, err
	}
	if err := s.ensureUnchanged(p); err != nil {
		return ExternalCloseReviewState{}, err
	}
	return s.Load(p.EntryClientOrderID)
}

// Decide is a human review receipt, NOT an attestation of Binance history or
// permission to settle. Concurrent callers race on one O_EXCL event filename.
func (s *ExternalCloseReviewLedger) Decide(clientID, stage, reviewer, note string, now time.Time) (ExternalCloseReviewState, error) {
	if s == nil || now.IsZero() || !externalReviewActor.MatchString(reviewer) || len(note) > 512 || strings.TrimSpace(note) == "" || strings.ContainsAny(note, "\x00\r") ||
		(stage != ExternalReviewAcknowledged && stage != ExternalReviewRejected) {
		return ExternalCloseReviewState{}, errors.New("invalid operator review request")
	}
	current, err := s.Load(clientID)
	if err != nil {
		return ExternalCloseReviewState{}, err
	}
	if err := s.ensureUnchanged(current.Root.Rehearsal); err != nil {
		return ExternalCloseReviewState{}, err
	}
	if len(current.Events) > 0 {
		e := current.Events[0]
		if e.Stage == stage && e.Reviewer == reviewer && e.Note == note {
			return current, nil
		}
		return ExternalCloseReviewState{}, errors.New("operator review already finalized; cannot overwrite")
	}
	at := now.UTC().Format(time.RFC3339Nano)
	started, _ := time.Parse(time.RFC3339Nano, current.Root.CreatedAtUTC)
	if now.UTC().Before(started) {
		return ExternalCloseReviewState{}, errors.New("operator review precedes staging")
	}
	e := ExternalCloseReviewEvent{Schema: 1, ClientOrderID: clientID, Sequence: 1, Stage: stage, AtUTC: at,
		PreviousChecksum: current.Root.Checksum, Reviewer: reviewer, Note: note, ReviewOnly: true}
	e.Checksum = e.digest()
	err = exclusiveReviewWrite(s.eventPath(clientID), e)
	if errors.Is(err, os.ErrExist) {
		saved, loadErr := s.Load(clientID)
		if loadErr == nil && len(saved.Events) == 1 && saved.Events[0].Stage == stage && saved.Events[0].Reviewer == reviewer && saved.Events[0].Note == note {
			return saved, nil
		}
	}
	if err != nil {
		return ExternalCloseReviewState{}, err
	}
	if err := s.ensureUnchanged(current.Root.Rehearsal); err != nil {
		return ExternalCloseReviewState{}, err
	}
	return s.Load(clientID)
}
