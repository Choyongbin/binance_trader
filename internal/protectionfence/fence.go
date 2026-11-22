// Package protectionfence provides a durable, no-replay boundary for proposed
// Binance Demo TP/SL mutations. It does NOT authorize or perform any exchange
// POST/DELETE, and it does not establish ownership, stream continuity or OCO.
// Integration must acquire the existing product coordinator's exclusive lease,
// independently verify signed account evidence, and keep HTTP 423 locked until
// full Demo certification. A file here is evidence of MAY_HAVE_SENT, not success.
package protectionfence

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

// ErrOutcomeUnknown is returned whenever an intent was already fenced. Even
// if the exchange was never contacted, an automatic replay is not allowed.
var ErrOutcomeUnknown = errors.New("protection mutation MAY_HAVE_SENT: signed recovery required; automatic retry forbidden")
var ErrCorruptFence = errors.New("protection mutation fence corrupt or conflicting: manual review required")

// The two independent conditional orders are NOT a Binance atomic OCO pair.
// Mutations are separate and never imply a safe subsequent mutation.
type Operation string

const (
	SubmitTP      Operation = "SUBMIT_TP"
	SubmitSL      Operation = "SUBMIT_SL"
	CancelSibling Operation = "CANCEL_SIBLING"
)

// Intent must be assembled from an immutable protection plan and independently
// verified signed evidence by the caller. SHA-256 digests are *identifiers*,
// not proof of authenticity or an authorization ticket.
type Intent struct {
	Environment              string    `json:"environment"`
	GroupID                  string    `json:"group_id"`
	Operation                Operation `json:"operation"`
	Symbol                   string    `json:"symbol"`
	PositionSide             string    `json:"position_side"`
	QuantityBTC              string    `json:"quantity_btc"`
	TargetClientAlgoID       string    `json:"target_client_algo_id"`
	TargetAlgoID             int64     `json:"target_algo_id,omitempty"`
	PlanSHA256               string    `json:"plan_sha256"`
	AccountFingerprintSHA256 string    `json:"account_fingerprint_sha256"`
	EvidenceSHA256           string    `json:"evidence_sha256"`
}

// Fence has no approval flag. Sealed before the first *possible* exchange
// call, it remains MAY_HAVE_SENT across process restarts and ACK losses.
type Fence struct {
	Schema    int    `json:"schema"`
	Intent    Intent `json:"intent"`
	State     string `json:"state"`
	CreatedAt string `json:"created_at"`
	Checksum  string `json:"checksum"`
}

const state = "MAY_HAVE_SENT_RECOVERY_ONLY"
const maxFences = 4096

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$`)
var positiveDecimal = regexp.MustCompile(`^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,8})?$`)

func isSHA256(v string) bool {
	if len(v) != 64 || strings.ToLower(v) != v {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}
func validID(v string) bool { return safeID.MatchString(v) }

func (i Intent) Validate() error {
	if i.Environment != "demo" || !validID(i.GroupID) || !validID(i.TargetClientAlgoID) || i.Symbol != "BTCUSDT" ||
		(i.PositionSide != "LONG" && i.PositionSide != "SHORT") ||
		!positiveDecimal.MatchString(i.QuantityBTC) ||
		i.QuantityBTC == "0" || strings.Trim(i.QuantityBTC, "0.") == "" ||
		!isSHA256(i.PlanSHA256) || !isSHA256(i.AccountFingerprintSHA256) || !isSHA256(i.EvidenceSHA256) {
		return errors.New("invalid Demo protection mutation identity or evidence digest")
	}
	switch i.Operation {
	case SubmitTP, SubmitSL:
		if i.TargetAlgoID != 0 {
			return errors.New("a first submit cannot identify an existing exchange Algo ID")
		}
	case CancelSibling:
		if i.TargetAlgoID <= 0 {
			return errors.New("sibling cancel requires a positive signed exchange Algo ID")
		}
	default:
		return errors.New("unknown protection mutation operation")
	}
	return nil
}

func (f Fence) digest() string {
	f.Checksum = ""
	b, _ := json.Marshal(f)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (f Fence) validate() error {
	if f.Schema != 1 || f.State != state || f.CreatedAt == "" ||
		f.Intent.Validate() != nil || !isSHA256(f.Checksum) || f.Checksum != f.digest() {
		return ErrCorruptFence
	}
	t, err := time.Parse(time.RFC3339Nano, f.CreatedAt)
	if err != nil || t.IsZero() {
		return ErrCorruptFence
	}
	return nil
}

// Store never performs exchange network activity. Multiple processes may race
// to create the same file: O_EXCL ensures at most one succeeds. A directory
// level single-writer lease is still required by the product orchestration.
type Store struct{ dir string }

// Open creates a private directory. Do not place it on a network filesystem,
// or accept a data directory from an unauthenticated HTTP request.
func Open(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("data directory is required")
	}
	dir := filepath.Join(dataDir, "trading", "product_mutation_fences")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrCorruptFence
	}
	return &Store{dir: dir}, nil
}

// One immutable account-level slot deliberately serializes even DIFFERENT
// groups/operations across processes, including two TP/SL legs. A future
// signed-reconciliation protocol must be implemented before further actions.
func (s *Store) name(_ Intent) string {
	return filepath.Join(s.dir, "unreconciled-mutation.json")
}
func (s *Store) inspectPath(path string) (Fence, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return Fence{}, err
	}
	if !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Size() > 16384 {
		return Fence{}, ErrCorruptFence
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Fence{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var f Fence
	if dec.Decode(&f) != nil || dec.Decode(new(any)) != io.EOF || f.validate() != nil ||
		path != s.name(f.Intent) {
		return Fence{}, ErrCorruptFence
	}
	return f, nil
}

// InspectAll scans *every* local fence so a corrupted or orphaned record
// blocks creation of new records rather than being silently ignored.
// This scan does not claim the wider account/ownership ledgers are complete.
func (s *Store) InspectAll() ([]Fence, error) {
	if s == nil {
		return nil, ErrCorruptFence
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	if len(entries) > maxFences {
		return nil, ErrCorruptFence
	}
	fences := make([]Fence, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			return nil, ErrCorruptFence
		}
		f, err := s.inspectPath(filepath.Join(s.dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("inspect fence %q: %w", e.Name(), err)
		}
		fences = append(fences, f)
	}
	return fences, nil
}

// Inspect reads one recorded outcome. Never use a missing record as proof
// that Binance has not received an earlier request from legacy code.
func (s *Store) Inspect(i Intent) (Fence, error) {
	if s == nil || i.Validate() != nil {
		return Fence{}, ErrCorruptFence
	}
	f, err := s.inspectPath(s.name(i))
	if err != nil {
		return Fence{}, err
	}
	if f.Intent != i {
		if f.Intent.GroupID == i.GroupID && f.Intent.Operation == i.Operation {
			return Fence{}, ErrCorruptFence
		}
		return Fence{}, ErrOutcomeUnknown
	}
	return f, nil
}

// SealBeforePossibleSend is a write-ahead safety marker, not authorization
// to send. On ANY second call (even with exactly matching identity) it returns
// ErrOutcomeUnknown and must lead to signed GET-only recovery. A crash or disk
// error may leave a corrupt marker, which also fails closed.
func (s *Store) SealBeforePossibleSend(i Intent, now time.Time) (Fence, error) {
	if s == nil || i.Validate() != nil || now.IsZero() {
		return Fence{}, ErrCorruptFence
	}
	existing, err := s.InspectAll()
	if err != nil {
		return Fence{}, err
	}
	if len(existing) > 0 {
		if existing[0].Intent.GroupID == i.GroupID && existing[0].Intent.Operation == i.Operation && existing[0].Intent != i {
			return Fence{}, ErrCorruptFence
		}
		return Fence{}, ErrOutcomeUnknown
	}
	f := Fence{Schema: 1, Intent: i, State: state, CreatedAt: now.UTC().Format(time.RFC3339Nano)}
	f.Checksum = f.digest()
	b, err := json.Marshal(f)
	if err != nil {
		return Fence{}, err
	}
	path := s.name(i)
	fd, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		// Corrupted, mismatched or incomplete prior record -> manual review.
		if prior, readErr := s.inspectPath(path); readErr != nil {
			return Fence{}, ErrCorruptFence
		} else if prior.Intent.GroupID == i.GroupID && prior.Intent.Operation == i.Operation && prior.Intent != i {
			return Fence{}, ErrCorruptFence
		}
		return Fence{}, ErrOutcomeUnknown
	}
	if err != nil {
		return Fence{}, err
	}
	if _, err = fd.Write(b); err == nil {
		err = fd.Sync()
	}
	closeErr := fd.Close()
	if err != nil {
		return Fence{}, err
	} // never remove uncertain record
	if closeErr != nil {
		return Fence{}, closeErr
	}
	// On a filesystem lacking durable directory metadata guarantees, the
	// *caller* must not claim exactly-once ordering. Fsync on file guarantees
	// data submission but cannot establish that the exchange received a POST.
	return f, nil
}
