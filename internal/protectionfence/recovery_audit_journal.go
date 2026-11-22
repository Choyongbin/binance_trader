package protectionfence

// An append-only, crash-conservative, local READ-ONLY-recovery audit journal.
// "Read-only" describes exchange operations; this file writes LOCAL audit
// evidence. The journal never clears a mutation fence or authorizes trading.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const maxRecoveryAuditRecords = 1024
const maxRecoveryAuditBytes = 65536

var ErrAuditIntegrity = errors.New("protection recovery audit journal is incomplete or corrupt: operator reconciliation required")

type RecoveryAuditRecord struct {
	Schema         int              `json:"schema"`
	Sequence       int              `json:"sequence"`
	FenceChecksum  string           `json:"fence_checksum"`
	PreviousSHA256 string           `json:"previous_sha256,omitempty"`
	RecordedAt     time.Time        `json:"recorded_at"`
	Recovery       RecoveryReview   `json:"recovery"`
	Exposure       ExposureSnapshot `json:"exposure"`
	ExposureReview ExposureReview   `json:"exposure_review"`
	Checksum       string           `json:"checksum"`
}

func (a RecoveryAuditRecord) digest() string {
	a.Checksum = ""
	raw, _ := json.Marshal(a)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validRecoveryReview(r RecoveryReview, f Fence, now time.Time) bool {
	if r.GroupID != f.Intent.GroupID || r.Operation != f.Intent.Operation ||
		r.TargetClientAlgoID != f.Intent.TargetClientAlgoID || !r.ManualReviewRequired ||
		r.TradingAuthorized || r.SubmitAuthorized || r.CancelAuthorized || r.RetryAuthorized || r.SettlementAuthorized ||
		len(r.Reasons) == 0 || len(r.Reasons) > 32 ||
		r.FirstObservedAt.IsZero() || r.SecondObservedAt.IsZero() ||
		r.SecondObservedAt.Before(r.FirstObservedAt) || r.SecondObservedAt.After(now.Add(time.Second)) ||
		now.Sub(r.SecondObservedAt) > 5*time.Second || now.Sub(r.FirstObservedAt) > 5*time.Second {
		return false
	}
	for _, reason := range r.Reasons {
		if reason == "" || len(reason) > 512 {
			return false
		}
	}
	switch r.State {
	case RecoveryAbsentUnproven, RecoveryTerminalNeedsAudit:
		return true
	case RecoverySubmitObserved:
		return f.Intent.Operation == SubmitTP || f.Intent.Operation == SubmitSL
	case RecoveryCancelStillActive, RecoveryCancelObserved:
		return f.Intent.Operation == CancelSibling
	default:
		return false
	}
}

func (a RecoveryAuditRecord) validate(f Fence, previous string) error {
	if a.Schema != 1 || a.Sequence < 1 || a.FenceChecksum != f.Checksum ||
		a.PreviousSHA256 != previous || !isSHA256(a.Checksum) || a.Checksum != a.digest() ||
		a.RecordedAt.IsZero() || a.RecordedAt.After(time.Now().UTC().Add(time.Second)) ||
		!validRecoveryReview(a.Recovery, f, a.RecordedAt) {
		return ErrAuditIntegrity
	}
	expected := AssessExposure(f, a.Exposure, a.RecordedAt)
	if !reflect.DeepEqual(expected, a.ExposureReview) || expected.TradingAuthorized || expected.SubmitAuthorized ||
		expected.CancelAuthorized || expected.RetryAuthorized || expected.SettlementAuthorized || !expected.ManualReviewRequired {
		return ErrAuditIntegrity
	}
	return nil
}

func (s *Store) auditDir() string {
	return filepath.Join(filepath.Dir(s.dir), "signed_recovery_audit_v14")
}
func auditName(i int) string { return fmt.Sprintf("review-%06d.json", i) }

func readAuditFile(path string) (RecoveryAuditRecord, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return RecoveryAuditRecord{}, err
	}
	if !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 || st.Size() > maxRecoveryAuditBytes {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return RecoveryAuditRecord{}, err
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	var result RecoveryAuditRecord
	if d.Decode(&result) != nil || d.Decode(new(any)) != io.EOF {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	return result, nil
}

// ReadRecoveryAudit does NOT create the journal. A nonexistent journal means
// there is no local audit trail, NOT that the exchange never received an order.
func (s *Store) ReadRecoveryAudit() ([]RecoveryAuditRecord, error) {
	if s == nil {
		return nil, ErrAuditIntegrity
	}
	fence, err := s.InspectAll()
	if err != nil || len(fence) != 1 {
		return nil, ErrAuditIntegrity
	}
	root := s.auditDir()
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return []RecoveryAuditRecord{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrAuditIntegrity
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	if len(entries) > maxRecoveryAuditRecords {
		return nil, ErrAuditIntegrity
	}
	out := make([]RecoveryAuditRecord, 0, len(entries))
	var prev string
	for index, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || entry.Name() != auditName(index+1) {
			return nil, ErrAuditIntegrity
		}
		item, err := readAuditFile(filepath.Join(root, entry.Name()))
		if err != nil || item.Sequence != index+1 || item.validate(fence[0], prev) != nil {
			return nil, ErrAuditIntegrity
		}
		if index > 0 {
			last := out[index-1]
			if item.RecordedAt.Before(last.RecordedAt) ||
				item.Recovery.SecondObservedAt.Before(last.Recovery.SecondObservedAt) {
				return nil, ErrAuditIntegrity
			}
		}
		out = append(out, item)
		prev = item.Checksum
	}
	return out, nil
}

// AppendRecoveryAudit persists an immutable classification with O_EXCL after
// existing records have been fully validated. It performs NO Binance API calls
// and has NO ability to release a fence or approve another mutation.
// The caller must hold the product coordinator lease across the signed reads
// and this append. Concurrent writers race safely: only one O_EXCL succeeds.
func (s *Store) AppendRecoveryAudit(recovery RecoveryReview, exposure ExposureSnapshot, now time.Time) (RecoveryAuditRecord, error) {
	if s == nil || now.IsZero() {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	fences, err := s.InspectAll()
	if err != nil || len(fences) != 1 {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	f := fences[0]
	if !validRecoveryReview(recovery, f, now.UTC()) {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	previous, err := s.ReadRecoveryAudit()
	if err != nil {
		return RecoveryAuditRecord{}, err
	}
	if len(previous) >= maxRecoveryAuditRecords {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	if len(previous) > 0 {
		last := previous[len(previous)-1]
		if now.Before(last.RecordedAt) || !recovery.SecondObservedAt.After(last.Recovery.SecondObservedAt) {
			return RecoveryAuditRecord{}, ErrOutcomeUnknown
		}
	}
	record := RecoveryAuditRecord{Schema: 1, Sequence: len(previous) + 1, FenceChecksum: f.Checksum,
		Recovery: recovery, Exposure: exposure, RecordedAt: now.UTC(), ExposureReview: AssessExposure(f, exposure, now.UTC())}
	if len(previous) > 0 {
		record.PreviousSHA256 = previous[len(previous)-1].Checksum
	}
	record.Checksum = record.digest()
	if record.validate(f, record.PreviousSHA256) != nil {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	raw, err := json.Marshal(record)
	if err != nil || len(raw) > maxRecoveryAuditBytes {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	root := s.auditDir()
	if err = os.Mkdir(root, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return RecoveryAuditRecord{}, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return RecoveryAuditRecord{}, ErrAuditIntegrity
	}
	path := filepath.Join(root, auditName(record.Sequence))
	fd, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return RecoveryAuditRecord{}, ErrOutcomeUnknown
	}
	if err != nil {
		return RecoveryAuditRecord{}, err
	}
	writeErr := error(nil)
	if _, writeErr = fd.Write(raw); writeErr == nil {
		writeErr = fd.Sync()
	}
	closeErr := fd.Close()
	if writeErr != nil {
		return RecoveryAuditRecord{}, writeErr
	}
	if closeErr != nil {
		return RecoveryAuditRecord{}, closeErr
	}
	// A successful file Sync is not a guarantee that Windows directory
	// metadata survived a power loss. Do not treat this as exactly-once proof.
	return record, nil
}
