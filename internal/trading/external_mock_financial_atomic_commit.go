package trading

// Dev.3.131: ISOLATED OFFLINE multi-ledger commit-boundary simulator.
// It never mutates trading Lifecycle, Ownership, wallet, financial accounting,
// or Binance. A single immutable final marker makes three SIMULATED ledger
// views visible together, after independent source revalidation. It is NOT
// an actual multi-ledger financial COMMIT or permission to settle a position.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
)

const (
	ExternalMockCommitAbsent          = "OFFLINE_MOCK_COMMIT_ABSENT_NOT_POSTED"
	ExternalMockCommitStaging         = "OFFLINE_MOCK_COMMIT_STAGING_NOT_POSTED"
	ExternalMockCommitVisible         = "OFFLINE_MOCK_THREE_LEDGERS_VISIBLE_NOT_POSTED"
	ExternalMockCommitAcknowledgement = "SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT"
)

var mockCommitFiles = [...]string{"intent.json", "lifecycle.json", "ownership.json", "financial.json", "commit.json"}
var mockCommitKinds = [...]string{"INTENT", "LIFECYCLE", "OWNERSHIP", "FINANCIAL", "COMMIT"}

// Every candidate is a source-bound proposal reference, NOT a posted amount.
// No model-level funding or final PnL can be obtained from the bounded inputs.
type ExternalMockLedgerRow struct {
	ClientOrderID            string       `json:"client_order_id"`
	ModelID                  string       `json:"model_id"`
	PositionSide             PositionSide `json:"position_side"`
	AtomicRehearsalSHA256    string       `json:"atomic_rehearsal_sha256"`
	FinancialPreflightSHA256 string       `json:"financial_preflight_sha256"`
}

type ExternalMockCommitRecord struct {
	Schema               int                     `json:"schema"`
	Stage                string                  `json:"stage"`
	BatchKey             string                  `json:"batch_key"`
	ConsistencySHA256    string                  `json:"consistency_sha256"`
	PreviousChecksum     string                  `json:"previous_checksum,omitempty"`
	Rows                 []ExternalMockLedgerRow `json:"rows"`
	PostingApplied       bool                    `json:"posting_applied"`
	SettlementAuthorized bool                    `json:"settlement_authorized"`
	TradingAuthorized    bool                    `json:"trading_authorized"`
	Checksum             string                  `json:"checksum"`
}

func (r ExternalMockCommitRecord) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

type ExternalMockCommitInspection struct {
	Schema                int                     `json:"schema"`
	Stage                 string                  `json:"stage"`
	BatchKey              string                  `json:"batch_key"`
	ConsistencySHA256     string                  `json:"consistency_sha256"`
	CompletedSteps        int                     `json:"completed_steps"`
	SimulatedViewsVisible bool                    `json:"simulated_views_visible"`
	Lifecycle             []ExternalMockLedgerRow `json:"lifecycle,omitempty"`
	Ownership             []ExternalMockLedgerRow `json:"ownership,omitempty"`
	Financial             []ExternalMockLedgerRow `json:"financial,omitempty"`
	CommitSHA256          string                  `json:"commit_sha256,omitempty"`
	PostingApplied        bool                    `json:"posting_applied"`
	SettlementAuthorized  bool                    `json:"settlement_authorized"`
	TradingAuthorized     bool                    `json:"trading_authorized"`
}

type ExternalMockFinancialCommitStore struct{ root, dir string }

// The caller must supply a PRE-EXISTING offline root, not the source data root.
func NewExternalMockFinancialCommitStore(root string) (*ExternalMockFinancialCommitStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("existing separate mock root required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	st, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe mock root")
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	return &ExternalMockFinancialCommitStore{root: canonical, dir: filepath.Join(canonical, "offline_mock_multi_ledger_commits")}, nil
}

func externalMockOverlaps(a, b string) bool {
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		rel, e := filepath.Rel(pair[0], pair[1])
		if e != nil {
			return true
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
			return true
		}
	}
	return false
}

func (s *ExternalMockFinancialCommitStore) sourceState(ctx context.Context, registry *ExternalGlobalEvidenceReservationStore, events []ExternalFinancialEventSource) (ExternalGlobalAtomicConsistency, error) {
	var zero ExternalGlobalAtomicConsistency
	if ctx == nil || s == nil || registry == nil || len(events) < 2 || len(events) > 64 {
		return zero, errors.New("context, store, registry and 2..64 events required")
	}
	for _, path := range append([]string{registry.root}, func() []string {
		z := make([]string, 0, len(events))
		for _, e := range events {
			z = append(z, e.DataDir)
		}
		return z
	}()...) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return zero, err
		}
		abs, err = filepath.EvalSymlinks(abs)
		if err != nil {
			return zero, err
		}
		if externalMockOverlaps(s.root, abs) {
			return zero, errors.New("mock commit root overlaps registry or original source")
		}
	}
	state, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		return zero, err
	}
	if state.Schema != 1 || state.Stage != ExternalGlobalAtomicConsistent || !state.AllSimulatedPairsVisible ||
		len(state.Candidates) < 2 || state.DigestSHA256 != state.digest() ||
		state.SharedAccountProven || state.CompleteHistoryProven || state.FundingAllocated ||
		state.PostingApplied || state.SettlementAuthorized || state.TradingAuthorized {
		return zero, errors.New("unsealed, changed or authorizing simulated evidence refused")
	}
	return state, nil
}

func mockExpectedRecords(state ExternalGlobalAtomicConsistency) [5]ExternalMockCommitRecord {
	rows := make([]ExternalMockLedgerRow, 0, len(state.Candidates))
	for _, c := range state.Candidates {
		rows = append(rows, ExternalMockLedgerRow{c.ClientOrderID, c.ModelID, c.PositionSide, c.AtomicCommitSHA256, c.FinancialPreflightSHA256})
	}
	var out [5]ExternalMockCommitRecord
	prev := ""
	for i := range out {
		out[i] = ExternalMockCommitRecord{Schema: 1, Stage: mockCommitKinds[i], BatchKey: state.BatchKey, ConsistencySHA256: state.DigestSHA256, PreviousChecksum: prev, Rows: rows}
		out[i].Checksum = out[i].digest()
		prev = out[i].Checksum
	}
	return out
}

func mockCheckDirectory(path string, create bool) (bool, error) {
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		if err = os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return false, err
		}
		st, err = os.Lstat(path)
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("unsafe mock ledger directory")
	}
	return true, nil
}

func mockReadRecord(path string) (ExternalMockCommitRecord, error) {
	var out ExternalMockCommitRecord
	st, err := os.Lstat(path)
	if err != nil {
		return out, err
	}
	if !st.Mode().IsRegular() || st.Size() < 2 || st.Size() > 256<<10 {
		return out, errors.New("unsafe or oversized immutable checkpoint")
	}
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, (256<<10)+1))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&out); err != nil {
		return out, err
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return out, errors.New("checkpoint contains trailing data")
	}
	if out.Checksum != out.digest() {
		return out, errors.New("checkpoint digest mismatch")
	}
	return out, nil
}

func mockWriteExclusive(path string, rec ExternalMockCommitRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, werr := f.Write(append(b, '\n'))
	if werr == nil && n != len(b)+1 {
		werr = errors.New("short checkpoint write")
	}
	if werr == nil {
		werr = f.Sync()
	}
	cerr := f.Close()
	if werr != nil || cerr != nil {
		return fmt.Errorf("torn immutable checkpoint; manual recovery required: write=%v close=%v", werr, cerr)
	}
	if runtime.GOOS != "windows" {
		d, e := os.Open(filepath.Dir(path))
		if e != nil {
			return e
		}
		e = d.Sync()
		_ = d.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

func (s *ExternalMockFinancialCommitStore) scan(state ExternalGlobalAtomicConsistency) (ExternalMockCommitInspection, int, error) {
	expected := mockExpectedRecords(state)
	result := ExternalMockCommitInspection{Schema: 1, Stage: ExternalMockCommitAbsent, BatchKey: state.BatchKey, ConsistencySHA256: state.DigestSHA256}
	rootOK, err := mockCheckDirectory(s.dir, false)
	if err != nil {
		return result, 0, err
	}
	if !rootOK {
		return result, 0, nil
	}
	path := filepath.Join(s.dir, "batch-"+state.BatchKey)
	exists, err := mockCheckDirectory(path, false)
	if err != nil {
		return result, 0, err
	}
	if !exists {
		return result, 0, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return result, 0, err
	}
	for _, entry := range entries {
		allowed := false
		for _, file := range mockCommitFiles {
			if entry.Name() == file {
				allowed = true
				break
			}
		}
		if !allowed || !entry.Type().IsRegular() {
			return result, 0, errors.New("unexpected or unsafe file in mock commit batch")
		}
	}
	count := 0
	missing := false
	for i, name := range mockCommitFiles {
		r, e := mockReadRecord(filepath.Join(path, name))
		if errors.Is(e, os.ErrNotExist) {
			missing = true
			continue
		}
		if e != nil {
			return result, 0, fmt.Errorf("%s: %w", name, e)
		}
		if missing {
			return result, 0, errors.New("mock commit checkpoint sequence gap")
		}
		if !reflect.DeepEqual(r, expected[i]) {
			return result, 0, fmt.Errorf("%s: changed or foreign immutable batch", name)
		}
		count++
	}
	result.CompletedSteps = count
	if count > 0 {
		result.Stage = ExternalMockCommitStaging
	}
	if count == len(mockCommitFiles) {
		result.Stage = ExternalMockCommitVisible
		result.SimulatedViewsVisible = true
		result.Lifecycle = append([]ExternalMockLedgerRow(nil), expected[1].Rows...)
		result.Ownership = append([]ExternalMockLedgerRow(nil), expected[2].Rows...)
		result.Financial = append([]ExternalMockLedgerRow(nil), expected[3].Rows...)
		result.CommitSHA256 = expected[4].Checksum
	}
	return result, count, nil
}

// Inspect checks the original evidence AGAIN and never returns a partial ledger
// view before the last immutable commit marker is valid. It writes nothing.
func (s *ExternalMockFinancialCommitStore) Inspect(ctx context.Context, registry *ExternalGlobalEvidenceReservationStore, events []ExternalFinancialEventSource) (ExternalMockCommitInspection, error) {
	var zero ExternalMockCommitInspection
	state, err := s.sourceState(ctx, registry, events)
	if err != nil {
		return zero, err
	}
	result, _, err := s.scan(state)
	if err != nil {
		return zero, err
	}
	fresh, err := s.sourceState(ctx, registry, events)
	if err != nil || !reflect.DeepEqual(state, fresh) {
		return zero, errors.New("original evidence changed during mock commit inspection")
	}
	return result, nil
}

// Advance writes exactly ONE requested stage. Attempts to skip stages, change
// an existing snapshot, or recover a torn checkpoint fail closed. This routine
// cannot post a real transaction even with a valid external review receipt.
func (s *ExternalMockFinancialCommitStore) Advance(ctx context.Context, registry *ExternalGlobalEvidenceReservationStore, events []ExternalFinancialEventSource, stage, ack string) (ExternalMockCommitInspection, error) {
	var zero ExternalMockCommitInspection
	if ack != ExternalMockCommitAcknowledgement {
		return zero, errors.New("explicit simulation-only acknowledgement required")
	}
	target := -1
	for i, k := range mockCommitKinds {
		if k == stage {
			target = i
		}
	}
	if target < 0 {
		return zero, errors.New("unknown mock commit stage")
	}
	state, err := s.sourceState(ctx, registry, events)
	if err != nil {
		return zero, err
	}
	_, count, err := s.scan(state)
	if err != nil {
		return zero, err
	}
	if target > count {
		return zero, errors.New("mock commit stage cannot skip an unsealed predecessor")
	}
	if target == count {
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		if _, err = mockCheckDirectory(s.dir, true); err != nil {
			return zero, err
		}
		batch := filepath.Join(s.dir, "batch-"+state.BatchKey)
		if _, err = mockCheckDirectory(batch, true); err != nil {
			return zero, err
		}
		expected := mockExpectedRecords(state)
		err = mockWriteExclusive(filepath.Join(batch, mockCommitFiles[target]), expected[target])
		if errors.Is(err, os.ErrExist) {
			return zero, errors.New("concurrent mock writer: re-inspect before retrying")
		}
		if err != nil {
			return zero, err
		}
	}
	return s.Inspect(ctx, registry, events)
}
