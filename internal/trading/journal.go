package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// State = PREPARED before side effects; UNKNOWN when transport may have
// reached the exchange; ACKNOWLEDGED is not synonymous with FILLED.
var ErrNeedsReconciliation = errors.New("기존 요청 결과가 불명확합니다. 거래소 상태를 확인하기 전 다시 주문할 수 없습니다")

type State string

const (
	Prepared     State = "PREPARED"
	Unknown      State = "UNKNOWN"
	Acknowledged State = "ACKNOWLEDGED"
	Rejected     State = "REJECTED"
	Reconciled   State = "RECONCILED"
)

type Record struct {
	Schema           int          `json:"schema"`
	Sequence         int          `json:"sequence,omitempty"`
	PreviousChecksum string       `json:"previous_checksum,omitempty"`
	Intent           Intent       `json:"intent"`
	IntentDigest     string       `json:"intent_digest"`
	Plan             PlannedOrder `json:"plan"`
	State            State        `json:"state"`
	ExchangeOrderID  int64        `json:"exchange_order_id,omitempty"`
	ExchangeStatus   string       `json:"exchange_status,omitempty"`
	UpdatedAt        string       `json:"updated_at"`
	Checksum         string       `json:"checksum,omitempty"`
	// Never include API keys or raw exchange error bodies in this journal.
}

func (r Record) digest() string {
	copy := r
	copy.Checksum = ""
	raw, _ := json.Marshal(copy)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (r *Record) seal() { r.Checksum = r.digest() }

func supportedJournalSchema(schema int) bool { return schema == 1 || schema == 2 }

func newPreparedRecord(intent Intent, digest string, plan PlannedOrder, now time.Time) Record {
	r := Record{Schema: 2, Sequence: 0, Intent: intent, IntentDigest: digest, Plan: plan, State: Prepared, UpdatedAt: now.UTC().Format(time.RFC3339Nano)}
	r.seal()
	return r
}

type Submission struct {
	ClientOrderID string
	OrderID       int64
	Status        string // e.g. NEW, PARTIALLY_FILLED, FILLED; not assumed if empty
}

type Executor interface {
	Submit(context.Context, PlannedOrder) (Submission, error)
}

// Reconciler must query the exchange by clientOrderID. It must NEVER submit.
type Reconciler interface {
	QueryByClientOrderID(context.Context, string, string) (Submission, error)
}

type Engine struct {
	mu       sync.Mutex // serialize all models and manual requests within process
	dir      string
	executor Executor
	// This stage is a library tested with a mock executor only. The HTTP
	// server intentionally does not instantiate it with a live exchange.
}

func NewEngine(dataDir string, executor Executor) (*Engine, error) {
	if executor == nil {
		return nil, errors.New("주문 실행기가 필요합니다")
	}
	dir := filepath.Join(dataDir, "trading", "requests")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &Engine{dir: dir, executor: executor}, nil
}
func (e *Engine) path(plan PlannedOrder) string {
	return filepath.Join(e.dir, plan.ClientOrderID+".json")
}

// readRecord starts with the immutable PREPARED reservation and replays
// immutable state sidecars. This avoids replacing an existing JSON file:
// os.Rename-over-existing is not portable on Windows and can lose the only
// crash-safe reservation if implemented as remove+rename.
func readRecord(path string) (Record, error) {
	original, err := decodeRecord(path)
	if err != nil {
		return Record{}, err
	}
	if original.State != Prepared {
		return Record{}, errors.New("요청 원본은 PREPARED 상태여야 합니다")
	}
	dir := filepath.Dir(path)
	prefix := strings.TrimSuffix(filepath.Base(path), ".json") + ".state."
	files, err := os.ReadDir(dir)
	if err != nil {
		return Record{}, err
	}
	expected := 1
	latest := original
	previousChecksum := original.Checksum
	previousAt, _ := time.Parse(time.RFC3339Nano, original.UpdatedAt)
	for _, file := range files {
		name := file.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if len(name) != len(prefix)+6+len(".json") || !strings.HasSuffix(name, ".json") || file.IsDir() || file.Type()&os.ModeSymlink != 0 {
			return Record{}, errors.New("요청 상태 파일 이름에 이상이 있습니다. 수동 검사가 필요합니다")
		}
		number := name[len(prefix) : len(prefix)+6]
		if number != fmt.Sprintf("%06d", expected) {
			return Record{}, errors.New("요청 상태 파일이 누락되었거나 중복되었습니다")
		}
		next, e := decodeRecord(filepath.Join(dir, name))
		if e != nil {
			return Record{}, e
		}
		if next.IntentDigest != original.IntentDigest || next.Intent != original.Intent || next.Plan != original.Plan || next.State == Prepared || next.State == "" {
			return Record{}, errors.New("요청 상태 파일의 내용이 기존 예약과 일치하지 않습니다")
		}
		if original.Schema == 2 {
			if next.Schema != 2 || next.Sequence != expected || next.PreviousChecksum != previousChecksum {
				return Record{}, errors.New("요청 상태 SHA-256 체인 또는 순번이 일치하지 않습니다")
			}
			if !validJournalTransition(latest.State, next.State) {
				return Record{}, errors.New("요청 상태 전이가 허용되지 않습니다")
			}
		} else if next.Schema != 1 {
			return Record{}, errors.New("legacy 요청 저널에 혼합 schema sidecar가 있습니다")
		}
		nextAt, _ := time.Parse(time.RFC3339Nano, next.UpdatedAt)
		if nextAt.Before(previousAt) {
			return Record{}, errors.New("요청 상태 시각 순서가 역전되었습니다")
		}
		latest = next
		previousChecksum = next.Checksum
		previousAt = nextAt
		expected++
	}
	return latest, nil
}

func validJournalTransition(from, to State) bool {
	switch from {
	case Prepared:
		return to == Unknown || to == Acknowledged || to == Rejected || to == Reconciled
	case Unknown, Acknowledged, Rejected:
		return to == Reconciled
	case Reconciled:
		return to == Reconciled
	default:
		return false
	}
}

func decodeRecord(path string) (Record, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Record{}, err
	}
	var r Record
	dec := json.NewDecoder(newReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return Record{}, fmt.Errorf("기존 요청 기록을 읽을 수 없습니다: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) || (r.Schema != 1 && r.Schema != 2) || r.Plan.ClientOrderID == "" || r.IntentDigest == "" {
		return Record{}, errors.New("기존 요청 기록이 불완전합니다. 수동 검증이 필요합니다")
	}
	if _, err := time.Parse(time.RFC3339Nano, r.UpdatedAt); err != nil {
		return Record{}, errors.New("요청 기록 시각이 손상되었습니다")
	}
	plan, err := r.Intent.Plan()
	if err != nil || plan != r.Plan {
		return Record{}, errors.New("요청 기록 주문 계획이 원본 intent와 일치하지 않습니다")
	}
	digest, err := digestIntent(r.Intent)
	if err != nil || digest != r.IntentDigest {
		return Record{}, errors.New("요청 기록 intent digest가 일치하지 않습니다")
	}
	if r.Schema == 1 {
		if r.Sequence != 0 || r.PreviousChecksum != "" || r.Checksum != "" {
			return Record{}, errors.New("legacy 요청 기록에 예상하지 않은 chain 필드가 있습니다")
		}
		return r, nil
	}
	if r.Checksum == "" || r.Checksum != r.digest() {
		return Record{}, errors.New("요청 기록 SHA-256 무결성 검증에 실패했습니다")
	}
	if r.Sequence == 0 {
		if r.PreviousChecksum != "" || r.State != Prepared {
			return Record{}, errors.New("요청 원본 chain 형식이 유효하지 않습니다")
		}
	} else if r.PreviousChecksum == "" || r.State == Prepared {
		return Record{}, errors.New("요청 상태 sidecar chain 형식이 유효하지 않습니다")
	}
	switch r.State {
	case Prepared, Unknown:
		if r.ExchangeOrderID != 0 || r.ExchangeStatus != "" {
			return Record{}, errors.New("주문 미확정 상태에 거래소 식별자가 포함되어 있습니다")
		}
	case Acknowledged, Reconciled:
		if r.ExchangeOrderID <= 0 || r.ExchangeStatus == "" {
			return Record{}, errors.New("주문 확인 상태의 거래소 증적이 불완전합니다")
		}
	case Rejected:
	default:
		return Record{}, errors.New("알 수 없는 요청 상태")
	}
	return r, nil
}

func newReader(raw []byte) *byteReader { return &byteReader{raw} }

type byteReader struct{ data []byte }

func (b *byteReader) Read(dst []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(dst, b.data)
	b.data = b.data[n:]
	return n, nil
}

// ensureNew uses O_EXCL so a SECOND PROCESS cannot submit a duplicate even
// if it bypasses the per-engine mutex. Existing PREPARED records are treated
// as potentially submitted after a crash; only explicit reconciliation helps.
func (e *Engine) ensureNew(plan PlannedOrder, intent Intent, digest string) (Record, bool, error) {
	path := e.path(plan)
	record := newPreparedRecord(intent, digest, plan, time.Now().UTC())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		old, err := readRecord(path)
		if err != nil {
			return Record{}, false, err
		}
		if old.IntentDigest != digest || old.Plan != plan {
			return Record{}, false, errors.New("동일한 요청 ID에 다른 주문 내용이 들어왔습니다")
		}
		return old, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	body, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		f.Close()
		return Record{}, false, err
	}
	if _, err = f.Write(append(body, '\n')); err == nil {
		err = f.Sync()
	}
	cerr := f.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		// Deliberately KEEP the incomplete record: removing it could cause a
		// duplicate if an external process races. Halt for manual inspection.
		return Record{}, false, fmt.Errorf("요청 사전 기록 실패: %w", err)
	}
	return record, true, nil
}

// publish adds a NEW immutable state file. Never delete or replace the
// original reservation, and never remove a partially written state file:
// that would allow a stale reader to assume the request was never handled.
func (e *Engine) publish(rec Record) error {
	basePath := e.path(rec.Plan)
	original, err := decodeRecord(basePath)
	if err != nil {
		return err
	}
	base := strings.TrimSuffix(basePath, ".json")
	if original.Schema == 1 {
		// Preserve legacy dev.3.13 chains byte-for-byte compatible. New roots use
		// schema 2; we never silently splice a new checksum format into old data.
		rec.Schema, rec.Sequence, rec.PreviousChecksum, rec.Checksum = 1, 0, "", ""
		rec.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		raw, err := json.MarshalIndent(rec, "", "  ")
		if err != nil {
			return err
		}
		for i := 1; i <= 10000; i++ {
			path := fmt.Sprintf("%s.state.%06d.json", base, i)
			f, openErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if errors.Is(openErr, os.ErrExist) {
				continue
			}
			if openErr != nil {
				return openErr
			}
			_, writeErr := f.Write(append(raw, '\n'))
			if writeErr == nil {
				writeErr = f.Sync()
			}
			closeErr := f.Close()
			if writeErr == nil {
				writeErr = closeErr
			}
			if writeErr != nil {
				return fmt.Errorf("요청 상태 디스크 기록 실패: %w", writeErr)
			}
			return nil
		}
		return errors.New("요청 상태 파일이 너무 많습니다. 수동 점검이 필요합니다")
	}

	for attempt := 0; attempt < 10000; attempt++ {
		latest, err := readRecord(basePath)
		if err != nil {
			return err
		}
		if latest.Schema != 2 || latest.Checksum == "" {
			return errors.New("schema 2 요청 chain 기준점을 확인할 수 없습니다")
		}
		if latest.Intent != rec.Intent || latest.IntentDigest != rec.IntentDigest || latest.Plan != rec.Plan {
			return errors.New("요청 상태 sidecar가 원본 intent와 일치하지 않습니다")
		}
		if !validJournalTransition(latest.State, rec.State) {
			return errors.New("요청 상태 전이가 허용되지 않습니다")
		}
		candidate := rec
		candidate.Schema = 2
		candidate.Sequence = latest.Sequence + 1
		candidate.PreviousChecksum = latest.Checksum
		candidate.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		candidate.Checksum = ""
		candidate.seal()
		path := fmt.Sprintf("%s.state.%06d.json", base, candidate.Sequence)
		raw, err := json.MarshalIndent(candidate, "", "  ")
		if err != nil {
			return err
		}
		f, openErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(openErr, os.ErrExist) {
			continue // concurrent writer won; reload and re-chain
		}
		if openErr != nil {
			return openErr
		}
		_, writeErr := f.Write(append(raw, '\n'))
		if writeErr == nil {
			writeErr = f.Sync()
		}
		closeErr := f.Close()
		if writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			return fmt.Errorf("요청 상태 디스크 기록 실패: %w", writeErr)
		}
		return nil
	}
	return errors.New("요청 상태 파일이 너무 많습니다. 수동 점검이 필요합니다")
}

// Submit intentionally accepts an explicit guard checked immediately before
// persistence. For production, the guard must enforce connected DEMO, fresh
// Hedge/isolated/leverage preflight, account constraints, and order limits.
// This library is NOT a production-exposed order execution endpoint.
func (e *Engine) Submit(ctx context.Context, i Intent, guard func(context.Context, Intent) error) (Record, error) {
	plan, err := i.Plan()
	if err != nil {
		return Record{}, err
	}
	digest, err := digestIntent(i)
	if err != nil {
		return Record{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// Duplicate check comes BEFORE the guard. A caller can't resend after
	// a restart just because its session or config has changed.
	if old, err := readRecord(e.path(plan)); err == nil {
		if old.IntentDigest != digest || old.Plan != plan {
			return Record{}, errors.New("요청 ID 재사용 감지: 기존 요청과 내용이 다릅니다")
		}
		if old.State == Prepared || old.State == Unknown {
			return old, ErrNeedsReconciliation
		}
		return old, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Record{}, err
	}
	if guard == nil {
		return Record{}, errors.New("주문 안전 점검이 지정되지 않았습니다")
	}
	if err := guard(ctx, i); err != nil {
		return Record{}, fmt.Errorf("주문 안전 점검 실패: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	rec, created, err := e.ensureNew(plan, i, digest)
	if err != nil {
		return Record{}, err
	}
	if !created {
		if rec.State == Prepared || rec.State == Unknown {
			return rec, ErrNeedsReconciliation
		}
		return rec, nil
	}
	// An executor timeout or malformed ACK is AMBIGUOUS, not rejection.
	ack, sendErr := e.executor.Submit(ctx, plan)
	rec.State = Unknown
	if sendErr == nil && ack.ClientOrderID == plan.ClientOrderID && ack.OrderID > 0 && ack.Status != "" {
		rec.State, rec.ExchangeOrderID, rec.ExchangeStatus = Acknowledged, ack.OrderID, ack.Status
	}
	if err := e.publish(rec); err != nil {
		return Record{}, fmt.Errorf("주문 결과를 영속화하지 못했습니다. 자동 재시도 금지: %w", err)
	}
	if sendErr != nil {
		return rec, fmt.Errorf("주문 결과 불명: 거래소 조회 필요 (자동 재전송 없음): %w", sendErr)
	}
	if rec.State == Unknown {
		return rec, errors.New("거래소 응답 불완전: 실제 주문 확인 전 재전송 금지")
	}
	return rec, nil
}

func (e *Engine) Lookup(intent Intent) (Record, error) {
	plan, err := intent.Plan()
	if err != nil {
		return Record{}, err
	}
	digest, err := digestIntent(intent)
	if err != nil {
		return Record{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	old, err := readRecord(e.path(plan))
	if err != nil {
		return Record{}, err
	}
	if old.IntentDigest != digest || old.Plan != plan {
		return Record{}, errors.New("동일한 ID의 요청 내용이 일치하지 않습니다")
	}
	return old, nil
}
func (e *Engine) Reconcile(ctx context.Context, i Intent, query Reconciler) (Record, error) {
	if query == nil {
		return Record{}, errors.New("거래소 주문 조회기가 필요합니다")
	}
	plan, err := i.Plan()
	if err != nil {
		return Record{}, err
	}
	digest, err := digestIntent(i)
	if err != nil {
		return Record{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, err := readRecord(e.path(plan))
	if err != nil {
		return Record{}, err
	}
	if rec.IntentDigest != digest || rec.Plan != plan {
		return Record{}, errors.New("요청 ID 충돌 또는 기록 손상")
	}
	ack, err := query.QueryByClientOrderID(ctx, i.Symbol, plan.ClientOrderID)
	if err != nil {
		return rec, fmt.Errorf("주문 상태를 확인하지 못했습니다. 현재 상태 유지: %w", err)
	}
	if ack.ClientOrderID != plan.ClientOrderID || ack.OrderID <= 0 || ack.Status == "" {
		return rec, errors.New("거래소 조회 결과가 불완전합니다. 요청 상태를 보류합니다")
	}
	// A missing exchange order is never proof that submission did not occur.
	rec.State, rec.ExchangeOrderID, rec.ExchangeStatus = Reconciled, ack.OrderID, ack.Status
	if err := e.publish(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}
