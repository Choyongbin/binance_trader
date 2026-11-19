package trading

// Durable order execution lifecycle for FUTURE operator-approved Demo order
// execution. Nothing in this file is wired to an HTTP order route. The store
// exists to make every potentially side-effecting transition append-only and
// restart-visible before a live Demo execution adapter is ever enabled.
//
// The linear lifecycle is intentionally conservative:
//   RESERVED -> MAY_HAVE_SENT -> SIGNED_OBSERVED -> FILLS_IMPORTED
//            -> WHOLE_SIDE_RECONCILED -> SETTLED
//
// MAY_HAVE_SENT is written BEFORE the executor is called. A process crash,
// timeout, malformed ACK, or disk error after that marker can never cause an
// automatic replay. Recovery may only use signed read-only queries by the
// deterministic clientOrderID.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

type ExecutionStage string

const (
	ExecutionReserved        ExecutionStage = "RESERVED"
	ExecutionMayHaveSent     ExecutionStage = "MAY_HAVE_SENT"
	ExecutionSignedObserved  ExecutionStage = "SIGNED_OBSERVED"
	ExecutionFillsImported   ExecutionStage = "FILLS_IMPORTED"
	ExecutionWholeReconciled ExecutionStage = "WHOLE_SIDE_RECONCILED"
	ExecutionSettled         ExecutionStage = "SETTLED"
)

var (
	ErrExecutionDispatchAlreadyMarked = errors.New("주문 전송 가능성 표식이 이미 존재합니다. 자동 재전송 금지")
	ErrExecutionStageOrder            = errors.New("주문 실행 상태 전이가 순서와 일치하지 않습니다")
	ErrExecutionLifecyclePending      = errors.New("미종료 주문 실행 lifecycle이 있어 새 계정 변이를 시작할 수 없습니다")
)

var executionRootName = regexp.MustCompile(`^bt_[0-9a-f]{28}\.json$`)
var executionStageName = regexp.MustCompile(`^bt_[0-9a-f]{28}\.stage\.[0-9]{6}\.json$`)

type ExecutionLifecycleRoot struct {
	Schema              int            `json:"schema"`
	ClientOrderID       string         `json:"client_order_id"`
	ReservationChecksum string         `json:"reservation_checksum"`
	IntentDigest        string         `json:"intent_digest"`
	Plan                PlannedOrder   `json:"plan"`
	Stage               ExecutionStage `json:"stage"`
	CreatedAt           string         `json:"created_at"`
	Checksum            string         `json:"checksum"`
}

func (r ExecutionLifecycleRoot) digest() string {
	r.Checksum = ""
	raw, _ := json.Marshal(r)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func (r *ExecutionLifecycleRoot) seal() { r.Checksum = r.digest() }

type ExecutionLifecycleEvent struct {
	Schema           int            `json:"schema"`
	ClientOrderID    string         `json:"client_order_id"`
	Sequence         int            `json:"sequence"`
	Stage            ExecutionStage `json:"stage"`
	At               string         `json:"at"`
	PreviousChecksum string         `json:"previous_checksum"`

	ExchangeOrderID int64  `json:"exchange_order_id,omitempty"`
	ExchangeStatus  string `json:"exchange_status,omitempty"`
	ExecutedQty     string `json:"executed_qty,omitempty"`
	SignedCheckedAt string `json:"signed_checked_at,omitempty"`

	ImportedFillCount  int    `json:"imported_fill_count,omitempty"`
	ImportedFillDigest string `json:"imported_fill_digest,omitempty"`

	SignedSnapshotAsOf string `json:"signed_snapshot_as_of,omitempty"`
	OwnerRemainingQty  string `json:"owner_remaining_qty,omitempty"`
	ExchangeSideQty    string `json:"exchange_side_qty,omitempty"`

	ReservationDoneChecksum string `json:"reservation_done_checksum,omitempty"`
	Checksum                string `json:"checksum"`
}

func (e ExecutionLifecycleEvent) digest() string {
	e.Checksum = ""
	raw, _ := json.Marshal(e)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func (e *ExecutionLifecycleEvent) seal() { e.Checksum = e.digest() }

type ExecutionLifecycle struct {
	Root   ExecutionLifecycleRoot    `json:"root"`
	Events []ExecutionLifecycleEvent `json:"events"`
	Stage  ExecutionStage            `json:"stage"`
}

type ExecutionLifecycleStore struct {
	mu       sync.Mutex
	root     string
	dir      string
	requests string
	reserve  string
}

func NewExecutionLifecycleStore(dataDir string) (*ExecutionLifecycleStore, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 디렉터리가 필요합니다")
	}
	root := filepath.Join(dataDir, "trading")
	dir := filepath.Join(root, "execution_lifecycle")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "requests"), 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "reservations"), 0700); err != nil {
		return nil, err
	}
	return &ExecutionLifecycleStore{root: root, dir: dir, requests: filepath.Join(root, "requests"), reserve: filepath.Join(root, "reservations")}, nil
}

func executionRootPath(dir, clientID string) string {
	return filepath.Join(dir, clientID+".json")
}
func executionEventPath(dir, clientID string, seq int) string {
	return filepath.Join(dir, fmt.Sprintf("%s.stage.%06d.json", clientID, seq))
}

func decodeExecutionRoot(path string) (ExecutionLifecycleRoot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ExecutionLifecycleRoot{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var r ExecutionLifecycleRoot
	if err := dec.Decode(&r); err != nil {
		return r, err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return r, errors.New("실행 lifecycle 원본 뒤에 추가 데이터가 있습니다")
	}
	if r.Schema != 1 || !executionRootName.MatchString(filepath.Base(path)) || filepath.Base(path) != r.ClientOrderID+".json" ||
		!reservationName.MatchString(r.ClientOrderID+".json") || r.Stage != ExecutionReserved || r.Checksum == "" || r.Checksum != r.digest() {
		return r, errors.New("실행 lifecycle 원본이 손상되었습니다")
	}
	if r.ReservationChecksum == "" || r.IntentDigest == "" || r.Plan.ClientOrderID != r.ClientOrderID {
		return r, errors.New("실행 lifecycle 원본 연결 정보가 불완전합니다")
	}
	if _, err := time.Parse(time.RFC3339Nano, r.CreatedAt); err != nil {
		return r, errors.New("실행 lifecycle 생성 시각이 손상되었습니다")
	}
	return r, nil
}

func decodeExecutionEvent(path string) (ExecutionLifecycleEvent, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ExecutionLifecycleEvent{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var e ExecutionLifecycleEvent
	if err := dec.Decode(&e); err != nil {
		return e, err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return e, errors.New("실행 lifecycle 상태 뒤에 추가 데이터가 있습니다")
	}
	if e.Schema != 1 || e.Sequence <= 0 || e.Checksum == "" || e.Checksum != e.digest() {
		return e, errors.New("실행 lifecycle 상태가 손상되었습니다")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.At); err != nil {
		return e, errors.New("실행 lifecycle 상태 시각이 손상되었습니다")
	}
	return e, nil
}

func nextExecutionStage(stage ExecutionStage) (ExecutionStage, bool) {
	switch stage {
	case ExecutionReserved:
		return ExecutionMayHaveSent, true
	case ExecutionMayHaveSent:
		return ExecutionSignedObserved, true
	case ExecutionSignedObserved:
		return ExecutionFillsImported, true
	case ExecutionFillsImported:
		return ExecutionWholeReconciled, true
	case ExecutionWholeReconciled:
		return ExecutionSettled, true
	default:
		return "", false
	}
}

func validateExecutionEventShape(e ExecutionLifecycleEvent) error {
	// Stage-specific evidence is required exactly where it first becomes
	// authoritative. Later stages do not copy prior mutable exchange fields;
	// the hash chain preserves those facts.
	switch e.Stage {
	case ExecutionMayHaveSent:
		if e.ExchangeOrderID != 0 || e.ExchangeStatus != "" || e.ExecutedQty != "" || e.SignedCheckedAt != "" || e.ImportedFillCount != 0 || e.ImportedFillDigest != "" || e.SignedSnapshotAsOf != "" || e.OwnerRemainingQty != "" || e.ExchangeSideQty != "" || e.ReservationDoneChecksum != "" {
			return errors.New("MAY_HAVE_SENT 상태에 허용되지 않는 증적 필드가 있습니다")
		}
	case ExecutionSignedObserved:
		if e.ExchangeOrderID <= 0 || e.ExchangeStatus == "" || e.ExecutedQty == "" || e.SignedCheckedAt == "" || e.ImportedFillCount != 0 || e.ImportedFillDigest != "" || e.SignedSnapshotAsOf != "" || e.OwnerRemainingQty != "" || e.ExchangeSideQty != "" || e.ReservationDoneChecksum != "" {
			return errors.New("SIGNED_OBSERVED 증적이 불완전합니다")
		}
	case ExecutionFillsImported:
		if e.ExchangeOrderID <= 0 || e.ExchangeStatus == "" || e.ExecutedQty == "" || e.SignedCheckedAt == "" || e.ImportedFillCount < 0 || e.ImportedFillDigest == "" || e.SignedSnapshotAsOf != "" || e.OwnerRemainingQty != "" || e.ExchangeSideQty != "" || e.ReservationDoneChecksum != "" {
			return errors.New("FILLS_IMPORTED 증적이 불완전합니다")
		}
	case ExecutionWholeReconciled:
		if e.ExchangeOrderID != 0 || e.ExchangeStatus != "" || e.ExecutedQty != "" || e.SignedCheckedAt != "" || e.ImportedFillCount != 0 || e.ImportedFillDigest != "" || e.SignedSnapshotAsOf == "" || e.OwnerRemainingQty == "" || e.ExchangeSideQty == "" || e.ReservationDoneChecksum != "" {
			return errors.New("WHOLE_SIDE_RECONCILED 증적이 불완전합니다")
		}
	case ExecutionSettled:
		if e.ExchangeOrderID != 0 || e.ExchangeStatus != "" || e.ExecutedQty != "" || e.SignedCheckedAt != "" || e.ImportedFillCount != 0 || e.ImportedFillDigest != "" || e.SignedSnapshotAsOf != "" || e.OwnerRemainingQty != "" || e.ExchangeSideQty != "" || e.ReservationDoneChecksum == "" {
			return errors.New("SETTLED 증적이 불완전합니다")
		}
	default:
		return errors.New("알 수 없는 실행 lifecycle 상태")
	}
	return nil
}

func loadExecutionLifecycleFromDir(dir, clientID string) (ExecutionLifecycle, error) {
	root, err := decodeExecutionRoot(executionRootPath(dir, clientID))
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	prefix := clientID + ".stage."
	expectedSeq := 1
	previousStage := ExecutionReserved
	previousChecksum := root.Checksum
	previousAt, _ := time.Parse(time.RFC3339Nano, root.CreatedAt)
	lifecycle := ExecutionLifecycle{Root: root, Events: []ExecutionLifecycleEvent{}, Stage: ExecutionReserved}
	for _, f := range files {
		name := f.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !executionStageName.MatchString(name) {
			return ExecutionLifecycle{}, errors.New("실행 lifecycle 상태 파일 이름 또는 형식이 비정상입니다")
		}
		wantName := fmt.Sprintf("%s.stage.%06d.json", clientID, expectedSeq)
		if name != wantName {
			return ExecutionLifecycle{}, errors.New("실행 lifecycle 상태 순번이 누락되었거나 중복되었습니다")
		}
		event, err := decodeExecutionEvent(filepath.Join(dir, name))
		if err != nil {
			return ExecutionLifecycle{}, err
		}
		next, ok := nextExecutionStage(previousStage)
		if !ok || event.ClientOrderID != clientID || event.Sequence != expectedSeq || event.Stage != next || event.PreviousChecksum != previousChecksum {
			return ExecutionLifecycle{}, errors.New("실행 lifecycle 해시 체인 또는 상태 순서가 일치하지 않습니다")
		}
		if err := validateExecutionEventShape(event); err != nil {
			return ExecutionLifecycle{}, err
		}
		at, _ := time.Parse(time.RFC3339Nano, event.At)
		if at.Before(previousAt) {
			return ExecutionLifecycle{}, errors.New("실행 lifecycle 시각 순서가 역전되었습니다")
		}
		lifecycle.Events = append(lifecycle.Events, event)
		lifecycle.Stage = event.Stage
		previousStage = event.Stage
		previousChecksum = event.Checksum
		previousAt = at
		expectedSeq++
	}
	return lifecycle, nil
}

func (s *ExecutionLifecycleStore) validateReservation(r Reservation) error {
	if r.Schema != 1 || r.ClientOrderID == "" || r.Checksum == "" || r.Checksum != r.digest() {
		return errors.New("예약 내용이 유효하지 않습니다")
	}
	path := filepath.Join(s.reserve, r.ClientOrderID+".json")
	onDisk, err := decodeReservation(path)
	if err != nil {
		return fmt.Errorf("실행 lifecycle 원본 예약 확인 실패: %w", err)
	}
	if onDisk != r {
		return errors.New("실행 lifecycle 예약과 저장된 예약이 일치하지 않습니다")
	}
	return nil
}

func ensurePreparedJournalFromReservation(requestDir string, r Reservation, now time.Time) error {
	plan, err := r.Intent.Plan()
	if err != nil || plan.ClientOrderID != r.ClientOrderID {
		return errors.New("예약 주문 계획이 유효하지 않습니다")
	}
	path := filepath.Join(requestDir, plan.ClientOrderID+".json")
	digest, _ := digestIntent(r.Intent)
	if old, err := readRecord(path); err == nil {
		if old.Intent != r.Intent || old.IntentDigest != digest || old.Plan != plan {
			return errors.New("기존 주문 저널과 예약이 일치하지 않습니다")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	rec := newPreparedRecord(r.Intent, digest, plan, now)
	raw, _ := json.MarshalIndent(rec, "", "  ")
	if err := exclusiveDurable(path, raw); err != nil {
		if errors.Is(err, os.ErrExist) {
			old, readErr := readRecord(path)
			if readErr != nil || old.Intent != r.Intent || old.IntentDigest != digest || old.Plan != plan {
				return errors.New("동시에 생성된 주문 저널을 안전하게 확인할 수 없습니다")
			}
			return nil
		}
		return err
	}
	return nil
}

// Begin binds the immutable reservation to an immutable PREPARED request
// journal and creates the RESERVED lifecycle root. It performs no network IO.
func (s *ExecutionLifecycleStore) Begin(r Reservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateReservation(r); err != nil {
		return ExecutionLifecycle{}, err
	}
	if now.IsZero() || now.After(time.Now().UTC().Add(time.Second)) {
		return ExecutionLifecycle{}, errors.New("실행 lifecycle 생성 시각이 유효하지 않습니다")
	}
	if err := ensurePreparedJournalFromReservation(s.requests, r, now); err != nil {
		return ExecutionLifecycle{}, err
	}
	plan, _ := r.Intent.Plan()
	digest, _ := digestIntent(r.Intent)
	root := ExecutionLifecycleRoot{Schema: 1, ClientOrderID: r.ClientOrderID, ReservationChecksum: r.Checksum, IntentDigest: digest, Plan: plan, Stage: ExecutionReserved, CreatedAt: now.UTC().Format(time.RFC3339Nano)}
	root.seal()
	path := executionRootPath(s.dir, r.ClientOrderID)
	raw, _ := json.MarshalIndent(root, "", "  ")
	if err := exclusiveDurable(path, raw); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return ExecutionLifecycle{}, err
		}
		lifecycle, readErr := loadExecutionLifecycleFromDir(s.dir, r.ClientOrderID)
		if readErr != nil {
			return ExecutionLifecycle{}, readErr
		}
		if lifecycle.Root != root {
			return ExecutionLifecycle{}, errors.New("기존 실행 lifecycle이 현재 예약과 일치하지 않습니다")
		}
		return lifecycle, nil
	}
	return ExecutionLifecycle{Root: root, Events: []ExecutionLifecycleEvent{}, Stage: ExecutionReserved}, nil
}

func (s *ExecutionLifecycleStore) loadLocked(r Reservation) (ExecutionLifecycle, error) {
	if err := s.validateReservation(r); err != nil {
		return ExecutionLifecycle{}, err
	}
	lifecycle, err := loadExecutionLifecycleFromDir(s.dir, r.ClientOrderID)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	digest, _ := digestIntent(r.Intent)
	plan, _ := r.Intent.Plan()
	if lifecycle.Root.ReservationChecksum != r.Checksum || lifecycle.Root.IntentDigest != digest || lifecycle.Root.Plan != plan {
		return ExecutionLifecycle{}, errors.New("실행 lifecycle이 예약과 일치하지 않습니다")
	}
	journal, err := readRecord(filepath.Join(s.requests, r.ClientOrderID+".json"))
	if err != nil {
		return ExecutionLifecycle{}, fmt.Errorf("실행 lifecycle 주문 저널 검증 실패: %w", err)
	}
	if journal.Intent != r.Intent || journal.IntentDigest != digest || journal.Plan != plan {
		return ExecutionLifecycle{}, errors.New("실행 lifecycle 주문 저널과 예약이 일치하지 않습니다")
	}
	return lifecycle, nil
}

func (s *ExecutionLifecycleStore) appendEventLocked(r Reservation, current ExecutionLifecycle, event ExecutionLifecycleEvent) (ExecutionLifecycle, error) {
	next, ok := nextExecutionStage(current.Stage)
	if !ok || event.Stage != next {
		return ExecutionLifecycle{}, ErrExecutionStageOrder
	}
	seq := len(current.Events) + 1
	prev := current.Root.Checksum
	if len(current.Events) > 0 {
		prev = current.Events[len(current.Events)-1].Checksum
	}
	event.Schema = 1
	event.ClientOrderID = r.ClientOrderID
	event.Sequence = seq
	event.PreviousChecksum = prev
	if event.At == "" {
		event.At = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if err := validateExecutionEventShape(event); err != nil {
		return ExecutionLifecycle{}, err
	}
	at, err := time.Parse(time.RFC3339Nano, event.At)
	if err != nil || at.After(time.Now().UTC().Add(time.Second)) {
		return ExecutionLifecycle{}, errors.New("실행 lifecycle 전이 시각이 유효하지 않습니다")
	}
	previousAt, _ := time.Parse(time.RFC3339Nano, current.Root.CreatedAt)
	if len(current.Events) > 0 {
		previousAt, _ = time.Parse(time.RFC3339Nano, current.Events[len(current.Events)-1].At)
	}
	if at.Before(previousAt) {
		return ExecutionLifecycle{}, errors.New("실행 lifecycle 전이 시각이 이전 상태보다 빠릅니다")
	}
	event.seal()
	raw, _ := json.MarshalIndent(event, "", "  ")
	path := executionEventPath(s.dir, r.ClientOrderID, seq)
	if err := exclusiveDurable(path, raw); err != nil {
		return ExecutionLifecycle{}, fmt.Errorf("실행 lifecycle 상태 기록 충돌 또는 실패: %w", err)
	}
	current.Events = append(current.Events, event)
	current.Stage = event.Stage
	return current, nil
}

func (s *ExecutionLifecycleStore) Load(r Reservation) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(r)
}

// durableFinalGuardExecutor is intentionally sealed to this package. Future
// live dispatch code cannot plug in an arbitrary Executor that forgets the
// second freshness check between durable MAY_HAVE_SENT and the exchange POST.
// Approved executors must run finalCheck immediately before their side effect.
type durableFinalGuardExecutor interface {
	SubmitWithFinalCheck(context.Context, PlannedOrder, func(context.Context) error) (Submission, error)
	durableFinalGuardContract()
}

func (s *ExecutionLifecycleStore) MarkMayHaveSent(r Reservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(r)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	if current.Stage != ExecutionReserved {
		return current, ErrExecutionDispatchAlreadyMarked
	}
	return s.appendEventLocked(r, current, ExecutionLifecycleEvent{Stage: ExecutionMayHaveSent, At: now.UTC().Format(time.RFC3339Nano)})
}

// DispatchOnce is deliberately internal-library only. The MAY_HAVE_SENT marker
// is durably fsync'd before the executor is invoked. A second call after any
// crash/restart returns ErrExecutionDispatchAlreadyMarked and NEVER submits.
func (s *ExecutionLifecycleStore) DispatchOnce(ctx context.Context, r Reservation, executor durableFinalGuardExecutor, guard func(context.Context, Intent) error) (Submission, error) {
	if executor == nil || guard == nil {
		return Submission{}, errors.New("주문 실행기와 최신 안전 점검기가 필요합니다")
	}
	if err := guard(ctx, r.Intent); err != nil {
		return Submission{}, fmt.Errorf("주문 실행 직전 안전 점검 실패: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Submission{}, err
	}
	if _, err := s.MarkMayHaveSent(r, time.Now().UTC()); err != nil {
		return Submission{}, err
	}

	// Re-run the caller's freshness gate inside the sealed executor immediately
	// before the POST. The first check protects marking; this second check
	// protects against account/market changes during the fsync window.
	ack, submitErr := executor.SubmitWithFinalCheck(ctx, mustPlan(r.Intent), func(inner context.Context) error {
		return guard(inner, r.Intent)
	})
	// Legacy request journal remains the ownership ledger's source of intent.
	// ACK is not trusted as signed evidence; it is only a diagnostic sidecar.
	rec, readErr := readRecord(filepath.Join(s.requests, r.ClientOrderID+".json"))
	if readErr != nil {
		return ack, fmt.Errorf("주문 전송 이후 저널을 읽을 수 없습니다. 자동 재전송 금지: %w", readErr)
	}
	rec.State = Unknown
	if submitErr == nil && ack.ClientOrderID == r.ClientOrderID && ack.OrderID > 0 && ack.Status != "" {
		rec.State = Acknowledged
		rec.ExchangeOrderID = ack.OrderID
		rec.ExchangeStatus = ack.Status
	}
	engine := &Engine{dir: s.requests}
	if err := engine.publish(rec); err != nil {
		return ack, fmt.Errorf("주문 전송 결과 저널 기록 실패. clientOrderID 서명 조회 전 재전송 금지: %w", err)
	}
	if submitErr != nil {
		return ack, fmt.Errorf("주문 결과 불명: clientOrderID 서명 조회 필요, 자동 재전송 금지: %w", submitErr)
	}
	if rec.State != Acknowledged {
		return ack, errors.New("주문 ACK가 불완전합니다. clientOrderID 서명 조회 전 재전송 금지")
	}
	return ack, nil
}

func mustPlan(i Intent) PlannedOrder {
	p, _ := i.Plan()
	return p
}

func isExecutionTerminalOrderStatus(status string) bool {
	switch status {
	case "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
		return true
	default:
		return false
	}
}

func (s *ExecutionLifecycleStore) RecordSignedObservation(r Reservation, observation OrderObservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(r)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	if current.Stage != ExecutionMayHaveSent {
		return current, ErrExecutionStageOrder
	}
	if observation.CheckedAt.IsZero() || observation.CheckedAt.After(now.Add(time.Second)) || now.Sub(observation.CheckedAt) > 5*time.Second {
		return current, errors.New("최신 서명 주문 조회가 필요합니다")
	}
	rec, err := readRecord(filepath.Join(s.requests, r.ClientOrderID+".json"))
	if err != nil {
		return current, err
	}
	executed, err := verifyRemoteOrder(rec, observation.Order)
	if err != nil {
		return current, err
	}
	// Signed GET becomes the canonical journal observation even when the POST
	// timed out or its ACK was malformed.
	if rec.State != Reconciled || rec.ExchangeOrderID != observation.Order.OrderID || rec.ExchangeStatus != observation.Order.Status {
		rec.State = Reconciled
		rec.ExchangeOrderID = observation.Order.OrderID
		rec.ExchangeStatus = observation.Order.Status
		if err := (&Engine{dir: s.requests}).publish(rec); err != nil {
			return current, fmt.Errorf("서명 주문 관측 저널 저장 실패: %w", err)
		}
	}
	return s.appendEventLocked(r, current, ExecutionLifecycleEvent{
		Stage: ExecutionSignedObserved, At: now.UTC().Format(time.RFC3339Nano),
		ExchangeOrderID: observation.Order.OrderID, ExchangeStatus: observation.Order.Status,
		ExecutedQty: formatOwnershipDecimal(executed), SignedCheckedAt: observation.CheckedAt.UTC().Format(time.RFC3339Nano),
	})
}

func digestClientFills(fills []StoredFill) string {
	sorted := append([]StoredFill(nil), fills...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Trade.TradeID < sorted[j].Trade.TradeID })
	h := sha256.New()
	for _, f := range sorted {
		h.Write([]byte(f.Checksum))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *ExecutionLifecycleStore) RecordFillsImported(r Reservation, store *OwnershipStore, observation OrderObservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(r)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	if current.Stage != ExecutionSignedObserved {
		return current, ErrExecutionStageOrder
	}
	if store == nil || filepath.Clean(store.journalDir) != s.requests || filepath.Clean(store.fillDir) != filepath.Join(s.root, "fills") {
		return current, errors.New("같은 데이터 디렉터리의 체결 장부가 필요합니다")
	}
	if observation.CheckedAt.IsZero() || observation.CheckedAt.After(now.Add(time.Second)) || now.Sub(observation.CheckedAt) > 5*time.Second {
		return current, errors.New("체결 반영 후 최신 서명 주문 조회가 필요합니다")
	}
	if !isExecutionTerminalOrderStatus(observation.Order.Status) {
		return current, errors.New("주문이 아직 종료되지 않아 체결 집합을 고정할 수 없습니다")
	}
	rec, err := readRecord(filepath.Join(s.requests, r.ClientOrderID+".json"))
	if err != nil {
		return current, err
	}
	executed, err := verifyRemoteOrder(rec, observation.Order)
	if err != nil {
		return current, err
	}
	store.mu.Lock()
	allFills, err := store.readFills()
	store.mu.Unlock()
	if err != nil {
		return current, err
	}
	selected := make([]StoredFill, 0)
	sum := new(big.Rat)
	for _, f := range allFills {
		if f.ClientOrderID != r.ClientOrderID {
			continue
		}
		if f.Trade.OrderID != observation.Order.OrderID {
			return current, errors.New("저장 체결의 주문 ID가 최신 서명 주문과 다릅니다")
		}
		q, err := positiveRat(f.Trade.Quantity)
		if err != nil {
			return current, err
		}
		sum.Add(sum, q)
		selected = append(selected, f)
	}
	if sum.Cmp(executed) != 0 {
		return current, errors.New("최종 주문 누적 체결 수량과 저장된 전체 체결 합계가 일치하지 않습니다")
	}
	// Empty terminal/rejected orders still need a deterministic digest.
	fillDigest := digestClientFills(selected)
	return s.appendEventLocked(r, current, ExecutionLifecycleEvent{
		Stage: ExecutionFillsImported, At: now.UTC().Format(time.RFC3339Nano),
		ExchangeOrderID: observation.Order.OrderID, ExchangeStatus: observation.Order.Status,
		ExecutedQty: formatOwnershipDecimal(executed), SignedCheckedAt: observation.CheckedAt.UTC().Format(time.RFC3339Nano),
		ImportedFillCount: len(selected), ImportedFillDigest: fillDigest,
	})
}

func ownerAndExchangeSide(report OwnershipReport, snapshot exchange.Snapshot, r Reservation) (string, string, error) {
	ownerRemaining := new(big.Rat)
	for _, p := range report.Owned {
		if p.Source == r.Intent.Source && p.ModelID == r.Intent.ModelID && p.Symbol == r.Intent.Symbol && p.Side == r.Intent.PositionSide {
			q, err := signedRat(p.Quantity)
			if err != nil || q.Sign() < 0 {
				return "", "", errors.New("소유자 잔량이 유효하지 않습니다")
			}
			ownerRemaining.Add(ownerRemaining, q)
		}
	}
	exchangeSide := new(big.Rat)
	for _, p := range snapshot.Positions {
		if p.Symbol != r.Intent.Symbol || p.Side != string(r.Intent.PositionSide) {
			continue
		}
		q, err := signedRat(p.Qty)
		if err != nil {
			return "", "", errors.New("거래소 포지션 잔량이 유효하지 않습니다")
		}
		q.Abs(q)
		exchangeSide.Add(exchangeSide, q)
	}
	return formatOwnershipDecimal(ownerRemaining), formatOwnershipDecimal(exchangeSide), nil
}

func (s *ExecutionLifecycleStore) RecordWholeSideReconciled(r Reservation, store *OwnershipStore, snapshot exchange.Snapshot, orders map[string]OrderObservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(r)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	if current.Stage != ExecutionFillsImported {
		return current, ErrExecutionStageOrder
	}
	if store == nil || filepath.Clean(store.journalDir) != s.requests || filepath.Clean(store.fillDir) != filepath.Join(s.root, "fills") {
		return current, errors.New("같은 데이터 디렉터리의 체결 장부가 필요합니다")
	}
	report, err := store.Audit(snapshot, orders, now)
	if err != nil || !report.ReadyForAssessment {
		return current, errors.New("전체 소유권/거래소 포지션 감사가 완료되지 않았습니다")
	}
	obs, ok := orders[r.ClientOrderID]
	if !ok || !isExecutionTerminalOrderStatus(obs.Order.Status) {
		return current, errors.New("현재 주문의 종료 상태를 전체 감사에서 확인할 수 없습니다")
	}
	prior := current.Events[len(current.Events)-1]
	if obs.Order.OrderID != prior.ExchangeOrderID || obs.Order.Status != prior.ExchangeStatus || formatDecimalOrEmpty(obs.Order.ExecutedQty) != prior.ExecutedQty {
		return current, errors.New("FILLS_IMPORTED 이후 주문 상태가 변경되었습니다. 새 증적 절차가 필요합니다")
	}
	ownerRemaining, exchangeSide, err := ownerAndExchangeSide(report, snapshot, r)
	if err != nil {
		return current, err
	}
	return s.appendEventLocked(r, current, ExecutionLifecycleEvent{
		Stage: ExecutionWholeReconciled, At: now.UTC().Format(time.RFC3339Nano),
		SignedSnapshotAsOf: snapshot.AsOf, OwnerRemainingQty: ownerRemaining, ExchangeSideQty: exchangeSide,
	})
}

func formatDecimalOrEmpty(v string) string {
	q, err := decimalRat(v, true)
	if err != nil {
		return ""
	}
	return formatOwnershipDecimal(q)
}

// Finalize performs a fresh Coordinator.Settle audit. If a crash happened
// after the reservation done marker was fsync'd but before SETTLED was appended,
// calling Finalize again is safe: Settle is idempotent and the done marker is
// independently checksum-verified before the lifecycle is closed.
func (s *ExecutionLifecycleStore) Finalize(r Reservation, c *Coordinator, store *OwnershipStore, snapshot exchange.Snapshot, orders map[string]OrderObservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(r)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	if current.Stage == ExecutionSettled {
		return current, nil
	}
	if current.Stage != ExecutionWholeReconciled {
		return current, ErrExecutionStageOrder
	}
	if c == nil || store == nil || filepath.Clean(c.reserveDir) != s.reserve || filepath.Clean(store.journalDir) != s.requests {
		return current, errors.New("동일 데이터 디렉터리의 coordinator/체결 장부가 필요합니다")
	}
	if err := c.Settle(r, store, snapshot, orders, now); err != nil {
		return current, err
	}
	donePath := filepath.Join(s.reserve, r.ClientOrderID+".done.json")
	done, err := decodeReservationDone(donePath, r)
	if err != nil || done.Schema != 2 || !done.OwnershipReady || done.Checksum == "" || done.Checksum != done.digest() {
		return current, errors.New("예약 종료 증적을 검증할 수 없습니다")
	}
	return s.appendEventLocked(r, current, ExecutionLifecycleEvent{Stage: ExecutionSettled, At: now.UTC().Format(time.RFC3339Nano), ReservationDoneChecksum: done.Checksum})
}

// RecoverSettled closes only the narrow crash window where Coordinator.Settle
// already persisted a sealed v2 done marker but the lifecycle SETTLED sidecar
// was not written. It performs no network IO and cannot manufacture a done
// marker from local assumptions.
func (s *ExecutionLifecycleStore) RecoverSettled(r Reservation, now time.Time) (ExecutionLifecycle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(r)
	if err != nil {
		return ExecutionLifecycle{}, err
	}
	if current.Stage == ExecutionSettled {
		return current, nil
	}
	if current.Stage != ExecutionWholeReconciled {
		return current, ErrExecutionStageOrder
	}
	done, err := decodeReservationDone(filepath.Join(s.reserve, r.ClientOrderID+".done.json"), r)
	if err != nil || done.Schema != 2 || !done.OwnershipReady || done.Checksum == "" || done.Checksum != done.digest() {
		return current, errors.New("복구 가능한 예약 종료 증적이 없습니다")
	}
	whole := current.Events[len(current.Events)-1]
	if whole.Stage != ExecutionWholeReconciled || done.Action != r.Intent.Action || done.PositionSide != r.Intent.PositionSide {
		return current, errors.New("예약 종료 증적과 lifecycle 주문 방향이 일치하지 않습니다")
	}
	wholeAt, _ := time.Parse(time.RFC3339Nano, whole.At)
	settledAt, _ := time.Parse(time.RFC3339Nano, done.SettledAt)
	if settledAt.Before(wholeAt) {
		return current, errors.New("예약 종료 증적이 전체 포지션 대조보다 오래되었습니다")
	}
	// The v2 done marker is allowed to contain a NEWER fresh audit than the
	// earlier WHOLE_SIDE_RECONCILED checkpoint. Coordinator.Settle itself
	// requires the lifecycle to be at WHOLE_SIDE_RECONCILED and reruns the
	// entire signed ownership audit before writing this sealed marker. This is
	// exactly the crash window this recovery method is meant to close.
	return s.appendEventLocked(r, current, ExecutionLifecycleEvent{Stage: ExecutionSettled, At: now.UTC().Format(time.RFC3339Nano), ReservationDoneChecksum: done.Checksum})
}

// executionLifecycleStatusForReservation is a read-only helper used by the
// coordinator fence. Missing lifecycle means a legacy reservation; present but
// incomplete lifecycle must keep the pooled side fenced even if a done marker
// was written just before a crash.
func executionLifecycleStatusForReservation(dataDir string, r Reservation) (ExecutionStage, bool, error) {
	dir := filepath.Join(dataDir, "trading", "execution_lifecycle")
	path := executionRootPath(dir, r.ClientOrderID)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", true, errors.New("실행 lifecycle 원본 파일 형식이 안전하지 않습니다")
	}
	lifecycle, err := loadExecutionLifecycleFromDir(dir, r.ClientOrderID)
	if err != nil {
		return "", true, err
	}
	if lifecycle.Root.ReservationChecksum != r.Checksum {
		return "", true, errors.New("실행 lifecycle과 예약 체크섬이 일치하지 않습니다")
	}
	journal, err := readRecord(filepath.Join(dataDir, "trading", "requests", r.ClientOrderID+".json"))
	if err != nil {
		return "", true, fmt.Errorf("실행 lifecycle 주문 저널 검증 실패: %w", err)
	}
	plan, _ := r.Intent.Plan()
	digest, _ := digestIntent(r.Intent)
	if journal.Intent != r.Intent || journal.IntentDigest != digest || journal.Plan != plan || lifecycle.Root.Plan != plan || lifecycle.Root.IntentDigest != digest {
		return "", true, errors.New("실행 lifecycle/예약/주문 저널 연결 정보가 일치하지 않습니다")
	}
	return lifecycle.Stage, true, nil
}

const (
	ExecutionAuditEmpty   = "EMPTY"
	ExecutionAuditClear   = "ALL_SETTLED"
	ExecutionAuditPending = "PENDING_OR_AMBIGUOUS"
	ExecutionAuditBlocked = "BLOCKED_CORRUPT_OR_INCONSISTENT"
)

type ExecutionLifecycleAuditItem struct {
	ClientOrderID string         `json:"client_order_id"`
	PositionSide  PositionSide   `json:"position_side"`
	Action        Action         `json:"action"`
	Stage         ExecutionStage `json:"stage"`
	Blockers      []string       `json:"blockers"`
}

type ExecutionLifecycleAudit struct {
	State                string                        `json:"state"`
	Total                int                           `json:"total"`
	Settled              int                           `json:"settled"`
	Pending              int                           `json:"pending"`
	Items                []ExecutionLifecycleAuditItem `json:"items"`
	TradingAuthorized    bool                          `json:"trading_authorized"` // ALWAYS false
	ManualReviewRequired bool                          `json:"manual_review_required"`
}

func validExecutionArchiveEntry(parent, name string, isDir bool) bool {
	if parent != "" || isDir {
		return false
	}
	return executionRootName.MatchString(name) || executionStageName.MatchString(name)
}

var executionRequestStateName = regexp.MustCompile(`^bt_[0-9a-f]{28}\.state\.[0-9]{6}\.json$`)

func validExecutionRequestEntry(parent, name string, isDir bool) bool {
	return parent == "" && !isDir && (journalName.MatchString(name) || executionRequestStateName.MatchString(name))
}

func validExecutionFillEntry(parent, name string, isDir bool) bool {
	return parent == "" && !isDir && fillName.MatchString(name)
}

// snapshotExecutionEvidence binds every local directory read by the lifecycle
// auditor into one deterministic digest. This closes the dev.3.13 TOCTOU gap
// where execution_lifecycle stayed unchanged while reservations/requests/fills
// could change underneath an accidentally-online audit.
func snapshotExecutionEvidence(dataDir string) (string, error) {
	type source struct {
		name     string
		validate func(string, string, bool) bool
	}
	sources := []source{
		{"execution_lifecycle", validExecutionArchiveEntry},
		{"reservations", validPooledReservationArchiveEntry},
		{"requests", validExecutionRequestEntry},
		{"fills", validExecutionFillEntry},
	}
	h := sha256.New()
	for _, src := range sources {
		digest, files, dirs, err := snapshotDir(filepath.Join(dataDir, "trading", src.name), src.validate)
		if err != nil {
			return "", fmt.Errorf("%s 증적 snapshot 실패: %w", src.name, err)
		}
		_, _ = fmt.Fprintf(h, "%d:%s:%s:%d:%d;", len(src.name), src.name, digest, files, dirs)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ReviewExecutionLifecycles is strictly read-only and intended for an offline
// stopped data copy. Any unknown/orphan/tampered file is an error, not a repair
// opportunity. It never authorizes trading.
func ReviewExecutionLifecycles(dataDir string) (ExecutionLifecycleAudit, error) {
	if dataDir == "" {
		return ExecutionLifecycleAudit{}, errors.New("데이터 디렉터리가 필요합니다")
	}
	dir := filepath.Join(dataDir, "trading", "execution_lifecycle")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return ExecutionLifecycleAudit{State: ExecutionAuditEmpty, Items: []ExecutionLifecycleAuditItem{}, ManualReviewRequired: true}, nil
	}
	if err != nil {
		return ExecutionLifecycleAudit{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ExecutionLifecycleAudit{}, errors.New("실행 lifecycle 감사 경로가 안전한 디렉터리가 아닙니다")
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return ExecutionLifecycleAudit{}, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ExecutionLifecycleAudit{}, err
	}
	roots := make([]string, 0)
	knownRoots := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || (!executionRootName.MatchString(entry.Name()) && !executionStageName.MatchString(entry.Name())) {
			return ExecutionLifecycleAudit{}, fmt.Errorf("알 수 없는 실행 lifecycle 파일: %s", entry.Name())
		}
		if executionRootName.MatchString(entry.Name()) {
			clientID := strings.TrimSuffix(entry.Name(), ".json")
			roots = append(roots, clientID)
			knownRoots[clientID] = true
		}
	}
	for _, entry := range entries {
		if executionStageName.MatchString(entry.Name()) {
			clientID := strings.Split(entry.Name(), ".stage.")[0]
			if !knownRoots[clientID] {
				return ExecutionLifecycleAudit{}, errors.New("원본이 없는 실행 lifecycle 상태 파일이 있습니다")
			}
		}
	}
	sort.Strings(roots)
	report := ExecutionLifecycleAudit{State: ExecutionAuditEmpty, Items: []ExecutionLifecycleAuditItem{}, ManualReviewRequired: true}
	if len(roots) > 0 {
		report.State = ExecutionAuditClear
	}
	for _, clientID := range roots {
		lifecycle, err := loadExecutionLifecycleFromDir(dir, clientID)
		if err != nil {
			return ExecutionLifecycleAudit{}, err
		}
		reservation, err := decodeReservation(filepath.Join(dataDir, "trading", "reservations", clientID+".json"))
		if err != nil {
			return ExecutionLifecycleAudit{}, fmt.Errorf("실행 lifecycle 원본 예약 누락/손상: %w", err)
		}
		if lifecycle.Root.ReservationChecksum != reservation.Checksum {
			return ExecutionLifecycleAudit{}, errors.New("실행 lifecycle과 예약 체크섬 불일치")
		}
		journal, err := readRecord(filepath.Join(dataDir, "trading", "requests", clientID+".json"))
		if err != nil {
			return ExecutionLifecycleAudit{}, fmt.Errorf("실행 lifecycle 주문 저널 누락/손상: %w", err)
		}
		plan, _ := reservation.Intent.Plan()
		digest, _ := digestIntent(reservation.Intent)
		if journal.Intent != reservation.Intent || journal.IntentDigest != digest || journal.Plan != plan || lifecycle.Root.IntentDigest != digest || lifecycle.Root.Plan != plan {
			return ExecutionLifecycleAudit{}, errors.New("실행 lifecycle/예약/주문 저널 연결 정보 불일치")
		}
		item := ExecutionLifecycleAuditItem{ClientOrderID: clientID, PositionSide: reservation.Intent.PositionSide, Action: reservation.Intent.Action, Stage: lifecycle.Stage, Blockers: []string{}}
		report.Total++
		if lifecycle.Stage == ExecutionSettled {
			done, err := decodeReservationDone(filepath.Join(dataDir, "trading", "reservations", clientID+".done.json"), reservation)
			if err != nil || done.Schema != 2 || done.Checksum == "" || lifecycle.Events[len(lifecycle.Events)-1].ReservationDoneChecksum != done.Checksum {
				return ExecutionLifecycleAudit{}, errors.New("SETTLED lifecycle의 예약 종료 증적이 손상되거나 불일치합니다")
			}
			report.Settled++
		} else {
			report.Pending++
			report.State = ExecutionAuditPending
			switch lifecycle.Stage {
			case ExecutionReserved:
				item.Blockers = append(item.Blockers, "예약만 존재: 실행 직전 안전 점검 후 MAY_HAVE_SENT 전이 전까지 전송 금지")
			case ExecutionMayHaveSent:
				item.Blockers = append(item.Blockers, "전송 여부 불명확: clientOrderID 서명 조회만 허용, 자동 재전송 금지")
			case ExecutionSignedObserved:
				item.Blockers = append(item.Blockers, "서명 주문 관측 완료: 종료 주문의 전체 체결 import 필요")
			case ExecutionFillsImported:
				item.Blockers = append(item.Blockers, "체결 import 완료: 전체 owner/거래소 Hedge 방향 재대조 필요")
			case ExecutionWholeReconciled:
				if _, err := decodeReservationDone(filepath.Join(dataDir, "trading", "reservations", clientID+".done.json"), reservation); err == nil {
					item.Blockers = append(item.Blockers, "예약 종료 증적은 있으나 SETTLED sidecar 미기록: RecoverSettled만 허용")
				} else {
					item.Blockers = append(item.Blockers, "전체 방향 대조 완료: fresh Settle 또는 기존 sealed done 증적 복구 필요")
				}
			}
		}
		report.Items = append(report.Items, item)
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return ExecutionLifecycleAudit{}, err
	}
	if before != after {
		return ExecutionLifecycleAudit{}, errors.New("실행 lifecycle 감사 중 lifecycle/reservation/request/fill 증적이 변경되었습니다")
	}
	return report, nil
}
