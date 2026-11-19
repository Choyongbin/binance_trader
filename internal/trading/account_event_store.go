package trading

// Persistent append-only audit log of normalized PRIVATE account events.
// This is independent of the v0.3.7 order/fill ownership ledger. Stream
// messages only cause reconciliation; NEVER treat their mere arrival as proof
// that a missing trade/order does not exist.
import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

var eventName = regexp.MustCompile(`^[a-f0-9]{64}\.json$`)
var eventDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// EventStore keeps a conservative process-local HALT flag and an exclusive
// disk lock. Crash leaves lock in place for manual exchange reconciliation.
// It intentionally exposes NO "ReadyForTrading" method.
type EventStore struct {
	mu     sync.Mutex
	dir    string
	lock   string
	halted bool
	closed bool
	// A single observation session can be stopped both by the observer and its
	// owning recovery coordinator. One durable DISCONNECT is sufficient.
	disconnectRecorded bool
	reason             string
}

type eventEnvelope struct {
	Schema int          `json:"schema"`
	Event  AccountEvent `json:"event"`
	Digest string       `json:"record_sha256"`
}

func (r eventEnvelope) sum() string {
	r.Digest = ""
	b, _ := json.Marshal(r)
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:])
}

func OpenEventStore(dataDir string) (*EventStore, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 디렉터리가 필요합니다")
	}
	dir := filepath.Join(dataDir, "trading", "user_events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock := filepath.Join(dir, "exclusive.active")
	if err := os.Mkdir(lock, 0700); err != nil {
		return nil, errors.New("계정 이벤트 저장소가 사용 중이거나 수동 복구가 필요합니다")
	}
	store := &EventStore{dir: dir, lock: lock, halted: true, reason: "계정 이벤트 시작/재시작: 독립 계정·체결 전수 감사 필요"}
	if _, err := store.ReadAll(); err != nil { // intentionally keep lock on uncertain disk state
		return nil, err
	}
	return store, nil
}

func (s *EventStore) Stop(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.halted = true
	if reason != "" {
		s.reason = reason
	}
}
func (s *EventStore) Status() (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.halted, s.reason
}
func (s *EventStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.halted = true
	s.reason = "계정 이벤트 저장소 종료: 재기동 후 재감사 필요"
	return os.Remove(s.lock) // only remove empty lock acquired by this process
}

// Append retains every normalized relevant event exactly once per semantic
// payload hash. Duplicate delivery does not create a second fill. A persisted
// stream record is NOT itself an imported signed userTrade/fill.
func (s *EventStore) Append(e AccountEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, errors.New("저장소가 종료되었습니다")
	}
	s.halted = true
	s.reason = "계정 이벤트 감지: 주문·체결·포지션 재검증 필요"
	if !eventDigest.MatchString(e.Digest) || e.Kind == "" || e.EventTime <= 0 {
		return false, errors.New("비정상 계정 이벤트")
	}
	path := filepath.Join(s.dir, e.Digest+".json")
	record := eventEnvelope{Schema: 1, Event: e}
	record.Digest = record.sum()
	body, _ := json.MarshalIndent(record, "", "  ")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		b, err := os.ReadFile(path)
		if err != nil {
			return false, err
		}
		var prev eventEnvelope
		if json.Unmarshal(b, &prev) != nil || prev.Schema != 1 || prev.Digest != prev.sum() || prev.Event.Digest != e.Digest || prev.Event.Kind != e.Kind {
			return false, errors.New("기존 이벤트 파일 불일치: 수동 검사 필요")
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = f.Write(append(body, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return false, fmt.Errorf("이벤트 기록 불확실: %w", err)
	}
	return true, nil
}

// ReadAll validates all files, including their hashes, before returning any.
// Events can be timestamp-equal; returned order is deterministic, not a claim
// of exact chronological sequencing across heterogeneous event types.
func (s *EventStore) ReadAll() ([]AccountEvent, error) {
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	out := make([]AccountEvent, 0)
	for _, file := range files {
		if file.Name() == "exclusive.active" && file.IsDir() {
			continue
		}
		if file.IsDir() || file.Type()&os.ModeSymlink != 0 || !eventName.MatchString(file.Name()) {
			return nil, errors.New("이벤트 저장소의 알 수 없는 파일")
		}
		b, err := os.ReadFile(filepath.Join(s.dir, file.Name()))
		if err != nil {
			return nil, err
		}
		var r eventEnvelope
		if err := json.Unmarshal(b, &r); err != nil || r.Schema != 1 || r.Digest != r.sum() || file.Name() != r.Event.Digest+".json" || r.Event.Kind == "" || r.Event.EventTime <= 0 {
			return nil, errors.New("이벤트 기록 손상: 수동 복구 필요")
		}
		out = append(out, r.Event)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventTime != out[j].EventTime {
			return out[i].EventTime < out[j].EventTime
		}
		return out[i].Digest < out[j].Digest
	})
	return out, nil
}

// RecordDiscontinuity writes an immutable marker after ANY stream loss,
// listenKey rotation or unknown event. It is not safe to remove this marker
// without externally comparing a complete signed REST snapshot and fills.
func (s *EventStore) RecordDiscontinuity(reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("이벤트 저장소 종료")
	}
	// Duplicate coordinator and observer teardown used to create two separate
	// historical DISCONNECT markers for one stop. This optimization is valid
	// ONLY within the same live EventStore, never across sessions or restarts.
	// Failed writes do not set the flag; the owning recovery keeps fail-closed.
	if reason == "DISCONNECT" && s.disconnectRecorded {
		return nil
	}
	s.halted = true
	s.reason = "이벤트 스트림 불연속: 독립 서명 조회/과거 체결 감사가 필요합니다"
	switch reason {
	case "STARTUP", "DISCONNECT", "ROTATE", "EXPIRED", "KEEPALIVE_FAILED", "UNKNOWN_EVENT", "CANCEL_UNCERTAIN":
	default:
		reason = "STREAM_DISCONTINUITY"
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	marker := struct {
		Schema int    `json:"schema"`
		Reason string `json:"reason"`
		At     string `json:"at"`
	}{1, reason, time.Now().UTC().Format(time.RFC3339Nano)}
	b, _ := json.Marshal(marker)
	// Keep durable markers in a SEPARATE directory from the event hash index.
	dir := filepath.Join(filepath.Dir(s.dir), "stream_discontinuities")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, hex.EncodeToString(nonce[:])+".json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	c := f.Close()
	if err == nil {
		err = c
	}
	if err == nil {
		if reason == "STARTUP" {
			// Each newly persisted observation epoch may have its own stop.
			s.disconnectRecorded = false
		} else if reason == "DISCONNECT" {
			s.disconnectRecorded = true
		}
	}
	return err
}
