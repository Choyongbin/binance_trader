package trading

// Durable ONE-SHOT cancel intent for individual Binance Demo algo siblings.
// A signed DELETE has non-idempotent effects in racing trigger scenarios;
// after dispatch is recorded, a crash/network timeout MUST NOT automatically
// re-issue DELETE. Signed GET, complete fills and account reconciliation are
// required before the remaining owner quantity is considered resolved.
import (
	"binance_trader/internal/exchange"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var cancelGroupRE = regexp.MustCompile(`^[A-Za-z0-9_-]{3,60}$`)
var ErrCancelAlreadyAttempted = errors.New("기존 취소 인텐트가 전송되었거나 전송 여부가 불명확합니다. 자동 재취소 금지")

type CancelIntent struct {
	Schema            int    `json:"schema"`
	GroupID           string `json:"group_id"`
	SiblingAlgoID     int64  `json:"sibling_algo_id"`
	OwnerClientAlgoID string `json:"owner_client_algo_id"`
	CreatedAt         string `json:"created_at"`
	Digest            string `json:"digest"`
}

func (c CancelIntent) seal() string {
	c.Digest = ""
	b, _ := json.Marshal(c)
	v := sha256.Sum256(b)
	return hex.EncodeToString(v[:])
}
func (c CancelIntent) base(dir string) string { return filepath.Join(dir, c.GroupID+"_"+c.Digest[:16]) }

type CancelIntentStore struct {
	mu  sync.Mutex
	dir string
}

func NewCancelIntentStore(dataDir string) (*CancelIntentStore, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 디렉터리가 필요합니다")
	}
	dir := filepath.Join(dataDir, "trading", "algo_cancel_intents")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &CancelIntentStore{dir: dir}, nil
}
func exclusiveDurable(path string, payload []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(payload, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	// Deliberately keep incomplete file on error: no unsafe retry after crash.
	return err
}

// Plan saves immutable target identity before ANY side effect. The exact same
// input is idempotent; conflicts within one group always fail closed.
func (s *CancelIntentStore) Plan(group string, algoID int64, ownerClientAlgoID string) (CancelIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !cancelGroupRE.MatchString(group) || algoID <= 0 || !cancelGroupRE.MatchString(ownerClientAlgoID) {
		return CancelIntent{}, errors.New("보호 그룹/조건부 주문 ID 오류")
	}
	// One group allows exactly one cancellation target. Never accept a second
	// independently selected sibling after an ambiguous first cancellation.
	path := filepath.Join(s.dir, group+".json")
	if raw, err := os.ReadFile(path); err == nil {
		var prior CancelIntent
		if json.Unmarshal(raw, &prior) != nil || prior.Schema != 1 || prior.Digest != prior.seal() || prior.GroupID != group || prior.SiblingAlgoID != algoID || prior.OwnerClientAlgoID != ownerClientAlgoID {
			return CancelIntent{}, errors.New("보호 그룹에 다른 취소 인텐트 또는 손상 파일 존재")
		}
		return prior, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return CancelIntent{}, err
	}
	c := CancelIntent{Schema: 1, GroupID: group, SiblingAlgoID: algoID, OwnerClientAlgoID: ownerClientAlgoID, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	c.Digest = c.seal()
	b, _ := json.MarshalIndent(c, "", "  ")
	if err := exclusiveDurable(path, b); err != nil {
		return CancelIntent{}, errors.New("취소 인텐트 기록 충돌 또는 기록 실패: 수동 조회 필요")
	}
	return c, nil
}

// MarkMayHaveSent MUST be persisted before calling a DELETE and can succeed
// only once. Even if a crash occurs before network transmission, the new
// process may ONLY query, never replay the ambiguous DELETE automatically.
func (s *CancelIntentStore) MarkMayHaveSent(c CancelIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.Schema != 1 || !cancelGroupRE.MatchString(c.GroupID) || c.Digest != c.seal() {
		return errors.New("취소 인텐트 내용 오류")
	}
	raw, err := os.ReadFile(filepath.Join(s.dir, c.GroupID+".json"))
	if err != nil {
		return err
	}
	var onDisk CancelIntent
	if json.Unmarshal(raw, &onDisk) != nil || onDisk != c {
		return errors.New("취소 인텐트 변경 또는 손상")
	}
	marker := struct {
		Digest string `json:"digest"`
		State  string `json:"state"`
		At     string `json:"at"`
	}{c.Digest, "MAY_HAVE_SENT", time.Now().UTC().Format(time.RFC3339Nano)}
	b, _ := json.Marshal(marker)
	if err := exclusiveDurable(filepath.Join(s.dir, c.GroupID+".dispatch.json"), b); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrCancelAlreadyAttempted
		}
		return fmt.Errorf("취소 디스패치 기록 불확실: %w", err)
	}
	return nil
}

// Inspect validates persistent state. An observed signed terminal status
// NEVER constitutes owner closure or a released model reservation by itself.
func (s *CancelIntentStore) Inspect(group string) (CancelIntent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !cancelGroupRE.MatchString(group) {
		return CancelIntent{}, false, errors.New("그룹 ID 오류")
	}
	raw, err := os.ReadFile(filepath.Join(s.dir, group+".json"))
	if err != nil {
		return CancelIntent{}, false, err
	}
	var c CancelIntent
	if json.Unmarshal(raw, &c) != nil || c.Schema != 1 || c.Digest != c.seal() || c.GroupID != group {
		return CancelIntent{}, false, errors.New("기존 취소 인텐트 손상")
	}
	raw, err = os.ReadFile(filepath.Join(s.dir, group+".dispatch.json"))
	if errors.Is(err, os.ErrNotExist) {
		return c, false, nil
	}
	if err != nil {
		return CancelIntent{}, false, err
	}
	var marker struct {
		Digest string `json:"digest"`
		State  string `json:"state"`
		At     string `json:"at"`
	}
	if json.Unmarshal(raw, &marker) != nil || marker.Digest != c.Digest || marker.State != "MAY_HAVE_SENT" || marker.At == "" {
		return CancelIntent{}, false, errors.New("취소 디스패치 손상")
	}
	if err := s.validatePersistedCancelOutcomeLocked(c); err != nil {
		return CancelIntent{}, false, err
	}
	return c, true, nil
}

type AlgoCancelTransport interface {
	CancelDemoAlgoOnce(context.Context, string, string, int64) (exchange.DemoAlgoResult, error)
}

// AttemptAlgoCancel is INTERNAL AND UNWIRED; caller must have already proven
// the exact correct sibling ID via signed observation and durable group state.
// This method itself does NOT check the complete account audit; consequently
// the app must NOT expose it until external prerequisites are implemented.
func (s *CancelIntentStore) AttemptAlgoCancel(ctx context.Context, api AlgoCancelTransport, key, secret string, c CancelIntent) (exchange.DemoAlgoResult, error) {
	if api == nil || key == "" || secret == "" {
		return exchange.DemoAlgoResult{}, errors.New("Demo 취소 어댑터 또는 인증 정보 없음")
	}
	if err := s.MarkMayHaveSent(c); err != nil {
		return exchange.DemoAlgoResult{}, err
	}
	out, err := api.CancelDemoAlgoOnce(ctx, key, secret, c.SiblingAlgoID)
	if err != nil {
		return exchange.DemoAlgoResult{}, fmt.Errorf("취소 결과 불확실: %w", err)
	}
	if err := validateCancelObservation(c, out); err != nil {
		return exchange.DemoAlgoResult{}, err
	}
	switch out.Status {
	case "CANCELED", "CANCELLED", "EXPIRED", "TRIGGERED", "FINISHED":
		return out, nil // does not release any ownership reservation
	default:
		return exchange.DemoAlgoResult{}, errors.New("취소 후 terminal 상태 미확인: 자동 재취소 금지")
	}
}
