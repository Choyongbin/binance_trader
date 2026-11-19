package trading

// Durable at-most-once manual LIMIT order cancellation intent. No HTTP handler
// exposes this store. Do not infer order/position closure from DELETE ACK:
// signed readback, complete immutable fills and full ownership/account audit
// are separate requirements. A timeout/crash after marker prevents any replay.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

var ErrManualCancelAttempted = errors.New("취소 전송 또는 전송 여부 불명: 자동 재취소 금지")

type ManualCancelIntent struct {
	Schema          int    `json:"schema"`
	ClientOrderID   string `json:"client_order_id"`
	ExchangeOrderID int64  `json:"exchange_order_id"`
	OriginalDigest  string `json:"original_digest"`
	PlannedQty      string `json:"planned_qty"`
	CreatedAt       string `json:"created_at"`
	Digest          string `json:"digest"`
}

func (c ManualCancelIntent) seal() string {
	c.Digest = ""
	raw, _ := json.Marshal(c)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

type ManualCancelStore struct {
	mu              sync.Mutex
	dir, journalDir string
}

func OpenManualCancelStore(dataDir string) (*ManualCancelStore, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 디렉터리 누락")
	}
	dir := filepath.Join(dataDir, "trading", "manual_cancels")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &ManualCancelStore{dir: dir, journalDir: filepath.Join(dataDir, "trading", "requests")}, nil
}
func (s *ManualCancelStore) path(id string) string { return filepath.Join(s.dir, id+".json") }
func (s *ManualCancelStore) read(id string) (ManualCancelIntent, bool, error) {
	if !journalName.MatchString(id + ".json") {
		return ManualCancelIntent{}, false, errors.New("주문 ID 오류")
	}
	raw, err := os.ReadFile(s.path(id))
	if err != nil {
		return ManualCancelIntent{}, false, err
	}
	var in ManualCancelIntent
	if json.Unmarshal(raw, &in) != nil || in.Schema != 1 || in.ClientOrderID != id || in.ExchangeOrderID <= 0 || in.Digest == "" || in.Digest != in.seal() {
		return ManualCancelIntent{}, false, errors.New("수동 취소 인텐트 손상")
	}
	raw, err = os.ReadFile(filepath.Join(s.dir, id+".attempt.json"))
	if errors.Is(err, os.ErrNotExist) {
		return in, false, nil
	}
	if err != nil {
		return ManualCancelIntent{}, false, err
	}
	var attempt struct {
		Digest string `json:"digest"`
		State  string `json:"state"`
	}
	if json.Unmarshal(raw, &attempt) != nil || attempt.Digest != in.Digest || attempt.State != "MAY_HAVE_SENT" {
		return ManualCancelIntent{}, false, errors.New("취소 전송 표식 손상")
	}
	return in, true, nil
}
func (s *ManualCancelStore) Inspect(id string) (ManualCancelIntent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read(id)
}

type VerifiedManualCancelReader interface {
	QueryDemoOrder(context.Context, string, string, string, string) (exchange.OrderResult, error)
}

// PlanVerified reads the original immutable journal AND an independent signed
// exchange order for the specific order ID; only nonterminal manual LIMIT
// orders can be cancelled. No browser-derived status can drive cancellation.
func (s *ManualCancelStore) PlanVerified(ctx context.Context, reader VerifiedManualCancelReader, key, secret, id string) (ManualCancelIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reader == nil || key == "" || secret == "" || !journalName.MatchString(id+".json") {
		return ManualCancelIntent{}, errors.New("수동 지정가 서명 조회 인자 오류")
	}
	record, err := readRecord(filepath.Join(s.journalDir, id+".json"))
	if err != nil {
		return ManualCancelIntent{}, err
	}
	plan, err := record.Intent.Plan()
	if err != nil || record.Plan != plan || plan.ClientOrderID != id || record.Intent.Source != Manual || record.Intent.Action != Enter || record.Intent.OrderType != Limit ||
		(record.State != Acknowledged && record.State != Reconciled) {
		return ManualCancelIntent{}, errors.New("취소할 수동 지정가의 검증된 주문 저널이 없습니다")
	}
	digest, err := digestIntent(record.Intent)
	if err != nil || digest != record.IntentDigest {
		return ManualCancelIntent{}, errors.New("주문 저널 해시 불일치")
	}
	if old, _, err := s.read(id); err == nil {
		if old.OriginalDigest != record.IntentDigest {
			return ManualCancelIntent{}, errors.New("다른 주문의 취소 기록 충돌")
		}
		return old, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ManualCancelIntent{}, err
	}
	remote, err := reader.QueryDemoOrder(ctx, key, secret, plan.Symbol, id)
	if err != nil {
		return ManualCancelIntent{}, err
	}
	executed, err := verifyRemoteOrder(record, remote)
	if err != nil {
		return ManualCancelIntent{}, err
	}
	if executed.Sign() < 0 || (remote.Status != "NEW" && remote.Status != "PARTIALLY_FILLED") ||
		(record.ExchangeOrderID > 0 && record.ExchangeOrderID != remote.OrderID) {
		return ManualCancelIntent{}, errors.New("미체결/부분 체결된 해당 주문이 아니므로 취소 계획 차단")
	}
	in := ManualCancelIntent{Schema: 1, ClientOrderID: id, ExchangeOrderID: remote.OrderID, OriginalDigest: digest, PlannedQty: record.Plan.Quantity, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	in.Digest = in.seal()
	raw, _ := json.MarshalIndent(in, "", "  ")
	if err := exclusiveDurable(s.path(id), raw); err != nil {
		return ManualCancelIntent{}, fmt.Errorf("수동 취소 계획 기록 충돌: %w", err)
	}
	return in, nil
}
func (s *ManualCancelStore) MarkMayHaveSent(in ManualCancelIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	original, already, err := s.read(in.ClientOrderID)
	if err != nil {
		return err
	}
	if original.Digest != in.Digest || original.ExchangeOrderID != in.ExchangeOrderID {
		return errors.New("수동 취소 계획 바뀜")
	}
	if already {
		return ErrManualCancelAttempted
	}
	b, _ := json.Marshal(map[string]string{"digest": original.Digest, "state": "MAY_HAVE_SENT"})
	if err := exclusiveDurable(filepath.Join(s.dir, in.ClientOrderID+".attempt.json"), b); err != nil {
		return ErrManualCancelAttempted
	}
	return nil
}

type ManualCancelTransport interface {
	CancelDemoOrder(context.Context, string, string, string, string) (exchange.OrderResult, error)
}

// Mock-usable internal adapter, NOT a complete pre-trade gate. The later
// live integration must independently enforce one coordinator owner and a
// fresh account/stream baseline before even calling MarkMayHaveSent.
func (s *ManualCancelStore) AttemptOnce(ctx context.Context, api ManualCancelTransport, key, secret string, in ManualCancelIntent) (exchange.OrderResult, error) {
	if api == nil || key == "" || secret == "" {
		return exchange.OrderResult{}, errors.New("Demo 취소 어댑터 또는 키 없음")
	}
	if err := s.MarkMayHaveSent(in); err != nil {
		return exchange.OrderResult{}, err
	}
	result, err := api.CancelDemoOrder(ctx, key, secret, "BTCUSDT", in.ClientOrderID)
	if err != nil {
		return exchange.OrderResult{}, fmt.Errorf("취소 결과 불명확, 서명 재조회 필수: %w", err)
	}
	if result.ClientOrderID != in.ClientOrderID || result.OrderID != in.ExchangeOrderID || result.Symbol != "BTCUSDT" ||
		result.Status != "CANCELED" && result.Status != "FILLED" && result.Status != "EXPIRED" && result.Status != "REJECTED" && result.Status != "EXPIRED_IN_MATCH" {
		return exchange.OrderResult{}, errors.New("취소 후 서명 조회가 해당 주문의 종료를 증명하지 않습니다")
	}
	planned, err := pos(in.PlannedQty)
	executed, e := decimalRat(result.ExecutedQty, true)
	if err != nil || e != nil || executed.Cmp(planned) > 0 {
		return exchange.OrderResult{}, errors.New("취소 후 체결 수량 오류: 추가 독립 거래소 체결 감사 필수")
	}
	return result, nil // NEVER release reservations or claim position closed
}

// AuditAll detects orphaned MAY_HAVE_SENT files and checks the backing
// immutable journal identity before any future coordinator uses this store.
func (s *ManualCancelStore) AuditAll() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, err
	}
	ids := make(map[string]bool)
	for _, f := range files {
		if f.IsDir() {
			return 0, errors.New("잘못된 수동 취소 디렉터리")
		}
		name := f.Name()
		id := strings.TrimSuffix(name, ".json")
		if strings.HasSuffix(name, ".attempt.json") {
			id = strings.TrimSuffix(name, ".attempt.json")
		} else if !strings.HasSuffix(name, ".json") {
			return 0, errors.New("알 수 없는 수동 취소 파일")
		}
		if !journalName.MatchString(id + ".json") {
			return 0, errors.New("수동 취소 파일 이름 오류")
		}
		ids[id] = true
	}
	for id := range ids {
		in, _, err := s.read(id)
		if err != nil {
			return 0, err
		}
		rec, err := readRecord(filepath.Join(s.journalDir, id+".json"))
		if err != nil {
			return 0, err
		}
		p, e := rec.Intent.Plan()
		digest, d := digestIntent(rec.Intent)
		if e != nil || d != nil || p != rec.Plan || rec.Intent.Source != Manual || rec.Intent.OrderType != Limit || rec.Intent.Action != Enter ||
			digest != in.OriginalDigest || rec.Plan.Quantity != in.PlannedQty || rec.ExchangeOrderID > 0 && rec.ExchangeOrderID != in.ExchangeOrderID {
			return 0, errors.New("수동 취소 원본 기록과 불일치")
		}
	}
	return len(ids), nil
}
