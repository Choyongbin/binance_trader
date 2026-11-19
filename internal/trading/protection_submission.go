package trading

// Crash-safe protective-leg PRE-DISPATCH state, independent from the regular
// order journal. There is no automatic pair submission: two independent Binance
// Algo orders are NOT OCO. This internal store is intentionally NOT wired to
// HTTP, the live Binance transport, or the strategy engine.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

var ErrProtectionAttempted = errors.New("보호 주문 전송 여부 불명: 같은 레그 재전송 금지, 서명된 거래소 조회와 수동 복구 필요")
var ErrProtectionApproval = errors.New("새 보호 주문의 실제 전송은 전체 계정 감사와 승인 절차 연결 전 비활성화")

type ProtectionDispatchPlan struct {
	Schema       int                   `json:"schema"`
	GroupID      string                `json:"group_id"`
	Source       Source                `json:"source"`
	ModelID      string                `json:"model_id,omitempty"`
	PositionSide PositionSide          `json:"position_side"`
	TP           ProtectionInstruction `json:"tp"`
	SL           ProtectionInstruction `json:"sl"`
	CreatedAt    string                `json:"created_at"`
	Digest       string                `json:"digest"`
}

func (p ProtectionDispatchPlan) digest() string {
	p.Digest = ""
	b, _ := json.Marshal(p)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validateLegInstruction(o ProtectionInstruction, side PositionSide, kind string) error {
	wantType, wantWorking, wantSide := "TAKE_PROFIT_MARKET", "CONTRACT_PRICE", "SELL"
	if kind == "sl" {
		wantType = "STOP_MARKET"
		wantWorking = "MARK_PRICE"
	}
	if side == Short {
		wantSide = "BUY"
	}
	if o.Symbol != "BTCUSDT" || o.AlgoType != "CONDITIONAL" || o.Side != wantSide || o.PositionSide != side ||
		o.OrderType != wantType || o.WorkingType != wantWorking || !requestIDPattern.MatchString(o.ClientAlgoID) {
		return errors.New("보호 주문 종류·방향·클라이언트 ID 오류")
	}
	if _, err := pos(o.Quantity); err != nil {
		return err
	}
	if _, err := pos(o.TriggerPrice); err != nil {
		return err
	}
	return nil
}
func (p ProtectionDispatchPlan) validate() error {
	if p.Schema != 1 || !requestIDPattern.MatchString(p.GroupID) ||
		(p.Source != Manual && p.Source != Model) ||
		(p.Source == Model && !modelIDPattern.MatchString(p.ModelID)) ||
		(p.Source == Manual && p.ModelID != "") ||
		(p.PositionSide != Long && p.PositionSide != Short) ||
		p.TP.ClientAlgoID == p.SL.ClientAlgoID || p.TP.Quantity != p.SL.Quantity ||
		p.Digest == "" || p.Digest != p.digest() {
		return errors.New("보호 주문 그룹 및 체크섬 오류")
	}
	if err := validateLegInstruction(p.TP, p.PositionSide, "tp"); err != nil {
		return err
	}
	return validateLegInstruction(p.SL, p.PositionSide, "sl")
}

type ProtectionSubmissionStore struct {
	mu  sync.Mutex
	dir string
}

func OpenProtectionSubmissionStore(dataDir string) (*ProtectionSubmissionStore, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 폴더 없음")
	}
	dir := filepath.Join(dataDir, "trading", "protection_submissions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &ProtectionSubmissionStore{dir: dir}, nil
}
func (s *ProtectionSubmissionStore) base(group string) string { return filepath.Join(s.dir, group) }
func (s *ProtectionSubmissionStore) read(group string) (ProtectionDispatchPlan, error) {
	if !requestIDPattern.MatchString(group) {
		return ProtectionDispatchPlan{}, errors.New("보호 그룹 ID 오류")
	}
	raw, err := os.ReadFile(s.base(group) + ".json")
	if err != nil {
		return ProtectionDispatchPlan{}, err
	}
	var p ProtectionDispatchPlan
	if err = json.Unmarshal(raw, &p); err != nil {
		return ProtectionDispatchPlan{}, err
	}
	if p.GroupID != group {
		return ProtectionDispatchPlan{}, errors.New("보호 그룹 ID 불일치")
	}
	if err = p.validate(); err != nil {
		return ProtectionDispatchPlan{}, err
	}
	return p, nil
}

func (s *ProtectionSubmissionStore) scanPlansLocked() ([]ProtectionDispatchPlan, error) {
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	groups := make(map[string]bool)
	suffixes := []string{".tp.attempt.json", ".tp.observed.json", ".sl.attempt.json", ".sl.observed.json"}
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 {
			return nil, errors.New("보호 제출 디렉터리에 정상 파일이 아닌 항목")
		}
		name := f.Name()
		recognized := false
		if strings.HasSuffix(name, ".json") {
			for _, suffix := range suffixes {
				if strings.HasSuffix(name, suffix) {
					base := strings.TrimSuffix(name, suffix)
					if !requestIDPattern.MatchString(base) {
						return nil, errors.New("보호 제출 레그 이름 오류")
					}
					groups[base] = true
					recognized = true
					break
				}
			}
			if !recognized {
				base := strings.TrimSuffix(name, ".json")
				if !requestIDPattern.MatchString(base) {
					return nil, errors.New("알 수 없는 보호 제출 파일")
				}
				groups[base] = true
				recognized = true
			}
		}
		if !recognized {
			return nil, errors.New("알 수 없는 보호 제출 파일")
		}
	}
	names := make([]string, 0, len(groups))
	for group := range groups {
		names = append(names, group)
	}
	sort.Strings(names)
	plans := make([]ProtectionDispatchPlan, 0, len(names))
	for _, group := range names {
		plan, err := s.read(group)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.New("보호 제출 레그 표식에 대응하는 원본 계획이 없습니다")
			}
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// ReviewAllPlans is a read-only local safety diagnostic. A conflict is
// returned in the review state instead of being hidden by a successful scan.
// Corrupt/orphan files still return an error.
func (s *ProtectionSubmissionStore) ReviewAllPlans() (ProtectionArmingReview, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plans, err := s.scanPlansLocked()
	if err != nil {
		return ProtectionArmingReview{}, err
	}
	active, _, err := activeProtectionPlansLocked(s, plans)
	if err != nil {
		return ProtectionArmingReview{}, err
	}
	return ReviewProtectionArmingPlans(active), nil
}

// Prepare persists a fully deterministic owner-sized pair; it does not place
// orders, reserve quantity or establish a complete signed ownership audit.
func (s *ProtectionSubmissionStore) Prepare(group string, owner OwnerPosition, draft ProtectionPreview) (ProtectionDispatchPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if draft.LiveSubmissionEnabled || owner.Symbol != "BTCUSDT" || owner.Side != draft.TP.PositionSide || owner.Side != draft.SL.PositionSide ||
		owner.Quantity != draft.TP.Quantity || owner.Quantity != draft.SL.Quantity ||
		draft.Leverage != 15 || draft.TargetGrossROI != "0.05" {
		return ProtectionDispatchPlan{}, errors.New("보호 주문 계획과 소유 수량 불일치")
	}
	if !requestIDPattern.MatchString(group) {
		return ProtectionDispatchPlan{}, errors.New("보호 그룹 ID 오류")
	}
	plans, err := s.scanPlansLocked()
	if err != nil {
		return ProtectionDispatchPlan{}, err
	}
	activePlans, settledGroups, err := activeProtectionPlansLocked(s, plans)
	if err != nil {
		return ProtectionDispatchPlan{}, err
	}
	for _, existing := range plans {
		if existing.GroupID != group {
			continue
		}
		if settledGroups[group] {
			return ProtectionDispatchPlan{}, errors.New("이미 SETTLED된 보호 그룹 ID는 재사용할 수 없습니다")
		}
		if existing.TP != draft.TP || existing.SL != draft.SL || existing.Source != owner.Source || existing.ModelID != owner.ModelID {
			return ProtectionDispatchPlan{}, errors.New("기존 보호 그룹 ID 재사용 충돌")
		}
		if review := ReviewProtectionArmingPlans(activePlans); review.State == ProtectionArmingPooledConflict {
			return ProtectionDispatchPlan{}, ErrPooledProtectionConflict
		}
		return existing, nil
	}
	p := ProtectionDispatchPlan{Schema: 1, GroupID: group, Source: owner.Source, ModelID: owner.ModelID, PositionSide: owner.Side,
		TP: draft.TP, SL: draft.SL, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	p.Digest = p.digest()
	if err := p.validate(); err != nil {
		return ProtectionDispatchPlan{}, err
	}
	candidate := append(append([]ProtectionDispatchPlan{}, activePlans...), p)
	if review := ReviewProtectionArmingPlans(candidate); review.State == ProtectionArmingPooledConflict {
		return ProtectionDispatchPlan{}, ErrPooledProtectionConflict
	}
	raw, _ := json.MarshalIndent(p, "", "  ")
	if err := exclusiveDurable(s.base(group)+".json", raw); err != nil {
		return ProtectionDispatchPlan{}, fmt.Errorf("보호 계획 기록 불확실: %w", err)
	}
	return p, nil
}

type observedProtectionMarker struct {
	PlanDigest   string `json:"plan_digest"`
	ClientAlgoID string `json:"client_algo_id"`
	AlgoID       int64  `json:"algo_id"`
	State        string `json:"state"`
	At           string `json:"at"`
	Checksum     string `json:"checksum"`
}

func (o observedProtectionMarker) digest() string {
	o.Checksum = ""
	raw, _ := json.Marshal(o)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

type ProtectionLegState struct {
	GroupID              string    `json:"group_id"`
	Leg                  string    `json:"leg"`
	Prepared             bool      `json:"prepared"`
	MayHaveSent          bool      `json:"may_have_sent"`
	SignedObserved       bool      `json:"signed_observed"`
	ObservedAlgoID       int64     `json:"observed_algo_id,omitempty"`
	ObservedAt           time.Time `json:"-"` // internal only: historical observation is not evidence of CURRENT active TP
	ManualReviewRequired bool      `json:"manual_review_required"`
	LiveAuthorized       bool      `json:"live_authorized"`
}

func (s *ProtectionSubmissionStore) legPaths(group, leg string) (string, string, error) {
	if leg != "tp" && leg != "sl" {
		return "", "", errors.New("tp/sl만 허용")
	}
	return s.base(group) + "." + leg + ".attempt.json", s.base(group) + "." + leg + ".observed.json", nil
}

// Inspect re-verifies the durable plan and markers. An incomplete/orphan file
// blocks progress. Always reports live_authorized=false.
func (s *ProtectionSubmissionStore) inspectLocked(group, leg string) (ProtectionLegState, error) {
	p, err := s.read(group)
	if err != nil {
		return ProtectionLegState{}, err
	}
	attempt, observed, _ := s.legPaths(group, leg)
	state := ProtectionLegState{GroupID: group, Leg: leg, Prepared: true, ManualReviewRequired: true}
	expected := p.TP
	if leg == "sl" {
		expected = p.SL
	}
	raw, err := os.ReadFile(attempt)
	if errors.Is(err, os.ErrNotExist) {
		if _, obsErr := os.Stat(observed); obsErr == nil {
			return state, errors.New("전송 전 표식 없이 관측 파일 존재")
		}
		return state, nil
	}
	if err != nil {
		return state, err
	}
	var a struct {
		PlanDigest   string `json:"plan_digest"`
		ClientAlgoID string `json:"client_algo_id"`
		State        string `json:"state"`
	}
	if json.Unmarshal(raw, &a) != nil || a.PlanDigest != p.Digest || a.ClientAlgoID != expected.ClientAlgoID || a.State != "MAY_HAVE_SENT" {
		return state, errors.New("보호 레그 전송 기록 손상")
	}
	state.MayHaveSent = true
	raw, err = os.ReadFile(observed)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	var o observedProtectionMarker
	if json.Unmarshal(raw, &o) != nil || o.PlanDigest != p.Digest || o.ClientAlgoID != expected.ClientAlgoID ||
		o.AlgoID <= 0 || o.State != "SIGNED_ACTIVE_OBSERVED" || o.Checksum == "" || o.Checksum != o.digest() {
		return state, errors.New("서명 관측 기록 또는 체크섬 손상")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, o.At)
	if err != nil || observedAt.After(time.Now().UTC().Add(time.Second)) {
		return state, errors.New("서명 관측 시각 손상")
	}
	state.SignedObserved = true
	state.ObservedAlgoID = o.AlgoID
	state.ObservedAt = observedAt
	return state, nil
}
func (s *ProtectionSubmissionStore) Inspect(group, leg string) (ProtectionLegState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, err := s.legPaths(group, leg); err != nil {
		return ProtectionLegState{}, err
	}
	return s.inspectLocked(group, leg)
}

// MarkMayHaveSent intentionally does NOT let an internal library authorize
// a live submission by itself. The first leg can be persisted for MOCK tests;
// the second leg requires a verified first-leg observation and, for live use, the independent full-account signed audit is enforced only by ArmDemoProtection.
func (s *ProtectionSubmissionStore) MarkMayHaveSent(group, leg string, simulatedOnly bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !simulatedOnly {
		return ErrProtectionApproval
	}
	state, err := s.inspectLocked(group, leg)
	if err != nil {
		return err
	}
	if state.MayHaveSent {
		return ErrProtectionAttempted
	}
	if leg == "sl" {
		first, err := s.inspectLocked(group, "tp")
		if err != nil {
			return err
		}
		if !first.SignedObserved || first.ObservedAt.IsZero() || time.Since(first.ObservedAt) > 5*time.Second {
			return errors.New("SL 시뮬레이션 직전에 TP의 독립 서명 재조회 필요 (관측 5초 제한)")
		}
		// This only checks the age of a mock-stage persisted TP observation.
		// It is NOT a fresh account-wide audit or proof that TP remains active.
	}
	p, _ := s.read(group)
	o := p.TP
	if leg == "sl" {
		o = p.SL
	}
	raw, _ := json.Marshal(map[string]string{"plan_digest": p.Digest, "client_algo_id": o.ClientAlgoID, "state": "MAY_HAVE_SENT"})
	attempt, _, _ := s.legPaths(group, leg)
	if err := exclusiveDurable(attempt, raw); err != nil {
		return ErrProtectionAttempted
	}
	return nil
}

// SignedObservation is a read-only mock-injected check for the staged
// simulation. Real approval MUST replace mock transport and separately audit
// the complete account before *either* network POST is allowed.
type SignedProtectionReader interface {
	QueryDemoAlgo(context.Context, string, string, int64) (exchange.DemoAlgoResult, error)
}

func (s *ProtectionSubmissionStore) RecordObserved(ctx context.Context, group, leg string, algoID int64, reader SignedProtectionReader, key, secret string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reader == nil || key == "" || secret == "" || algoID <= 0 {
		return errors.New("서명 조회 어댑터 또는 ID 누락")
	}
	state, err := s.inspectLocked(group, leg)
	if err != nil {
		return err
	}
	if !state.MayHaveSent || state.SignedObserved {
		return errors.New("전송 표식 또는 관측 상태 불일치")
	}
	p, _ := s.read(group)
	expected := p.TP
	if leg == "sl" {
		expected = p.SL
	}
	// Network read is *outside* the production UI. No POST is issued here.
	o, err := reader.QueryDemoAlgo(ctx, key, secret, algoID)
	if err != nil {
		return err
	}
	want, got := expected.TriggerPrice, string(o.TriggerPrice)
	x, e1 := pos(want)
	y, e2 := pos(got)
	if o.AlgoID != algoID || o.ClientAlgoID != expected.ClientAlgoID || o.Symbol != expected.Symbol || o.Side != expected.Side ||
		o.PositionSide != string(expected.PositionSide) || o.Type != expected.OrderType || o.AlgoType != "CONDITIONAL" ||
		o.WorkingType != expected.WorkingType || o.ClosePosition || o.ReduceOnly ||
		(o.Status != "NEW" && o.Status != "WORKING") || e1 != nil || e2 != nil || x.Cmp(y) != 0 {
		return errors.New("보호 주문 독립 서명 조회가 원래 계획과 다릅니다")
	}
	oq, err := pos(string(o.Quantity))
	wq, e := pos(expected.Quantity)
	if err != nil || e != nil || oq.Cmp(wq) != 0 {
		return errors.New("보호 주문 소유 수량 불일치")
	}
	marker := observedProtectionMarker{PlanDigest: p.Digest, ClientAlgoID: expected.ClientAlgoID,
		AlgoID: algoID, State: "SIGNED_ACTIVE_OBSERVED", At: time.Now().UTC().Format(time.RFC3339Nano)}
	marker.Checksum = marker.digest()
	raw, _ := json.Marshal(marker)
	_, observed, _ := s.legPaths(group, leg)
	if err := exclusiveDurable(observed, raw); err != nil {
		return errors.New("서명 관측 기록 충돌 또는 디스크 오류")
	}
	return nil
}

// AuditAll scans the complete persisted submission directory, not just the
// group the caller happened to ask about. Orphan/unknown files, reused client
// IDs and reused exchange Algo IDs invalidate the whole review.
func (s *ProtectionSubmissionStore) AuditAll() ([]ProtectionLegState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plans, err := s.scanPlansLocked()
	if err != nil {
		return nil, err
	}
	activePlans, _, err := activeProtectionPlansLocked(s, plans)
	if err != nil {
		return nil, err
	}
	if review := ReviewProtectionArmingPlans(activePlans); review.State == ProtectionArmingPooledConflict {
		return nil, ErrPooledProtectionConflict
	}
	result := make([]ProtectionLegState, 0, len(plans)*2)
	allClientIDs := make(map[string]string)
	allAlgoIDs := make(map[int64]string)
	for _, p := range plans {
		group := p.GroupID
		for _, leg := range []string{"tp", "sl"} {
			state, err := s.inspectLocked(group, leg)
			if err != nil {
				return nil, err
			}
			order := p.TP
			if leg == "sl" {
				order = p.SL
			}
			if prev := allClientIDs[order.ClientAlgoID]; prev != "" {
				return nil, fmt.Errorf("서로 다른 소유자 그룹에서 보호 클라이언트 ID 중복: %s", prev)
			}
			allClientIDs[order.ClientAlgoID] = group
			if state.ObservedAlgoID > 0 {
				if prev := allAlgoIDs[state.ObservedAlgoID]; prev != "" {
					return nil, errors.New("보호 주문 AlgoID 중복")
				}
				allAlgoIDs[state.ObservedAlgoID] = group
			}
			result = append(result, state)
		}
	}
	return result, nil
}
