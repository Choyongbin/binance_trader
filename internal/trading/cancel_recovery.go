package trading

// Read-only recovery for an already-attempted sibling Algo cancellation.
// This code never issues DELETE/POST. After a crash, timeout, or ambiguous
// cancel result it may only perform a signed GET and durably record what was
// observed. A triggered/finished sibling is NOT treated as cancelled; fills
// and the full pooled-side position must be reconciled separately.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
)

type CancelRecoveryState string

const (
	CancelRecoveryPendingActive CancelRecoveryState = "ACTIVE_RETRY_FORBIDDEN"
	CancelRecoveryCanceled      CancelRecoveryState = "CANCELED_CONFIRMED_BY_SIGNED_GET"
	CancelRecoveryTriggered     CancelRecoveryState = "TRIGGERED_REQUIRES_FILL_RECONCILIATION"
	CancelRecoveryHalt          CancelRecoveryState = "HALT_AND_MANUAL_RECONCILE"
)

type CancelRecoveryResult struct {
	State                CancelRecoveryState `json:"state"`
	GroupID              string              `json:"group_id"`
	SiblingAlgoID        int64               `json:"sibling_algo_id"`
	ClientAlgoID         string              `json:"client_algo_id"`
	ObservedStatus       string              `json:"observed_status"`
	ObservedAt           time.Time           `json:"observed_at"`
	ActualOrderID        string              `json:"actual_order_id,omitempty"`
	ActualQty            string              `json:"actual_qty,omitempty"`
	ReplayAllowed        bool                `json:"replay_allowed"`     // ALWAYS false
	TradingAuthorized    bool                `json:"trading_authorized"` // ALWAYS false
	ManualReviewRequired bool                `json:"manual_review_required"`
	Reason               string              `json:"reason"`
}

type cancelOutcomeMarker struct {
	Schema        int    `json:"schema"`
	IntentDigest  string `json:"intent_digest"`
	GroupID       string `json:"group_id"`
	SiblingAlgoID int64  `json:"sibling_algo_id"`
	ClientAlgoID  string `json:"client_algo_id"`
	Status        string `json:"status"`
	ActualOrderID string `json:"actual_order_id,omitempty"`
	ActualQty     string `json:"actual_qty,omitempty"`
	ObservedAt    string `json:"observed_at"`
	Checksum      string `json:"checksum"`
}

func (m cancelOutcomeMarker) digest() string {
	m.Checksum = ""
	raw, _ := json.Marshal(m)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *CancelIntentStore) validatePersistedCancelOutcomeLocked(c CancelIntent) error {
	path := filepath.Join(s.dir, c.GroupID+".outcome.json")
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var m cancelOutcomeMarker
	if json.Unmarshal(raw, &m) != nil || m.Schema != 1 || m.Checksum == "" || m.Checksum != m.digest() ||
		m.IntentDigest != c.Digest || m.GroupID != c.GroupID || m.SiblingAlgoID != c.SiblingAlgoID || m.ClientAlgoID != c.OwnerClientAlgoID || m.ObservedAt == "" {
		return errors.New("취소 결과 기록 손상 또는 취소 인텐트 불일치")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.ObservedAt); err != nil {
		return errors.New("취소 결과 관측 시각 손상")
	}
	switch m.Status {
	case "CANCELED", "CANCELLED", "EXPIRED", "TRIGGERED", "FINISHED":
		return nil
	default:
		return errors.New("취소 결과에 비종료 상태가 기록되어 있습니다")
	}
}

func validateCancelObservation(c CancelIntent, o exchange.DemoAlgoResult) error {
	if c.Schema != 1 || c.Digest == "" || c.Digest != c.seal() {
		return errors.New("취소 인텐트 손상")
	}
	if o.AlgoID != c.SiblingAlgoID || o.ClientAlgoID != c.OwnerClientAlgoID || o.Symbol != "BTCUSDT" {
		return errors.New("서명 조회가 원래 형제 보호 주문과 일치하지 않습니다")
	}
	if o.PositionSide != "LONG" && o.PositionSide != "SHORT" {
		return errors.New("형제 보호 주문 positionSide 오류")
	}
	if o.Side != "BUY" && o.Side != "SELL" {
		return errors.New("형제 보호 주문 side 오류")
	}
	if o.AlgoType != "CONDITIONAL" || o.ClosePosition || o.ReduceOnly {
		return errors.New("형제 보호 주문 종류 또는 플래그 불일치")
	}
	if _, err := pos(string(o.Quantity)); err != nil {
		return errors.New("형제 보호 주문 수량 오류")
	}
	return nil
}

// RecoverAttemptedCancel performs signed GET only. It is valid only after the
// durable MAY_HAVE_SENT marker exists; therefore an ambiguous cancellation is
// never replayed automatically even when the current order still looks active.
func (s *CancelIntentStore) RecoverAttemptedCancel(ctx context.Context, reader SignedProtectionReader, key, secret, group string) (CancelRecoveryResult, error) {
	result := CancelRecoveryResult{GroupID: group, ReplayAllowed: false, TradingAuthorized: false, ManualReviewRequired: true}
	if reader == nil || key == "" || secret == "" {
		result.State = CancelRecoveryHalt
		result.Reason = "서명 조회 어댑터 또는 인증 정보 없음"
		return result, errors.New(result.Reason)
	}
	c, attempted, err := s.Inspect(group)
	if err != nil {
		result.State = CancelRecoveryHalt
		result.Reason = "취소 인텐트 또는 디스패치 기록 검증 실패"
		return result, err
	}
	result.SiblingAlgoID = c.SiblingAlgoID
	result.ClientAlgoID = c.OwnerClientAlgoID
	if !attempted {
		result.State = CancelRecoveryHalt
		result.Reason = "MAY_HAVE_SENT 이전 취소 인텐트는 재시작 복구 대상이 아닙니다"
		return result, errors.New(result.Reason)
	}

	o, err := reader.QueryDemoAlgo(ctx, key, secret, c.SiblingAlgoID)
	if err != nil {
		result.State = CancelRecoveryHalt
		result.Reason = "서명 조회 실패: 취소 재시도 금지, 수동 대조 필요"
		return result, err
	}
	if err := validateCancelObservation(c, o); err != nil {
		result.State = CancelRecoveryHalt
		result.Reason = err.Error()
		return result, err
	}

	result.ObservedStatus = o.Status
	result.ObservedAt = time.Now().UTC()
	result.ActualOrderID = o.ActualOrderID
	result.ActualQty = string(o.ActualQty)
	switch o.Status {
	case "CANCELED", "CANCELLED", "EXPIRED":
		result.State = CancelRecoveryCanceled
		result.Reason = "서명 GET으로 형제 보호 주문 종료를 확인했습니다. 소유 수량 해제 전 전체 포지션/체결 재대조가 필요합니다"
	case "TRIGGERED", "FINISHED":
		result.State = CancelRecoveryTriggered
		result.Reason = "형제 주문도 발동했을 수 있습니다. 취소로 간주하지 말고 실제 체결과 pooled-side 포지션을 전수 대조해야 합니다"
	case "NEW", "WORKING", "TRIGGERING":
		result.State = CancelRecoveryPendingActive
		result.Reason = "형제 주문이 아직 활성 상태입니다. 과거 취소 결과가 불명확하므로 자동 DELETE 재시도는 금지됩니다"
		return result, nil
	default:
		result.State = CancelRecoveryHalt
		result.Reason = "알 수 없는 형제 보호 주문 상태"
		return result, errors.New(result.Reason)
	}

	marker := cancelOutcomeMarker{
		Schema: 1, IntentDigest: c.Digest, GroupID: c.GroupID,
		SiblingAlgoID: c.SiblingAlgoID, ClientAlgoID: c.OwnerClientAlgoID,
		Status: o.Status, ActualOrderID: o.ActualOrderID, ActualQty: string(o.ActualQty),
		ObservedAt: result.ObservedAt.Format(time.RFC3339Nano),
	}
	marker.Checksum = marker.digest()
	raw, _ := json.MarshalIndent(marker, "", "  ")
	path := filepath.Join(s.dir, c.GroupID+".outcome.json")
	if existing, readErr := os.ReadFile(path); readErr == nil {
		var prior cancelOutcomeMarker
		if json.Unmarshal(existing, &prior) != nil || prior.Checksum == "" || prior.Checksum != prior.digest() ||
			prior.IntentDigest != marker.IntentDigest || prior.GroupID != marker.GroupID || prior.SiblingAlgoID != marker.SiblingAlgoID || prior.ClientAlgoID != marker.ClientAlgoID {
			result.State = CancelRecoveryHalt
			result.Reason = "기존 취소 결과 기록 손상 또는 대상 불일치"
			return result, errors.New(result.Reason)
		}
		// A later signed observation may legitimately move from active to a
		// terminal state, but once a terminal outcome is archived we never
		// overwrite it. Conflicting terminal evidence is fail-closed.
		if prior.Status != marker.Status || prior.ActualOrderID != marker.ActualOrderID || prior.ActualQty != marker.ActualQty {
			result.State = CancelRecoveryHalt
			result.Reason = "기존 terminal 취소 결과와 새 signed GET 결과가 다릅니다"
			return result, errors.New(result.Reason)
		}
		return result, nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		result.State = CancelRecoveryHalt
		result.Reason = "취소 결과 기록 읽기 실패"
		return result, readErr
	}
	if err := exclusiveDurable(path, raw); err != nil {
		result.State = CancelRecoveryHalt
		result.Reason = "취소 결과 기록 실패: 수동 대조 필요"
		return result, err
	}
	return result, nil
}
