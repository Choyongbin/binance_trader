package trading

// This shape-only review connects an imported entry lifecycle to a proposed owner-sized
// protection pair without granting permission to submit either Algo order.
// In particular, a signed position snapshot does NOT prove that the whole
// account's fills, funding, or pooled-side ownership have been reconciled.
import (
	"errors"
	"fmt"
	"time"

	"binance_trader/internal/exchange"
)

type EntryProtectionHandoffReview struct {
	State                      string         `json:"state"` // BLOCKED / LOCAL_HANDOFF_SHAPE_REVIEW_NOT_AUTHORIZED
	ClientOrderID              string         `json:"client_order_id"`
	GroupID                    string         `json:"group_id"`
	LifecycleStage             ExecutionStage `json:"lifecycle_stage"`
	ImportedFillCount          int            `json:"imported_fill_count"`
	Blockers                   []string       `json:"blockers"`
	SignedPreflightChecked     bool           `json:"signed_preflight_checked"`
	TradingAuthorized          bool           `json:"trading_authorized"`           // ALWAYS false
	ProtectionSubmitAuthorized bool           `json:"protection_submit_authorized"` // ALWAYS false
	SettlementAuthorized       bool           `json:"settlement_authorized"`        // ALWAYS false
	ManualReviewRequired       bool           `json:"manual_review_required"`       // ALWAYS true
}

// validateHandoffLifecycleChain applies the same append-only stage order and
// digest checks as the on-disk lifecycle reader. A last-event-only check can
// incorrectly treat a fabricated FILLS_IMPORTED event as complete evidence.
func validateHandoffLifecycleChain(l ExecutionLifecycle) error {
	root := l.Root
	if root.Schema != 1 || root.Stage != ExecutionReserved || root.Checksum == "" || root.Checksum != root.digest() {
		return errors.New("lifecycle root checksum/stage mismatch")
	}
	previousStage := ExecutionReserved
	previousChecksum := root.Checksum
	previousAt, err := time.Parse(time.RFC3339Nano, root.CreatedAt)
	if err != nil {
		return fmt.Errorf("lifecycle root timestamp: %w", err)
	}
	for index, e := range l.Events {
		next, ok := nextExecutionStage(previousStage)
		if !ok || e.Schema != 1 || e.ClientOrderID != root.ClientOrderID ||
			e.Sequence != index+1 || e.Stage != next || e.PreviousChecksum != previousChecksum ||
			e.Checksum == "" || e.Checksum != e.digest() {
			return fmt.Errorf("lifecycle event %d chain/order/checksum mismatch", index+1)
		}
		if err := validateExecutionEventShape(e); err != nil {
			return fmt.Errorf("lifecycle event %d shape: %w", index+1, err)
		}
		at, err := time.Parse(time.RFC3339Nano, e.At)
		if err != nil || at.Before(previousAt) {
			return fmt.Errorf("lifecycle event %d timestamp/order invalid", index+1)
		}
		previousAt, previousStage, previousChecksum = at, e.Stage, e.Checksum
	}
	if l.Stage != previousStage {
		return errors.New("lifecycle derived stage mismatch")
	}
	return nil
}

// ReviewEntryProtectionHandoff is pure and performs no network, disk or order
// mutations. The caller must supply authenticated, locally verified evidence;
// this function must not be exposed as a trading authorization gate.
func ReviewEntryProtectionHandoff(r Reservation, lifecycle ExecutionLifecycle, owner OwnerPosition,
	plan ProtectionDispatchPlan, signed exchange.ExecutionPreflightSnapshot, now time.Time) EntryProtectionHandoffReview {
	out := EntryProtectionHandoffReview{State: "BLOCKED", ClientOrderID: r.ClientOrderID,
		GroupID: plan.GroupID, LifecycleStage: lifecycle.Stage, Blockers: []string{}, ManualReviewRequired: true}
	block := func(msg string) { out.Blockers = append(out.Blockers, msg) }

	planned, planErr := r.Intent.Plan()
	if r.Schema != 1 || r.Checksum == "" || r.Checksum != r.digest() || r.Intent.Validate() != nil ||
		r.Intent.Action != Enter || r.ClientOrderID == "" || planErr != nil || planned.ClientOrderID != r.ClientOrderID ||
		lifecycle.Root.ClientOrderID != r.ClientOrderID || lifecycle.Root.ReservationChecksum != r.Checksum ||
		lifecycle.Root.IntentDigest != r.IntentDigest || lifecycle.Root.Plan != planned || lifecycle.Root.Checksum != lifecycle.Root.digest() {
		block("진입 예약·Lifecycle 원본 연결 또는 체크섬 불일치")
	}
	if err := validateHandoffLifecycleChain(lifecycle); err != nil {
		block("전체 Lifecycle 해시 체인·상태 전이 불일치: " + err.Error())
	}
	if lifecycle.Stage != ExecutionFillsImported || len(lifecycle.Events) == 0 {
		block("전체 진입 체결의 FILLS_IMPORTED 증거가 없습니다")
	} else {
		last := lifecycle.Events[len(lifecycle.Events)-1]
		if last.Stage != ExecutionFillsImported || last.Checksum != last.digest() || last.ImportedFillCount <= 0 ||
			last.ImportedFillDigest == "" || last.ExchangeOrderID <= 0 || last.ExchangeStatus != "FILLED" {
			block("최종 진입 체결 이벤트의 ID·상태·체결 해시가 불완전합니다")
		} else {
			out.ImportedFillCount = last.ImportedFillCount
			qty, err := positiveRat(last.ExecutedQty)
			if err != nil || qty.Sign() <= 0 {
				block("서명 확인된 진입 체결 수량이 유효하지 않습니다")
			} else if owned, err := positiveRat(owner.Quantity); err != nil || qty.Cmp(owned) != 0 {
				block("진입 체결량과 단일 소유자 잔량이 불일치합니다")
			}
		}
	}
	if plan.validate() != nil || owner.Symbol != "BTCUSDT" || owner.Source != r.Intent.Source ||
		owner.ModelID != r.Intent.ModelID || owner.Side != r.Intent.PositionSide ||
		plan.Source != owner.Source || plan.ModelID != owner.ModelID || plan.PositionSide != owner.Side {
		block("보호 계획·진입 예약·포지션 소유자 연결 불일치")
	} else {
		expected, err := positiveRat(plan.TP.Quantity)
		owned, ownErr := positiveRat(owner.Quantity)
		if err != nil || ownErr != nil || expected.Cmp(owned) != 0 {
			block("보호 그룹 수량과 소유자 수량 불일치")
		}
	}
	if signed.Environment != "demo" || signed.OpenOrders == nil || signed.OpenAlgos == nil ||
		signed.FinishedAt.IsZero() || signed.FinishedAt.After(now.Add(time.Second)) || now.Sub(signed.FinishedAt) > 5*time.Second ||
		signed.StartedAt.IsZero() || signed.StartedAt.After(signed.FinishedAt) || signed.FinishedAt.Sub(signed.StartedAt) > 30*time.Second {
		block("신선하고 완전한 Demo signed account bracket이 필요합니다")
	} else {
		out.SignedPreflightChecked = true
		// Exactly zero open conditional orders is required for a *proposed*
		// first pair; any existing Algo leg requires its own read-back/recovery.
		if err := validateProtectionPreflight(signed, owner, map[int64]ProtectionInstruction{}); err != nil {
			block("계정 전체 보호주문 preflight 불일치: " + err.Error())
		}
	}
	if len(out.Blockers) == 0 {
		out.State = "LOCAL_HANDOFF_SHAPE_REVIEW_NOT_AUTHORIZED"
		block("실제 TP/SL 제출 전 전체 체결·소유권·기존 보호주문 감사 및 명시적 승인 필요; 두 레그는 OCO가 아닙니다")
	}
	return out
}

// Enforce that review flags cannot be mistaken for an authenticated execution
// ticket. Keeping this separate makes callers fail closed on future changes.
var ErrEntryProtectionHandoffNotAuthorized = errors.New("진입-보호주문 인계 검토는 실제 Algo 주문 권한이 아닙니다")

func (r EntryProtectionHandoffReview) RequireTradingAuthorization() error {
	return ErrEntryProtectionHandoffNotAuthorized
}
