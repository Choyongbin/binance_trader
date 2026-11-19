package trading

import (
	"errors"
	"fmt"

	"binance_trader/internal/exchange"
)

// EntryProtectionContinuity is a read-only joint assessment of an imported
// entry, its protection checkpoints, and a fresh signed exchange snapshot.
// It NEVER grants a submission, retry, cancel, settlement, or trading ticket.
// In particular, two observed legs do not establish an atomic OCO pair.
type EntryProtectionContinuity struct {
	ClientOrderID              string   `json:"client_order_id"`
	GroupID                    string   `json:"group_id"`
	State                      string   `json:"state"`
	CheckpointState            string   `json:"checkpoint_state"`
	Blockers                   []string `json:"blockers"`
	SignedSnapshotMatched      bool     `json:"signed_snapshot_matched"`
	TradingAuthorized          bool     `json:"trading_authorized"`
	ProtectionSubmitAuthorized bool     `json:"protection_submit_authorized"`
	RetryAuthorized            bool     `json:"retry_authorized"`
	SettlementAuthorized       bool     `json:"settlement_authorized"`
	ManualReviewRequired       bool     `json:"manual_review_required"`
}

// InspectEntryProtectionContinuityGroup is the explicit-group version; it only
// reports whether the previously observed legs are still an exact match.
func InspectEntryProtectionContinuityGroup(store *ProtectionSubmissionStore, group string, reservation Reservation,
	lifecycle ExecutionLifecycle, owner OwnerPosition, signed exchange.ExecutionPreflightSnapshot) (EntryProtectionContinuity, error) {
	out := EntryProtectionContinuity{ClientOrderID: reservation.ClientOrderID, GroupID: group, State: "BLOCKED", Blockers: []string{}, ManualReviewRequired: true}
	if store == nil {
		return out, errors.New("protection submission store 없음")
	}
	plan, err := store.read(group)
	if err != nil {
		return out, err
	}
	checkpoint, err := InspectProtectionArmingCheckpoint(store, group)
	if err != nil {
		return out, err
	}
	out.CheckpointState = checkpoint.State
	critical := false
	block := func(msg string) { out.Blockers = append(out.Blockers, msg); critical = true }
	warn := func(msg string) { out.Blockers = append(out.Blockers, msg) }
	entryPlan, entryPlanErr := reservation.Intent.Plan()
	intentDigest, digestErr := digestIntent(reservation.Intent)
	if reservation.Schema != 1 || reservation.ClientOrderID == "" ||
		reservation.Checksum == "" || reservation.Checksum != reservation.digest() ||
		reservation.Intent.Action != Enter || reservation.Intent.Validate() != nil ||
		entryPlanErr != nil || entryPlan.ClientOrderID != reservation.ClientOrderID ||
		digestErr != nil || intentDigest != reservation.IntentDigest ||
		lifecycle.Root.ClientOrderID != reservation.ClientOrderID ||
		lifecycle.Root.ReservationChecksum != reservation.Checksum ||
		lifecycle.Root.IntentDigest != reservation.IntentDigest ||
		lifecycle.Root.Plan != entryPlan ||
		lifecycle.Stage != ExecutionFillsImported || validateHandoffLifecycleChain(lifecycle) != nil {
		block("진입 예약·실행 계획 또는 전체 Lifecycle 검증 실패")
	}
	if len(lifecycle.Events) == 0 {
		block("진입 체결 이벤트 누락")
	} else {
		last := lifecycle.Events[len(lifecycle.Events)-1]
		if last.Stage != ExecutionFillsImported || last.ImportedFillCount <= 0 || last.ImportedFillDigest == "" || last.ExchangeStatus != "FILLED" || last.ExchangeOrderID <= 0 {
			block("완전한 진입 체결 기록 없음")
		} else {
			// A valid hash chain proves only that the events were preserved;
			// it does not prove that the imported quantity matches this owner.
			// Compare exact decimals, never floating-point quantities.
			filled, fillErr := positiveRat(last.ExecutedQty)
			owned, ownerErr := positiveRat(owner.Quantity)
			requested, requestedErr := positiveRat(reservation.Intent.Quantity)
			if fillErr != nil || ownerErr != nil || requestedErr != nil ||
				filled.Cmp(owned) != 0 || filled.Cmp(requested) != 0 {
				block("서명 진입 체결 수량·예약 수량·소유자 수량 불일치")
			}
		}
	}
	if plan.validate() != nil || plan.GroupID != group || reservation.Intent.Symbol != "BTCUSDT" ||
		lifecycle.Root.IntentDigest != reservation.IntentDigest ||
		lifecycle.Root.Plan.ClientOrderID != reservation.ClientOrderID ||
		lifecycle.Root.Plan.Quantity != reservation.Intent.Quantity ||
		owner.Symbol != "BTCUSDT" ||
		owner.Side != reservation.Intent.PositionSide || owner.Source != reservation.Intent.Source ||
		owner.ModelID != reservation.Intent.ModelID || owner.Side != plan.PositionSide ||
		owner.Source != plan.Source || owner.ModelID != plan.ModelID ||
		owner.Quantity != plan.TP.Quantity || owner.Quantity != plan.SL.Quantity {
		block("보호 계획과 진입 소유권 일치 실패")
	}
	if checkpoint.State == "BLOCKED" {
		block("보호주문 체크포인트 무결성 확인 실패")
	}
	for _, reason := range checkpoint.Blockers {
		warn(reason)
	}
	// Each observed leg's exchange ID must be independently confirmed in a
	// CURRENT signed view. In-flight legs are never inferred to be absent.
	expected := map[int64]ProtectionInstruction{}
	if checkpoint.TP.SignedObserved {
		if checkpoint.TP.ObservedAlgoID <= 0 {
			block("TP Algo ID 누락")
		} else {
			expected[checkpoint.TP.ObservedAlgoID] = plan.TP
		}
	}
	if checkpoint.SL.SignedObserved {
		if checkpoint.SL.ObservedAlgoID <= 0 || checkpoint.SL.ObservedAlgoID == checkpoint.TP.ObservedAlgoID {
			block("SL Algo ID 누락 또는 충돌")
		} else {
			expected[checkpoint.SL.ObservedAlgoID] = plan.SL
		}
	}
	if err := validateProtectionPreflight(signed, owner, expected); err != nil {
		block(fmt.Sprintf("최신 signed 계정·보호주문 일치 실패: %v", err))
	} else {
		out.SignedSnapshotMatched = true
	}
	switch checkpoint.State {
	case "PREPARED_NEVER_DISPATCHED":
		out.State = "ENTRY_IMPORTED_PROTECTION_UNSENT_REVIEW"
	case "TP_MAY_HAVE_SENT_UNRESOLVED", "SL_MAY_HAVE_SENT_UNRESOLVED":
		out.State = "AMBIGUOUS_SUBMISSION_NO_RETRY"
	case "TP_OBSERVED_SL_NOT_DISPATCHED":
		out.State = "ONE_LEG_OBSERVED_MANUAL_REVIEW"
	case "TWO_LEGS_PREVIOUSLY_OBSERVED_RECHECK_REQUIRED":
		out.State = "TWO_LEGS_SIGNED_RECHECK_MANUAL_REVIEW"
	default:
		out.State = "BLOCKED"
	}
	// Retain the incident category for manual triage; an unverified signed
	// view or malformed entry always becomes an explicit BLOCKED verdict.
	if critical || !out.SignedSnapshotMatched || lifecycle.Stage != ExecutionFillsImported ||
		validateHandoffLifecycleChain(lifecycle) != nil || checkpoint.State == "BLOCKED" {
		out.State = "BLOCKED"
	}
	// Even a completely matching read-back is NOT a ticket to submit/cancel.
	// Signed data is valid only for its bounded time window and the two legs
	// do not form an exchange-atomic OCO transaction.
	return out, nil
}
