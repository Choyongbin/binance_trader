package trading

// A deliberately supervised, GET-only bridge between the durable entry
// lifecycle and the existing protection preview/checkpoint machinery.
//
// The only mutations made here are local, append-only signed-fill recovery and
// a deterministic, non-executable protection plan. This function NEVER
// submits/cancels a Binance order, creates a trading authorization, or settles
// an execution. The locked product runtime may call it for a local plan only.
import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
)

// EntryProtectionSignedPreflightReader intentionally exposes ONLY signed GET.
// Unlike ProtectionArmingTransport, it is impossible to submit an Algo order
// through this interface. Tests deliberately pass an adapter with POST support
// to verify no POST is ever used by the bridge.
type EntryProtectionSignedPreflightReader interface {
	FetchDemoExecutionPreflight(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error)
}

const (
	EntryProtectionRecoveryPending = "ENTRY_RECOVERY_PENDING"
	EntryProtectionPreparedReview  = "ENTRY_RECOVERED_PROTECTION_PREPARED_REVIEW"
	EntryProtectionRecoveryBlocked = "ENTRY_PROTECTION_BLOCKED"
)

// RecoveredEntryProtectionReview is evidence for an OPERATOR, never an
// execution/approval ticket. In particular, a prepared plan does not attest
// that two independent Binance Algo orders behave like an atomic OCO pair.
type RecoveredEntryProtectionReview struct {
	State                      string                    `json:"state"`
	ClientOrderID              string                    `json:"client_order_id"`
	GroupID                    string                    `json:"group_id"`
	Recovery                   ExecutionRecoveryResult   `json:"recovery"`
	Owner                      OwnerPosition             `json:"owner,omitempty"`
	Plan                       ProtectionDispatchPlan    `json:"plan,omitempty"`
	Continuity                 EntryProtectionContinuity `json:"continuity"`
	PendingReason              string                    `json:"pending_reason,omitempty"`
	TradingAuthorized          bool                      `json:"trading_authorized"`           // always false
	ProtectionSubmitAuthorized bool                      `json:"protection_submit_authorized"` // always false
	SettlementAuthorized       bool                      `json:"settlement_authorized"`        // always false
	ManualReviewRequired       bool                      `json:"manual_review_required"`       // always true
}

// entryProtectionGroupID binds the protection clientAlgoIDs to the original
// durable entry clientOrderID, not an operator-generated random group. A
// second invocation cannot generate a new set of live-executable legs.
func entryProtectionGroupID(r Reservation) string {
	return "entry_" + r.ClientOrderID
}

// RecoverAndPrepareEntryProtectionSupervised performs:
//  1. Signed-only entry fill recovery, without exchange POST or settlement.
//  2. Full current signed journal/position ownership reconciliation.
//  3. Exact, single-owner hedge-side and FILLED-quantity enforcement.
//  4. Deterministic TP/SL preview and idempotent local plan preparation.
//  5. Independent signed account read-back and continuity inspection.
//
// RunOnce may call this through its product planner after signed fill recovery.
// A different, explicitly supervised function exists for sending TP/SL, and
// its separate operator authorization/incident recovery is still required.
// Passing a reader and transport does NOT allow this function to send orders.
func RecoverAndPrepareEntryProtectionSupervised(ctx context.Context,
	r Reservation, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	ownership *OwnershipStore, protection *ProtectionSubmissionStore,
	reader DemoTradeReader, signed EntryProtectionSignedPreflightReader, creds DemoCredentials,
	rules exchange.SymbolRules, contract, mark Quote,
) (RecoveredEntryProtectionReview, error) {
	out := RecoveredEntryProtectionReview{
		State: EntryProtectionRecoveryBlocked, ClientOrderID: r.ClientOrderID,
		GroupID: entryProtectionGroupID(r), ManualReviewRequired: true,
	}
	if ctx == nil {
		return out, errors.New("entry protection recovery context 없음")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if coordinator == nil || lifecycle == nil || ownership == nil || protection == nil || reader == nil || signed == nil || creds.APIKey == "" || creds.Secret == "" {
		return out, errors.New("entry protection recovery 필수 구성요소/서명 인증 누락")
	}
	if r.Intent.Action != Enter || r.Intent.Symbol != "BTCUSDT" {
		return out, errors.New("entry protection recovery는 BTCUSDT 진입만 허용합니다")
	}
	// These stores must represent the SAME original ledger; never join a valid
	// lifecycle from one account with a protection plan from another directory.
	if filepath.Clean(lifecycle.root) != filepath.Clean(coordinator.root) ||
		filepath.Clean(ownership.journalDir) != filepath.Join(coordinator.root, "requests") ||
		filepath.Clean(ownership.fillDir) != filepath.Join(coordinator.root, "fills") ||
		filepath.Clean(protection.dir) != filepath.Join(coordinator.root, "protection_submissions") {
		return out, errors.New("entry protection recovery 저장소 데이터 경로 불일치")
	}
	if err := coordinator.ownsLock(); err != nil {
		return out, fmt.Errorf("entry protection recovery coordinator lock 실패: %w", err)
	}
	// No exchange side effects: the reader interface cannot submit anything.
	recovery, err := lifecycle.RecoverOwnedEntryFillsOnly(ctx, r, coordinator, ownership, reader, creds)
	out.Recovery = recovery
	if err != nil {
		return out, fmt.Errorf("signed entry fill recovery 실패 (재전송 금지): %w", err)
	}
	if recovery.FinalStage != ExecutionFillsImported {
		out.State = EntryProtectionRecoveryPending
		out.PendingReason = recovery.PendingReason
		return out, nil
	}
	current, err := lifecycle.Load(r) // re-open/check the entire durable hash chain
	if err != nil || current.Stage != ExecutionFillsImported || validateHandoffLifecycleChain(current) != nil {
		return out, errors.New("recovered lifecycle 전체 체인 검증 실패")
	}
	last := current.Events[len(current.Events)-1]
	filled, fillErr := positiveRat(last.ExecutedQty)
	planned, planErr := positiveRat(r.Intent.Quantity)
	if last.ExchangeStatus != "FILLED" || last.ImportedFillCount <= 0 || fillErr != nil || planErr != nil || filled.Cmp(planned) != 0 {
		return out, errors.New("부분체결/취소/거절 또는 주문 수량 불일치: 보호주문을 준비하지 않습니다")
	}
	// Query every local order via authenticated GET; a locally restored fill is
	// not proof that the current pooled hedge side has the expected balance.
	orders, err := ownership.CollectDemoOrderObservations(ctx, reader, creds)
	if err != nil {
		return out, fmt.Errorf("signed order history 조회 실패: %w", err)
	}
	pre, err := signed.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, fmt.Errorf("signed account preflight 조회 실패: %w", err)
	}
	account, err := snapshotFromExecutionPreflight(pre)
	if err != nil {
		return out, err
	}
	audit, err := ownership.Audit(account, orders, time.Now().UTC())
	if err != nil || !audit.ReadyForAssessment {
		return out, fmt.Errorf("현재 signed 전체 계정/체결 소유권 감사 차단: %v, %v", err, audit.Blockers)
	}
	var owner OwnerPosition
	ownedOnSide := 0
	for _, p := range audit.Owned {
		if p.Symbol != r.Intent.Symbol || p.Side != r.Intent.PositionSide {
			continue
		}
		q, e := decimalRat(p.Quantity, true)
		if e != nil {
			return out, fmt.Errorf("signed 소유자 수량 오류: %w", e)
		}
		if q.Sign() == 0 {
			continue
		}
		ownedOnSide++
		if p.Source == r.Intent.Source && p.ModelID == r.Intent.ModelID {
			owner = p
		}
	}
	// Binance pools all models on a hedge side. An owner-sized TP and SL can
	// overclose a second owner's exposure; require EXCLUSIVE side ownership.
	if ownedOnSide != 1 || owner.Symbol != "BTCUSDT" {
		return out, errors.New("동일 Hedge 방향에 단독 소유자 1명만 있어야 보호 계획을 준비할 수 있습니다")
	}
	ownedQty, err := positiveRat(owner.Quantity)
	if err != nil || ownedQty.Cmp(planned) != 0 {
		return out, errors.New("현재 소유권 잔량과 단일 진입의 서명 체결량이 불일치합니다")
	}
	if err := validateProtectionPreflight(pre, owner, map[int64]ProtectionInstruction{}); err != nil {
		return out, fmt.Errorf("보호주문 preflight 불일치: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	preview, err := PreviewProtection(owner, rules, contract, mark, out.GroupID, time.Now().UTC())
	if err != nil {
		return out, fmt.Errorf("보호주문 가격/거래소 규칙 검사 실패: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	plan, err := protection.Prepare(out.GroupID, owner, preview) // ONLY local plan, never POST
	if err != nil {
		return out, fmt.Errorf("보호주문 계획 준비 실패: %w", err)
	}
	out.Owner = owner
	out.Plan = plan
	// Inspect a fresh signed snapshot AFTER the local plan is persisted. A
	// competing external order or new position must invalidate this review.
	fresh, err := signed.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return out, fmt.Errorf("보호주문 준비 후 signed account 재조회 실패: %w", err)
	}
	continuity, err := InspectEntryProtectionContinuityGroup(protection, out.GroupID, r, current, owner, fresh)
	out.Continuity = continuity
	if err != nil {
		return out, err
	}
	if continuity.State != "ENTRY_IMPORTED_PROTECTION_UNSENT_REVIEW" ||
		continuity.CheckpointState != "PREPARED_NEVER_DISPATCHED" || !continuity.SignedSnapshotMatched || len(continuity.Blockers) != 0 {
		return out, fmt.Errorf("보호주문 미전송 연속성 검토 차단: %s %v", continuity.State, continuity.Blockers)
	}
	out.State = EntryProtectionPreparedReview
	return out, nil
}

// Explicitly decline attempts to treat the bridge's report as trading or
// protection-submission authority, including successful PREPARED reviews.
func (RecoveredEntryProtectionReview) RequireProtectionSubmission() error {
	return ErrProtectionApproval
}
