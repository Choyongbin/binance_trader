package trading

// Restart-safe recovery for a durable execution lifecycle.
//
// This code intentionally accepts only read-only signed exchange operations.
// It can never submit/replay an order. MAY_HAVE_SENT therefore remains a
// durable ambiguity fence until the deterministic clientOrderID is observed
// through signed REST and all later ownership/account evidence is reconciled.
import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
)

// ExecutionRecoveryReader is deliberately read-only. exchange.Client satisfies
// this interface, while an executor/order submitter does not need to be passed
// into recovery at all.
type ExecutionRecoveryReader interface {
	DemoTradeReader
	Fetch(context.Context, string, string) (exchange.Snapshot, error)
}

type ExecutionRecoveryResult struct {
	ClientOrderID     string         `json:"client_order_id"`
	InitialStage      ExecutionStage `json:"initial_stage"`
	FinalStage        ExecutionStage `json:"final_stage"`
	Progressed        bool           `json:"progressed"`
	ImportedFillCount int            `json:"imported_fill_count"`
	PendingReason     string         `json:"pending_reason,omitempty"`
	TradingAuthorized bool           `json:"trading_authorized"` // always false
}

func recoveryPending(result ExecutionRecoveryResult, stage ExecutionStage, reason string) (ExecutionRecoveryResult, error) {
	result.FinalStage = stage
	result.PendingReason = reason
	return result, nil
}

func (s *ExecutionLifecycleStore) recoveryRecord(r Reservation) (Record, error) {
	if err := s.validateReservation(r); err != nil {
		return Record{}, err
	}
	rec, err := readRecord(filepath.Join(s.requests, r.ClientOrderID+".json"))
	if err != nil {
		return Record{}, err
	}
	plan, planErr := r.Intent.Plan()
	digest, digestErr := digestIntent(r.Intent)
	if planErr != nil || digestErr != nil || rec.Plan != plan || rec.IntentDigest != digest || rec.Plan.ClientOrderID != r.ClientOrderID {
		return Record{}, errors.New("복구 대상 주문 저널과 예약이 일치하지 않습니다")
	}
	return rec, nil
}

func (s *ExecutionLifecycleStore) collectRecoveryAccountEvidence(ctx context.Context, store *OwnershipStore, reader ExecutionRecoveryReader, creds DemoCredentials) (map[string]OrderObservation, exchange.Snapshot, error) {
	orders, err := store.CollectDemoOrderObservations(ctx, reader, creds)
	if err != nil {
		return nil, exchange.Snapshot{}, fmt.Errorf("전체 주문 서명 조회 실패: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, exchange.Snapshot{}, err
	}
	snapshot, err := reader.Fetch(ctx, creds.APIKey, creds.Secret)
	if err != nil {
		return nil, exchange.Snapshot{}, fmt.Errorf("최신 Demo 계정 스냅샷 조회 실패: %w", err)
	}
	return orders, snapshot, nil
}

// RecoverExecution resumes one already-created lifecycle without any exchange
// side effect. It is safe to call again after a crash: every local transition
// is append-only/idempotent and no path has access to an order submit method.
//
// A non-terminal remote order is a normal pending result, not an error. The
// caller may invoke recovery again later. Unknown/missing/contradictory signed
// evidence is an error and remains fail-closed.
// RecoverExecution performs FULL signed recovery, including whole-side account
// reconciliation and settlement. An external/manual close can make this fail
// closed even after the entry fills have been safely imported.
func (s *ExecutionLifecycleStore) RecoverExecution(ctx context.Context, r Reservation, coordinator *Coordinator, store *OwnershipStore, reader ExecutionRecoveryReader, creds DemoCredentials) (ExecutionRecoveryResult, error) {
	return s.recoverWithScope(ctx, r, coordinator, store, reader, reader, creds)
}

// RecoverOwnedEntryFillsOnly stops at FILLS_IMPORTED, WITHOUT attempting to
// reconcile the whole hedge side, consume a reservation, or mark settlement.
// It exists for an already-submitted OWNED ENTRY whose exchange fill exists
// but whose lifecycle was interrupted (for example an externally closed SHORT).
// This is NOT an offline archive replay: every fill is fetched again through
// signed GET, cross-checked with the original local intent, written as an
// immutable O_EXCL journal, and rechecked through a new signed order read.
// It has LOCAL financial side effects and MUST NOT be automatically invoked
// from the HTTP server or an unapproved CLI. No exchange POST is possible.
// After success, whole-side account reconciliation and external-close
// ownership/funding settlement remain blocked until independently verified.
func (s *ExecutionLifecycleStore) RecoverOwnedEntryFillsOnly(ctx context.Context, r Reservation, coordinator *Coordinator, store *OwnershipStore, reader DemoTradeReader, creds DemoCredentials) (ExecutionRecoveryResult, error) {
	if ctx == nil {
		return ExecutionRecoveryResult{ClientOrderID: r.ClientOrderID, TradingAuthorized: false}, errors.New("entry-only recovery requires a context")
	}
	if r.Intent.Action != Enter {
		return ExecutionRecoveryResult{ClientOrderID: r.ClientOrderID, TradingAuthorized: false}, errors.New("entry-only recovery refuses non-entry orders")
	}
	return s.recoverWithScope(ctx, r, coordinator, store, reader, nil, creds)
}

// accountReader is nil in entry-only mode: no account/whole-side reconciliation
// or final settlement can be reached. A non-nil reader retains the original
// full recovery behavior. Both paths share the same signed fill-import code.
func (s *ExecutionLifecycleStore) recoverWithScope(ctx context.Context, r Reservation, coordinator *Coordinator, store *OwnershipStore, reader DemoTradeReader, accountReader ExecutionRecoveryReader, creds DemoCredentials) (ExecutionRecoveryResult, error) {
	result := ExecutionRecoveryResult{ClientOrderID: r.ClientOrderID, TradingAuthorized: false}
	if s == nil || coordinator == nil || store == nil || reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return result, errors.New("복구에는 동일 데이터 경로의 lifecycle/coordinator/체결 장부와 Demo 서명 조회가 필요합니다")
	}
	if filepath.Clean(coordinator.reserveDir) != s.reserve || filepath.Clean(store.journalDir) != s.requests || filepath.Clean(store.fillDir) != filepath.Join(s.root, "fills") {
		return result, errors.New("복구 구성요소의 데이터 디렉터리가 서로 다릅니다")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if _, err := s.recoveryRecord(r); err != nil {
		return result, err
	}

	current, err := s.Load(r)
	if err != nil {
		return result, err
	}
	result.InitialStage = current.Stage
	result.FinalStage = current.Stage

	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		switch current.Stage {
		case ExecutionSettled:
			result.FinalStage = current.Stage
			return result, nil

		case ExecutionReserved:
			return recoveryPending(result, current.Stage, "MAY_HAVE_SENT가 없어 거래소 전송 가능성을 복구할 수 없습니다")

		case ExecutionMayHaveSent:
			rec, err := s.recoveryRecord(r)
			if err != nil {
				return result, err
			}
			evidence, err := FetchSignedOrderEvidence(ctx, reader, creds, rec)
			if err != nil {
				return result, fmt.Errorf("MAY_HAVE_SENT 서명 복구 실패; 자동 재전송 금지: %w", err)
			}
			now := time.Now().UTC()
			current, err = s.RecordSignedObservation(r, OrderObservation{Order: evidence.After, CheckedAt: evidence.AfterReadAt}, now)
			if err != nil {
				return result, err
			}
			result.Progressed = true
			result.FinalStage = current.Stage
			if !isExecutionTerminalOrderStatus(evidence.After.Status) {
				return recoveryPending(result, current.Stage, "거래소 주문이 아직 종료 상태가 아닙니다")
			}

		case ExecutionSignedObserved:
			// ImportDemoOrder independently brackets userTrades with two signed
			// order GETs. This deliberately re-checks the remote state rather
			// than trusting evidence captured by a previous process/run.
			added, err := store.ImportDemoOrder(ctx, reader, creds, r.Intent)
			if err != nil {
				return result, fmt.Errorf("서명 체결 반영 실패: %w", err)
			}
			result.ImportedFillCount += added
			if added > 0 {
				// ImportDemoOrder durably appended new signed fills even if the
				// remote order remains non-terminal and the lifecycle stage cannot
				// advance yet. Surface that durable progress to operators/recovery
				// callers instead of falsely reporting a no-op restart.
				result.Progressed = true
			}

			rec, err := s.recoveryRecord(r)
			if err != nil {
				return result, err
			}
			evidence, err := FetchSignedOrderEvidence(ctx, reader, creds, rec)
			if err != nil {
				return result, fmt.Errorf("체결 반영 후 주문 재검증 실패: %w", err)
			}
			if !isExecutionTerminalOrderStatus(evidence.After.Status) {
				return recoveryPending(result, current.Stage, "체결 조회 중 주문이 다시 비종료 상태로 관측되어 고정하지 않습니다")
			}
			now := time.Now().UTC()
			current, err = s.RecordFillsImported(r, store, OrderObservation{Order: evidence.After, CheckedAt: evidence.AfterReadAt}, now)
			if err != nil {
				return result, err
			}
			result.Progressed = true
			result.FinalStage = current.Stage

		case ExecutionFillsImported:
			if accountReader == nil {
				return recoveryPending(result, current.Stage, "진입 체결은 로컬에 복구했지만 외부 청산·전체 계정·모델 소유권·펀딩 정산은 미검증; 거래 잠금 유지")
			}
			orders, snapshot, err := s.collectRecoveryAccountEvidence(ctx, store, accountReader, creds)
			if err != nil {
				return result, err
			}
			now := time.Now().UTC()
			current, err = s.RecordWholeSideReconciled(r, store, snapshot, orders, now)
			if err != nil {
				return result, fmt.Errorf("전체 소유권/포지션 복구 대조 실패: %w", err)
			}
			result.Progressed = true
			result.FinalStage = current.Stage

		case ExecutionWholeReconciled:
			if accountReader == nil {
				return recoveryPending(result, current.Stage, "진입 체결 복구 범위를 초과한 whole-side settlement은 실행하지 않음")
			}
			// Do not reuse the preceding account snapshot. Final settlement gets
			// another fresh all-order + account read so the done marker attests
			// the state immediately preceding settlement.
			orders, snapshot, err := s.collectRecoveryAccountEvidence(ctx, store, accountReader, creds)
			if err != nil {
				return result, err
			}
			now := time.Now().UTC()
			current, err = s.Finalize(r, coordinator, store, snapshot, orders, now)
			if err != nil {
				return result, fmt.Errorf("복구 settlement 실패: %w", err)
			}
			result.Progressed = true
			result.FinalStage = current.Stage

		default:
			return result, errors.New("알 수 없는 실행 lifecycle 상태입니다")
		}
	}
}

// ReconcileDispatched performs the signed, read-only remote recovery for the
// single clientOrderID just submitted through this runtime. The lifecycle and
// ownership writes are append-only evidence; the remote reader exposes no
// submit/cancel API. No result from this method is a trading authorization.
// In particular, a manual external close after entry may cause account/owner
// mismatch. That is an expected fail-closed condition, NOT a reason to mark
// the entry settled by force.
func (r *AutoExecutionRuntime) ReconcileDispatched(ctx context.Context, clientOrderID string, reader ExecutionRecoveryReader, creds DemoCredentials) (ExecutionRecoveryResult, error) {
	result := ExecutionRecoveryResult{ClientOrderID: clientOrderID, TradingAuthorized: false}
	if r == nil || r.Path == nil || r.coordinator == nil || r.Path.lifecycle == nil || r.Path.ownership == nil || reader == nil || clientOrderID == "" {
		return result, errors.New("signed 복구 구성요소 또는 clientOrderID가 없습니다")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if r.Path.control != nil {
		if err := r.Path.control.Guard(r.Path.lease)(ctx); err != nil {
			return result, fmt.Errorf("signed 복구 중 run lease 차단: %w", err)
		}
	}
	reservations, err := r.coordinator.scan()
	if err != nil {
		return result, fmt.Errorf("복구 예약 스캔 실패: %w", err)
	}
	var target *Reservation
	for i := range reservations {
		if reservations[i].ClientOrderID == clientOrderID {
			target = &reservations[i]
			break
		}
	}
	if target == nil || target.Intent.Source != Model || target.Intent.ModelID != r.Path.modelID {
		return result, errors.New("복구할 모델 소유 reservation을 확인할 수 없습니다")
	}
	return r.Path.lifecycle.RecoverExecution(ctx, *target, r.coordinator, r.Path.ownership, reader, creds)
}
