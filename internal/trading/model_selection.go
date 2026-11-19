package trading

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
)

// VerifyModelSelectionSafe guards CHANGING a durable model preference, not
// trading authorization. It never mutates exchange, lifecycle or ownership.
// A signed, fresh, flat Demo account AND consistent flat local evidence are
// necessary. These are deliberately insufficient for authorizing new orders.
func VerifyModelSelectionSafe(dataDir string, snapshot exchange.Snapshot, now time.Time) error {
	if dataDir == "" {
		return errors.New("모델 전환에 데이터 디렉터리가 필요합니다")
	}
	// Refuse symlinked or non-directory evidence roots before any audit
	// (the account selection handler must never follow user-controlled links).
	for _, name := range []string{"trading", "trading/reservations", "trading/requests", "trading/fills", "trading/execution_lifecycle", "trading/protection_fills"} {
		path := filepath.Join(dataDir, filepath.FromSlash(name))
		info, statErr := os.Lstat(path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return fmt.Errorf("모델 전환 차단: 증거 경로 확인 실패: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("모델 전환 차단: 증거 경로는 실제 디렉터리여야 합니다")
		}
	}
	// A separate supervised Demo CLI can hold this process-independent lock.
	// Do not switch model identity while another executor owns the account.
	coordinatorLock := filepath.Join(dataDir, "trading", "coordinator.active")
	if _, err := os.Lstat(coordinatorLock); err == nil {
		return errors.New("모델 전환 차단: 다른 주문 조정기가 실행 중이거나 종료 증거가 남았습니다")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("모델 전환 차단: 주문 조정기 잠금 확인 실패: %w", err)
	}
	audit, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return fmt.Errorf("모델 전환 차단: lifecycle 감사 실패: %w", err)
	}
	if audit.Pending != 0 || (audit.State != ExecutionAuditEmpty && audit.State != ExecutionAuditClear) {
		return fmt.Errorf("모델 전환 차단: 미정산 lifecycle 상태=%s, pending=%d", audit.State, audit.Pending)
	}
	if snapshot.Environment != "demo" || snapshot.Status != "connected" || snapshot.Positions == nil || snapshot.Orders == nil || snapshot.AlgoOrders == nil || snapshot.Balances == nil {
		return errors.New("모델 전환 차단: 완전한 signed Demo 계정 조회가 필요합니다")
	}
	asOf, err := time.Parse(time.RFC3339Nano, snapshot.AsOf)
	if err != nil || asOf.After(now.Add(time.Second)) || now.Sub(asOf) > 5*time.Second {
		return errors.New("모델 전환 차단: signed 계정 조회가 오래되었거나 유효하지 않습니다")
	}
	if len(snapshot.Orders) != 0 || len(snapshot.AlgoOrders) != 0 {
		return errors.New("모델 전환 차단: 미체결 일반/조건부 주문이 있습니다")
	}
	for _, p := range snapshot.Positions {
		if p.Symbol != "BTCUSDT" || (p.Side != "LONG" && p.Side != "SHORT") {
			return errors.New("모델 전환 차단: 지원하지 않는 포지션 데이터")
		}
		qty, err := signedRat(p.Qty)
		if err != nil || qty.Sign() != 0 {
			return errors.New("모델 전환 차단: signed 계정 포지션이 비어 있지 않거나 유효하지 않습니다")
		}
	}
	// Reservations from legacy/supervised paths can exist even when the
	// execution-lifecycle archive is EMPTY. Never treat them as a flat account.
	reserveDir := filepath.Join(dataDir, "trading", "reservations")
	if _, err := os.Lstat(reserveDir); err == nil {
		c := &Coordinator{reserveDir: reserveDir}
		reservations, scanErr := c.scan()
		if scanErr != nil {
			return fmt.Errorf("모델 전환 차단: 예약 기록 검증 실패: %w", scanErr)
		}
		for _, reservation := range reservations {
			if !c.isDone(reservation) {
				return errors.New("모델 전환 차단: 종료되지 않은 주문 예약이 있습니다")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("모델 전환 차단: 예약 기록 조회 실패: %w", err)
	}
	// Local owner/hedge history must also be flat. This catches manually closed
	// exchange positions whose ownership fills have not yet been imported.
	owners, err := NewOwnershipStore(dataDir)
	if err != nil {
		return fmt.Errorf("모델 전환 차단: 소유권 저장소 오류: %w", err)
	}
	owners.mu.Lock()
	defer owners.mu.Unlock()
	records, err := owners.ScanJournal()
	if err != nil {
		return fmt.Errorf("모델 전환 차단: 주문 기록 손상: %w", err)
	}
	fills, err := owners.readFills()
	if err != nil {
		return fmt.Errorf("모델 전환 차단: 체결 기록 손상: %w", err)
	}
	protectionFills, err := readValidatedProtectionFillsForStore(filepath.Dir(owners.fillDir), fills)
	if err != nil {
		return fmt.Errorf("모델 전환 차단: 보호 주문 체결 증거 오류: %w", err)
	}
	lots, err := applyProtectionFills(records, fills, protectionFills)
	if err != nil {
		return fmt.Errorf("모델 전환 차단: 소유권 재생 실패: %w", err)
	}
	for _, record := range records {
		if record.State != Reconciled {
			return errors.New("모델 전환 차단: 미확정 주문 저널이 있습니다")
		}
	}
	for _, lot := range lots {
		if lot.qty.Cmp(new(big.Rat)) != 0 {
			return errors.New("모델 전환 차단: 로컬 모델/수동 소유 포지션이 남아 있습니다")
		}
	}
	return nil
}
