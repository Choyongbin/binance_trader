package trading

// A wallet edit changes only a local accounting budget. Historical evidence
// can be retained after it is checked, while uncertain in-flight work must
// still block. This audit never authorizes a Binance order.
import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
)

// CheckWalletEditHistory returns an evidence fingerprint and whether historical
// files exist. The caller must compare the fingerprint after its signed,
// whole-account read and immediately before changing the wallet.
func CheckWalletEditHistory(dataDir string) (string, bool, error) {
	if dataDir == "" {
		return "", false, errors.New("로컬 기록 경로 누락")
	}
	root := filepath.Join(dataDir, "trading")
	settledProtectionGroups := make(map[string]bool)
	// An active or stale continuous private observer makes a wallet edit
	// ambiguous, even when every historical trade already settled.
	if _, err := os.Lstat(filepath.Join(root, "continuous_monitor", "exclusive.active")); err == nil {
		return "", false, errors.New("상시 계정 관측 또는 미해결 관측 잠금이 존재합니다")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	// The coordinator lock is always blocking, including when empty.
	if _, err := os.Lstat(filepath.Join(root, "coordinator.active")); err == nil {
		return "", false, errors.New("거래 조정기 잠금이 남아 있습니다")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	// A running or crashed bounded recovery/reconnect diagnostic is an
	// unfinished account-observation lifecycle. A CURRENT flat account does
	// not prove that its evidence epoch completed cleanly, so wallet mutation
	// stays blocked until the durable exclusive marker is reconciled.
	for _, lock := range []struct {
		rel string
		msg string
	}{
		{filepath.Join("recovery_sessions", "exclusive.active"), "복구 진단이 실행 중이거나 비정상 종료되었습니다"},
		{filepath.Join("reconnect_watch_sessions", "exclusive.active"), "재연결 진단이 실행 중이거나 비정상 종료되었습니다"},
	} {
		if info, err := os.Lstat(filepath.Join(root, lock.rel)); err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return "", false, errors.New(lock.msg + ": 잠금 형식이 안전하지 않습니다")
			}
			return "", false, errors.New(lock.msg)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", false, err
		}
	}

	// Validate the durable execution lifecycle itself, not just reservations.
	// This closes the orphan/tamper case where a stale lifecycle file could
	// otherwise be ignored after its reservation happened to look terminal.
	if _, err := os.Lstat(filepath.Join(root, "execution_lifecycle")); err == nil {
		audit, auditErr := ReviewExecutionLifecycles(dataDir)
		if auditErr != nil {
			return "", false, fmt.Errorf("주문 실행 lifecycle 검증 실패: %w", auditErr)
		}
		if audit.Pending != 0 {
			return "", false, errors.New("완료되지 않은 주문 실행 lifecycle이 있습니다")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}

	// Purely PREPARED, checksum-verified protective previews may be retained:
	// they never sent an exchange request. Any leg that MAY_HAVE_SENT, orphan,
	// invalid marker or unknown file still blocks. No old active order is
	// inferred to be terminal from a CURRENT flat snapshot alone.
	if _, err := os.Lstat(filepath.Join(root, "protection_submissions")); err == nil {
		protective, e := OpenProtectionSubmissionStore(dataDir)
		if e != nil {
			return "", false, e
		}
		protective.mu.Lock()
		plans, e := protective.scanPlansLocked()
		var active []ProtectionDispatchPlan
		if e == nil {
			active, settledProtectionGroups, e = activeProtectionPlansLocked(protective, plans)
		}
		if e == nil {
			for _, p := range active {
				for _, leg := range []string{"tp", "sl"} {
					state, inspectErr := protective.inspectLocked(p.GroupID, leg)
					if inspectErr != nil {
						e = inspectErr
						break
					}
					if state.MayHaveSent || state.SignedObserved {
						e = errors.New("이전에 전송 시도한 보호 주문의 독립 종료 증명이 필요합니다")
						break
					}
				}
				if e != nil {
					break
				}
			}
		}
		protective.mu.Unlock()
		if e != nil {
			return "", false, fmt.Errorf("보호 주문 보존 기록 손상: %w", e)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}

	// A prepared cancellation which was NEVER dispatched can be retained,
	// provided its immutable journal is terminal (validated below). Once an
	// attempt exists, a fresh per-order signed readback is still required.
	if _, err := os.Lstat(filepath.Join(root, "manual_cancels")); err == nil {
		manual, e := OpenManualCancelStore(dataDir)
		if e != nil {
			return "", false, e
		}
		if _, e = manual.AuditAll(); e != nil {
			return "", false, fmt.Errorf("수동 취소 보존 기록 손상: %w", e)
		}
		entries, e := os.ReadDir(filepath.Join(root, "manual_cancels"))
		if e != nil {
			return "", false, e
		}
		for _, f := range entries {
			if !strings.HasSuffix(f.Name(), ".json") || strings.HasSuffix(f.Name(), ".attempt.json") {
				continue
			}
			id := strings.TrimSuffix(f.Name(), ".json")
			_, attempted, e := manual.Inspect(id)
			if e != nil {
				return "", false, e
			}
			if attempted {
				return "", false, errors.New("전송 시도한 수동 취소의 주문별 서명 종료 증명이 필요합니다")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	// Algo-cancel records can be historical only when the owning protection
	// group itself has reached checksum-verified SETTLED. A terminal cancel
	// outcome alone is not enough because a racing sibling may have filled.
	if entries, err := os.ReadDir(filepath.Join(root, "algo_cancel_intents")); err == nil {
		cancelStore, e := NewCancelIntentStore(dataDir)
		if e != nil {
			return "", false, e
		}
		groups := make(map[string]bool)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				return "", false, errors.New("조건부 주문 취소 기록에 알 수 없는 파일이 있습니다")
			}
			name := entry.Name()
			group := strings.TrimSuffix(name, ".json")
			group = strings.TrimSuffix(group, ".dispatch")
			group = strings.TrimSuffix(group, ".outcome")
			if !cancelGroupRE.MatchString(group) {
				return "", false, errors.New("조건부 주문 취소 기록 이름 오류")
			}
			groups[group] = true
		}
		for group := range groups {
			if !settledProtectionGroups[group] {
				return "", false, errors.New("조건부 주문 취소 기록의 protection settlement가 완료되지 않았습니다")
			}
			c, _, e := cancelStore.Inspect(group)
			if e != nil {
				return "", false, e
			}
			protective, e := OpenProtectionSubmissionStore(dataDir)
			if e != nil {
				return "", false, e
			}
			plan, e := protective.read(group)
			if e != nil {
				return "", false, e
			}
			matched := false
			for _, leg := range []string{"tp", "sl"} {
				state, inspectErr := protective.inspectLocked(group, leg)
				if inspectErr != nil {
					return "", false, inspectErr
				}
				instruction := plan.TP
				if leg == "sl" {
					instruction = plan.SL
				}
				if state.SignedObserved && state.ObservedAlgoID == c.SiblingAlgoID && instruction.ClientAlgoID == c.OwnerClientAlgoID {
					matched = true
				}
			}
			if !matched {
				return "", false, errors.New("조건부 주문 취소 기록이 SETTLED protection plan의 signed AlgoID와 연결되지 않습니다")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	store, err := NewOwnershipStore(dataDir)
	if err != nil {
		return "", false, err
	}
	records, err := store.ScanJournal()
	if err != nil {
		return "", false, err
	}
	fills, err := store.readFills()
	if err != nil {
		return "", false, err
	}
	protectionFills, err := readValidatedProtectionFillsForStore(root, fills)
	if err != nil {
		return "", false, err
	}
	lots, err := replayWithProtection(records, fills, protectionFills)
	if err != nil {
		return "", false, err
	}
	for _, l := range lots {
		if l.qty.Sign() != 0 {
			return "", false, errors.New("로컬 체결에 미청산 포지션이 남아 있습니다")
		}
	}
	// A FILLED order without its complete local fills must not masquerade as a
	// flat historical position. The signed full account is checked separately.
	filledQty := make(map[string]*big.Rat)
	for _, f := range fills {
		q, err := positiveRat(f.Trade.Quantity)
		if err != nil {
			return "", false, err
		}
		if filledQty[f.ClientOrderID] == nil {
			filledQty[f.ClientOrderID] = new(big.Rat)
		}
		filledQty[f.ClientOrderID].Add(filledQty[f.ClientOrderID], q)
	}
	// Prepared-only manual cancels are harmless only when their source order
	// has since reached a validated terminal local state. AuditAll validates
	// their persisted intent->journal identity, but not terminal status.
	if entries, err := os.ReadDir(filepath.Join(root, "manual_cancels")); err == nil {
		byID := make(map[string]Record, len(records))
		for _, r := range records {
			byID[r.Plan.ClientOrderID] = r
		}
		for _, f := range entries {
			if !strings.HasSuffix(f.Name(), ".json") || strings.HasSuffix(f.Name(), ".attempt.json") {
				continue
			}
			id := strings.TrimSuffix(f.Name(), ".json")
			r, ok := byID[id]
			if !ok || r.State != Reconciled && r.State != Rejected {
				return "", false, errors.New("취소 준비 원본 주문의 종료 상태가 검증되지 않았습니다")
			}
			switch r.ExchangeStatus {
			case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED":
			default:
				return "", false, errors.New("취소 준비 원본 주문의 최종 상태 불명")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	for _, r := range records {
		q, err := positiveRat(r.Plan.Quantity)
		if err != nil {
			return "", false, err
		}
		completeFills := filledQty[r.Plan.ClientOrderID] != nil && filledQty[r.Plan.ClientOrderID].Cmp(q) == 0
		if r.State == Acknowledged && completeFills {
			// The import performed independent signed order/trade reads. An
			// ACK alone without all persisted fills is never sufficient.
			continue
		}
		if r.State != Reconciled && r.State != Rejected {
			return "", false, errors.New("종료 여부가 불명확한 로컬 주문이 있습니다")
		}
		switch r.ExchangeStatus {
		case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED":
		default:
			return "", false, errors.New("종료 상태가 검증되지 않은 로컬 주문이 있습니다")
		}
		if r.ExchangeStatus == "FILLED" {
			if !completeFills {
				return "", false, errors.New("완료 주문의 로컬 체결 수량이 누락되었습니다")
			}
		}
	}
	reserveDir := filepath.Join(root, "reservations")
	if _, err := os.Lstat(reserveDir); err == nil {
		c := &Coordinator{reserveDir: reserveDir}
		items, err := c.scan()
		if err != nil {
			return "", false, err
		}
		for _, item := range items {
			if !c.isDone(item) {
				return "", false, errors.New("완료되지 않은 거래 예약이 있습니다")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	if _, err := readRecoveryEvidenceAllowMissing(dataDir); err != nil {
		return "", false, err
	}
	// Hash every byte, name and directory tag, including state sidecars, so a
	// concurrent file change invalidates the signed observation before mutation.
	h := sha256.New()
	history := false
	for _, name := range []string{"requests", "fills", "protection_fills", "protection_settlements", "reservations", "execution_lifecycle", "user_events", "stream_discontinuities", "protection_submissions", "algo_cancel_intents", "manual_cancels"} {
		files, err := os.ReadDir(filepath.Join(root, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || len(files) > 100000 {
			return "", false, errors.New("거래 증적 목록 확인 실패 또는 파일 개수 초과")
		}
		for _, file := range files {
			if !file.Type().IsRegular() {
				return "", false, errors.New("정상 파일이 아닌 거래 증적 발견")
			}
			path := filepath.Join(root, name, file.Name())
			f, err := os.Open(path)
			if err != nil {
				return "", false, err
			}
			info, err := f.Stat()
			if err != nil || info.Size() > 2<<20 {
				f.Close()
				return "", false, errors.New("거래 증적의 크기를 확인할 수 없습니다")
			}
			part := sha256.New()
			_, err = io.Copy(part, f)
			closeErr := f.Close()
			if err != nil || closeErr != nil {
				return "", false, errors.New("거래 증적 읽기 실패")
			}
			fmt.Fprintf(h, "%s/%s:%x\n", name, file.Name(), part.Sum(nil))
			history = true
		}
	}
	return hex.EncodeToString(h.Sum(nil)), history, nil
}
