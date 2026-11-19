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

// ModelLocalExposure is an UNATTESTED model attribution from checksum-verified
// local fills. It is not proof that independent external orders did not occur.
type ModelLocalExposure struct {
	Source   Source       `json:"source"`
	ModelID  string       `json:"model_id,omitempty"`
	Side     PositionSide `json:"position_side"`
	Quantity string       `json:"quantity"`
}

// MultiModelEvidenceStatus is a READ-ONLY, non-authorizing snapshot of current
// signed account and immutable local evidence. It is NOT an attestation of
// complete exchange trade history: historical external fills, funding, and
// manual exits cannot be proven solely from a current signed account snapshot.
// In particular, an apparently flat/consistent account never unlocks trading.
type MultiModelEvidenceStatus struct {
	State                    string               `json:"state"`
	ObservedAt               string               `json:"observed_at"`
	SignedAccountFresh       bool                 `json:"signed_account_fresh"`
	LocalSnapshotConsistent  bool                 `json:"local_snapshot_consistent"`
	HistoricalTradesVerified bool                 `json:"historical_trades_verified"`
	OwnershipVerified        bool                 `json:"ownership_verified"`
	RiskPreviewReady         bool                 `json:"risk_preview_ready"`
	TradingAuthorized        bool                 `json:"trading_authorized"`
	ExecutionEnabled         bool                 `json:"execution_enabled"`
	LifecycleState           string               `json:"lifecycle_state,omitempty"`
	LifecyclePending         int                  `json:"lifecycle_pending"`
	ActiveReservations       int                  `json:"active_reservations"`
	LocalOrders              int                  `json:"local_orders"`
	LocalFills               int                  `json:"local_fills"`
	LocalProtectionFills     int                  `json:"local_protection_fills"`
	ModelLocalExposures      []ModelLocalExposure `json:"model_local_exposures"`
	SignedLongQty            string               `json:"signed_long_qty,omitempty"`
	SignedShortQty           string               `json:"signed_short_qty,omitempty"`
	OwnerLongQty             string               `json:"owner_long_qty,omitempty"`
	OwnerShortQty            string               `json:"owner_short_qty,omitempty"`
	Blockers                 []string             `json:"blockers"`
}

// InspectMultiModelEvidence NEVER constructs stores (their constructors can
// mkdir), fetches private APIs, or changes a reservation/ledger. Callers must
// supply a snapshot from an independently authenticated Demo Manager. Even
// when local position quantities match, no signed per-order/trade history was
// supplied and the result cannot be treated as execution-ready.
func InspectMultiModelEvidence(dataDir string, signed exchange.Snapshot, now time.Time) MultiModelEvidenceStatus {
	out := MultiModelEvidenceStatus{State: "BLOCKED", ObservedAt: now.UTC().Format(time.RFC3339Nano), Blockers: []string{}, ModelLocalExposures: []ModelLocalExposure{}, SignedLongQty: "0", SignedShortQty: "0", OwnerLongQty: "0", OwnerShortQty: "0"}
	block := func(s string) { out.Blockers = append(out.Blockers, s) }
	if dataDir == "" || now.IsZero() {
		block("증거 디렉터리 또는 관측 시각 누락")
		return out
	}
	if signed.Environment != "demo" || signed.Status != "connected" || signed.Balances == nil || signed.Positions == nil || signed.Orders == nil || signed.AlgoOrders == nil {
		block("완전한 signed Demo 계정 스냅샷 없음")
	} else if at, err := time.Parse(time.RFC3339Nano, signed.AsOf); err != nil || at.After(now.Add(time.Second)) || now.Sub(at) > 5*time.Second {
		block("signed 계정 스냅샷 오래됨 또는 시각 오류")
	} else {
		out.SignedAccountFresh = true
	}
	if len(signed.Orders) != 0 || len(signed.AlgoOrders) != 0 {
		block("거래소 미체결 일반/조건부 주문 존재")
	}
	// Never follow a symlink in the evidence directory hierarchy, even on
	// Windows where directory junctions/symlinks must block this assessment.
	for _, rel := range []string{"", "trading", "trading/execution_lifecycle", "trading/reservations", "trading/requests", "trading/fills", "trading/protection_fills"} {
		path := filepath.Join(dataDir, filepath.FromSlash(rel))
		st, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			// An absent subdirectory is not evidence that it is empty/settled.
			if rel != "" && rel != "trading/execution_lifecycle" && rel != "trading/protection_fills" {
				block("로컬 증거 디렉터리 누락: " + rel)
			}
			continue
		}
		if err != nil || st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
			block("로컬 증거 경로가 안전하지 않음: " + rel)
			return out
		}
	}
	for _, lock := range []string{"trading/coordinator.active", "trading/mutation_gate.active"} {
		_, err := os.Lstat(filepath.Join(dataDir, filepath.FromSlash(lock)))
		if err == nil {
			block("계정 변경/주문 조정 잠금 존재: " + lock)
		} else if !errors.Is(err, os.ErrNotExist) {
			block("계정 변경/주문 조정 잠금 조회 실패")
		}
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		block("로컬 기록 사전 해시 실패: " + err.Error())
		return out
	}
	lifecycle, err := ReviewExecutionLifecycles(dataDir)
	if err != nil {
		block("Lifecycle 감사 오류: " + err.Error())
	} else {
		out.LifecycleState = lifecycle.State
		out.LifecyclePending = lifecycle.Pending
		if lifecycle.Pending != 0 || (lifecycle.State != ExecutionAuditClear && lifecycle.State != ExecutionAuditEmpty) {
			block(fmt.Sprintf("미정산 Lifecycle: %s, pending=%d", lifecycle.State, lifecycle.Pending))
		}
	}
	dir := filepath.Join(dataDir, "trading", "reservations")
	if st, statErr := os.Lstat(dir); statErr == nil && st.IsDir() && st.Mode()&os.ModeSymlink == 0 {
		c := &Coordinator{reserveDir: dir} // NO exclusive lock and NO mkdir
		reservations, e := c.scan()
		if e != nil {
			block("예약 기록 감사 오류: " + e.Error())
		} else {
			for _, r := range reservations {
				if !c.isDone(r) {
					out.ActiveReservations++
				}
			}
			if out.ActiveReservations > 0 {
				block(fmt.Sprintf("미종료 주문 예약 %d건", out.ActiveReservations))
			}
		}
	}
	requests := filepath.Join(dataDir, "trading", "requests")
	fills := filepath.Join(dataDir, "trading", "fills")
	if st1, e1 := os.Lstat(requests); e1 == nil && st1.IsDir() && st1.Mode()&os.ModeSymlink == 0 {
		if st2, e2 := os.Lstat(fills); e2 == nil && st2.IsDir() && st2.Mode()&os.ModeSymlink == 0 {
			owners := &OwnershipStore{journalDir: requests, fillDir: fills} // read-only instance
			// Empty observation map is deliberate: a signed account snapshot is NOT
			// independent signed proof that recorded orders were filled correctly.
			report, e := owners.Audit(signed, map[string]OrderObservation{}, now)
			if e != nil {
				block("소유권 감사 오류: " + e.Error())
			} else {
				out.LocalOrders = report.ObservedOrders
				out.LocalFills = report.ObservedFills
				out.LocalProtectionFills = report.ObservedProtectionFills
				ownedLong, ownedShort := new(big.Rat), new(big.Rat)
				for _, p := range report.Owned {
					out.ModelLocalExposures = append(out.ModelLocalExposures, ModelLocalExposure{Source: p.Source, ModelID: p.ModelID, Side: p.Side, Quantity: p.Quantity})
					qty, parseErr := decimalRat(p.Quantity, true)
					if parseErr != nil {
						block("로컬 소유 수량 형식 오류")
						continue
					}
					if p.Side == Long {
						ownedLong.Add(ownedLong, qty)
					} else if p.Side == Short {
						ownedShort.Add(ownedShort, qty)
					} else {
						block("로컬 소유 포지션 방향 오류")
					}
				}
				out.OwnerLongQty, out.OwnerShortQty = formatOwnershipDecimal(ownedLong), formatOwnershipDecimal(ownedShort)
				for _, reason := range report.Blockers {
					block("소유권: " + reason)
				}
				if report.ReadyForAssessment && len(report.Blockers) == 0 {
					out.LocalSnapshotConsistent = true
				}
			}
		}
	}
	if !out.SignedAccountFresh {
		out.LocalSnapshotConsistent = false
	}
	// Independently display signed aggregate quantities only when valid.
	long, short, e := signedSideQuantities(signed)
	if e != nil {
		block("signed 포지션 수량/방향 오류: " + e.Error())
		out.LocalSnapshotConsistent = false
	} else {
		out.SignedLongQty, out.SignedShortQty = long, short
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil || before != after {
		block("로컬 증거가 감사 도중 변경되었거나 해시 검증 실패")
		out.LocalSnapshotConsistent = false
	}
	if len(out.Blockers) == 0 && out.LocalSnapshotConsistent {
		out.State = "CURRENT_SNAPSHOT_CONSISTENT_HISTORY_UNVERIFIED"
	} else {
		out.State = "BLOCKED_MANUAL_REVIEW_REQUIRED"
		out.LocalSnapshotConsistent = false
	}
	// No historical external-order/funding attestation is available to this
	// endpoint. Trading AND risk preview remain forbidden even when state is
	// CURRENT_SNAPSHOT_CONSISTENT_HISTORY_UNVERIFIED.
	return out
}

func signedSideQuantities(s exchange.Snapshot) (string, string, error) {
	long, short := new(big.Rat), new(big.Rat)
	for _, p := range s.Positions {
		if p.Symbol != "BTCUSDT" || (p.Side != string(Long) && p.Side != string(Short)) {
			return "", "", errors.New("unsupported symbol/position side")
		}
		v, err := signedRat(p.Qty)
		if err != nil {
			return "", "", err
		}
		if p.Side == string(Long) {
			if v.Sign() < 0 {
				return "", "", errors.New("negative LONG")
			}
			long.Add(long, v)
		} else {
			if v.Sign() > 0 {
				return "", "", errors.New("positive SHORT")
			}
			short.Sub(short, v)
		}
	}
	return formatOwnershipDecimal(long), formatOwnershipDecimal(short), nil
}
