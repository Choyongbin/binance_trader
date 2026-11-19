package trading

// An intentionally conservative LOCAL guard used by the allocation-only UI.
// It is not full signed Binance account reconciliation and never authorizes
// orders. Once local order/event evidence exists, changing the internal wallet
// requires a future explicit authenticated reconcile-and-freeze operation.
import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func CheckWalletEditQuietState(dataDir string) error {
	if dataDir == "" {
		return errors.New("검증할 로컬 데이터 경로가 필요합니다")
	}
	paths := []string{
		filepath.Join("trading", "requests"),
		filepath.Join("trading", "reservations"),
		filepath.Join("trading", "fills"),
		filepath.Join("trading", "protection_submissions"),
		filepath.Join("trading", "algo_cancel_intents"),
		filepath.Join("trading", "manual_cancels"),
		filepath.Join("trading", "coordinator.active"),
		filepath.Join("trading", "continuous_monitor", "exclusive.active"),
		filepath.Join("trading", "user_events"),
		filepath.Join("trading", "stream_discontinuities"),
	}
	for _, rel := range paths {
		path := filepath.Join(dataDir, rel)
		files, err := os.ReadDir(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("로컬 장부 확인 실패 (%s): %w", rel, err)
		}
		// A coordinator lock is itself evidence of an interrupted operation,
		// even when its directory happens to be empty.
		if rel == filepath.Join("trading", "coordinator.active") || rel == filepath.Join("trading", "continuous_monitor", "exclusive.active") {
			return errors.New("거래 조정기 잠금이 남아 있습니다: 수동 점검 필요")
		}
		// Even a stale .active lock means we cannot safely assume the
		// ledger is quiet. Do not silently ignore unknown/hidden entries.
		if len(files) > 0 {
			return fmt.Errorf("%s에 거래/계정 기록 또는 잠금이 있습니다: 배정 변경 전 독립 계정 감사 필요", rel)
		}
	}
	return nil
}
