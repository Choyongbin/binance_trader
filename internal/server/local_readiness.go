package server

// Local-only, read-only operator diagnostic. The result is neither a signed
// exchange assertion nor permission to change a wallet or submit orders.
import (
	"errors"
	"net/http"
	"os"
	"path/filepath"

	"binance_trader/internal/trading"
)

func (s *Server) localReadiness(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수는 지원하지 않습니다")
		return
	}
	held, gateErr := trading.MutationGatePresent(s.dataDir)
	_, historical, auditErr := trading.CheckWalletEditHistory(s.dataDir)
	lock := filepath.Join(s.dataDir, "trading", "coordinator.active")
	_, coordErr := os.Lstat(lock)
	coordLocked := coordErr == nil
	coordUnknown := coordErr != nil && !errors.Is(coordErr, os.ErrNotExist)
	status := "SIGNED_DEMO_AUDIT_REQUIRED"
	switch {
	case held || gateErr != nil || coordLocked || coordUnknown:
		status = "LOCK_PRESENT_MANUAL_REVIEW"
	case auditErr != nil:
		status = "LOCAL_EVIDENCE_BLOCKED_MANUAL_REVIEW"
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"status":                              status,
		"history_inspectable":                 auditErr == nil,
		"history_present":                     historical,
		"mutation_gate_present":               held,
		"coordinator_lock_present":            coordLocked,
		"trading_authorized":                  false,
		"wallet_edit_authorized":              false,
		"signed_whole_account_audit_required": true,
		"manual_review_required":              true,
		"note":                                "로컬 진단은 거래소 주문·포지션 종료 증명이 아닙니다. 불명확한 전송 기록과 잠금은 삭제하지 마세요.",
	})
}
