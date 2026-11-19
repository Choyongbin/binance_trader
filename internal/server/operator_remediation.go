package server

import (
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// Read-only remediation guidance derived from the same immutable evidence as
// operator-status. It never repairs, deletes, retries or unlocks anything.
func (s *Server) operatorRemediation(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "Demo 운영자 진단만 지원합니다")
		return
	}
	plan, err := trading.AnalyzeOperatorRemediation(s.dataDir, time.Now().UTC())
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "복구 가이드 증적 손상·변경 또는 불완전 상태: 수동 조사 필요")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	respondJSON(w, http.StatusOK, plan)
}
