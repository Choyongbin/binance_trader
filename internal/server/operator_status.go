package server

import (
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// Unified local operator view. It is intentionally historical/read-only and
// cannot decrypt credentials, query Binance, clear evidence, or authorize a
// trade. Any corrupt or changing source fails the whole response closed.
func (s *Server) operatorUnifiedStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "Demo 운영자 진단만 지원합니다")
		return
	}
	status, err := trading.AnalyzeOperatorUnifiedStatus(s.dataDir, time.Now().UTC())
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "통합 운영 증적 손상·변경 또는 불완전 상태: 수동 조사 필요")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	respondJSON(w, http.StatusOK, status)
}
