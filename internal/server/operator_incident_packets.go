package server

import (
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// Read-only, in-memory sealed summaries for CRITICAL remediation blockers.
// No packet is written into the inspected data tree and no repair is attempted.
func (s *Server) operatorIncidentPackets(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "Demo 운영자 진단만 지원합니다")
		return
	}
	packets, err := trading.BuildOperatorIncidentPackets(s.dataDir, time.Now().UTC())
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "incident packet 증적 손상·변경 또는 불완전 상태: offline 수동 조사 필요")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	respondJSON(w, http.StatusOK, packets)
}
