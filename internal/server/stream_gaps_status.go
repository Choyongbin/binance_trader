package server

import (
	"binance_trader/internal/app"
	"binance_trader/internal/trading"
	"net/http"
	"time"
)

// No credentials, no upstream calls and no changes to any saved evidence.
func (s *Server) streamGapsStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "Demo 기록만 조회할 수 있습니다")
		return
	}
	summary, err := trading.AnalyzeStreamGaps(s.dataDir, time.Now().UTC())
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "계정 스트림 증적이 변동/손상/미완료되었을 수 있습니다: 거래 잠금 및 수동 검토 유지")
		return
	}
	respondJSON(w, http.StatusOK, summary)
}
