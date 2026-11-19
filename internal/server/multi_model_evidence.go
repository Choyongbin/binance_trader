package server

import (
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// The endpoint makes no exchange requests: it only examines the most recently
// authenticated signed Demo snapshot and sealed local evidence in place.
// It neither allocates a wallet nor authorizes orders, including on success.
func (s *Server) multiModelEvidence(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "쿼리 파라미터를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "Real 환경에서는 Demo 다중 모델 증거를 제공하지 않습니다")
		return
	}
	out := trading.InspectMultiModelEvidence(s.dataDir, s.demo.Snapshot(), time.Now().UTC())
	respondJSON(w, http.StatusOK, out)
}
