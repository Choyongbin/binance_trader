package server

import (
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// Only immutable local evidence, never an upstream request or an unlock. The
// response contains neither session identifiers nor raw events/credentials.
func (s *Server) restartForensicsStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "query is not supported")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "Demo forensic history only")
		return
	}
	v, err := trading.AnalyzeRestartForensics(s.dataDir, time.Now().UTC())
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "forensic evidence incomplete or changing; no automatic recovery")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	respondJSON(w, http.StatusOK, v)
}
