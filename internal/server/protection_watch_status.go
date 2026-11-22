package server

import (
	"net/http"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// protectionWatchStatus reports the server-owned periodic GET-only observer.
// Reading this API never starts a Binance request or enables a trading route.
// The information is volatile and cannot certify continuous protection or
// account-wide ownership: even an empty local inventory remains unverified.
func (s *Server) protectionWatchStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "query is not supported")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo protection observation only")
		return
	}
	if s.protectionWatcher == nil {
		respondJSON(w, 200, trading.ProductProtectionWatchStatus{
			State: trading.ProtectionWatchNotStarted, ManualReviewRequired: true,
		})
		return
	}
	respondJSON(w, 200, s.protectionWatcher.Status())
}
