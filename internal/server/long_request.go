package server

import (
	"net/http"
	"time"
)

// extendWriteDeadline overrides the process-wide HTTP WriteTimeout for a small
// set of explicitly bounded, operator-triggered diagnostics. The server stays
// loopback-only and each caller still owns a stricter context timeout. Ignoring
// ErrNotSupported keeps httptest.ResponseRecorder and wrapped writers usable.
// This does not make the operation asynchronous or authorize trading.
func extendWriteDeadline(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
}
