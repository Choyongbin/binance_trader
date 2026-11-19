package server

// Historical only: the internal double-signed-read diagnostic is never
// launched from HTTP and can never issue a manual/automatic order permit.
import (
	"binance_trader/internal/trading"
	"net/http"
)

func (s *Server) boundedDriftStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	reports, err := trading.LoadBoundedDriftReviews(s.dataDir)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "이중 관측 기록 손상: 거래 잠금 및 수동 복구가 필요합니다")
		return
	}
	out := map[string]any{"status": "NOT_RUN", "report_count": len(reports), "trading_authorized": false,
		"continuous_coverage_proven": false, "manual_review_required": true, "historical_only": true}
	if len(reports) > 0 {
		last := reports[len(reports)-1]
		out["status"] = last.State
		out["checked_at"] = last.CheckedAt
		out["event_count"] = last.EventCount
		out["gap_count"] = last.GapCount
		out["blockers"] = last.Blockers
	}
	respondJSON(w, http.StatusOK, out)
}
