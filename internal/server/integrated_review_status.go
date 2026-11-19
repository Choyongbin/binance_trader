package server

import (
	"binance_trader/internal/trading"
	"net/http"
)

// Historical diagnostic, not a live account state or a route to place orders.
func (s *Server) integratedReviewStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	results, err := trading.LoadIntegratedReviews(s.dataDir)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "통합 진단 기록 손상: 거래 잠금 및 수동 확인 필요")
		return
	}
	out := map[string]any{"trading_authorized": false, "stream_continuity_proven": false,
		"manual_review_required": true, "historical_only": true,
		"report_count": len(results), "status": "NOT_RUN"}
	if len(results) > 0 {
		latest := results[len(results)-1]
		out["status"] = latest.State
		out["checked_at"] = latest.CheckedAt
		out["owner_count"] = latest.OwnerCount
		out["active_model_reservations"] = latest.ActiveModelReservations
		out["active_manual_reservations"] = latest.ActiveManualReservations
		out["blockers"] = latest.Blockers
	}
	respondJSON(w, http.StatusOK, out)
}
