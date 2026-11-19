package server

import (
	"binance_trader/internal/trading"
	"net/http"
)

// Historical, read-only review display. No decrypted credentials, no private
// WebSocket dial and no order permit, including after a matching diagnosis.
func (s *Server) streamReviewStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 파라미터를 지원하지 않습니다")
		return
	}
	reports, err := trading.LoadStreamReviews(s.dataDir)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "진단 기록 손상: 거래는 잠금 상태이며 수동 조사가 필요합니다")
		return
	}
	out := map[string]any{"trading_authorized": false, "manual_review_required": true, "stream_continuity_proven": false,
		"report_count": len(reports), "status": "NOT_RUN", "historical_only": true}
	if len(reports) > 0 {
		last := reports[0]
		for _, p := range reports[1:] {
			if p.ReadAt.After(last.ReadAt) {
				last = p
			}
		}
		out["status"] = last.Review.State
		out["last_checked_at"] = last.ReadAt
		out["matched_order_count"] = last.Review.MatchedOrderCount
		out["matched_trade_events"] = last.Review.MatchedTradeEvents
		out["gap_count"] = last.GapCount
		out["blockers"] = last.Review.Blockers
	}
	respondJSON(w, http.StatusOK, out)
}
