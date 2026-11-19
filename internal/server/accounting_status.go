package server

// Read-only historical accounting display. No unsigned/browser amounts are
// accepted, and this route cannot calculate or authorize actual Demo orders.
import (
	"binance_trader/internal/trading"
	"net/http"
)

func (s *Server) accountingStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	reports, err := trading.LoadAccountingReports(s.dataDir)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "회계 보고서 손상: 거래 잠금 및 수동 조사 필요")
		return
	}
	out := map[string]any{"status": "NOT_RUN", "report_count": len(reports), "historical_only": true,
		"trading_authorized": false, "actual_net_verified": false,
		"funding_status": "PROVISIONAL_OR_UNALLOCATED"}
	if len(reports) > 0 {
		last := reports[len(reports)-1]
		out["status"] = "HISTORICAL_ESTIMATE_ONLY"
		out["created_at"] = last.CreatedAt
		out["window_start_ms"] = last.WindowStartMS
		out["window_end_ms"] = last.WindowEndMS
		out["known_orders"] = last.KnownOrders
		out["known_fills"] = last.KnownFills
		out["income_rows"] = last.IncomeRows
		out["owners"] = last.Owners
		out["account_funding_by_asset"] = last.AccountFundingByAsset
		out["unallocated_funding_by_asset"] = last.UnallocatedFundingByAsset
		out["warnings"] = last.Warnings
	}
	respondJSON(w, http.StatusOK, out)
}
