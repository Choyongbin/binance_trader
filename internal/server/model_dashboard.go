package server

import (
	"net/http"
	"slices"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// ModelDashboard is an informational, server-composed snapshot. Quantities
// below are UNATTESTED local records, not proven exchange positions. In
// particular a missing local record does not prove an exchange-side zero.
// Final model PnL is deliberately absent until fully signed, allocated
// funding/commission/settlement evidence becomes available.
type modelDashboardRow struct {
	ModelID              string   `json:"model_id"`
	Selected             bool     `json:"selected"`
	LocalLongQuantities  []string `json:"local_long_quantities"`
	LocalShortQuantities []string `json:"local_short_quantities"`
	AttributionStatus    string   `json:"attribution_status"`
	MarketSignal         string   `json:"market_signal"`
	RealizedPnLStatus    string   `json:"realized_pnl_status"`
	UnrealizedPnLStatus  string   `json:"unrealized_pnl_status"`
}

type modelDashboardView struct {
	ObservedAt               string                       `json:"observed_at"`
	SelectedModelIDs         []string                     `json:"selected_model_ids"`
	Models                   []modelDashboardRow          `json:"models"`
	SignedAccountFresh       bool                         `json:"signed_account_fresh"`
	LocalSnapshotConsistent  bool                         `json:"local_snapshot_consistent"`
	HistoricalTradesVerified bool                         `json:"historical_trades_verified"`
	OwnershipVerified        bool                         `json:"ownership_verified"`
	RiskPreviewReady         bool                         `json:"risk_preview_ready"`
	TradingAuthorized        bool                         `json:"trading_authorized"`
	ExecutionEnabled         bool                         `json:"execution_enabled"`
	AutoStartLocked          bool                         `json:"auto_start_locked"`
	AutoStopLocked           bool                         `json:"auto_stop_locked"`
	ManualOrderLocked        bool                         `json:"manual_order_locked"`
	AutomaticResume          bool                         `json:"automatic_resume"`
	RunControl               trading.AutoRunStatus        `json:"run_control"`
	Controller               trading.AutoControllerStatus `json:"controller"`
	EvidenceState            string                       `json:"evidence_state"`
	RiskState                string                       `json:"risk_state"`
	RiskBlockers             []string                     `json:"risk_blockers"`
	ActiveReservations       int                          `json:"active_reservations"`
	LifecyclePending         int                          `json:"lifecycle_pending"`
	SignedLongQty            string                       `json:"signed_long_qty"`
	SignedShortQty           string                       `json:"signed_short_qty"`
	AccountingReportCount    int                          `json:"accounting_report_count"`
	AccountingStatus         string                       `json:"accounting_status"`
	PnLAttributionStatus     string                       `json:"pnl_attribution_status"`
	MarketSignalState        string                       `json:"market_signal_state"`
	MarketSignalAssumption   string                       `json:"market_signal_assumption"`
}

// GET-only, no private exchange calls, writes, key access or permission changes.
// A corrupt journal/index fails closed instead of returning misleading numbers.
func (s *Server) modelDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수를 지원하지 않습니다")
		return
	}
	config := s.store.Snapshot()
	if config.Environment != app.Demo {
		respondError(w, http.StatusLocked, "Demo 모델 운영 현황만 지원합니다")
		return
	}
	if s.autoControl == nil || s.autoController == nil || s.strategyRegistry == nil {
		respondError(w, http.StatusServiceUnavailable, "자동매매 운영 상태가 구성되지 않았습니다")
		return
	}
	run, err := s.autoControl.Status()
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "자동매매 재시작/상태 증거를 확인할 수 없습니다")
		return
	}
	reports, err := trading.LoadAccountingReports(s.dataDir)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "과거 회계 원본 검증 실패: 수동 조사 필요")
		return
	}
	now := time.Now().UTC()
	proof := trading.InspectMultiModelEvidence(s.dataDir, s.demo.Snapshot(), now)
	market := s.modelSignalStatusAt(now)
	rows := make([]modelDashboardRow, 0, len(s.strategyRegistry.IDs()))
	for _, id := range s.strategyRegistry.IDs() {
		row := modelDashboardRow{
			ModelID: id, Selected: slices.Contains(config.SelectedModelIDs, id),
			LocalLongQuantities: []string{}, LocalShortQuantities: []string{},
			AttributionStatus: "NO_VERIFIED_MODEL_POSITION", MarketSignal: "UNAVAILABLE",
			RealizedPnLStatus: "UNVERIFIED", UnrealizedPnLStatus: "UNVERIFIED",
		}
		for _, p := range proof.ModelLocalExposures {
			if p.ModelID != id || p.Source != trading.Model {
				continue
			}
			switch p.Side {
			case trading.Long:
				row.LocalLongQuantities = append(row.LocalLongQuantities, p.Quantity)
			case trading.Short:
				row.LocalShortQuantities = append(row.LocalShortQuantities, p.Quantity)
			}
		}
		if len(row.LocalLongQuantities) != 0 || len(row.LocalShortQuantities) != 0 {
			row.AttributionStatus = "LOCAL_ONLY_UNVERIFIED"
		}
		// Model signals are hypothetical public market observations only,
		// never validated ownership/risk signals. Guard against stale selection.
		if market.State == "MARKET_SIGNALS_ONLY" && slices.Equal(config.SelectedModelIDs, market.SelectedModelIDs) {
			for _, signal := range market.Signals {
				if signal.ModelID == id {
					row.MarketSignal = signal.Direction
					break
				}
			}
		}
		rows = append(rows, row)
	}
	blockers := slices.Clone(proof.Blockers)
	// Never silently discard local model evidence that is not registered in
	// this frozen runtime: surface an explicit operator blocker instead.
	for _, exposure := range proof.ModelLocalExposures {
		if exposure.Source != trading.Model {
			continue
		}
		if _, err := s.strategyRegistry.Resolve(exposure.ModelID); err != nil {
			blockers = append(blockers, "등록되지 않은 모델 소유 기록 존재: 수동 증거 조사 필요")
			break
		}
	}
	if run.RestartBlock {
		blockers = append(blockers, "이전 프로세스 자동매매 세대의 재시작 차단")
	}
	controller := s.autoController.Status()
	if controller.Running || controller.FailClosed || controller.SignedReviewRequired {
		blockers = append(blockers, "Controller 활성/장애/서명 재검증 상태: 자동매매 진입 차단")
	}
	if len(blockers) == 0 {
		blockers = append(blockers, "전체 외부 거래·수수료·펀딩·모델 소유권 검증 미완료")
	}
	status := "NOT_RUN"
	if len(reports) != 0 {
		status = "HISTORICAL_ESTIMATE_ONLY"
	}
	w.Header().Set("Cache-Control", "no-store")
	respondJSON(w, http.StatusOK, modelDashboardView{
		ObservedAt: now.Format(time.RFC3339Nano), SelectedModelIDs: append([]string{}, config.SelectedModelIDs...), Models: rows,
		SignedAccountFresh: proof.SignedAccountFresh, LocalSnapshotConsistent: proof.LocalSnapshotConsistent,
		HistoricalTradesVerified: false, OwnershipVerified: false, RiskPreviewReady: false,
		TradingAuthorized: false, ExecutionEnabled: false, AutoStartLocked: true, AutoStopLocked: true,
		ManualOrderLocked: true, AutomaticResume: false, RunControl: run, Controller: controller,
		EvidenceState: proof.State, RiskState: "BLOCKED_MANUAL_REVIEW_REQUIRED", RiskBlockers: blockers,
		ActiveReservations: proof.ActiveReservations, LifecyclePending: proof.LifecyclePending,
		SignedLongQty: proof.SignedLongQty, SignedShortQty: proof.SignedShortQty,
		AccountingReportCount: len(reports), AccountingStatus: status,
		PnLAttributionStatus: "MODEL_FINAL_NET_AND_UNREALIZED_UNVERIFIED",
		MarketSignalState:    market.State, MarketSignalAssumption: market.Assumption,
	})
}
