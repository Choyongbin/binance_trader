package server

// Experimental finite Demo private-WS dwell bracketed by TWO independent
// signed whole-account REST observations. Not a daemon, no retry/permission.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

func (s *Server) recoverySoakStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "조회 매개변수를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo 진단 전용입니다")
		return
	}
	reports, e := trading.LoadRecoveryProbes(s.dataDir)
	if e != nil {
		respondError(w, 503, "불변 복구 증적 손상: 수동 확인 필요")
		return
	}
	result := map[string]any{"status": "NOT_RUN", "historical_only": true, "trading_authorized": false, "stream_continuity_proven": false, "manual_review_required": true, "diagnostic_busy": s.accountDiagnosticBusy.Load(), "report_count": 0}
	for i := len(reports) - 1; i >= 0; i-- {
		p := reports[i]
		if p.SoakRequestedSec == 0 {
			continue
		}
		result["report_count"] = result["report_count"].(int) + 1
		if result["status"] == "NOT_RUN" {
			result["status"] = p.Stage
			result["started_at"] = p.StartedAt
			result["finished_at"] = p.FinishedAt
			result["observer_connected"] = p.ObserverConnected
			result["rest_reviewed"] = p.RESTReviewed
			result["second_rest_reviewed"] = p.SecondRESTReviewed
			result["soak_completed"] = p.SoakCompleted
			if p.FirstExposureSHA256 != "" && p.SecondExposureSHA256 != "" {
				result["first_last_exposure_equal"] = (p.FirstExposureSHA256 == p.SecondExposureSHA256)
			}
			result["soak_requested_seconds"] = p.SoakRequestedSec
			result["blockers"] = p.Blockers
		}
	}
	respondJSON(w, 200, result)
}

func (s *Server) runRecoverySoak(w http.ResponseWriter, r *http.Request) {
	// 90s dwell + up to 100s bounded reconciliation must outlive the global
	// 45s server write timeout. The request context remains the hard stop.
	extendWriteDeadline(w, 205*time.Second)
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo 계정 진단만 허용됩니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "쿼리 매개변수 불허")
		return
	}
	if !s.accountDiagnosticBusy.CompareAndSwap(false, true) {
		respondError(w, 409, "다른 계정 진단 실행 중")
		return
	}
	defer s.accountDiagnosticBusy.Store(false)
	s.recoveryBusy.Store(true)
	defer s.recoveryBusy.Store(false)
	var in struct {
		Passphrase      string `json:"passphrase"`
		LookbackHours   int    `json:"lookback_hours"`
		DurationSeconds int    `json:"duration_seconds"`
	}
	if decodeStrict(r, &in) != nil || in.Passphrase == "" || (in.LookbackHours != 24 && in.LookbackHours != 72 && in.LookbackHours != 120) || (in.DurationSeconds != 30 && in.DurationSeconds != 90) {
		respondError(w, 400, "Demo 암호, 24/72/120h, 30/90s 중 정확히 선택하세요")
		return
	}
	fingerprint := s.demo.KeyFingerprint()
	if s.demo.Snapshot().Status != "connected" || fingerprint == "" {
		respondError(w, 423, "현재 인증된 Demo 계정이 필요합니다")
		return
	}
	key, secret, err := s.vault.ReadDemo(in.Passphrase)
	if err != nil || key == "" || secret == "" {
		respondError(w, 403, "저장된 Demo 암호 불일치")
		return
	}
	hashed := sha256.Sum256([]byte(key))
	if hex.EncodeToString(hashed[:]) != fingerprint {
		respondError(w, 409, "연결된 Demo 계정 지문 불일치")
		return
	}
	if _, err := trading.LoadRecoveryProbes(s.dataDir); err != nil {
		respondError(w, 503, "기존 복구 증적 손상: 재실행하지 말고 수동 조사")
		return
	}
	// The HTTP request must remain alive. Closing the terminal cancels this
	// finite observation, archivably BLOCKED, never background-authorized.
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(in.DurationSeconds)*time.Second+100*time.Second)
	defer cancel()
	since := time.Now().UTC().Add(-time.Duration(in.LookbackHours) * time.Hour).UnixMilli()
	report, runErr := s.soakRunner(ctx, trading.DemoCredentials{APIKey: key, Secret: secret}, s.dataDir, since, time.Duration(in.DurationSeconds)*time.Second)
	if s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint {
		respondError(w, 409, "진단 중 계정 변경: 과거 결과 불신, 거래 잠금 유지")
		return
	}
	records, loadErr := trading.LoadRecoveryProbes(s.dataDir)
	if loadErr != nil {
		respondError(w, 503, "종료 진단 증적 재검증 실패")
		return
	}
	persisted := false
	for _, p := range records {
		if p.ID == report.ID && p.SHA256 == report.SHA256 && p.SoakRequestedSec == in.DurationSeconds && !p.TradingAuthorized && p.ManualReviewRequired {
			persisted = true
		}
	}
	if !persisted {
		respondError(w, 503, "진단 보고서 일관성 오류: 수동 검토")
		return
	}
	if runErr != nil || ctx.Err() != nil || report.Stage != "REVIEWED" || !report.SecondRESTReviewed || !report.SoakCompleted || !report.ObserverConnected {
		respondError(w, 409, "읽기 전용 장시간 진단 미완료/중단. 과거 기록을 검토하세요")
		return
	}
	respondJSON(w, 200, map[string]any{"status": "BOUNDED_SOAK_REVIEWED_NOT_CONTINUOUS", "stage": report.Stage, "started_at": report.StartedAt, "finished_at": report.FinishedAt, "soak_requested_seconds": report.SoakRequestedSec, "soak_completed": report.SoakCompleted, "observer_connected": report.ObserverConnected, "first_rest_reviewed": report.RESTReviewed, "second_rest_reviewed": report.SecondRESTReviewed, "first_last_exposure_equal": report.FirstExposureSHA256 == report.SecondExposureSHA256, "blockers": report.Blockers, "historical_only": true, "trading_authorized": false, "stream_continuity_proven": false, "manual_review_required": true})
}
