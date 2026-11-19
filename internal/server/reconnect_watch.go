package server

// Operator-triggered, finite read-only reconnect engineering test. The only
// outcome is immutable historical evidence; neither a successful signed REST
// review nor a reconnect changes the process-wide trading LOCKED gate.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

func (s *Server) reconnectWatchStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "조회 매개변수 불허")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo 읽기 전용 진단 전용")
		return
	}
	reports, e := trading.LoadReconnectWatches(s.dataDir)
	if e != nil {
		respondError(w, 503, "재연결 증적 무결성 실패: 수동 조사 필요")
		return
	}
	out := map[string]any{"status": "NOT_RUN", "report_count": len(reports), "diagnostic_busy": s.accountDiagnosticBusy.Load(), "trading_authorized": false, "stream_continuity_proven": false, "manual_review_required": true, "historical_only": true}
	lock := filepath.Join(s.dataDir, "trading", "reconnect_watch_sessions", "exclusive.active")
	if info, lockErr := os.Lstat(lock); lockErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			respondError(w, 503, "재연결 진단 잠금 상태 손상: 수동 조사 필요")
			return
		}
		out["diagnostic_running_or_stale_lock"] = true
		out["status"] = "RUNNING_OR_CRASH_INTERRUPTED"
	} else if !errors.Is(lockErr, os.ErrNotExist) {
		respondError(w, 503, "재연결 진단 잠금 상태 검사 실패")
		return
	}
	if len(reports) > 0 {
		x := reports[len(reports)-1]
		if out["diagnostic_running_or_stale_lock"] != true {
			out["status"] = x.Stage
		}
		out["started_at"] = x.StartedAt
		out["finished_at"] = x.FinishedAt
		out["reconnects"] = x.Reconnects
		out["epochs"] = x.Epochs
		out["final_rest_reviewed"] = x.FinalRESTReviewed
		out["blockers"] = x.Blockers
	}
	respondJSON(w, 200, out)
}

func (s *Server) runReconnectWatch(w http.ResponseWriter, r *http.Request) {
	extendWriteDeadline(w, 205*time.Second)
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo 계정 전용")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "쿼리 매개변수 불허")
		return
	}
	if !s.accountDiagnosticBusy.CompareAndSwap(false, true) {
		respondError(w, 409, "다른 비공개 진단 실행 중")
		return
	}
	defer s.accountDiagnosticBusy.Store(false)
	s.recoveryBusy.Store(true)
	defer s.recoveryBusy.Store(false)
	var input struct {
		Passphrase      string `json:"passphrase"`
		LookbackHours   int    `json:"lookback_hours"`
		DurationSeconds int    `json:"duration_seconds"`
	}
	if decodeStrict(r, &input) != nil || input.Passphrase == "" || (input.LookbackHours != 24 && input.LookbackHours != 72 && input.LookbackHours != 120) || (input.DurationSeconds != 30 && input.DurationSeconds != 90) {
		respondError(w, 400, "Demo 암호, 24/72/120h, 30/90s 선택")
		return
	}
	fingerprint := s.demo.KeyFingerprint()
	if s.demo.Snapshot().Status != "connected" || fingerprint == "" {
		respondError(w, 423, "연결된 Demo 계정 필요")
		return
	}
	key, secret, e := s.vault.ReadDemo(input.Passphrase)
	if e != nil || key == "" || secret == "" {
		respondError(w, 403, "암호 확인 실패")
		return
	}
	h := sha256.Sum256([]byte(key))
	if hex.EncodeToString(h[:]) != fingerprint {
		respondError(w, 409, "연결된 Demo 계정 불일치")
		return
	}
	if _, e = trading.LoadReconnectWatches(s.dataDir); e != nil {
		respondError(w, 503, "기존 재연결 증적 검증 실패")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(input.DurationSeconds)*time.Second+100*time.Second)
	defer cancel()
	since := time.Now().UTC().Add(-time.Duration(input.LookbackHours) * time.Hour).UnixMilli()
	report, runErr := s.reconnectRunner(ctx, trading.DemoCredentials{APIKey: key, Secret: secret}, s.dataDir, since, time.Duration(input.DurationSeconds)*time.Second)
	if s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint {
		respondError(w, 409, "관측 중 계정 변경: 기록 불신")
		return
	}
	archived, loadErr := trading.LoadReconnectWatches(s.dataDir)
	if loadErr != nil {
		respondError(w, 503, "재연결 진단 기록 무결성 검증 실패")
		return
	}
	saved := false
	for _, x := range archived {
		if x.ID != "" && x.ID == report.ID && x.SHA256 == report.SHA256 && !x.TradingAuthorized && !x.StreamContinuityProven && x.ManualReviewRequired && x.HistoricalOnly {
			saved = true
		}
	}
	if !saved {
		respondError(w, 503, "재연결 진단 종료 증적 불일치")
		return
	}
	if runErr != nil || ctx.Err() != nil || report.Stage != "OBSERVED" || !report.FinalRESTReviewed {
		respondError(w, 409, "관측 중단/차단: 과거 증적 수동 확인 필요")
		return
	}
	respondJSON(w, 200, map[string]any{"status": "FINITE_RECONNECT_TEST_ONLY", "stage": report.Stage, "reconnects": report.Reconnects, "epochs": report.Epochs, "final_rest_reviewed": report.FinalRESTReviewed, "blockers": report.Blockers, "historical_only": true, "trading_authorized": false, "stream_continuity_proven": false, "manual_review_required": true})
}
