package server

// Operator-triggered, BOUNDED and strictly READ-ONLY Demo recovery diagnosis.
// A short-lived private account WebSocket overlaps a signed whole-account
// REST baseline. The observer is deliberately STOPPED on return, therefore
// even a fully reviewed result NEVER permits manual or automatic orders.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

func (s *Server) runRecoveryProbe(w http.ResponseWriter, r *http.Request) {
	extendWriteDeadline(w, 125*time.Second)
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 계정 복구 진단은 지원하지 않습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "복구 진단 매개변수는 JSON만 허용합니다")
		return
	}
	// Share a dedicated exclusive in-process guard with the existing private
	// handshake probe. Each durable store additionally enforces an on-disk
	// exclusive lock. A crash does not silently clear an incomplete review.
	if !s.accountDiagnosticBusy.CompareAndSwap(false, true) {
		respondError(w, http.StatusConflict, "다른 비공개 계정 진단이 실행 중입니다")
		return
	}
	defer s.accountDiagnosticBusy.Store(false)
	s.recoveryBusy.Store(true)
	defer s.recoveryBusy.Store(false)

	var input struct {
		Passphrase    string `json:"passphrase"`
		LookbackHours int    `json:"lookback_hours"`
	}
	if decodeStrict(r, &input) != nil || input.Passphrase == "" ||
		(input.LookbackHours != 24 && input.LookbackHours != 72 && input.LookbackHours != 120) {
		respondError(w, http.StatusBadRequest, "암호와 최근 24·72·120시간 중 조회 범위를 입력하세요")
		return
	}
	session := s.demo.Snapshot()
	fingerprint := s.demo.KeyFingerprint()
	if session.Status != "connected" || fingerprint == "" {
		respondError(w, http.StatusLocked, "먼저 Demo 계정을 인증하여 연결해 주세요")
		return
	}
	key, secret, err := s.vault.ReadDemo(input.Passphrase)
	if err != nil || secret == "" {
		respondError(w, http.StatusForbidden, "저장된 Demo 암호를 확인해 주세요")
		return
	}
	hash := sha256.Sum256([]byte(key))
	if hex.EncodeToString(hash[:]) != fingerprint {
		respondError(w, http.StatusConflict, "저장된 인증 파일과 연결 중인 Demo 계정이 다릅니다")
		return
	}
	// Never trust a pre-existing corrupt / interrupted immutable diagnostic.
	// OpenRecoveryProbeStore will also check these before its exclusive lock.
	if _, err := trading.LoadRecoveryProbes(s.dataDir); err != nil {
		respondError(w, http.StatusServiceUnavailable, "기존 복구 증적이 손상되어 수동 확인이 필요합니다")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 110*time.Second)
	defer cancel()
	since := time.Now().UTC().Add(-time.Duration(input.LookbackHours) * time.Hour).UnixMilli()
	probe, err := s.recoveryRunner(ctx, trading.DemoCredentials{APIKey: key, Secret: secret}, s.dataDir, since)
	// Never return an upstream error: it could embed API secrets or a listenKey.
	if s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint {
		respondError(w, http.StatusConflict, "진단 중 Demo 계정이 변경되었습니다. 과거 보고서는 현재 계정의 상태 증명이 아닙니다")
		return
	}
	if err != nil || ctx.Err() != nil {
		respondError(w, http.StatusConflict, "계정 스트림 또는 서명된 계정 복구 진단이 중단되었습니다. 기록을 확인하고 필요하면 수동 조사하세요")
		return
	}
	// The runner must have durably archived the terminal result. A fake,
	// interrupted, or altered run must not be displayed as a completed review.
	records, readErr := trading.LoadRecoveryProbes(s.dataDir)
	if readErr != nil || probe.ID == "" || probe.Stage != "REVIEWED" || probe.TradingAuthorized || !probe.ManualReviewRequired || !probe.StaleAfterObservation {
		respondError(w, http.StatusServiceUnavailable, "복구 진단 종료 기록의 무결성을 확인할 수 없습니다")
		return
	}
	var persisted bool
	for _, rec := range records {
		if rec.ID == probe.ID && rec.Stage == "REVIEWED" && rec.SHA256 == probe.SHA256 {
			persisted = true
			break
		}
	}
	if !persisted {
		respondError(w, http.StatusServiceUnavailable, "완료된 복구 진단 원본과 저장된 증적이 다릅니다")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"status":                   "HISTORICAL_REVIEW_ONLY",
		"stage":                    probe.Stage,
		"started_at":               probe.StartedAt,
		"finished_at":              probe.FinishedAt,
		"observer_connected":       probe.ObserverConnected,
		"rest_reviewed":            probe.RESTReviewed,
		"event_count":              probe.EventCount,
		"discontinuity_count":      probe.DiscontinuityCount,
		"blockers":                 probe.Blockers,
		"stale_after_observation":  true,
		"trading_authorized":       false,
		"stream_continuity_proven": false,
		"manual_review_required":   true,
		"message":                  "과거 Demo 비공개 WS와 서명 전체 REST 관측을 비교했습니다. 관측 종료로 연속성은 보장되지 않으며 주문은 계속 잠겨 있습니다.",
	})
}
