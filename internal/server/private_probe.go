package server

// Demo-only, one-shot PRIVATE WebSocket transport diagnosis. Requires an
// authenticated Demo session AND a fresh local encrypted-vault passphrase.
// This route cannot submit/cancel orders, nor turn a diagnosis into a permit.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

func (s *Server) privateProbeStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "조회 매개변수를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 사설 스트림 진단은 지원하지 않습니다")
		return
	}
	reports, err := trading.LoadPrivateProbes(s.dataDir)
	if err != nil {
		respondError(w, 503, "사설 스트림 과거 기록이 손상되었습니다. 수동 검토가 필요합니다")
		return
	}
	status := map[string]any{"historical_only": true, "trading_authorized": false, "stream_continuity_proven": false, "report_count": len(reports), "status": "NOT_RUN", "busy": s.privateProbeBusy.Load()}
	if len(reports) > 0 {
		last := reports[0].Result
		status["status"] = last.Status
		status["checked_at"] = last.FinishedAt
		status["handshake_ok"] = last.HandshakeOK
		status["event_count"] = last.EventCount
		status["event_types"] = last.EventTypes
		status["credential_bound"] = last.CredentialBound
		status["dial_route"] = last.DialRoute
		status["dial_phase"] = last.DialPhase
		status["dial_http_status"] = last.DialHTTPStatus
		status["dial_elapsed_ms"] = last.DialElapsedMS
	}
	respondJSON(w, 200, status)
}
func (s *Server) runPrivateProbe(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 사설 스트림 진단은 지원하지 않습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "진단 설정값은 서버에서 고정합니다")
		return
	}
	if !s.accountDiagnosticBusy.CompareAndSwap(false, true) {
		respondError(w, http.StatusConflict, "다른 비공개 계정 진단이 실행 중입니다")
		return
	}
	defer s.accountDiagnosticBusy.Store(false)
	if !s.privateProbeBusy.CompareAndSwap(false, true) {
		respondError(w, http.StatusConflict, "다른 사설 스트림 진단이 실행 중입니다")
		return
	}
	defer s.privateProbeBusy.Store(false)
	var input struct {
		Passphrase string `json:"passphrase"`
	}
	if decodeStrict(r, &input) != nil || input.Passphrase == "" {
		respondError(w, 400, "암호를 다시 입력해 주세요")
		return
	}
	session := s.demo.Snapshot()
	fingerprint := s.demo.KeyFingerprint()
	if session.Status != "connected" || fingerprint == "" {
		respondError(w, 423, "인증된 Demo 계정 연결이 먼저 필요합니다")
		return
	}
	key, secret, err := s.vault.ReadDemo(input.Passphrase)
	if err != nil {
		respondError(w, 403, "Demo 암호 확인에 실패했습니다")
		return
	}
	// The vault is independent of the live connection. Never allow a stale
	// encrypted file / switched Demo account to bind this probe accidentally.
	hash := sha256.Sum256([]byte(key))
	if hex.EncodeToString(hash[:]) != fingerprint || secret == "" {
		respondError(w, 409, "현재 연결된 Demo 계정과 암호화 저장 파일이 다릅니다")
		return
	}
	// v0.4.4 intentionally verifies PRIVATE transport and schema only; complete
	// signed historical REST account reconciliation remains separate and locked.
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	probe := s.privateProbeRunner(ctx, key)
	probe.CredentialBound = true
	if s.store.Snapshot().Environment != app.Demo || s.demo.KeyFingerprint() != fingerprint || s.demo.Snapshot().Status != "connected" {
		probe.Status = "SESSION_CHANGED"
		probe.CredentialBound = false
	}
	if _, err = trading.ArchivePrivateProbe(s.dataDir, probe); err != nil {
		respondError(w, 503, "사설 스트림 진단 기록을 안전하게 저장하지 못했습니다. 수동 검토가 필요합니다")
		return
	}
	respondJSON(w, 200, probe)
}
