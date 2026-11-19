package server

// Explicit, operator-approved, read-only Demo income/report generation. It
// does NOT activate trade routes, persist API credentials, attest continuity,
// or silently ingest external exchange fills into an owner journal.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

func (s *Server) generateAccounting(w http.ResponseWriter, r *http.Request) {
	extendWriteDeadline(w, 95*time.Second)
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 환경에서는 읽기 전용 회계 생성도 지원하지 않습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "요청 매개변수는 JSON 본문으로만 전달합니다")
		return
	}
	if !s.accountingBusy.CompareAndSwap(false, true) {
		respondError(w, http.StatusConflict, "이미 회계 진단이 실행 중입니다")
		return
	}
	defer s.accountingBusy.Store(false)
	var input struct {
		Passphrase    string `json:"passphrase"`
		LookbackHours int    `json:"lookback_hours"`
	}
	if decodeStrict(r, &input) != nil || input.Passphrase == "" || (input.LookbackHours != 24 && input.LookbackHours != 72 && input.LookbackHours != 120) {
		respondError(w, 400, "저장된 암호와 24·72·120시간 중 조회 범위를 입력하세요")
		return
	}
	session := s.demo.Snapshot()
	fingerprint := s.demo.KeyFingerprint()
	if session.Status != "connected" || fingerprint == "" {
		respondError(w, 423, "Demo 계정 인증 연결이 먼저 필요합니다")
		return
	}
	key, secret, err := s.vault.ReadDemo(input.Passphrase)
	if err != nil || secret == "" {
		respondError(w, 403, "저장된 Demo 파일 암호가 올바르지 않습니다")
		return
	}
	keyDigest := sha256.Sum256([]byte(key))
	if hex.EncodeToString(keyDigest[:]) != fingerprint {
		respondError(w, 409, "현재 연결된 Demo 인증과 저장된 키가 다릅니다")
		return
	}
	if _, err = trading.LoadAccountingReports(s.dataDir); err != nil {
		respondError(w, 503, "기존 회계 원본이 손상되어 진단을 중단했습니다")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 80*time.Second)
	defer cancel()
	started := time.Now().UTC()
	// All account and fill amounts are sourced from the signed Demo reader
	// and local immutable journals, NEVER user/browser supplied decimals.
	report, before, err := trading.PrepareAttestedAccounting(ctx, s.dataDir, s.accountingReader,
		trading.DemoCredentials{APIKey: key, Secret: secret}, started.Add(-time.Duration(input.LookbackHours)*time.Hour).UnixMilli(), time.Now)
	if err != nil {
		respondError(w, 409, "Demo 이력과 로컬 주문·체결 기록이 완전히 일치하지 않거나 서명된 조회에 실패했습니다. 기간·이전 거래·주문 기록을 수동 확인하세요.")
		return
	}
	// The Demo login may be replaced while the slow signed query executes.
	// Refuse to archive a report into the wrong account's local workspace.
	if s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint || ctx.Err() != nil {
		respondError(w, 409, "조회 중 Demo 연결 상태가 변경되었습니다. 보고서를 저장하지 않습니다")
		return
	}
	last, err := trading.LocalAccountingFingerprint(s.dataDir)
	if err != nil || last != before {
		respondError(w, 409, "조회 이후 로컬 주문·체결 기록이 변경되어 보고서를 저장하지 않습니다")
		return
	}
	// All local writes remain immutable diagnostics, not a running ledger.
	if err := trading.ArchiveAccounting(s.dataDir, &report); err != nil {
		respondError(w, 503, "회계 보고서 저장이 불확실합니다. 기존 기록을 수동 확인하세요")
		return
	}
	respondJSON(w, 200, map[string]any{
		"status": "BOUNDED_HISTORICAL_ESTIMATE_ONLY", "report_id": report.ID,
		"window_start_ms": report.WindowStartMS, "window_end_ms": report.WindowEndMS,
		"known_orders": report.KnownOrders, "known_fills": report.KnownFills, "income_rows": report.IncomeRows,
		"owner_count": len(report.Owners), "evidence_coverage": report.EvidenceCoverage,
		"historical_only": true, "trading_authorized": false, "actual_net_verified": false,
		"index_requires_manual_rebuild": true,
		"message":                       "읽기 전용 과거 보고서를 저장했습니다. SQLite 조회에 반영하려면 report-index CLI를 검증 후 실행하세요.",
	})
}
