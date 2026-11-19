package server

// The operator must explicitly arm this READ-ONLY monitor on every server
// start. No password is saved or silently decrypted during restart. GET only
// reports its process-local live state or verified, fail-closed disk evidence.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

// monitorRunner exists for deterministic offline fault-injection tests.
type monitorRunner func(context.Context, trading.DemoCredentials, string, time.Duration, func(trading.MonitorCheckpoint)) (trading.MonitorCheckpoint, error)

type monitorControl struct {
	mu          sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	live        trading.MonitorCheckpoint
	lastError   string
	fingerprint string
}

func (s *Server) stopContinuousMonitor() bool {
	s.monitor.mu.Lock()
	cancel := s.monitor.cancel
	done := s.monitor.done
	s.monitor.mu.Unlock()
	if cancel == nil {
		return true
	}
	cancel()
	select {
	case <-done:
		return true
	case <-time.After(7 * time.Second):
		return false
	}
}
func (s *Server) monitorStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "query is not supported")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo monitor only")
		return
	}
	s.monitor.mu.Lock()
	running := s.monitor.done != nil
	live := s.monitor.live
	errLabel := s.monitor.lastError
	fingerprint := s.monitor.fingerprint
	s.monitor.mu.Unlock()
	uncertain := false
	if running && (s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint) {
		live.State = trading.MonitorUncertain
		live.Reason = "ACCOUNT_CHANGED"
		live.ExposureSHA256 = ""
		live.LastSignedRESTAt = time.Time{}
		live.StreamContinuityProven = false
		live.ContinuityScope = ""
		live.ContinuitySince = time.Time{}
		live.ContinuityGapDigest = ""
		live.Issues = []string{"Demo 계정 연결 상태 또는 인증 계정이 관측 시작과 다름"}
		uncertain = true
	}
	if !running {
		disk, stale, e := trading.ReadContinuousMonitor(s.dataDir)
		if e != nil {
			respondError(w, 503, "monitor archive corruption or inaccessible; trading locked")
			return
		}
		live = disk
		uncertain = stale
	}
	// Defensive copy: issues is a slice shared with the worker callback.
	live.Issues = append([]string{}, live.Issues...)
	continuityProven := running && !uncertain && trading.LiveMonitorContinuityProven(live, time.Now().UTC())
	continuityScope := ""
	continuitySince := time.Time{}
	if continuityProven {
		continuityScope = live.ContinuityScope
		continuitySince = live.ContinuitySince
	}
	respondJSON(w, 200, map[string]any{
		"running": running, "stale_or_interrupted": uncertain, "state": live.State, "epoch": live.Epoch,
		"reconnects": live.Reconnects, "last_signed_rest_at": live.LastSignedRESTAt,
		"last_signed_exposure_sha256": live.ExposureSHA256, "gap_digest": live.GapDigest,
		"continuity_scope": continuityScope, "continuity_since": continuitySince,
		"reason": live.Reason, "issues": live.Issues, "last_error_category": errLabel,
		"trading_authorized": false, "stream_continuity_proven": continuityProven, "manual_review_required": true,
		"read_only": true, "restart_requires_passphrase": true,
	})
}
func (s *Server) startContinuousMonitor(w http.ResponseWriter, r *http.Request) {
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo-only read-only monitor")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "query is not supported")
		return
	}
	var input struct {
		Passphrase    string `json:"passphrase"`
		LookbackHours int    `json:"lookback_hours"`
	}
	if decodeStrict(r, &input) != nil || input.Passphrase == "" || (input.LookbackHours != 24 && input.LookbackHours != 72 && input.LookbackHours != 120) {
		respondError(w, 400, "Demo passphrase and 24/72/120h lookback required")
		return
	}
	if !s.accountDiagnosticBusy.CompareAndSwap(false, true) {
		respondError(w, 409, "another private diagnostic/monitor active")
		return
	}
	release := true
	defer func() {
		if release {
			s.accountDiagnosticBusy.Store(false)
		}
	}()
	if held, e := trading.MutationGatePresent(s.dataDir); e != nil || held {
		respondError(w, 409, "wallet/coordinator mutation gate active")
		return
	}
	fingerprint := s.demo.KeyFingerprint()
	if fingerprint == "" || s.demo.Snapshot().Status != "connected" {
		respondError(w, 423, "connected Demo account required")
		return
	}
	key, secret, e := s.vault.ReadDemo(input.Passphrase)
	if e != nil || key == "" || secret == "" {
		respondError(w, 403, "vault passphrase invalid")
		return
	}
	h := sha256.Sum256([]byte(key))
	if hex.EncodeToString(h[:]) != fingerprint {
		respondError(w, 409, "vault account fingerprint changed")
		return
	}
	// Refuse to auto-clear an orphaned stream/monitor lock after a crash.
	disk, stale, e := trading.ReadContinuousMonitor(s.dataDir)
	if e != nil || stale {
		respondError(w, 503, "monitor archive/stale session requires offline manual review")
		return
	}
	_ = disk
	s.monitor.mu.Lock()
	if s.monitor.done != nil {
		s.monitor.mu.Unlock()
		respondError(w, 409, "monitor already running")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.monitor.cancel = cancel
	s.monitor.done = done
	s.monitor.live = trading.MonitorCheckpoint{Schema: 1, State: trading.MonitorUncertain, Issues: []string{"awaiting signed REST"}, ManualReviewRequired: true}
	s.monitor.lastError = ""
	s.monitor.fingerprint = fingerprint
	s.monitor.mu.Unlock()
	run := s.monitorRunner
	if run == nil {
		run = func(ctx context.Context, creds trading.DemoCredentials, dir string, lookback time.Duration, callback func(trading.MonitorCheckpoint)) (trading.MonitorCheckpoint, error) {
			return trading.WatchLiveDemoContinuously(ctx, exchange.NewDemoClient(), creds, dir, lookback, callback)
		}
	}
	go func() {
		defer func() {
			s.monitor.mu.Lock()
			s.monitor.cancel = nil
			s.monitor.done = nil
			s.monitor.fingerprint = ""
			s.monitor.mu.Unlock()
			s.accountDiagnosticBusy.Store(false)
			close(done)
		}()
		report, runErr := run(ctx, trading.DemoCredentials{APIKey: key, Secret: secret}, s.dataDir, time.Duration(input.LookbackHours)*time.Hour, func(cp trading.MonitorCheckpoint) {
			if s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint {
				cancel()
			}
			cp.Issues = append([]string{}, cp.Issues...)
			s.monitor.mu.Lock()
			s.monitor.live = cp
			s.monitor.mu.Unlock()
		})
		report.Issues = append([]string{}, report.Issues...)
		s.monitor.mu.Lock()
		s.monitor.live = report
		if runErr != nil {
			s.monitor.lastError = "MONITOR_STOPPED_OR_BLOCKED"
		}
		s.monitor.mu.Unlock()
	}()
	release = false
	respondJSON(w, 202, map[string]any{"state": "UNCERTAIN", "message": "operator-armed read-only monitoring; REST/account continuity unapproved", "trading_authorized": false, "stream_continuity_proven": false, "manual_review_required": true})
}
func (s *Server) stopContinuousMonitorHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "query is not supported")
		return
	}
	// Require strict empty JSON just like the other authenticated mutations.
	var input struct{}
	if decodeStrict(r, &input) != nil {
		respondError(w, 400, "empty JSON object required")
		return
	}
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	if !s.stopContinuousMonitor() {
		respondError(w, 503, "monitor did not stop; stream lock must be investigated")
		return
	}
	respondJSON(w, 200, map[string]any{"state": "STOPPED", "trading_authorized": false, "stream_continuity_proven": false, "manual_review_required": true})
}
