package server

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/trading"
)

func (s *Server) buildTradingReadinessInput(now time.Time) trading.TradingReadinessInput {
	in := trading.TradingReadinessInput{
		Environment:         string(s.store.Snapshot().Environment),
		MaxMarketAge:        5 * time.Second,
		MaxAccountAge:       20 * time.Second,
		ExecutionAuditState: trading.ExecutionAuditBlocked,
		ProtectionUnsettled: -1,
	}
	localErrors := []string{}

	market := s.live.Status()
	in.MarketState = market.State
	in.MarketReady = market.Ready
	in.MarketAge = time.Duration(market.AgeMs) * time.Millisecond

	account := s.demo.Snapshot()
	in.AccountStatus = account.Status
	in.AccountEnvironment = account.Environment
	if account.AsOf != "" {
		if at, err := time.Parse(time.RFC3339Nano, account.AsOf); err == nil {
			in.AccountAsOf = at
		} else if at, err := time.Parse(time.RFC3339, account.AsOf); err == nil {
			in.AccountAsOf = at
		} else {
			localErrors = append(localErrors, "account as_of 형식 오류")
		}
	}
	if s.readinessCapability != nil {
		in.AccountCanTradeKnown, in.AccountCanTrade = s.readinessCapability()
	}

	walletStatus, err := s.wallet.SnapshotFor(s.demo.KeyFingerprint())
	if err != nil {
		localErrors = append(localErrors, "wallet: "+err.Error())
	} else {
		in.WalletVerified = walletStatus.Verified
		in.WalletAssignedUSDT = walletStatus.AssignedUSDT
	}

	held, err := trading.MutationGatePresent(s.dataDir)
	if err != nil {
		in.MutationGateUnknown = true
		localErrors = append(localErrors, "mutation gate: "+err.Error())
	}
	in.MutationGatePresent = held

	if _, err := os.Lstat(filepath.Join(s.dataDir, "trading", "coordinator.active")); err == nil {
		in.CoordinatorLockPresent = true
	} else if !errors.Is(err, os.ErrNotExist) {
		localErrors = append(localErrors, "coordinator lock: "+err.Error())
	}

	execution, err := trading.ReviewExecutionLifecycles(s.dataDir)
	if err != nil {
		localErrors = append(localErrors, "execution lifecycle: "+err.Error())
	} else {
		in.ExecutionAuditState = execution.State
	}

	unsettled, arming, err := trading.ReviewProtectionReadiness(s.dataDir)
	if err != nil {
		localErrors = append(localErrors, "protection: "+err.Error())
	} else {
		in.ProtectionUnsettled = unsettled
		in.ProtectionArmingState = arming
	}

	if _, _, err := trading.CheckWalletEditHistory(s.dataDir); err != nil {
		// This broad local audit catches unresolved manual cancels, incomplete
		// reservations, open local lots, and corrupt event/gap evidence which
		// are not fully represented by the specialized readiness fields.
		localErrors = append(localErrors, "local evidence: "+err.Error())
	}
	if len(localErrors) > 0 {
		in.LocalEvidenceError = strings.Join(localErrors, " | ")
	}
	return in
}

func (s *Server) tradingReadiness(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "조회 매개변수는 지원하지 않습니다")
		return
	}
	now := time.Now()
	result, err := trading.EvaluateTradingReadiness(s.buildTradingReadinessInput(now), now)
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "자동매매 readiness 판정 실패: "+err.Error())
		return
	}
	respondJSON(w, http.StatusOK, result)
}
