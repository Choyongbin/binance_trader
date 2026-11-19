package server

import (
	"binance_trader/internal/app"
	"binance_trader/internal/trading"
	"context"
	"net/http"
	"time"
)

// Read-only demo quote and sizing diagnostic; never makes a write request and
// cannot obtain an order-execution interface. Broker-signed account/config
// are independently checked by the pure validator. Caller fields never
// supply market price, account size or exchange rule filters.
func (s *Server) manualPreview(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 주문 사전 점검은 잠겨 있습니다")
		return
	}
	var input trading.ManualPreviewInput
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, 400, "잘못된 시장가 사전 점검 형식")
		return
	}
	if (input.OrderType != trading.Market && input.OrderType != trading.Limit) || input.Side != trading.Long && input.Side != trading.Short {
		respondError(w, 400, "현재는 LONG/SHORT 시장가·지정가 사전 점검만 지원합니다")
		return
	}
	if input.OrderType == trading.Limit && input.LimitPrice == "" || input.OrderType == trading.Market && input.LimitPrice != "" {
		respondError(w, 400, "시장가/지정가 입력 가격 오류")
		return
	}
	if s.demo.Snapshot().Status != "connected" {
		respondError(w, 423, "먼저 Demo 계정을 연결해 주세요")
		return
	}
	fingerprint := s.demo.KeyFingerprint()
	if fingerprint == "" {
		respondError(w, 423, "Demo API 인증이 필요합니다")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	// On-demand signed refresh: the normal 10-second UI cache is too old
	// for a five-second trading-related read-only preview.
	if err := s.demo.Refresh(ctx); err != nil {
		respondError(w, 503, "Demo 계정 실시간 재조회에 실패했습니다")
		return
	}
	startAccount := s.demo.Snapshot()
	quote, err := s.manualQuote()
	if err != nil {
		respondError(w, 503, "실시간 WebSocket 시세가 준비되지 않았습니다")
		return
	}
	config, err := s.demo.InspectTradingConfiguration(ctx, "BTCUSDT")
	if err != nil {
		respondError(w, 503, "최신 Demo 거래소 설정 조회에 실패했습니다")
		return
	}
	rules, err := s.demo.InspectSymbolRules(ctx, "BTCUSDT")
	if err != nil {
		respondError(w, 503, "최신 Demo 거래소 주문 규칙 조회에 실패했습니다")
		return
	}
	// The two public/signed reads can take long. Reject an account refresh or
	// credential/environment change between the initial and final observations.
	if s.store.Snapshot().Environment != app.Demo || s.demo.KeyFingerprint() != fingerprint ||
		s.demo.Snapshot().AsOf != startAccount.AsOf || s.demo.Snapshot().Status != "connected" {
		respondError(w, 409, "사전 점검 중 계정 상태 또는 인증 정보가 변경되었습니다. 다시 조회해 주세요")
		return
	}
	walletState, err := s.wallet.SnapshotFor(fingerprint)
	if err != nil || !walletState.Verified {
		respondError(w, 409, "자동매매 지갑 장부 검증 실패: 수동 점검 필요")
		return
	}
	result, err := trading.PreviewManualMarket(input, rules, quote, startAccount, config, walletState.AssignedUSDT, time.Now().UTC())
	// An independent UI wallet mutation can invalidate the preview in flight.
	finalWallet, walletErr := s.wallet.SnapshotFor(fingerprint)
	if walletErr != nil || !finalWallet.Verified || finalWallet.Revision != walletState.Revision || finalWallet.AssignedUSDT != walletState.AssignedUSDT {
		respondError(w, 409, "사전 점검 중 자동매매 배정액이 변경되었습니다. 다시 조회해 주세요")
		return
	}
	if err != nil {
		respondError(w, 409, err.Error())
		return
	}
	respondJSON(w, 200, result)
}
