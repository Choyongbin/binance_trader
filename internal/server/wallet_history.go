package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/big"
	"time"

	"binance_trader/internal/exchange"
)

type walletAuditBalance struct {
	AvailableUSDT string
	FinishedAt    time.Time
}

// auditedWalletAvailable performs a fresh signed, whole-account Demo read.
// A bounded history is enough to check CURRENT exposure for an internal
// allocation edit; it is never continuous coverage or order authorization.
func (s *Server) auditedWalletAvailable(ctx context.Context, passphrase, fingerprint string) (walletAuditBalance, error) {
	key, secret, err := s.vault.ReadDemo(passphrase)
	if err != nil || secret == "" {
		return walletAuditBalance{}, errors.New("저장된 Demo 암호를 확인하세요")
	}
	digest := sha256.Sum256([]byte(key))
	if hex.EncodeToString(digest[:]) != fingerprint {
		return walletAuditBalance{}, errors.New("연결된 계정과 저장된 Demo API 키가 일치하지 않습니다")
	}
	if s.accountingReader == nil {
		return walletAuditBalance{}, errors.New("서명된 전체 계정 조회가 준비되지 않았습니다")
	}
	baseline, err := s.accountingReader.FetchDemoRecoveryBaseline(ctx, key, secret, time.Now().UTC().Add(-24*time.Hour).UnixMilli())
	if err != nil || ctx.Err() != nil {
		// Never return a third-party error; it might contain credential material.
		return walletAuditBalance{}, errors.New("Demo 전체 계정 서명 조회에 실패했습니다")
	}
	now := time.Now().UTC()
	if baseline.Environment != "demo" || baseline.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		baseline.StartedAt.IsZero() || baseline.FinishedAt.Before(baseline.StartedAt) || baseline.FinishedAt.After(now.Add(time.Second)) ||
		now.Sub(baseline.FinishedAt) > 10*time.Second || baseline.FinishedAt.Sub(baseline.StartedAt) > 30*time.Second ||
		baseline.WindowStartMS <= 0 || baseline.WindowEndMS <= baseline.WindowStartMS || baseline.WindowEndMS != baseline.StartedAt.UnixMilli() ||
		baseline.OpenOrders == nil || baseline.OpenAlgos == nil || len(baseline.OpenOrders) != 0 || len(baseline.OpenAlgos) != 0 ||
		baseline.RecentBTCOrders == nil || baseline.RecentBTCAlgos == nil || baseline.RecentBTCTrades == nil || baseline.RecentIncome == nil ||
		baseline.Portfolio.MultiAssetsMargin ||
		exchange.ValidatePortfolioSnapshot(baseline.Portfolio) != nil {
		return walletAuditBalance{}, errors.New("전체 계정 관측이 오래됐거나 미체결 주문이 있습니다")
	}
	for _, value := range []string{string(baseline.Portfolio.TotalInitialMargin), string(baseline.Portfolio.TotalPositionInitialMargin), string(baseline.Portfolio.TotalOpenOrderInitialMargin)} {
		margin, ok := new(big.Rat).SetString(value)
		if !ok || margin.Sign() != 0 {
			return walletAuditBalance{}, errors.New("전체 계정에 사용 중인 증거금이 있습니다")
		}
	}
	for _, position := range baseline.Portfolio.Positions {
		qty, ok := new(big.Rat).SetString(string(position.PositionAmt))
		if !ok || qty.Sign() != 0 {
			return walletAuditBalance{}, errors.New("전체 계정에 포지션이 남아 있습니다")
		}
	}
	for _, asset := range baseline.Portfolio.Assets {
		if asset.Asset == "USDT" {
			return walletAuditBalance{AvailableUSDT: string(asset.AvailableBalance), FinishedAt: baseline.FinishedAt}, nil
		}
	}
	return walletAuditBalance{}, errors.New("전체 계정에 USDT 잔액이 없습니다")
}
