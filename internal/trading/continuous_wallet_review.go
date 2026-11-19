package trading

// An advisory comparison of a signed *whole Demo account* against the local
// allocation. It is intentionally NOT a risk engine/order permit: there is no
// physical Binance collateral segregation for this internal wallet.
import (
	"errors"
	"math/big"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

func ReviewContinuousWalletExposure(dataDir, apiFingerprint string, b exchange.RecoveryBaseline) ([]string, error) {
	if dataDir == "" || len(apiFingerprint) != 64 || b.Environment != "demo" {
		return nil, errors.New("invalid account-bound wallet review inputs")
	}
	if e := exchange.ValidatePortfolioSnapshot(b.Portfolio); e != nil {
		return nil, errors.New("signed portfolio integrity failed")
	}
	w, e := wallet.Open(dataDir)
	if e != nil {
		return nil, errors.New("wallet record integrity failed")
	}
	status, e := w.SnapshotFor(apiFingerprint)
	if e != nil {
		return nil, errors.New("wallet/account binding review failed")
	}
	if !status.Verified {
		return []string{"로컬 배정 지갑의 인증 계정 연결 정보가 현재 Demo API Key와 다릅니다"}, nil
	}
	assigned, e := wallet.Decimal(status.AssignedUSDT, true)
	if e != nil {
		return nil, errors.New("invalid internal allocation amount")
	}
	issues := []string{}
	add := func(s string) { issues = append(issues, s) }
	if !b.Portfolio.CanTrade {
		add("Demo 계정 거래 가능 상태 미확인")
	}
	if b.Portfolio.MultiAssetsMargin {
		add("Multi-Assets 계정은 USDT 배정 한도 단독 검증이 불가능")
	}
	total, e := wallet.Decimal(string(b.Portfolio.TotalWalletBalance), true)
	if e != nil {
		return nil, errors.New("invalid signed wallet balance")
	}
	free, e := wallet.Decimal(string(b.Portfolio.AvailableBalance), true)
	if e != nil {
		return nil, errors.New("invalid signed available balance")
	}
	if assigned.Cmp(total) > 0 {
		add("배정 한도가 현재 서명된 계정 전체 지갑 잔고보다 큽니다")
	}
	if assigned.Cmp(free) > 0 {
		add("배정 한도가 현재 서명된 계정 가용 잔고보다 큽니다")
	}
	if len(b.OpenOrders) > 0 || len(b.OpenAlgos) > 0 {
		add("실제 계정에 모든 종목의 미체결 주문 또는 조건부 주문이 존재")
	}
	for _, p := range b.Portfolio.Positions {
		qty, ok := new(big.Rat).SetString(string(p.PositionAmt))
		if !ok {
			return nil, errors.New("invalid signed position amount")
		}
		if qty.Sign() != 0 {
			add("현재 서명 계정에 미청산 포지션 존재")
			break
		}
	}
	var usdtFound bool
	for _, asset := range b.Portfolio.Assets {
		amt, ok := new(big.Rat).SetString(string(asset.WalletBalance))
		if !ok {
			return nil, errors.New("invalid signed asset balance")
		}
		if asset.Asset == "USDT" {
			usdtFound = true
			usdtAvailable, ok := new(big.Rat).SetString(string(asset.AvailableBalance))
			if !ok {
				return nil, errors.New("invalid signed USDT available balance")
			}
			if assigned.Cmp(amt) > 0 || assigned.Cmp(usdtAvailable) > 0 {
				add("서명된 USDT 자산 지갑/가용 잔고가 내부 배정액보다 작음")
			}
		}
		if asset.Asset != "USDT" && amt.Sign() > 0 {
			add("USDT 외 자산 존재: 계정 전체 담보 위험 별도 검토 필요")
		}
	}
	if !usdtFound {
		return nil, errors.New("signed USDT asset missing")
	}
	// Equality of two samples never proves account reservation for this app.
	add("내부 배정은 실제 거래소 계정의 분리·독점 자금이 아니며 외부 거래 통제 미구현")
	return issues, nil
}
