package trading

import (
	"context"
	"errors"
	"time"

	"binance_trader/internal/exchange"
)

// NewFirstDemoLiveGate constructs a PREVIEW-ONLY implementation that performs
// signed, independent and fresh account/config queries just before a mock or
// future operator-approved entry. It does not give itself any credentials:
// a future controller must explicitly supply unlocked Demo-only credentials.
// It blocks any active BTC position/order pending ownership-ledger support.
func NewFirstDemoLiveGate(client *exchange.Client, intent Intent, sharePercent int) (Preflight, error) {
	if client == nil {
		return nil, errors.New("Demo 인증 조회기와 거래소 연결이 필요합니다")
	}
	if intent.Source != Manual || intent.Action != Enter || intent.OrderType != Market {
		return nil, errors.New("초기 실시간 주문 점검은 수동 시장가 진입만 지원합니다")
	}
	if sharePercent < 1 || sharePercent > 100 {
		return nil, errors.New("유효하지 않은 주문 비율")
	}
	expected, err := intent.Plan()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, p PlannedOrder, r exchange.SymbolRules, quote Quote, creds DemoCredentials) error {
		if p != expected {
			return errors.New("조회 중 주문 내용이 변경되었습니다")
		}
		if creds.APIKey == "" || creds.Secret == "" {
			return errors.New("Demo 키가 잠겼습니다")
		}
		snapshot, err := client.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, p.Symbol)
		if err != nil {
			return err
		}
		_, err = ValidateFirstDemoMarketEntryFresh(intent, p, r, quote, snapshot, sharePercent, time.Now().UTC())
		return err
	}, nil
}
