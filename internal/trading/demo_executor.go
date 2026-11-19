package trading

import (
	"context"
	"errors"
	"fmt"
	"time"

	"binance_trader/internal/exchange"
)

// DemoCredentials are ephemeral. The adapter only requests them during a
// call, avoiding serialized secrets and avoiding any UI-supplied credentials.
type DemoCredentials struct{ APIKey, Secret string }
type CredentialsProvider func(context.Context) (DemoCredentials, error)
type QuoteProvider func(context.Context, string) (Quote, error)
type Preflight func(context.Context, PlannedOrder, exchange.SymbolRules, Quote, DemoCredentials) error

// DemoExecutor is built but intentionally NOT instantiated in the web server.
// A future operator-approved workflow must wire Preflight to independently
// fetched account/config data and a reconciled position ledger.
type DemoExecutor struct {
	client      *exchange.Client
	credentials CredentialsProvider
	quote       QuoteProvider
	gate        Preflight
}

func NewDemoExecutor(client *exchange.Client, credentials CredentialsProvider, quote QuoteProvider, gate Preflight) (*DemoExecutor, error) {
	if client == nil || credentials == nil || quote == nil || gate == nil {
		return nil, errors.New("Demo 어댑터 필수 안전 점검기가 없습니다")
	}
	return &DemoExecutor{client: client, credentials: credentials, quote: quote, gate: gate}, nil
}
func (d *DemoExecutor) durableFinalGuardContract() {}

func (d *DemoExecutor) Submit(ctx context.Context, p PlannedOrder) (Submission, error) {
	return d.submit(ctx, p, nil)
}

// SubmitWithFinalCheck is the only executor contract accepted by the durable
// lifecycle dispatcher. finalCheck runs after account/config gate and quote
// freshness checks, immediately before SubmitDemoOrder.
func (d *DemoExecutor) SubmitWithFinalCheck(ctx context.Context, p PlannedOrder, finalCheck func(context.Context) error) (Submission, error) {
	if finalCheck == nil {
		return Submission{}, errors.New("최종 주문 안전 점검기가 필요합니다")
	}
	return d.submit(ctx, p, finalCheck)
}

func (d *DemoExecutor) submit(ctx context.Context, p PlannedOrder, finalCheck func(context.Context) error) (Submission, error) {
	if err := ctx.Err(); err != nil {
		return Submission{}, err
	}
	if p.Symbol != "BTCUSDT" || p.PositionSide != Long && p.PositionSide != Short {
		return Submission{}, errors.New("현재는 Demo BTCUSDT Hedge Mode만 지원합니다")
	}
	rules, err := d.client.FetchSymbolRules(ctx, p.Symbol)
	if err != nil {
		return Submission{}, fmt.Errorf("거래소 규칙 조회 실패: %w", err)
	}
	if p.Type != Market && p.Type != Limit {
		return Submission{}, errors.New("지원하지 않는 주문 종류")
	}
	q, err := d.quote(ctx, p.Symbol)
	if err != nil {
		return Submission{}, err
	}
	if q.Symbol != p.Symbol || q.ReceivedAt.IsZero() || q.ReceivedAt.After(time.Now().Add(time.Second)) || time.Since(q.ReceivedAt) > 3*time.Second {
		return Submission{}, errors.New("실시간 시장 가격이 오래되었습니다")
	}
	refPrice := q.Price
	if p.Type == Limit {
		refPrice = p.Price
	}
	if err = ValidatePlannedOrder(p, rules, refPrice, time.Now()); err != nil {
		return Submission{}, err
	}
	creds, err := d.credentials(ctx)
	if err != nil {
		return Submission{}, err
	}
	if creds.APIKey == "" || creds.Secret == "" {
		return Submission{}, errors.New("Demo 키가 잠겨 있습니다")
	}
	if err = d.gate(ctx, p, rules, q, creds); err != nil {
		return Submission{}, fmt.Errorf("Demo 주문 점검 실패: %w", err)
	}
	// Signed account/config reads can take long enough for the original quote
	// to become stale. Re-fetch the market quote after account preflight so the
	// final side-effect boundary never relies on the earlier price snapshot.
	freshQ, err := d.quote(ctx, p.Symbol)
	if err != nil {
		return Submission{}, fmt.Errorf("최종 시장 가격 재조회 실패: %w", err)
	}
	now := time.Now()
	if freshQ.Symbol != p.Symbol || freshQ.ReceivedAt.IsZero() || freshQ.ReceivedAt.After(now.Add(time.Second)) || now.Sub(freshQ.ReceivedAt) > 3*time.Second {
		return Submission{}, errors.New("계정 점검 이후 시장 데이터가 오래되었습니다. 신규 주문을 중단합니다")
	}
	validationPrice := freshQ.Price
	if p.Type == Limit {
		validationPrice = p.Price
	}
	if err := ValidatePlannedOrder(p, rules, validationPrice, now); err != nil {
		return Submission{}, err
	}
	if finalCheck != nil {
		if err := finalCheck(ctx); err != nil {
			return Submission{}, fmt.Errorf("MAY_HAVE_SENT 이후 최종 안전 점검 실패: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return Submission{}, err
		}
	}
	// This check is unconditional, including the manual/internal executor path.
	// STOP-after-POST has begun is inherently ambiguous and still needs signed
	// reconciliation; an in-flight HTTP POST cannot be rolled back locally.
	if err := ctx.Err(); err != nil {
		return Submission{}, err
	}
	result, err := d.client.SubmitDemoOrder(ctx, creds.APIKey, creds.Secret, exchange.NewOrderRequest{
		Symbol: p.Symbol, Side: p.ExchangeSide, PositionSide: string(p.PositionSide), Type: string(p.Type), Quantity: p.Quantity, Price: p.Price, TimeInForce: p.TimeInForce, ClientOrderID: p.ClientOrderID,
	})
	if err != nil {
		return Submission{}, err
	}
	return Submission{ClientOrderID: result.ClientOrderID, OrderID: result.OrderID, Status: result.Status}, nil
}
func (d *DemoExecutor) QueryByClientOrderID(ctx context.Context, symbol, clientID string) (Submission, error) {
	creds, err := d.credentials(ctx)
	if err != nil {
		return Submission{}, err
	}
	if creds.APIKey == "" || creds.Secret == "" {
		return Submission{}, errors.New("Demo 키가 잠겨 있습니다")
	}
	res, err := d.client.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, symbol, clientID)
	if err != nil {
		return Submission{}, err
	}
	return Submission{ClientOrderID: res.ClientOrderID, OrderID: res.OrderID, Status: res.Status}, nil
}
