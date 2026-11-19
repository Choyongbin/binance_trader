package exchange

// Low-level Demo-only TP/SL transport. Not connected to HTTP or an automatic
// strategy. It is UNSAFE to submit a naked leg without an exclusive coordinator,
// a persisted per-leg MAY_HAVE_SENT marker, fresh ownership/position checks and
// a separate post-submit signed GET. The transport never retries ambiguous POST.
import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/url"
)

type NewDemoProtection struct {
	ClientAlgoID string
	Symbol       string
	Side         string
	PositionSide string
	Type         string
	Quantity     string
	TriggerPrice string
	WorkingType  string
}

func (o NewDemoProtection) validate() error {
	if o.Symbol != "BTCUSDT" || !orderIDRE.MatchString(o.ClientAlgoID) ||
		(o.PositionSide != "LONG" && o.PositionSide != "SHORT") ||
		(o.Side != "SELL" && o.Side != "BUY") ||
		(o.PositionSide == "LONG" && o.Side != "SELL") ||
		(o.PositionSide == "SHORT" && o.Side != "BUY") ||
		(o.Type != "TAKE_PROFIT_MARKET" && o.Type != "STOP_MARKET") ||
		!orderDecimalRE.MatchString(o.Quantity) || !positiveNumber(o.Quantity) ||
		!orderDecimalRE.MatchString(o.TriggerPrice) || !positiveNumber(o.TriggerPrice) {
		return errors.New("보호 주문 종목·방향·수량·가격·ID가 유효하지 않습니다")
	}
	if o.Type == "TAKE_PROFIT_MARKET" && o.WorkingType != "CONTRACT_PRICE" ||
		o.Type == "STOP_MARKET" && o.WorkingType != "MARK_PRICE" {
		return errors.New("TP는 CONTRACT_PRICE, SL은 MARK_PRICE만 허용합니다")
	}
	return nil
}

// SubmitDemoProtection sends ONE new conditional order. Ack is NOT evidence
// of OCO, actual position coverage or the sibling being present. In Hedge
// Mode quantity is mandatory; closePosition and reduceOnly are not sent.
// Only the fully audited trading protection arming coordinator may call this method.
func (c *Client) SubmitDemoProtection(ctx context.Context, key, secret string, o NewDemoProtection) (DemoAlgoResult, error) {
	if err := c.demoOnly(); err != nil {
		return DemoAlgoResult{}, err
	}
	if err := o.validate(); err != nil {
		return DemoAlgoResult{}, err
	}
	params := url.Values{
		"algoType": {"CONDITIONAL"}, "symbol": {o.Symbol}, "side": {o.Side},
		"positionSide": {o.PositionSide}, "type": {o.Type}, "quantity": {o.Quantity},
		"triggerPrice": {o.TriggerPrice}, "workingType": {o.WorkingType},
		"clientAlgoId": {o.ClientAlgoID}, "newOrderRespType": {"ACK"},
	}
	raw, status, err := c.demoSignedRequest(ctx, http.MethodPost, "/fapi/v1/algoOrder", params, key, secret)
	if err != nil {
		return DemoAlgoResult{}, err
	}
	if status != http.StatusOK {
		return DemoAlgoResult{}, statusError(raw, status, false)
	}
	var ack struct {
		AlgoID        int64     `json:"algoId"`
		ClientAlgoID  string    `json:"clientAlgoId"`
		AlgoType      string    `json:"algoType"`
		Symbol        string    `json:"symbol"`
		Side          string    `json:"side"`
		PositionSide  string    `json:"positionSide"`
		OrderType     string    `json:"orderType"`
		Quantity      wsDecimal `json:"quantity"`
		TriggerPrice  wsDecimal `json:"triggerPrice"`
		WorkingType   string    `json:"workingType"`
		ClosePosition bool      `json:"closePosition"`
		ReduceOnly    bool      `json:"reduceOnly"`
		Status        string    `json:"algoStatus"`
	}
	if err := json.Unmarshal(raw, &ack); err != nil || ack.AlgoID <= 0 ||
		ack.ClientAlgoID != o.ClientAlgoID || ack.AlgoType != "CONDITIONAL" ||
		ack.Symbol != o.Symbol || ack.Side != o.Side || ack.PositionSide != o.PositionSide ||
		ack.OrderType != o.Type || ack.WorkingType != o.WorkingType ||
		ack.ClosePosition || ack.ReduceOnly || !validAlgoStatus(ack.Status) ||
		!samePositiveDecimal(string(ack.Quantity), o.Quantity) ||
		!samePositiveDecimal(string(ack.TriggerPrice), o.TriggerPrice) {
		return DemoAlgoResult{}, errors.New("조건부 주문 ACK 불완전/불일치: 같은 ID로 자동 재전송 금지")
	}
	return DemoAlgoResult{AlgoID: ack.AlgoID, ClientAlgoID: ack.ClientAlgoID, Symbol: ack.Symbol,
		Side: ack.Side, PositionSide: ack.PositionSide, Type: ack.OrderType,
		Status: ack.Status, Quantity: ack.Quantity, AlgoType: ack.AlgoType, TriggerPrice: ack.TriggerPrice, WorkingType: ack.WorkingType, ClosePosition: ack.ClosePosition, ReduceOnly: ack.ReduceOnly}, nil
}

func samePositiveDecimal(a, b string) bool {
	if !orderDecimalRE.MatchString(a) || !positiveNumber(a) ||
		!orderDecimalRE.MatchString(b) || !positiveNumber(b) {
		return false
	}
	x, ok := new(big.Rat).SetString(a)
	if !ok {
		return false
	}
	y, ok := new(big.Rat).SetString(b)
	return ok && x.Cmp(y) == 0
}
