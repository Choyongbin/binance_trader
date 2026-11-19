package exchange

// Authenticated READ and optional one-shot DELETE adapters for Demo USD-M
// conditional algo orders. New algo POST lives in demo_algo_submit.go. The live arming coordinator
// uses signed reads here only after durable attempt evidence: order ownership, stream baseline and cancellation
// intents must be verified before first live use.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type DemoAlgoResult struct {
	AlgoID        int64     `json:"algoId"`
	ClientAlgoID  string    `json:"clientAlgoId"`
	Symbol        string    `json:"symbol"`
	Side          string    `json:"side"`
	PositionSide  string    `json:"positionSide"`
	Status        string    `json:"algoStatus"`
	Type          string    `json:"orderType"`
	AlgoType      string    `json:"algoType"`
	TriggerPrice  wsDecimal `json:"triggerPrice"`
	WorkingType   string    `json:"workingType"`
	ClosePosition bool      `json:"closePosition"`
	ReduceOnly    bool      `json:"reduceOnly"`
	Quantity      wsDecimal `json:"quantity"`
	ActualOrderID string    `json:"actualOrderId"`
	ActualQty     wsDecimal `json:"actualQty"`
}

func validAlgoStatus(s string) bool {
	switch s {
	case "NEW", "WORKING", "TRIGGERING", "TRIGGERED", "FINISHED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED":
		return true
	}
	return false
}
func (a DemoAlgoResult) check(id int64) error {
	if a.AlgoID != id || a.ClientAlgoID == "" || a.Symbol != "BTCUSDT" ||
		(a.PositionSide != "LONG" && a.PositionSide != "SHORT") ||
		(a.Side != "BUY" && a.Side != "SELL") || !validAlgoStatus(a.Status) ||
		!decimalNonNegative(string(a.Quantity), false) {
		return errors.New("조건부 주문 조회 내용 불일치")
	}
	return nil
}
func (c *Client) QueryDemoAlgo(ctx context.Context, key, secret string, algoID int64) (DemoAlgoResult, error) {
	if err := c.demoOnly(); err != nil {
		return DemoAlgoResult{}, err
	}
	if algoID <= 0 {
		return DemoAlgoResult{}, errors.New("조건부 주문 ID가 필요합니다")
	}
	raw, code, err := c.demoSignedRequest(ctx, http.MethodGet, "/fapi/v1/algoOrder", url.Values{"algoId": {strconv.FormatInt(algoID, 10)}}, key, secret)
	if err != nil {
		return DemoAlgoResult{}, err
	}
	if code != http.StatusOK {
		return DemoAlgoResult{}, statusError(raw, code, true)
	}
	var a DemoAlgoResult
	if err := json.Unmarshal(raw, &a); err != nil {
		return DemoAlgoResult{}, errors.New("조건부 주문 조회 JSON 오류")
	}
	if err := a.check(algoID); err != nil {
		return DemoAlgoResult{}, err
	}
	return a, nil
}

// One cancellation attempt only, followed by readback. Ambiguous outcomes
// remain UNKNOWN. An ACK alone does not prove sibling order cancellation or
// zero remaining owner quantity; group evaluator must audit both.
func (c *Client) CancelDemoAlgoOnce(ctx context.Context, key, secret string, algoID int64) (DemoAlgoResult, error) {
	if err := c.demoOnly(); err != nil {
		return DemoAlgoResult{}, err
	}
	if algoID <= 0 {
		return DemoAlgoResult{}, errors.New("조건부 주문 ID가 필요합니다")
	}
	raw, status, err := c.demoSignedRequest(ctx, http.MethodDelete, "/fapi/v1/algoOrder", url.Values{"algoId": {strconv.FormatInt(algoID, 10)}}, key, secret)
	if err != nil {
		return DemoAlgoResult{}, errors.New("조건부 주문 취소 결과 불명확: 조회 후 수동 조치 필요")
	}
	if status != http.StatusOK {
		return DemoAlgoResult{}, statusError(raw, status, false)
	}
	var ack struct {
		AlgoID int64           `json:"algoId"`
		Code   json.RawMessage `json:"code"`
	}
	if err := json.Unmarshal(raw, &ack); err != nil || ack.AlgoID != algoID {
		return DemoAlgoResult{}, errors.New("조건부 주문 취소 ACK 오류: 서명 조회 필요")
	}
	observed, err := c.QueryDemoAlgo(ctx, key, secret, algoID)
	if err != nil {
		return DemoAlgoResult{}, fmt.Errorf("조건부 주문 취소 후 조회 불확실: %w", err)
	}
	// TRIGGERED/FINISHED is not "cancelled"; must reconcile executed fills.
	switch observed.Status {
	case "CANCELED", "CANCELLED", "EXPIRED", "TRIGGERED", "FINISHED":
		return observed, nil
	}
	return DemoAlgoResult{}, errors.New("조건부 주문 종료 미확인: 자동 재취소 금지")
}
