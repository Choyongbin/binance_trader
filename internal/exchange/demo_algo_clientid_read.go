package exchange

// Demo clientAlgoId lookup is a signed USER_DATA GET only. It is deliberately
// separate from all POST/DELETE transports: no caller may infer permission to
// retransmit an uncertain protection order from either a match or a -2013.
import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
)

// QueryDemoAlgoByClientID finds a previously attempted USD-M conditional
// order using the stable clientAlgoId even when the initial POST ACK did not
// provide its numeric algoId. It does not submit/cancel/authorize any order.
//
// ErrOrderNotFound is an INCONCLUSIVE bounded lookup result, never evidence
// that the exchange did not receive the order. All other API and transport
// failures must be reconciled as unknown outcomes (never automatic retry).
func (c *Client) QueryDemoAlgoByClientID(ctx context.Context, key, secret, clientAlgoID string) (DemoAlgoResult, error) {
	if err := c.demoOnly(); err != nil {
		return DemoAlgoResult{}, err
	}
	if ctx == nil || !orderIDRE.MatchString(clientAlgoID) {
		return DemoAlgoResult{}, errors.New("조건부 주문의 유효한 clientAlgoId와 context가 필요합니다")
	}
	raw, status, err := c.demoSignedRequest(ctx, http.MethodGet, "/fapi/v1/algoOrder",
		url.Values{"clientAlgoId": {clientAlgoID}}, key, secret)
	if err != nil {
		return DemoAlgoResult{}, err
	}
	if status != http.StatusOK {
		return DemoAlgoResult{}, statusError(raw, status, true)
	}
	// Preserve presence of security-critical boolean fields. A missing
	// closePosition/reduceOnly key must not silently become Go's false value.
	var presence struct {
		ClosePosition *bool `json:"closePosition"`
		ReduceOnly    *bool `json:"reduceOnly"`
	}
	if err := json.Unmarshal(raw, &presence); err != nil ||
		presence.ClosePosition == nil || presence.ReduceOnly == nil {
		return DemoAlgoResult{}, errors.New("signed Algo response missing required protection flags")
	}
	var result DemoAlgoResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return DemoAlgoResult{}, errors.New("clientAlgoId 조건부 주문의 signed GET JSON 오류")
	}
	if err := result.check(result.AlgoID); err != nil || result.AlgoID <= 0 ||
		result.ClientAlgoID != clientAlgoID || result.AlgoType != "CONDITIONAL" ||
		(result.Type != "STOP_MARKET" && result.Type != "TAKE_PROFIT_MARKET") ||
		(result.WorkingType != "MARK_PRICE" && result.WorkingType != "CONTRACT_PRICE") ||
		result.ClosePosition || result.ReduceOnly ||
		!orderDecimalRE.MatchString(string(result.Quantity)) ||
		!positiveNumber(string(result.Quantity)) ||
		!orderDecimalRE.MatchString(string(result.TriggerPrice)) ||
		!positiveNumber(string(result.TriggerPrice)) {
		return DemoAlgoResult{}, errors.New("clientAlgoId 조건부 주문의 identity/type/price/quantity 오류")
	}
	// The caller must compare side, workingType, trigger, quantity and
	// immutable plan identity again. A signed read alone proves neither
	// account-wide ownership nor an exchange-atomic OCO relationship.
	return result, nil
}
