package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// A missing order is NOT proof that an ambiguous POST failed: Binance can
// return -2013 temporarily or for old historical orders. NEVER re-submit on
// this result; reconciliation remains manual and read-only.
var ErrOrderNotFound = errors.New("거래소에서 주문을 찾지 못했습니다. 동일 주문 재전송 금지")

// These types intentionally omit reduceOnly: Binance forbids it in Hedge Mode.
// STOP/TP orders belong to the separate Algo Order API in a later release.
type NewOrderRequest struct {
	Symbol        string
	Side          string
	PositionSide  string
	Type          string
	Quantity      string
	Price         string
	TimeInForce   string
	ClientOrderID string
}
type OrderResult struct {
	Symbol        string `json:"symbol"`
	ClientOrderID string `json:"clientOrderId"`
	OrderID       int64  `json:"orderId"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	Status        string `json:"status"`
	OriginalQty   string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AveragePrice  string `json:"avgPrice"`
}

var orderIDRE = regexp.MustCompile(`^[.A-Z:/a-z0-9_-]{1,36}$`)
var orderDecimalRE = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,12})?$`)

func (r NewOrderRequest) valid() error {
	if r.Symbol != "BTCUSDT" || !orderIDRE.MatchString(r.ClientOrderID) {
		return errors.New("Demo BTCUSDT 및 유효한 주문 ID만 지원합니다")
	}
	if r.PositionSide != "LONG" && r.PositionSide != "SHORT" {
		return errors.New("Hedge Mode 포지션 방향이 필요합니다")
	}
	if r.Side != "BUY" && r.Side != "SELL" {
		return errors.New("BUY/SELL이 필요합니다")
	}
	if !orderDecimalRE.MatchString(r.Quantity) || !positiveNumber(r.Quantity) {
		return errors.New("수량이 유효하지 않습니다")
	}
	switch r.Type {
	case "MARKET":
		if r.Price != "" || r.TimeInForce != "" {
			return errors.New("시장가 주문의 지정가 필드가 유효하지 않습니다")
		}
	case "LIMIT":
		if r.TimeInForce != "GTC" || !orderDecimalRE.MatchString(r.Price) || !positiveNumber(r.Price) {
			return errors.New("지정가는 GTC와 유효한 가격이 필요합니다")
		}
	default:
		return errors.New("시장가 및 지정가만 지원합니다")
	}
	return nil
}
func positiveNumber(s string) bool {
	for _, c := range s {
		if c >= '1' && c <= '9' {
			return true
		}
	}
	return false
}
func orderStatusAllowed(s string) bool {
	switch s {
	case "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
		return true
	}
	return false
}
func (c *Client) demoOnly() error {
	if c == nil || (c.base != DemoBase && !c.localMock) {
		return errors.New("실거래 주문 및 조회가 비활성화되어 있습니다")
	}
	return nil
}
func (c *Client) demoSignedRequest(ctx context.Context, method, path string, params url.Values, key, secret string) ([]byte, int, error) {
	if err := c.demoOnly(); err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(key) == "" || secret == "" {
		return nil, 0, errors.New("Demo 인증 정보가 필요합니다")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return nil, 0, err
	}
	params.Set("timestamp", strconv.FormatInt(c.now().Add(offset).UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	query := params.Encode()
	payload := query + "&signature=" + signatureHex(query, secret)
	uri := c.base + path
	var body io.Reader
	if method == http.MethodGet || method == http.MethodDelete {
		uri += "?" + payload
	} else {
		body = strings.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		return nil, 0, errors.New("주문 요청을 생성하지 못했습니다")
	}
	req.Header.Set("X-MBX-APIKEY", key)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	// Signed order requests MUST NOT follow redirects with a credential header.
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, errors.New("거래소의 주문 결과를 확인할 수 없습니다. 주문 ID로 조회하기 전 재전송 금지")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, 0, errors.New("거래소 주문 응답을 읽지 못했습니다. 주문 ID로 다시 조회하세요")
	}
	return raw, resp.StatusCode, nil
}
func statusError(raw []byte, status int, query bool) error {
	var remote struct {
		Code int `json:"code"`
	}
	_ = json.Unmarshal(raw, &remote)
	if query && remote.Code == -2013 {
		return ErrOrderNotFound
	}
	if status == http.StatusTooManyRequests || status == 418 || remote.Code == -1003 {
		return errors.New("주문 API 호출 한도에 도달했습니다. 기존 주문 상태를 조회하세요")
	}
	if remote.Code == -1021 {
		return errors.New("거래소와 PC 시각이 일치하지 않습니다. 기존 주문 상태부터 조회하세요")
	}
	if remote.Code == -2015 || remote.Code == -2014 {
		return errors.New("Demo API 키의 권한 또는 IP 허용 목록을 확인하세요")
	}
	// Exchange error text may contain user-supplied data; never log raw body.
	return fmt.Errorf("Binance Demo 주문 API 응답: HTTP %d, code %d (재전송 금지)", status, remote.Code)
}

// SubmitDemoOrder is deliberately NOT connected to any web route or session
// credential provider in this release. Higher-level trading.DemoExecutor
// enforces a fresh independent preflight and exchange rules before calling.
func (c *Client) SubmitDemoOrder(ctx context.Context, key, secret string, r NewOrderRequest) (OrderResult, error) {
	if err := c.demoOnly(); err != nil {
		return OrderResult{}, err
	}
	if err := r.valid(); err != nil {
		return OrderResult{}, err
	}
	p := url.Values{"symbol": {r.Symbol}, "side": {r.Side}, "positionSide": {r.PositionSide}, "type": {r.Type}, "quantity": {r.Quantity}, "newClientOrderId": {r.ClientOrderID}, "newOrderRespType": {"ACK"}}
	if r.Type == "LIMIT" {
		p.Set("price", r.Price)
		p.Set("timeInForce", r.TimeInForce)
	}
	raw, status, err := c.demoSignedRequest(ctx, http.MethodPost, "/fapi/v1/order", p, key, secret)
	if err != nil {
		return OrderResult{}, err
	}
	if status != http.StatusOK {
		return OrderResult{}, statusError(raw, status, false)
	}
	var result OrderResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return OrderResult{}, errors.New("주문 ACK 형식 오류: 주문 ID 조회 전 재전송 금지")
	}
	if result.Symbol != r.Symbol || result.ClientOrderID != r.ClientOrderID || result.OrderID <= 0 || result.Side != r.Side || result.PositionSide != r.PositionSide || result.Type != r.Type || !orderStatusAllowed(result.Status) {
		return OrderResult{}, errors.New("주문 ACK 내용 불일치: 주문 ID 조회 전 재전송 금지")
	}
	return result, nil
}

// QueryDemoOrder performs READ-ONLY reconciliation using origClientOrderId.
func (c *Client) QueryDemoOrder(ctx context.Context, key, secret, symbol, clientID string) (OrderResult, error) {
	if err := c.demoOnly(); err != nil {
		return OrderResult{}, err
	}
	if symbol != "BTCUSDT" || !orderIDRE.MatchString(clientID) {
		return OrderResult{}, errors.New("유효한 BTCUSDT 주문 ID가 필요합니다")
	}
	p := url.Values{"symbol": {symbol}, "origClientOrderId": {clientID}}
	raw, status, err := c.demoSignedRequest(ctx, http.MethodGet, "/fapi/v1/order", p, key, secret)
	if err != nil {
		return OrderResult{}, err
	}
	if status != http.StatusOK {
		return OrderResult{}, statusError(raw, status, true)
	}
	var result OrderResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return OrderResult{}, errors.New("거래소 주문 조회 JSON 오류")
	}
	if result.Symbol != symbol || result.ClientOrderID != clientID || result.OrderID <= 0 || !orderStatusAllowed(result.Status) {
		return OrderResult{}, errors.New("거래소 주문 조회 내용 불완전")
	}
	return result, nil
}

// CancelDemoOrder issues at most ONE signed DELETE and then performs a fresh
// signed readback. Cancellation does not erase previously executed fills:
// higher-level ownership reconciliation MUST import partial fills and check
// position quantity before releasing any owner quantity reservation. This
// method is an unwired internal adapter; no HTTP/UI path exposes it.
// On ANY ambiguous failure callers must query, NEVER automatically re-DELETE.
func (c *Client) CancelDemoOrder(ctx context.Context, key, secret, symbol, clientID string) (OrderResult, error) {
	if err := c.demoOnly(); err != nil {
		return OrderResult{}, err
	}
	if symbol != "BTCUSDT" || !orderIDRE.MatchString(clientID) {
		return OrderResult{}, errors.New("유효한 Demo BTCUSDT 주문 ID가 필요합니다")
	}
	params := url.Values{"symbol": {symbol}, "origClientOrderId": {clientID}}
	raw, status, err := c.demoSignedRequest(ctx, http.MethodDelete, "/fapi/v1/order", params, key, secret)
	if err != nil {
		return OrderResult{}, fmt.Errorf("취소 결과 불확실: %w", err)
	}
	if status != http.StatusOK {
		return OrderResult{}, statusError(raw, status, false)
	}
	var ack OrderResult
	if err := json.Unmarshal(raw, &ack); err != nil || ack.Symbol != symbol || ack.ClientOrderID != clientID || ack.OrderID <= 0 || !orderStatusAllowed(ack.Status) {
		return OrderResult{}, errors.New("취소 ACK 불완전: 서명된 GET으로 확인 필요")
	}
	final, err := c.QueryDemoOrder(ctx, key, secret, symbol, clientID)
	if err != nil {
		return OrderResult{}, fmt.Errorf("취소 결과 조회 불확실: %w", err)
	}
	if final.OrderID != ack.OrderID || final.Symbol != symbol || final.ClientOrderID != clientID ||
		final.PositionSide != ack.PositionSide || final.Side != ack.Side || final.Type != ack.Type ||
		final.OriginalQty == "" || final.ExecutedQty == "" {
		return OrderResult{}, errors.New("취소 전후 주문 ID·방향·수량 조회 불일치")
	}
	switch final.Status {
	case "CANCELED", "FILLED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
		return final, nil
	default:
		return OrderResult{}, errors.New("취소 조회에서 주문이 아직 종료되지 않았습니다. 자동 재취소 금지")
	}
}
