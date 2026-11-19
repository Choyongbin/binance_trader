// Package exchange is a READ-ONLY Binance USD-M futures REST adapter.
// It has no trading POST endpoints. The production URL is an explicit allowlist.
package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DemoBase = "https://demo-fapi.binance.com"
const RealBase = "https://fapi.binance.com"

type Balance struct {
	Asset      string `json:"asset"`
	Wallet     string `json:"balance"`
	Available  string `json:"availableBalance"`
	CrossUnPnl string `json:"crossUnPnl"`
}
type Position struct {
	Symbol         string `json:"symbol"`
	Side           string `json:"positionSide"`
	Qty            string `json:"positionAmt"`
	Entry          string `json:"entryPrice"`
	BreakEven      string `json:"breakEvenPrice"`
	Mark           string `json:"markPrice"`
	PnL            string `json:"unRealizedProfit"`
	Liquidation    string `json:"liquidationPrice"`
	IsolatedMargin string `json:"isolatedMargin"`
	Leverage       string `json:"leverage"`
	MarginType     string `json:"marginType"`
}
type Order struct {
	ClientOrderID string `json:"clientOrderId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	Status        string `json:"status"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	OrderID       int64  `json:"orderId"`
	UpdateTime    int64  `json:"updateTime"`
}
type AlgoOrder struct {
	ClientAlgoID  string `json:"clientAlgoId"`
	ClosePosition bool   `json:"closePosition"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	OrderType     string `json:"orderType"`
	AlgoStatus    string `json:"algoStatus"`
	Quantity      string `json:"quantity"`
	TriggerPrice  string `json:"triggerPrice"`
	AlgoID        int64  `json:"algoId"`
	WorkingType   string `json:"workingType"`
}
type Snapshot struct {
	Status      string      `json:"status"`
	Environment string      `json:"environment"`
	AsOf        string      `json:"as_of"`
	Balances    []Balance   `json:"balances"`
	Positions   []Position  `json:"positions"`
	Orders      []Order     `json:"orders"`
	AlgoOrders  []AlgoOrder `json:"algo_orders"`
	Error       string      `json:"error,omitempty"`
}

type Client struct {
	base      string
	http      *http.Client
	now       func() time.Time
	localMock bool // only NewWithBase for local tests; never set in production
}

func NewDemoClient() *Client {
	return &Client{base: DemoBase, http: &http.Client{Timeout: 7 * time.Second}, now: time.Now}
}

// NewWithBase exists only for local mock tests; production always uses NewDemoClient.
func NewWithBase(base string, client *http.Client) *Client {
	u, err := url.Parse(base)
	isMock := err == nil && u.Scheme == "http" && (u.Hostname() == "localhost" || (net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback()))
	return &Client{base: base, http: client, now: time.Now, localMock: isMock}
}
func (c *Client) get(ctx context.Context, path string, values url.Values, apiKey string) ([]byte, error) {
	uri := c.base + path
	if len(values) > 0 {
		uri += "?" + values.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, errors.New("요청 생성에 실패했습니다")
	}
	if apiKey != "" {
		req.Header.Set("X-MBX-APIKEY", apiKey)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, errors.New("거래소에 연결할 수 없습니다. 인터넷과 API 주소를 확인하세요")
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return nil, errors.New("거래소 응답을 읽지 못했습니다")
	}
	if res.StatusCode != http.StatusOK {
		var remote struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(body, &remote)
		switch remote.Code {
		case -2015, -2014:
			return nil, errors.New("API 키, 권한 또는 IP 허용 목록을 확인하세요")
		case -1021:
			return nil, errors.New("요청 시간 오류: PC 시각을 동기화하세요")
		case -1003:
			return nil, errors.New("거래소 요청 제한에 도달했습니다. 잠시 후 다시 시도하세요")
		}
		return nil, fmt.Errorf("거래소 조회 실패 (HTTP %d / 코드 %d)", res.StatusCode, remote.Code)
	}
	return body, nil
}
func (c *Client) serverOffset(ctx context.Context) (time.Duration, error) {
	raw, err := c.get(ctx, "/fapi/v1/time", nil, "")
	if err != nil {
		return 0, err
	}
	var payload struct {
		ServerTime int64 `json:"serverTime"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || payload.ServerTime <= 0 {
		return 0, errors.New("거래소 서버 시간을 확인할 수 없습니다")
	}
	return time.Duration(payload.ServerTime-c.now().UnixMilli()) * time.Millisecond, nil
}
func (c *Client) signedGet(ctx context.Context, path string, params url.Values, key, secret string, offset time.Duration, out any) error {
	params.Set("timestamp", strconv.FormatInt(c.now().Add(offset).UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	query := params.Encode()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(query))
	query += "&signature=" + hex.EncodeToString(mac.Sum(nil))
	uri := c.base + path + "?" + query
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return errors.New("요청 생성에 실패했습니다")
	}
	req.Header.Set("X-MBX-APIKEY", key)
	// Never follow redirects while transmitting a signed URL and API key.
	// A 3xx response is an error, not authorization to contact another host.
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := hc.Do(req)
	if err != nil {
		return errors.New("거래소에 연결할 수 없습니다. 인터넷 연결을 확인하세요")
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return errors.New("거래소 응답을 읽지 못했습니다")
	}
	if res.StatusCode != 200 {
		var remote struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(raw, &remote)
		switch remote.Code {
		case -2015, -2014:
			return errors.New("API 키, 권한 또는 IP 허용 목록을 확인하세요")
		case -1021:
			return errors.New("요청 시간 오류: PC 시각을 동기화하세요")
		case -1003:
			return errors.New("요청 제한에 도달했습니다")
		}
		return fmt.Errorf("거래소 조회 실패 (HTTP %d / 코드 %d)", res.StatusCode, remote.Code)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return errors.New("거래소 JSON 응답 형식 오류")
	}
	return nil
}

// Fetch is all-or-nothing. No partial or previous balances are returned if
// any individual account endpoint fails.
func (c *Client) Fetch(ctx context.Context, key, secret string) (Snapshot, error) {
	if strings.TrimSpace(key) == "" || secret == "" {
		return Snapshot{}, errors.New("인증 정보가 없습니다")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	result := Snapshot{Status: "connected", Environment: "demo"}
	if err = c.signedGet(ctx, "/fapi/v3/balance", url.Values{}, key, secret, offset, &result.Balances); err != nil {
		return Snapshot{}, err
	}
	if err = c.signedGet(ctx, "/fapi/v3/positionRisk", url.Values{"symbol": {"BTCUSDT"}}, key, secret, offset, &result.Positions); err != nil {
		return Snapshot{}, err
	}
	if err = c.signedGet(ctx, "/fapi/v1/openOrders", url.Values{"symbol": {"BTCUSDT"}}, key, secret, offset, &result.Orders); err != nil {
		return Snapshot{}, err
	}
	if err = c.signedGet(ctx, "/fapi/v1/openAlgoOrders", url.Values{"symbol": {"BTCUSDT"}}, key, secret, offset, &result.AlgoOrders); err != nil {
		return Snapshot{}, err
	}
	if err := validateDisplaySnapshot(result, "BTCUSDT"); err != nil {
		return Snapshot{}, err
	}
	filtered := make([]Position, 0, len(result.Positions))
	for _, p := range result.Positions {
		qty, _ := new(big.Rat).SetString(p.Qty) // already strictly validated
		if qty.Sign() != 0 {
			filtered = append(filtered, p)
		}
	}
	result.Positions = filtered
	result.AsOf = c.now().UTC().Format(time.RFC3339)
	return result, nil
}

// signatureHex signs an already URL-encoded Binance parameter string.
func signatureHex(query, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}
