package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// TradingConfiguration is obtained from Binance, never inferred from the UI.
// Hedge mode is an ACCOUNT-WIDE setting. Margin and leverage are SYMBOL-WIDE.
type TradingConfiguration struct {
	Environment     string   `json:"environment"`
	Symbol          string   `json:"symbol"`
	HedgeMode       bool     `json:"hedge_mode"`
	MarginType      string   `json:"margin_type"`
	Leverage        int      `json:"leverage"`
	AutoAddMargin   bool     `json:"auto_add_margin"`
	AsOf            string   `json:"as_of"`
	SettingsMatchV1 bool     `json:"settings_match_v1"`
	Warnings        []string `json:"warnings"`
}

type ConfigInspector interface {
	InspectTradingConfiguration(context.Context, string, string, string) (TradingConfiguration, error)
}

type ConfigMutator interface {
	ConfigInspector
	SetHedgeModeForDemo(context.Context, string, string, bool) error
	SetIsolatedForDemo(context.Context, string, string) error
	SetLeverageForDemo(context.Context, string, string, int) error
}

// InspectTradingConfiguration is strictly read-only. It does not attempt to
// change account-wide Hedge Mode when it discovers a mismatch.
func (c *Client) InspectTradingConfiguration(ctx context.Context, key, secret, symbol string) (TradingConfiguration, error) {
	if symbol != "BTCUSDT" {
		return TradingConfiguration{}, errors.New("1차 지원 종목은 BTCUSDT뿐입니다")
	}
	if strings.TrimSpace(key) == "" || secret == "" {
		return TradingConfiguration{}, errors.New("데모 인증이 필요합니다")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return TradingConfiguration{}, err
	}
	var positionMode struct {
		DualSidePosition *bool `json:"dualSidePosition"`
	}
	if err := c.signedGet(ctx, "/fapi/v1/positionSide/dual", url.Values{}, key, secret, offset, &positionMode); err != nil {
		return TradingConfiguration{}, fmt.Errorf("포지션 모드 조회: %w", err)
	}
	if positionMode.DualSidePosition == nil {
		return TradingConfiguration{}, errors.New("포지션 모드 응답이 누락되었습니다")
	}
	var symbols []struct {
		Symbol        string `json:"symbol"`
		MarginType    string `json:"marginType"`
		Leverage      int    `json:"leverage"`
		AutoAddMargin *bool  `json:"isAutoAddMargin"`
	}
	if err := c.signedGet(ctx, "/fapi/v1/symbolConfig", url.Values{"symbol": {symbol}}, key, secret, offset, &symbols); err != nil {
		return TradingConfiguration{}, fmt.Errorf("종목 설정 조회: %w", err)
	}
	if len(symbols) != 1 || symbols[0].Symbol != symbol || (symbols[0].MarginType != "ISOLATED" && symbols[0].MarginType != "CROSSED") || symbols[0].Leverage < 1 || symbols[0].Leverage > 125 || symbols[0].AutoAddMargin == nil {
		return TradingConfiguration{}, errors.New("BTCUSDT 거래 설정 응답이 불완전하거나 유효하지 않습니다")
	}
	result := TradingConfiguration{
		Environment: "demo", Symbol: symbol, HedgeMode: *positionMode.DualSidePosition,
		MarginType: symbols[0].MarginType, Leverage: symbols[0].Leverage,
		AutoAddMargin: *symbols[0].AutoAddMargin,
		AsOf:          c.now().UTC().Format(time.RFC3339Nano),
		Warnings:      []string{},
	}
	if !result.HedgeMode {
		result.Warnings = append(result.Warnings, "단방향 모드입니다. Hedge Mode 전환은 계정 전체에 영향을 주므로 자동 변경하지 않습니다")
	}
	if result.MarginType != "ISOLATED" {
		result.Warnings = append(result.Warnings, "BTCUSDT 증거금 모드가 격리가 아닙니다")
	}
	if result.Leverage != 15 {
		result.Warnings = append(result.Warnings, "첫 검증 모델에 필요한 BTCUSDT 레버리지 15배와 다릅니다. 종목의 LONG·SHORT가 레버리지를 공유합니다")
	}
	if result.AutoAddMargin {
		result.Warnings = append(result.Warnings, "자동 격리 증거금 추가가 활성화되어 있습니다")
	}
	result.SettingsMatchV1 = result.HedgeMode && result.MarginType == "ISOLATED" && result.Leverage == 15 && !result.AutoAddMargin
	return result, nil
}

// configSignedPost is an internal building block, intentionally not exposed
// through an HTTP route yet. All production writes are restricted to DemoBase.
func (c *Client) configSignedPost(ctx context.Context, path string, params url.Values, key, secret string) error {
	if c.base != DemoBase && !c.localMock {
		return errors.New("실거래 계정 설정 변경은 비활성화되어 있습니다")
	}
	if key == "" || secret == "" {
		return errors.New("데모 인증 정보가 필요합니다")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return err
	}
	params.Set("timestamp", strconv.FormatInt(c.now().Add(offset).UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	query := params.Encode()
	signature := signatureHex(query, secret)
	// Binance accepts signed, URL-encoded POST parameters in the request body.
	form := query + "&signature=" + signature
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, strings.NewReader(form))
	if err != nil {
		return errors.New("설정 변경 요청 생성 실패")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-MBX-APIKEY", key)
	res, err := c.http.Do(req)
	if err != nil {
		return errors.New("설정 변경 결과를 확인하지 못했습니다. 재요청 전에 거래소에서 현재 설정을 조회하세요")
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if err != nil {
		return errors.New("설정 변경 응답을 읽지 못했습니다. 거래소 설정을 다시 조회하세요")
	}
	var reply map[string]json.RawMessage
	if err := json.Unmarshal(raw, &reply); err != nil || reply == nil {
		return errors.New("설정 변경 응답 형식 오류. 거래소 설정을 다시 조회하세요")
	}
	var code int
	if field, exists := reply["code"]; exists {
		if err := json.Unmarshal(field, &code); err != nil {
			return errors.New("설정 변경 응답 코드 오류")
		}
	}
	if res.StatusCode != http.StatusOK || (code != 0 && code != 200) {
		return fmt.Errorf("거래소 설정 변경 실패 (HTTP %d / 코드 %d). 실제 거래소 설정을 다시 조회하세요", res.StatusCode, code)
	}
	if path == "/fapi/v1/marginType" && code != 200 {
		return errors.New("격리 변경 확인 응답이 누락되었습니다. 실제 설정을 조회하세요")
	}
	if path == "/fapi/v1/leverage" {
		var returned struct {
			Symbol   string `json:"symbol"`
			Leverage int    `json:"leverage"`
		}
		if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&returned); err != nil || returned.Symbol != "BTCUSDT" || strconv.Itoa(returned.Leverage) != params.Get("leverage") {
			return errors.New("레버리지 변경 응답이 요청과 다릅니다. 실제 설정을 조회하세요")
		}
	}
	return nil
}

// Demo configuration writers are intentionally narrow and hard-blocked on non-Demo bases.
// A later phase must first check open orders, positions and explicit operator
// consent, then verify post-change configuration. Never call them on retry.
func (c *Client) SetHedgeModeForDemo(ctx context.Context, key, secret string, enabled bool) error {
	value := "false"
	if enabled {
		value = "true"
	}
	return c.configSignedPost(ctx, "/fapi/v1/positionSide/dual", url.Values{"dualSidePosition": {value}}, key, secret)
}
func (c *Client) SetIsolatedForDemo(ctx context.Context, key, secret string) error {
	return c.configSignedPost(ctx, "/fapi/v1/marginType", url.Values{"symbol": {"BTCUSDT"}, "marginType": {"ISOLATED"}}, key, secret)
}
func (c *Client) SetLeverageForDemo(ctx context.Context, key, secret string, leverage int) error {
	if leverage < 1 || leverage > 125 {
		return errors.New("레버리지는 1~125 범위여야 합니다")
	}
	return c.configSignedPost(ctx, "/fapi/v1/leverage", url.Values{"symbol": {"BTCUSDT"}, "leverage": {strconv.Itoa(leverage)}}, key, secret)
}
