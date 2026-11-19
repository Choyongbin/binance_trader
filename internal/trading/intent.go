// Package trading implements a fail-closed order request journal and the
// foundations of the shared manual/model order coordinator. Production HTTP
// routes for submission remain LOCKED until Demo E2E verification.
package trading

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type PositionSide string

const (
	Long  PositionSide = "LONG"
	Short PositionSide = "SHORT"
)

type Action string

const (
	Enter Action = "ENTER"
	Exit  Action = "EXIT"
)

type OrderType string

const (
	Market OrderType = "MARKET"
	Limit  OrderType = "LIMIT"
)

type Source string

const (
	Manual Source = "manual"
	Model  Source = "model"
)

// Each intent is a QUANTITY-based order plan, NOT a promise of fill.
// Conversion of USDT collateral to exchange-filter-conforming BTC quantity is
// implemented in the later manual-execution phase.
type Intent struct {
	RequestID    string       `json:"request_id"`
	Source       Source       `json:"source"`
	ModelID      string       `json:"model_id,omitempty"`
	Symbol       string       `json:"symbol"`
	PositionSide PositionSide `json:"position_side"`
	Action       Action       `json:"action"`
	OrderType    OrderType    `json:"order_type"`
	Quantity     string       `json:"quantity"`
	Price        string       `json:"price,omitempty"`
	// SignalCandleMS is set only by the automatic model path. It is part of
	// the durable intent digest so the same model/candle decision cannot be
	// silently remapped to a different order after restart. Legacy model
	// records may legitimately have zero.
	SignalCandleMS int64 `json:"signal_candle_ms,omitempty"`
}

type PlannedOrder struct {
	ClientOrderID string       `json:"client_order_id"`
	Symbol        string       `json:"symbol"`
	PositionSide  PositionSide `json:"position_side"`
	ExchangeSide  string       `json:"side"`
	Type          OrderType    `json:"type"`
	Quantity      string       `json:"quantity"`
	Price         string       `json:"price,omitempty"`
	TimeInForce   string       `json:"time_in_force,omitempty"`
}

var (
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,80}$`)
	modelIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)
	quantityPattern  = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,8})?$`)
)

func validPositiveDecimal(s string) bool {
	if !quantityPattern.MatchString(s) {
		return false
	}
	for _, c := range s {
		if c >= '1' && c <= '9' {
			return true
		}
	}
	return false
}
func (i Intent) Validate() error {
	if !requestIDPattern.MatchString(i.RequestID) {
		return errors.New("요청 ID는 8~80자의 영문·숫자·_-이어야 합니다")
	}
	if i.Source != Manual && i.Source != Model {
		return errors.New("알 수 없는 주문 출처입니다")
	}
	if i.Source == Model && !modelIDPattern.MatchString(i.ModelID) {
		return errors.New("유효한 모델 ID가 필요합니다")
	}
	if i.Source == Manual && i.ModelID != "" {
		return errors.New("수동 주문에 모델 ID를 지정할 수 없습니다")
	}
	if i.SignalCandleMS != 0 {
		if i.Source != Model || i.SignalCandleMS < 0 || i.SignalCandleMS%60000 != 0 {
			return errors.New("자동매매 신호 캔들 시각이 유효하지 않습니다")
		}
	}
	if i.Symbol != "BTCUSDT" {
		return errors.New("지원하지 않는 종목입니다")
	}
	if i.PositionSide != Long && i.PositionSide != Short {
		return errors.New("LONG/SHORT가 필요합니다")
	}
	if i.Action != Enter && i.Action != Exit {
		return errors.New("진입 또는 청산을 지정하세요")
	}
	if i.OrderType != Market && i.OrderType != Limit {
		return errors.New("시장가 또는 지정가만 지원합니다")
	}
	if !validPositiveDecimal(i.Quantity) {
		return errors.New("수량은 양의 십진 문자열이어야 합니다")
	}
	if i.OrderType == Limit && !validPositiveDecimal(i.Price) {
		return errors.New("지정가 주문에는 양의 지정 가격이 필요합니다")
	}
	if i.OrderType == Market && i.Price != "" {
		return errors.New("시장가 주문에 지정 가격을 넣을 수 없습니다")
	}
	return nil
}
func (i Intent) Plan() (PlannedOrder, error) {
	if err := i.Validate(); err != nil {
		return PlannedOrder{}, err
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{"demo", string(i.Source), i.ModelID, i.RequestID}, ":")))
	id := "bt_" + hex.EncodeToString(sum[:])[:28] // 31 chars; Binance max 36 chars
	side := "BUY"
	if (i.PositionSide == Long && i.Action == Exit) || (i.PositionSide == Short && i.Action == Enter) {
		side = "SELL"
	}
	plan := PlannedOrder{ClientOrderID: id, Symbol: i.Symbol, PositionSide: i.PositionSide, ExchangeSide: side, Type: i.OrderType, Quantity: i.Quantity, Price: i.Price}
	if i.OrderType == Limit {
		plan.TimeInForce = "GTC"
	}
	return plan, nil
}
func digestIntent(i Intent) (string, error) {
	raw, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("요청 직렬화 실패: %w", err)
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}
