package trading

// Private Binance account events are SIGNALS for reconciliation, not a source
// from which to reconstruct complete trading history: user stream has no
// documented gap-free sequence, ACCOUNT_UPDATE is sparse, and TP/SL may use
// separate algo status updates. This decoder stores only allowlisted data,
// never raw listenKey, and stops on unknown/unsafe events.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
)

type AccountEvent struct {
	Kind      string         `json:"kind"`
	EventTime int64          `json:"event_time"`
	TxTime    int64          `json:"tx_time,omitempty"`
	Order     *StreamOrder   `json:"order,omitempty"`
	Account   *StreamAccount `json:"account,omitempty"`
	// HASH identifies byte-for-byte equivalent JSON after key ordering; it
	// does not serve as a server-side sequence or prove completeness.
	Digest string `json:"digest"`
}

type StreamOrder struct {
	Symbol        string `json:"symbol"`
	ClientID      string `json:"client_id"`
	OrderID       int64  `json:"order_id"`
	TradeID       int64  `json:"trade_id"`
	PositionSide  string `json:"position_side"`
	Side          string `json:"side"`
	Execution     string `json:"execution"`
	Status        string `json:"status"`
	CumulativeQty string `json:"cumulative_qty"`
	LastQty       string `json:"last_qty"`
	LastPrice     string `json:"last_price"`
	Fee           string `json:"fee"`
	FeeAsset      string `json:"fee_asset"`
	RealizedPnL   string `json:"realized_pnl"`
}

type StreamAccount struct {
	Reason string `json:"reason"`
	// Sparse update => these arrays MUST NOT overwrite complete snapshots.
	Assets          []string `json:"changed_assets"`
	PositionSymbols []string `json:"changed_position_symbols"`
}

// AccountUpdate uses only the relevant codes; unknown events are NOT silently
// ignored as they can signify a changed balance/position/strategy.
func ParseAccountEvent(raw []byte) (AccountEvent, error) {
	if len(raw) == 0 || len(raw) > 128<<10 {
		return AccountEvent{}, errors.New("비정상 계정 이벤트 크기")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]json.RawMessage
	if err := dec.Decode(&obj); err != nil {
		return AccountEvent{}, errors.New("계정 이벤트 JSON 오류")
	}
	var second any
	if err := dec.Decode(&second); !errors.Is(err, io.EOF) {
		return AccountEvent{}, errors.New("JSON 뒤 추가 데이터")
	}
	// Combined stream envelope (if Demo provides one). No stream key is kept.
	if x, ok := obj["data"]; ok {
		if _, has := obj["stream"]; !has {
			return AccountEvent{}, errors.New("통합 스트림 형식 오류")
		}
		return ParseAccountEvent(x)
	}
	var hdr struct {
		Kind      string `json:"e"`
		EventTime int64  `json:"E"`
		TxTime    int64  `json:"T"`
	}
	if err := json.Unmarshal(raw, &hdr); err != nil || hdr.Kind == "" || hdr.EventTime <= 0 {
		return AccountEvent{}, errors.New("계정 이벤트 유형 또는 시간 누락")
	}
	if hdr.TxTime < 0 {
		return AccountEvent{}, errors.New("음수 이벤트 처리 시간")
	}
	// json.Unmarshal into interface uses float64; canonicalize with UseNumber.
	// We never store the complete raw content, especially listenKeyExpired.
	var parsed any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return AccountEvent{}, err
	}
	normalized, err := json.Marshal(parsed)
	if err != nil {
		return AccountEvent{}, err
	}
	digest := sha256.Sum256(normalized)
	event := AccountEvent{Kind: hdr.Kind, EventTime: hdr.EventTime, TxTime: hdr.TxTime, Digest: hex.EncodeToString(digest[:])}
	switch hdr.Kind {
	case "ORDER_TRADE_UPDATE":
		var wrapper struct {
			O struct {
				Symbol       string `json:"s"`
				ClientID     string `json:"c"`
				OrderID      int64  `json:"i"`
				TradeID      int64  `json:"t"`
				Side         string `json:"S"`
				PositionSide string `json:"ps"`
				Execution    string `json:"x"`
				Status       string `json:"X"`
				Cum          string `json:"z"`
				Last         string `json:"l"`
				LastPrice    string `json:"L"`
				Fee          string `json:"n"`
				Asset        string `json:"N"`
				PnL          string `json:"rp"`
			} `json:"o"`
		}
		// Use string-or-number decimals without losing their textual precision.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(obj["o"], &fields); err != nil || fields == nil {
			return AccountEvent{}, errors.New("계정 주문 데이터 없음")
		}
		// Decode non-decimal fields separately; numeric fields often numeric in Demo.
		delete(fields, "z")
		delete(fields, "l")
		delete(fields, "L")
		delete(fields, "n")
		delete(fields, "rp")
		plain, _ := json.Marshal(fields)
		if err := json.Unmarshal(plain, &wrapper.O); err != nil {
			return AccountEvent{}, errors.New("계정 주문 키 오류")
		}
		o := wrapper.O
		if o.Symbol == "" || o.ClientID == "" || o.OrderID <= 0 || (o.Side != "BUY" && o.Side != "SELL") || (o.PositionSide != "LONG" && o.PositionSide != "SHORT" && o.PositionSide != "BOTH") || o.Execution == "" || o.Status == "" {
			return AccountEvent{}, errors.New("계정 주문 필수 필드 누락")
		}
		decimals := make([]string, 0, 5)
		for _, name := range []string{"z", "l", "L", "n", "rp"} {
			val, err := decodeEventDecimal(fieldsFromRaw(obj["o"], name))
			if err != nil {
				return AccountEvent{}, fmt.Errorf("계정 주문 숫자 필드 %s 오류", name)
			}
			decimals = append(decimals, val)
		}
		// Exchange execution quantities/prices cannot be negative. Compare
		// exact rationals; textual "0.000" must not masquerade as a fill.
		accum, _ := new(big.Rat).SetString(decimals[0])
		last, _ := new(big.Rat).SetString(decimals[1])
		lastPrice, _ := new(big.Rat).SetString(decimals[2])
		fee, _ := new(big.Rat).SetString(decimals[3])
		if accum == nil || last == nil || lastPrice == nil || fee == nil || accum.Sign() < 0 || last.Sign() < 0 || lastPrice.Sign() < 0 || fee.Sign() < 0 || accum.Cmp(last) < 0 {
			return AccountEvent{}, errors.New("주문 수량·가격 또는 수수료 값 오류")
		}
		if o.Execution == "TRADE" && (o.TradeID <= 0 || last.Sign() == 0 || lastPrice.Sign() == 0) {
			return AccountEvent{}, errors.New("주문 실제 체결 필드 누락")
		}
		event.Order = &StreamOrder{Symbol: o.Symbol, ClientID: o.ClientID, OrderID: o.OrderID, TradeID: o.TradeID,
			PositionSide: o.PositionSide, Side: o.Side, Execution: o.Execution, Status: o.Status,
			CumulativeQty: decimals[0], LastQty: decimals[1], LastPrice: decimals[2], Fee: decimals[3],
			FeeAsset: o.Asset, RealizedPnL: decimals[4]}
	case "ACCOUNT_UPDATE":
		var body struct {
			A struct {
				M string `json:"m"`
				B []struct {
					Asset string `json:"a"`
				} `json:"B"`
				P []struct {
					Symbol string `json:"s"`
				} `json:"P"`
			} `json:"a"`
		}
		if err := json.Unmarshal(raw, &body); err != nil || body.A.M == "" || (body.A.B == nil && body.A.P == nil) {
			return AccountEvent{}, errors.New("계정 업데이트 필수 필드 누락")
		}
		acc := &StreamAccount{Reason: body.A.M}
		for _, b := range body.A.B {
			if b.Asset == "" {
				return AccountEvent{}, errors.New("계정 자산 코드 누락")
			}
			acc.Assets = append(acc.Assets, b.Asset)
		}
		for _, p := range body.A.P {
			if p.Symbol == "" {
				return AccountEvent{}, errors.New("포지션 심볼 누락")
			}
			acc.PositionSymbols = append(acc.PositionSymbols, p.Symbol)
		}
		event.Account = acc
	case "ACCOUNT_CONFIG_UPDATE", "TRADE_LITE", "MARGIN_CALL", "STRATEGY_UPDATE", "ALGO_UPDATE", "CONDITIONAL_ORDER_TRIGGER_REJECT", "listenKeyExpired":
		// Known potentially important events: immediately invalidate the
		// baseline. No assumption is made about what the unseen payload means.
	default:
		if !strings.HasPrefix(hdr.Kind, "_") {
			return AccountEvent{}, fmt.Errorf("지원하지 않는 계정 이벤트 유형: %q", hdr.Kind)
		}
		return AccountEvent{}, errors.New("알 수 없는 계정 이벤트")
	}
	return event, nil
}
func fieldsFromRaw(raw json.RawMessage, name string) json.RawMessage {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	return fields[name]
}
func decodeEventDecimal(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", errors.New("숫자 누락")
	}
	var s string
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
	} else {
		s = string(raw)
	}
	// Strict non-scientific decimal representation; no float64 conversion.
	negative := strings.TrimPrefix(s, "-")
	if len(negative) == 0 || len(negative) > 64 || negative[0] == '.' || negative[len(negative)-1] == '.' {
		return "", errors.New("숫자 오류")
	}
	dot := false
	digit := false
	for _, c := range negative {
		switch {
		case c == '.' && !dot:
			dot = true
		case c >= '0' && c <= '9':
			digit = true
		default:
			return "", errors.New("비정상 소수값")
		}
	}
	if !digit {
		return "", errors.New("숫자 없음")
	}
	return s, nil
}
