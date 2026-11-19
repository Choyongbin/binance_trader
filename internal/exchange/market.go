package exchange

// Public demo-market candles are deliberately separate from the authenticated
// account API. This service never accepts credentials or forwards user input to
// Binance; BTCUSDT and 1m are fixed until multi-symbol work is implemented.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const MarketSymbol = "BTCUSDT"

type Candle struct {
	OpenTime  int64   `json:"open_time"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	CloseTime int64   `json:"close_time"`
}
type MarketSnapshot struct {
	Symbol                string   `json:"symbol"`
	Interval              string   `json:"interval"`
	Source                string   `json:"source"`
	AsOf                  string   `json:"as_of"`
	ExchangeClockOffsetMs int64    `json:"exchange_clock_offset_ms"` // exchange server time minus local midpoint; diagnostic only
	Candles               []Candle `json:"candles"`
}

type MarketService struct {
	mu      sync.Mutex
	base    string
	client  *http.Client
	now     func() time.Time
	latest  MarketSnapshot
	fetched time.Time
}

func NewDemoMarketService() *MarketService {
	return &MarketService{base: DemoBase, client: &http.Client{Timeout: 6 * time.Second}, now: time.Now}
}

// NewMarketWithBase is for local test servers. Production uses only DemoBase.
func NewMarketWithBase(base string, client *http.Client) *MarketService {
	return &MarketService{base: base, client: client, now: time.Now}
}

func (s *MarketService) exchangeTime(ctx context.Context) (time.Time, int64, error) {
	before := s.now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/fapi/v1/time", nil)
	if err != nil {
		return time.Time{}, 0, err
	}
	res, err := s.client.Do(req)
	if err != nil {
		return time.Time{}, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return time.Time{}, 0, fmt.Errorf("exchange time HTTP %d", res.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 4096))
	if err != nil {
		return time.Time{}, 0, err
	}
	var body struct {
		ServerTime int64 `json:"serverTime"`
	}
	if json.Unmarshal(raw, &body) != nil || body.ServerTime <= 0 {
		return time.Time{}, 0, errors.New("거래소 서버 시각 형식 오류")
	}
	after := s.now()
	midpoint := before.Add(after.Sub(before) / 2)
	exchange := time.UnixMilli(body.ServerTime)
	return exchange, exchange.UnixMilli() - midpoint.UnixMilli(), nil
}
func (s *MarketService) Get(ctx context.Context) (MarketSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fetched.IsZero() && s.now().Sub(s.fetched) < 6*time.Second {
		return s.latest, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		s.base+"/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=200", nil)
	if err != nil {
		return MarketSnapshot{}, errors.New("시세 요청을 생성할 수 없습니다")
	}
	res, err := s.client.Do(req)
	if err != nil {
		return MarketSnapshot{}, errors.New("데모 시장 데이터에 연결할 수 없습니다")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return MarketSnapshot{}, fmt.Errorf("데모 시세 조회 실패 (HTTP %d)", res.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 300<<10))
	if err != nil {
		return MarketSnapshot{}, errors.New("시세 데이터 읽기 실패")
	}
	var rows [][]json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) < 100 || len(rows) > 200 {
		return MarketSnapshot{}, errors.New("시세 데이터 개수 또는 형식 오류")
	}
	result := MarketSnapshot{Symbol: MarketSymbol, Interval: "1m", Source: "BINANCE_FUTURES_DEMO_PUBLIC", Candles: make([]Candle, 0, len(rows))}
	var previous int64
	for _, row := range rows {
		if len(row) < 7 {
			return MarketSnapshot{}, errors.New("불완전한 캔들 데이터")
		}
		var openAt, closeAt int64
		if json.Unmarshal(row[0], &openAt) != nil || json.Unmarshal(row[6], &closeAt) != nil {
			return MarketSnapshot{}, errors.New("캔들 시각 형식 오류")
		}
		values := [5]float64{}
		for i, index := range []int{1, 2, 3, 4, 5} {
			var value string
			if json.Unmarshal(row[index], &value) != nil {
				return MarketSnapshot{}, errors.New("캔들 가격 형식 오류")
			}
			values[i], err = strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(values[i]) || math.IsInf(values[i], 0) || values[i] < 0 {
				return MarketSnapshot{}, errors.New("유효하지 않은 캔들 가격")
			}
		}
		if openAt <= previous || (previous > 0 && openAt-previous != 60000) ||
			closeAt != openAt+59999 || values[0] <= 0 || values[3] <= 0 ||
			values[1] < math.Max(values[0], values[3]) || values[2] > math.Min(values[0], values[3]) || values[2] > values[1] {
			return MarketSnapshot{}, errors.New("연속성 또는 OHLC 검증 실패")
		}
		result.Candles = append(result.Candles, Candle{openAt, values[0], values[1], values[2], values[3], values[4], closeAt})
		previous = openAt
	}
	// A successful kline HTTP response can still be frozen. Compare against
	// Binance's own public server clock instead of the Windows wall clock so
	// workstation time skew cannot reject valid candles or bless stale ones.
	exchangeNow, clockOffset, err := s.exchangeTime(ctx)
	if err != nil {
		return MarketSnapshot{}, errors.New("거래소 서버 시각을 확인할 수 없습니다")
	}
	latestOpen := time.UnixMilli(result.Candles[len(result.Candles)-1].OpenTime)
	age := exchangeNow.Sub(latestOpen)
	if age > 3*time.Minute || age < -time.Minute {
		return MarketSnapshot{}, errors.New("데모 시장 데이터의 최근 봉 시각이 Binance 서버 시각과 맞지 않습니다")
	}
	result.ExchangeClockOffsetMs = clockOffset
	result.AsOf = s.now().UTC().Format(time.RFC3339Nano)
	s.latest, s.fetched = result, s.now()
	return result, nil
}
