package exchange

// Demo market stream: one long-lived Binance public WebSocket connection feeds
// the entire local process. The browser polls this process every second; it
// never connects directly to Binance or receives any exchange API credentials.
// REST is used only to seed/reconcile 200 candles at startup/reconnection.
import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"binance_trader/internal/wsclient"
)

const demoKlineWS = "wss://demo-fstream.binance.com/market/ws/btcusdt@kline_1m"
const demoCombinedKlineWS = "wss://demo-fstream.binance.com/market/stream?streams=btcusdt@kline_1m"

const marketFreshFor = 5 * time.Second

// An upgraded socket is not a healthy market feed until it delivers a valid
// BTCUSDT 1m kline. Without this first-event deadline the status can remain
// "syncing" through repeated quiet upgrades and the alternate endpoint is
// not attempted promptly. Keep this only on the PUBLIC market socket.
var marketFirstKlineTimeout = 10 * time.Second

type LiveMarketStatus struct {
	State                  string `json:"state"`
	Transport              string `json:"transport"`
	StreamSymbol           string `json:"stream_symbol"`
	StreamInterval         string `json:"stream_interval"`
	MarketVenue            string `json:"market_venue"`
	PriceBasis             string `json:"price_basis"`
	EventTimeBasis         string `json:"event_time_basis"`
	FreshnessBasis         string `json:"freshness_basis"`
	LastEventMs            int64  `json:"last_event_ms"`
	LastReceivedAt         string `json:"last_received_at,omitempty"`
	AgeMs                  int64  `json:"age_ms"`
	EventReceiveClockDelta int64  `json:"event_receive_clock_delta_ms"`
	CurrentBarClosed       bool   `json:"current_bar_closed"`
	MessageCount           uint64 `json:"message_count"`
	ReconnectCount         uint64 `json:"reconnect_count"`
	LastError              string `json:"last_error,omitempty"`
	Endpoint               string `json:"endpoint,omitempty"`
	Ready                  bool   `json:"ready"`
}

type LiveMarketSnapshot struct {
	MarketSnapshot
	LiveMarketStatus
}

// wsDecimal preserves decimal text while accepting either quoted strings or
// JSON numbers. The official kline schema documents strings; the Demo venue
// has also emitted numeric low prices, so treating every OHLCV field as a
// mandatory JSON string can cause a permanent reconnect loop.
// Validation and finite-number checks happen in validateEvent/parseMarketFloat.
type wsDecimal string

func (d *wsDecimal) UnmarshalJSON(raw []byte) error {
	if len(raw) == 0 {
		return errors.New("empty kline decimal")
	}
	if raw[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		if len(value) > 64 {
			return errors.New("kline decimal is too long")
		}
		*d = wsDecimal(value)
		return nil
	}
	// json.Number preserves unquoted decimals without a float conversion.
	// Reject null/boolean/object/array before unmarshalling; null otherwise
	// unmarshals into a zero-value json.Number without raising an error.
	if raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
		return errors.New("kline decimal must be a quoted string or JSON number")
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return fmt.Errorf("invalid kline decimal JSON: %w", err)
	}
	if len(number.String()) > 64 {
		return errors.New("kline decimal is too long")
	}
	*d = wsDecimal(number.String())
	return nil
}

type streamEvent struct {
	Type   string `json:"e"`
	Time   int64  `json:"E"`
	Symbol string `json:"s"`
	K      struct {
		OpenTime  int64     `json:"t"`
		CloseTime int64     `json:"T"`
		Symbol    string    `json:"s"`
		Interval  string    `json:"i"`
		Open      wsDecimal `json:"o"`
		High      wsDecimal `json:"h"`
		Low       wsDecimal `json:"l"`
		Close     wsDecimal `json:"c"`
		Volume    wsDecimal `json:"v"`
		Closed    bool      `json:"x"`
	} `json:"k"`
}

var errMarketGap = errors.New("WS 캔들 누락 또는 종료 확인 실패: REST 재동기화 필요")

type StreamService struct {
	mu           sync.RWMutex
	rest         *MarketService
	urls         []string
	enabled      bool
	state        string
	latest       MarketSnapshot
	lastEvent    int64
	lastReceived time.Time
	lastClosed   bool
	messages     uint64
	reconnects   uint64
	nextURL      int
	endpoint     string
	lastError    string
	active       *wsclient.Conn
	now          func() time.Time
	startOnce    sync.Once
	stopOnce     sync.Once
	cancel       context.CancelFunc
	done         chan struct{}
}

func NewDemoStream(rest *MarketService) *StreamService {
	// User-observed working route takes precedence; legacy remains fallback.
	return newStream(rest, demoKlineWS, demoCombinedKlineWS)
}

// newStream also permits loopback ws URLs for offline integration tests.
func newStream(rest *MarketService, urls ...string) *StreamService {
	return &StreamService{rest: rest, urls: urls, enabled: true, state: "waiting", now: time.Now}
}

func (s *StreamService) SetEnabled(enabled bool) {
	s.mu.Lock()
	s.enabled = enabled
	if !enabled {
		s.state = "locked"
		s.latest = MarketSnapshot{}
		s.lastReceived = time.Time{}
		s.lastEvent = 0
		s.lastError = ""
		if s.active != nil {
			_ = s.active.Close()
		}
	} else if s.state == "locked" {
		s.state = "waiting"
	}
	s.mu.Unlock()
}

func (s *StreamService) Start() {
	s.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		s.cancel = cancel
		s.done = make(chan struct{})
		done := s.done
		s.mu.Unlock()
		go func() { defer close(done); s.run(ctx) }()
	})
}

func (s *StreamService) Close() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		cancel := s.cancel
		done := s.done
		active := s.active
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if active != nil {
			_ = active.Close()
		}
		if done != nil {
			<-done
		}
	})
}

func (s *StreamService) setState(state, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		s.state = "locked"
		return
	}
	s.state = state
	s.lastError = reason
	// A disconnected snapshot is never returned as current market data.
	if state != "live" {
		s.latest = MarketSnapshot{}
		s.lastReceived = time.Time{}
		s.lastEvent = 0
	}
}

func (s *StreamService) isEnabled() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.enabled }

func (s *StreamService) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if !s.isEnabled() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		s.setState("connecting", "")
		s.mu.RLock()
		before := s.messages
		s.mu.RUnlock()
		err := s.connectAndRead(ctx)
		s.mu.RLock()
		wasLive := s.messages > before
		s.mu.RUnlock()
		if wasLive {
			backoff = time.Second
		}
		if ctx.Err() != nil {
			return
		}
		if !s.isEnabled() {
			continue
		}
		if err != nil {
			s.setState("reconnecting", err.Error())
		} else {
			s.setState("reconnecting", "연결이 종료되어 재연결 중입니다")
		}
		s.mu.Lock()
		s.reconnects++
		// Advance to the next Demo-only URL after *any* failure, including
		// successfully upgraded connections that deliver non-kline data.
		// Previously the alternative URL was tried only for HTTP 400/404.
		if len(s.urls) > 1 {
			s.nextURL = (s.nextURL + 1) % len(s.urls)
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (s *StreamService) connectAndRead(ctx context.Context) error {
	s.mu.RLock()
	first := s.nextURL
	s.mu.RUnlock()
	var conn *wsclient.Conn
	var err error
	// Try both known Demo paths in order. If the first path upgrades but
	// produces a bad payload, run() rotates the preferred path on retry.
	for attempt := range s.urls {
		if ctx.Err() != nil || !s.isEnabled() {
			return context.Canceled
		}
		index := (first + attempt) % len(s.urls)
		address := s.urls[index]
		s.mu.Lock()
		s.endpoint = endpointLabel(address)
		s.mu.Unlock()
		conn, err = wsclient.Dial(ctx, address)
		if err == nil {
			s.mu.Lock()
			s.nextURL = index
			s.mu.Unlock()
			break
		}
	}
	if err != nil {
		return fmt.Errorf("데모 WebSocket 연결 실패 (%s): %w", s.Status().Endpoint, err)
	}
	s.mu.Lock()
	if !s.enabled {
		s.mu.Unlock()
		_ = conn.Close()
		return context.Canceled
	}
	s.active = conn
	s.state = "syncing"
	s.mu.Unlock()
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		if s.active == conn {
			s.active = nil
		}
		s.mu.Unlock()
	}()
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-finished:
		}
	}()
	defer close(finished)
	// A 200-candle REST baseline is necessary for EMA99/RSI/MACD. Connecting
	// first then fetching reduces (but does not eliminate) the handoff window.
	bootCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	baseline, err := s.rest.Get(bootCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("WS 시드 1분봉 조회 실패: %w", err)
	}
	if len(baseline.Candles) < 100 {
		return errors.New("초기 1분봉 개수 부족")
	}
	// Do not gate Binance event timestamps against the PC wall clock. REST and
	// WS overlap is reconciled by candle open time/monotonic volume below; this
	// avoids dropping valid exchange events when the Windows clock is skewed.
	s.mu.Lock()
	s.latest = cloneSnapshot(baseline)
	s.latest.Source = "BINANCE_FUTURES_DEMO_WEBSOCKET"
	s.state = "syncing"
	s.lastError = ""
	// Conservatively require a WS close event before rolling into a new bar.
	// Inferring closure from the local PC clock would make correctness depend on
	// workstation time sync. A missed close forces a REST resync instead.
	s.lastClosed = false
	s.mu.Unlock()
	born := time.NewTimer(23 * time.Hour)
	defer born.Stop()
	go func() {
		select {
		case <-born.C:
			_ = conn.Close()
		case <-finished:
		}
	}()
	firstKline := true
	firstKlineTimedOut := make(chan struct{})
	firstKlineTimer := time.AfterFunc(marketFirstKlineTimeout, func() {
		close(firstKlineTimedOut)
		_ = conn.Close()
	})
	defer firstKlineTimer.Stop()
	for ctx.Err() == nil && s.isEnabled() {
		frame, err := conn.ReadText()
		if err != nil {
			select {
			case <-firstKlineTimedOut:
				return errors.New("데모 WS 첫 1분봉 수신 제한시간 초과: 다른 Demo 경로로 전환")
			default:
			}
			return fmt.Errorf("데모 WS 메시지 수신 중단: %w", err)
		}
		event, err := decodeKlineFrame(frame)
		if err != nil {
			return fmt.Errorf("%s: %w", s.Status().Endpoint, err)
		}
		if event.Type != "kline" {
			continue
		}
		if firstKline {
			select {
			case <-firstKlineTimedOut:
				return errors.New("데모 WS 첫 1분봉 수신 제한시간 초과")
			default:
			}
		}
		s.mu.RLock()
		beforeAccepted := s.messages
		s.mu.RUnlock()
		if err := s.acceptEvent(event); err != nil {
			return err
		}
		s.mu.RLock()
		accepted := s.messages > beforeAccepted
		s.mu.RUnlock()
		if firstKline && accepted {
			firstKlineTimer.Stop()
			firstKline = false
		}
	}
	return context.Canceled
}

// endpointLabel intentionally strips query, fragment and user-info from status.
func endpointLabel(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "unknown"
	}
	return u.Host + u.EscapedPath()
}

// decodeKlineFrame supports Binance raw and combined market messages, as
// documented. Invalid text frames are reported with a strictly bounded hex
// sample. Only public market stream frames reach this function, never API keys.
func decodeKlineFrame(frame []byte) (streamEvent, error) {
	var event streamEvent
	if !utf8.Valid(frame) {
		return event, malformedWSFrame(frame, errors.New("UTF-8 형식 오류"))
	}
	var raw json.RawMessage
	if err := json.Unmarshal(frame, &raw); err != nil {
		return event, malformedWSFrame(frame, err)
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return event, malformedWSFrame(frame, errors.New("JSON 객체가 아닙니다"))
	}
	var envelope struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
		Code   *int            `json:"code"`
		Msg    string          `json:"msg"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return event, malformedWSFrame(frame, err)
	}
	if envelope.Code != nil {
		return event, fmt.Errorf("거래소 WS 오류 (code=%d)", *envelope.Code)
	}
	if len(envelope.Error) != 0 && string(bytes.TrimSpace(envelope.Error)) != "null" {
		return event, errors.New("거래소 WS 오류 envelope 수신")
	}
	if len(envelope.Data) > 0 {
		if envelope.Stream != "btcusdt@kline_1m" {
			return event, fmt.Errorf("예상하지 않은 WS 스트림: %.80s", envelope.Stream)
		}
		raw = bytes.TrimSpace(envelope.Data)
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return event, malformedWSFrame(raw, err)
	}
	// Binance WS may return administrative JSON events (e.g. subscription
	// acknowledgements); ignore these unless they carry an explicit error.
	return event, nil
}

func malformedWSFrame(frame []byte, reason error) error {
	const sampleLen = 48
	sample := frame
	if len(sample) > sampleLen {
		sample = sample[:sampleLen]
	}
	return fmt.Errorf("WS JSON 해석 실패 (%v): 길이=%d, 처음 %d바이트(hex)=%s",
		reason, len(frame), len(sample), hex.EncodeToString(sample))
}

func cloneSnapshot(p MarketSnapshot) MarketSnapshot {
	p.Candles = append([]Candle(nil), p.Candles...)
	return p
}

func validateEvent(event streamEvent, now time.Time) (Candle, error) {
	k := event.K
	if event.Type != "kline" || event.Symbol != MarketSymbol || k.Symbol != MarketSymbol || k.Interval != "1m" {
		return Candle{}, errors.New("WS 이벤트 종목 또는 봉 주기 불일치")
	}
	_ = now // freshness is based on local receive age, not exchange/local clock equality.
	if event.Time <= 0 {
		return Candle{}, errors.New("WS 이벤트 시각이 비어 있습니다")
	}
	if k.OpenTime <= 0 || k.OpenTime%60000 != 0 || k.CloseTime != k.OpenTime+59999 ||
		event.Time < k.OpenTime-5000 || event.Time > k.CloseTime+15000 {
		return Candle{}, errors.New("WS 1분봉 시각이 유효하지 않습니다")
	}
	nums := [5]float64{}
	for i, raw := range []wsDecimal{k.Open, k.High, k.Low, k.Close, k.Volume} {
		v, err := parseMarketFloat(string(raw))
		if err != nil {
			return Candle{}, err
		}
		nums[i] = v
	}
	if nums[0] <= 0 || nums[3] <= 0 || nums[1] < math.Max(nums[0], nums[3]) ||
		nums[2] > math.Min(nums[0], nums[3]) || nums[2] > nums[1] || nums[2] <= 0 {
		return Candle{}, errors.New("WS OHLC 데이터 검증 실패")
	}
	return Candle{OpenTime: k.OpenTime, Open: nums[0], High: nums[1], Low: nums[2], Close: nums[3], Volume: nums[4], CloseTime: k.CloseTime}, nil
}

func parseMarketFloat(raw string) (float64, error) {
	// strconv handles 1e309/NaN/Inf rejection below.
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
		return 0, errors.New("잘못된 WS 숫자 데이터")
	}
	return parsed, nil
}

func (s *StreamService) acceptEvent(event streamEvent) error {
	now := s.now()
	candle, err := validateEvent(event, now)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return context.Canceled
	}
	if event.Time < s.lastEvent {
		return nil
	} // old/duplicate packets must not roll data back
	n := len(s.latest.Candles)
	if n < 100 {
		return errors.New("WS 데이터 초기화 누락")
	}
	prev := s.latest.Candles[n-1]
	if candle.OpenTime < prev.OpenTime {
		return nil
	} // old previous candle after rollover
	if candle.OpenTime > prev.OpenTime+60000 {
		return errMarketGap
	}
	if candle.OpenTime == prev.OpenTime+60000 {
		if !s.lastClosed {
			return errMarketGap
		}
		s.latest.Candles = append(s.latest.Candles[1:], candle)
	} else {
		if s.lastClosed {
			return nil
		}
		// The REST baseline and the WS stream can overlap. Ignore buffered
		// earlier partial candles instead of rewinding cumulative volume or
		// losing an already-observed high/low. This also protects replayed
		// WS frames after a slow reader recovers.
		const epsilon = 1e-9
		if candle.Volume+epsilon < prev.Volume || candle.High+epsilon < prev.High || candle.Low > prev.Low+epsilon ||
			math.Abs(candle.Open-prev.Open) > epsilon {
			return nil
		}
		s.latest.Candles[n-1] = candle
	}
	s.lastClosed = event.K.Closed
	s.lastEvent = event.Time
	s.lastReceived = now
	s.latest.AsOf = now.UTC().Format(time.RFC3339Nano)
	s.state = "live"
	s.lastError = ""
	s.messages++
	return nil
}

func (s *StreamService) Status() LiveMarketStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	result := LiveMarketStatus{State: s.state, Transport: "WEBSOCKET", StreamSymbol: MarketSymbol, StreamInterval: "1m",
		MarketVenue: "BINANCE_USDS_M_FUTURES_DEMO", PriceBasis: "CONTRACT_KLINE_LAST_PRICE",
		EventTimeBasis: "BINANCE_EVENT_TIME_UTC_MS", FreshnessBasis: "LOCAL_RECEIVE_AGE",
		LastEventMs: s.lastEvent, CurrentBarClosed: s.lastClosed, MessageCount: s.messages, ReconnectCount: s.reconnects, LastError: s.lastError, Endpoint: s.endpoint}
	if !s.enabled {
		result.State = "locked"
		return result
	}
	if !s.lastReceived.IsZero() {
		result.LastReceivedAt = s.lastReceived.UTC().Format(time.RFC3339Nano)
		result.AgeMs = now.Sub(s.lastReceived).Milliseconds()
		if s.lastEvent > 0 {
			// Diagnostic only: this includes network latency AND PC/exchange clock
			// skew. It is never used to accept/reject market data.
			result.EventReceiveClockDelta = s.lastReceived.UnixMilli() - s.lastEvent
		}
		if result.AgeMs < 0 {
			result.AgeMs = 0
		}
	}
	result.Ready = s.state == "live" && !s.lastReceived.IsZero() && now.Sub(s.lastReceived) <= marketFreshFor && len(s.latest.Candles) >= 100
	if s.state == "live" && !result.Ready {
		result.State = "stale"
		result.LastError = "시장 데이터 수신이 지연되고 있습니다"
	}
	return result
}

func (s *StreamService) Get() (LiveMarketSnapshot, error) {
	status := s.Status()
	if !status.Ready {
		return LiveMarketSnapshot{LiveMarketStatus: status}, errors.New("실시간 WebSocket 시세가 준비되지 않았습니다")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Recheck under the data lock to avoid exposing a snapshot just cleared
	// by a concurrent environment switch/disconnect.
	if !s.enabled || s.state != "live" || s.now().Sub(s.lastReceived) > marketFreshFor || len(s.latest.Candles) < 100 {
		return LiveMarketSnapshot{LiveMarketStatus: s.StatusLocked()}, errors.New("실시간 시세가 변경 중입니다")
	}
	return LiveMarketSnapshot{MarketSnapshot: cloneSnapshot(s.latest), LiveMarketStatus: status}, nil
}

func (s *StreamService) StatusLocked() LiveMarketStatus {
	// Caller holds s.mu.RLock(). Used only in Get's failed double-check.
	return LiveMarketStatus{State: s.state, Transport: "WEBSOCKET", StreamSymbol: MarketSymbol, StreamInterval: "1m",
		MarketVenue: "BINANCE_USDS_M_FUTURES_DEMO", PriceBasis: "CONTRACT_KLINE_LAST_PRICE", EventTimeBasis: "BINANCE_EVENT_TIME_UTC_MS", FreshnessBasis: "LOCAL_RECEIVE_AGE", Endpoint: s.endpoint}
}
