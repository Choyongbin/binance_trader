package exchange

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sampleStreamEvent(now time.Time) streamEvent {
	var event streamEvent
	event.Type = "kline"
	event.Time = now.UnixMilli()
	event.Symbol = MarketSymbol
	event.K.Symbol = MarketSymbol
	event.K.Interval = "1m"
	event.K.OpenTime = now.Truncate(time.Minute).UnixMilli()
	event.K.CloseTime = event.K.OpenTime + 59999
	event.K.Open = "90200"
	event.K.High = "90210"
	event.K.Low = "90190"
	event.K.Close = "90201"
	event.K.Volume = "13.5"
	return event
}

func seededFeed(t *testing.T) (*StreamService, time.Time) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Minute).Add(25 * time.Second)
	svc := newStream(nil, "ws://localhost/not-used")
	svc.now = func() time.Time { return now }
	base := MarketSnapshot{Source: "BINANCE_FUTURES_DEMO_WEBSOCKET", Candles: make([]Candle, 200)}
	for i := range base.Candles {
		at := now.Truncate(time.Minute).Add(time.Duration(i-199) * time.Minute).UnixMilli()
		base.Candles[i] = Candle{at, 90200, 90210, 90190, 90201, 13.5, at + 59999}
	}
	svc.latest = base
	svc.state = "syncing"
	return svc, now
}

func TestStreamAcceptUpdateStaleAndIsolation(t *testing.T) {
	svc, now := seededFeed(t)
	event := sampleStreamEvent(now)
	if err := svc.acceptEvent(event); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.Get()
	if err != nil || !snapshot.Ready || snapshot.Source != "BINANCE_FUTURES_DEMO_WEBSOCKET" || snapshot.MessageCount != 1 {
		t.Fatalf("missing ready status %+v %v", snapshot.LiveMarketStatus, err)
	}
	snapshot.Candles[199].Close = -2
	original, _ := svc.Get()
	if original.Candles[199].Close < 0 {
		t.Fatal("mutable snapshot escaped")
	}
	// The WebSocket connection can buffer an event older than the REST seed.
	// Even with a larger E timestamp, declining cumulative volume must not
	// overwrite the newer REST candle values.
	buffered := event
	buffered.Time += 1
	buffered.K.Volume = "9.0"
	buffered.K.Close = "90204"
	if err := svc.acceptEvent(buffered); err != nil {
		t.Fatal(err)
	}
	original, _ = svc.Get()
	if original.Candles[199].Close != 90201 || original.MessageCount != 1 {
		t.Fatal("REST/WS overlap rewound the latest candle")
	}
	old := event
	old.Time--
	old.K.Close = "90200"
	if err := svc.acceptEvent(old); err != nil {
		t.Fatal(err)
	}
	original, _ = svc.Get()
	if original.Candles[199].Close != 90201 {
		t.Fatal("older update rewound the candle")
	}
	invalid := event
	invalid.Time += 100
	invalid.K.High = "80000"
	if err := svc.acceptEvent(invalid); err == nil {
		t.Fatal("bad OHLC accepted")
	}
	svc.now = func() time.Time { return now.Add(6 * time.Second) }
	if svc.Status().State != "stale" {
		t.Fatalf("staleness not detected: %+v", svc.Status())
	}
	if _, err := svc.Get(); err == nil {
		t.Fatal("stale market data exposed as live")
	}
	svc.SetEnabled(false)
	if svc.Status().Ready || svc.Status().State != "locked" {
		t.Fatal("real environment must lock market")
	}
}

func TestStreamRolloverRequiresClosedBar(t *testing.T) {
	svc, now := seededFeed(t)
	event := sampleStreamEvent(now)
	if err := svc.acceptEvent(event); err != nil {
		t.Fatal(err)
	}
	nextNow := now.Truncate(time.Minute).Add(60*time.Second + time.Second)
	svc.now = func() time.Time { return nextNow }
	next := sampleStreamEvent(nextNow)
	if err := svc.acceptEvent(next); err != errMarketGap {
		t.Fatalf("missing x=true not detected: %v", err)
	}
	svc.now = func() time.Time { return now }
	event.K.Closed = true
	event.Time += 300
	if err := svc.acceptEvent(event); err != nil {
		t.Fatal(err)
	}
	svc.now = func() time.Time { return nextNow }
	if err := svc.acceptEvent(next); err != nil {
		t.Fatalf("valid rollover failed: %v", err)
	}
	got, err := svc.Get()
	if err != nil || got.Candles[199].OpenTime != next.K.OpenTime || len(got.Candles) != 200 {
		t.Fatalf("bad rollover: %v %+v", err, got.LiveMarketStatus)
	}
}

func TestLocalWebSocketAndRESTSeedIntegration(t *testing.T) {
	remoteREST := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MBX-APIKEY") != "" {
			t.Errorf("invalid REST seed auth header %s", r.URL)
		}
		switch r.URL.Path {
		case "/fapi/v1/klines":
			_ = json.NewEncoder(w).Encode(mockKlines())
		case "/fapi/v1/time":
			_ = json.NewEncoder(w).Encode(map[string]any{"serverTime": time.Now().UnixMilli()})
		default:
			t.Errorf("invalid REST seed request %s", r.URL)
		}
	}))
	defer remoteREST.Close()
	remoteWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj := w.(http.Hijacker)
		wire, buf, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer wire.Close()
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		fmt.Fprintf(buf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
		_ = buf.Flush()
		time.Sleep(150 * time.Millisecond) // REST baseline must be ready first
		now := time.Now().UTC()
		event := sampleStreamEvent(now)
		// The REST fixture's final candle is current minute.
		event.K.Open = "90199"
		event.K.High = "90210"
		event.K.Low = "90190"
		event.K.Close = "90203"
		body, _ := json.Marshal(event)
		// The Demo venue has emitted k.l as an unquoted JSON number. Exercise
		// that exact variant across WebSocket decoding, validation and readiness.
		bodyWithNumericLow := bytes.Replace(body, []byte(`"l":"90190"`), []byte(`"l":90190`), 1)
		if bytes.Equal(bodyWithNumericLow, body) {
			t.Error("test fixture did not contain the expected quoted low")
			return
		}
		body = bodyWithNumericLow
		if len(body) < 126 {
			_, _ = buf.Write(append([]byte{0x81, byte(len(body))}, body...))
		} else if len(body) < 65536 {
			_, _ = buf.Write(append([]byte{0x81, 126, byte(len(body) >> 8), byte(len(body))}, body...))
		}
		_ = buf.Flush()
		time.Sleep(300 * time.Millisecond)
	}))
	defer remoteWS.Close()
	// This integration test uses the synthetic event through acceptEvent; the
	// full frame layer is tested separately in wsclient/client_test.go.
	rest := NewMarketWithBase(remoteREST.URL, remoteREST.Client())
	svc := newStream(rest, "ws"+strings.TrimPrefix(remoteWS.URL, "http")+"/market/ws/btcusdt@kline_1m")
	svc.Start()
	defer svc.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if svc.Status().State == "live" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("live stream not ready: %+v", svc.Status())
}

func TestDecodeRawCombinedAndMalformedDiagnostics(t *testing.T) {
	now := time.Now().UTC()
	ev := sampleStreamEvent(now)
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeKlineFrame(raw)
	if err != nil || got.Type != "kline" || got.K.Close != ev.K.Close {
		t.Fatalf("raw kline failed: %+v, %v", got, err)
	}
	combined, err := json.Marshal(map[string]any{"stream": "btcusdt@kline_1m", "data": ev})
	if err != nil {
		t.Fatal(err)
	}
	got, err = decodeKlineFrame(combined)
	if err != nil || got.Type != "kline" {
		t.Fatalf("combined kline failed: %+v, %v", got, err)
	}
	if _, err = decodeKlineFrame([]byte(`{"result":null,"id":1}`)); err != nil {
		t.Fatalf("administrative JSON must not restart stream: %v", err)
	}
	if _, err = decodeKlineFrame([]byte(`{"code":-1121,"msg":"Invalid symbol"}`)); err == nil || !strings.Contains(err.Error(), "-1121") {
		t.Fatalf("exchange error not classified: %v", err)
	}
	for _, bad := range [][]byte{{0x1f, 0x8b, 0x00}, []byte("not-json"), []byte(`"text"`)} {
		_, err = decodeKlineFrame(bad)
		if err == nil || !strings.Contains(err.Error(), "(hex)=") || !strings.Contains(err.Error(), "길이=") {
			t.Fatalf("malformed frame must retain bounded diagnostics: %q: %v", bad, err)
		}
	}
	if got := endpointLabel("wss://demo-fstream.binance.com/market/ws/test?token=DO_NOT_PRINT#fragment"); strings.Contains(got, "token") || !strings.Contains(got, "/market/ws/test") {
		t.Fatalf("unsafe endpoint label %q", got)
	}
}

func TestDemoEndpointFailoverAfterBadJSON(t *testing.T) {
	remoteREST := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/time" {
			_ = json.NewEncoder(w).Encode(map[string]any{"serverTime": time.Now().UnixMilli()})
			return
		}
		_ = json.NewEncoder(w).Encode(mockKlines())
	}))
	defer remoteREST.Close()
	// Two independently upgraded endpoints. The preferred one succeeds at
	// the handshake but sends malformed text; the next must be tried.
	serveFrames := func(bad bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			wire, b, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer wire.Close()
			sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			fmt.Fprintf(b, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
			_ = b.Flush()
			time.Sleep(200 * time.Millisecond)
			var body []byte
			if bad {
				body = []byte("invalid-JSON")
			} else {
				event := sampleStreamEvent(time.Now().UTC())
				event.K.Open = "90199"
				event.K.Close = "90203"
				body, _ = json.Marshal(event)
			}
			if len(body) < 126 {
				_, _ = b.Write(append([]byte{0x81, byte(len(body))}, body...))
			} else {
				_, _ = b.Write(append([]byte{0x81, 126, byte(len(body) >> 8), byte(len(body))}, body...))
			}
			_ = b.Flush()
			if !bad {
				time.Sleep(1500 * time.Millisecond)
			}
		}))
	}
	first := serveFrames(true)
	defer first.Close()
	second := serveFrames(false)
	defer second.Close()
	rest := NewMarketWithBase(remoteREST.URL, remoteREST.Client())
	svc := newStream(rest, "ws"+strings.TrimPrefix(first.URL, "http")+"/market/ws/btcusdt@kline_1m", "ws"+strings.TrimPrefix(second.URL, "http")+"/ws/btcusdt@kline_1m")
	svc.Start()
	defer svc.Close()
	// The retry path includes a backoff and a fresh REST seed. Give the
	// second endpoint enough wall time under a parallel full-suite CPU load;
	// this is still bounded and does not relax any readiness assertion.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status := svc.Status()
		if status.Ready {
			if status.MessageCount < 1 || status.ReconnectCount < 1 || !strings.Contains(status.Endpoint, "/ws/btcusdt") {
				t.Fatalf("failover state invalid: %+v", status)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Demo second endpoint not tried after invalid payload: %+v", svc.Status())
}

// In production the documented Futures kline fields are quoted decimal
// strings. In the reported Demo frame, at least k.l is an unquoted number.
// Neither representation should trigger a reconnect loop. Other fields can
// vary independently without relaxing finite/OHLC validation.
func TestDecodeDemoNumericOHLCV(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Minute).Add(25 * time.Second)
	e := sampleStreamEvent(now)
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		fields map[string]string
	}{
		{"numeric-low-only", map[string]string{"l": `90189.75`}},
		{"all-numeric", map[string]string{"o": `90200`, "h": `90210`, "l": `90189.75`, "c": `90201`, "v": `14.0`}},
		{"scientific-volume", map[string]string{"l": `90189.75`, "v": `1.4e1`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &envelope); err != nil {
				t.Fatal(err)
			}
			var k map[string]json.RawMessage
			if err := json.Unmarshal(envelope["k"], &k); err != nil {
				t.Fatal(err)
			}
			for key, value := range tc.fields {
				k[key] = json.RawMessage(value)
			}
			envelope["k"], _ = json.Marshal(k)
			frame, _ := json.Marshal(envelope)
			got, err := decodeKlineFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			if got.K.Low != "90189.75" {
				t.Fatalf("low lost precision: %q", got.K.Low)
			}
			if _, err := validateEvent(got, now); err != nil {
				t.Fatal(err)
			}
			// The Binance combined stream envelope must work too.
			combined, _ := json.Marshal(map[string]any{"stream": "btcusdt@kline_1m", "data": json.RawMessage(frame)})
			if _, err := decodeKlineFrame(combined); err != nil {
				t.Fatal(err)
			}
			svc, _ := seededFeed(t)
			svc.now = func() time.Time { return now }
			if err := svc.acceptEvent(got); err != nil {
				t.Fatal(err)
			}
			status := svc.Status()
			if !status.Ready || status.MessageCount != 1 {
				t.Fatalf("numeric demo did not become ready: %+v", status)
			}
		})
	}
}

func TestDecodeRejectsNonNumericKlineValues(t *testing.T) {
	for _, bad := range []string{`null`, `true`, `{}`, `[]`, `"bogus"`, `1e309`, `-2`} {
		t.Run(bad, func(t *testing.T) {
			var d wsDecimal
			err := json.Unmarshal([]byte(bad), &d)
			if bad == `"bogus"` || bad == `1e309` || bad == `-2` {
				// This layer preserves the input; financial checks reject it.
				if err != nil {
					t.Fatal(err)
				}
				if _, err := parseMarketFloat(string(d)); err == nil {
					t.Fatalf("invalid value accepted: %q", d)
				}
			} else if err == nil {
				t.Fatalf("invalid JSON type accepted: %s", bad)
			}
		})
	}
}

func TestMarketEventValidationDoesNotDependOnPCLocalClock(t *testing.T) {
	exchangeNow := time.Now().UTC().Truncate(time.Minute).Add(20 * time.Second)
	event := sampleStreamEvent(exchangeNow)
	// Simulate a badly skewed PC wall clock. Exchange event/candle timestamps
	// are internally consistent and must not be discarded because of the PC.
	if _, err := validateEvent(event, exchangeNow.Add(2*time.Hour)); err != nil {
		t.Fatalf("valid Binance event rejected due to local clock skew: %v", err)
	}
	if _, err := validateEvent(event, exchangeNow.Add(-2*time.Hour)); err != nil {
		t.Fatalf("valid Binance event rejected due to reverse local clock skew: %v", err)
	}

	svc, _ := seededFeed(t)
	svc.now = func() time.Time { return exchangeNow.Add(2 * time.Hour) }
	if err := svc.acceptEvent(event); err != nil {
		t.Fatal(err)
	}
	status := svc.Status()
	if !status.Ready || status.MarketVenue != "BINANCE_USDS_M_FUTURES_DEMO" ||
		status.PriceBasis != "CONTRACT_KLINE_LAST_PRICE" || status.EventTimeBasis != "BINANCE_EVENT_TIME_UTC_MS" ||
		status.FreshnessBasis != "LOCAL_RECEIVE_AGE" {
		t.Fatalf("market provenance/freshness metadata missing: %+v", status)
	}
	if status.EventReceiveClockDelta < int64((time.Hour).Milliseconds()) {
		t.Fatalf("diagnostic clock delta did not expose skew: %+v", status)
	}
}

// Binance retired the old /ws root in April 2026. Both production fallback
// paths must therefore use the new /market prefix, with raw or combined mode.
func TestCurrentDemoMarketURLsUseDocumentedMarketPaths(t *testing.T) {
	want := []string{
		"wss://demo-fstream.binance.com/market/ws/btcusdt@kline_1m",
		"wss://demo-fstream.binance.com/market/stream?streams=btcusdt@kline_1m",
	}
	got := NewDemoStream(NewDemoMarketService()).urls
	if len(got) != len(want) {
		t.Fatalf("expected both market routes: %v", got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("unsafe/outdated Demo market URL: %s", got[i])
		}
	}
}
