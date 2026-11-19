package trading

// Deliberately short, operator-triggered, READ-ONLY Demo private WebSocket
// transport probe. A successful HTTP upgrade (even with a matching signed
// account session) does not prove stream completeness or authorize trading.
// Never archive listenKeys, API keys, secret keys, raw events or socket URLs.
import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wsclient"
)

type DemoKeyIssuer interface {
	StartDemoUserStream(context.Context, string) (string, error)
}

type PrivateTransportResult struct {
	Schema                 int       `json:"schema"`
	Status                 string    `json:"status"` // HANDSHAKE_ONLY, EVENTS_OBSERVED, or specific failure stage
	StartedAt              time.Time `json:"started_at"`
	FinishedAt             time.Time `json:"finished_at"`
	HandshakeOK            bool      `json:"handshake_ok"`
	EventCount             int       `json:"event_count"`
	EventTypes             []string  `json:"event_types"`
	CredentialBound        bool      `json:"credential_bound"`           // set by authenticated server only
	TradingAuthorized      bool      `json:"trading_authorized"`         // always false
	StreamContinuityProven bool      `json:"stream_continuity_proven"`   // always false
	HistoricalOnly         bool      `json:"historical_only"`            // always true
	Note                   string    `json:"note"`                       // allowlisted static messages only
	DialRoute              string    `json:"dial_route,omitempty"`       // static, non-secret route label; set after a dial only
	DialPhase              string    `json:"dial_phase,omitempty"`       // allowlisted stage; legacy archives omit it
	DialHTTPStatus         int       `json:"dial_http_status,omitempty"` // numeric status only: NO headers or body
	DialElapsedMS          int64     `json:"dial_elapsed_ms,omitempty"`
}

const privateProbeDisclaimer = "일시적인 연결 진단만 수행했습니다. 과거 누락 이벤트, 실제 주문 상태 및 계정 연속성은 증명하지 않습니다."

// ProbePrivateTransport observes the connection for at most observeWindow.
// Even without incoming events, an authenticated successful WebSocket upgrade
// is a useful transport check. No listenKey DELETE is issued: POST /listenKey
// may return a key already in use by another client, and deleting it would
// break that client. Do not retry a failed WS path indefinitely.
func ProbePrivateTransport(ctx context.Context, issuer DemoKeyIssuer, apiKey string, dial exchange.UserStreamDialer, observeWindow time.Duration) PrivateTransportResult {
	result := PrivateTransportResult{Schema: 1, Status: "NOT_STARTED", StartedAt: time.Now().UTC(), EventTypes: []string{}, HistoricalOnly: true,
		Note: privateProbeDisclaimer}
	finish := func(status string) PrivateTransportResult {
		result.Status = status
		result.FinishedAt = time.Now().UTC()
		return result
	}
	if issuer == nil || apiKey == "" || dial == nil || observeWindow < 100*time.Millisecond || observeWindow > 12*time.Second {
		return finish("INVALID_CONFIGURATION")
	}
	// Initiating a listenKey is an account read-stream action, NOT an order.
	keyCtx, stopKey := context.WithTimeout(ctx, 7*time.Second)
	key, err := issuer.StartDemoUserStream(keyCtx, apiKey)
	stopKey()
	if err != nil || key == "" {
		return finish("LISTEN_KEY_FAILED")
	}
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	connected := make(chan struct{}, 1)
	dialReport := make(chan wsclient.SafeDialDiagnostic, 1)
	dialDuration := make(chan int64, 1)
	done := make(chan error, 1)
	kinds := make(map[string]int)
	var rejected atomic.Bool
	// The callback is invoked in the reader goroutine. After cancellation, wait
	// for goroutine termination before reading the event map, avoiding races.
	go func() {
		done <- exchange.StreamDemoUserEvents(probeCtx, key, func(dialCtx context.Context, u string) (exchange.UserEventConn, error) {
			started := time.Now()
			conn, e := dial(dialCtx, u)
			ms := time.Since(started).Milliseconds()
			select {
			case dialDuration <- ms:
			default:
			}
			if e != nil {
				// An injected dialer may embed sensitive URLs; classify only.
				select {
				case dialReport <- wsclient.DiagnoseDial(e):
				default:
				}
			}
			if e == nil {
				select {
				case connected <- struct{}{}:
				default:
				}
			}
			return conn, e
		}, func(raw []byte) error {
			ev, e := ParseAccountEvent(raw)
			if e != nil {
				rejected.Store(true)
				return errors.New("private event schema rejected")
			}
			if ev.Kind == "listenKeyExpired" {
				rejected.Store(true)
				return errors.New("expired private listenKey")
			}
			kinds[ev.Kind]++
			return nil
		})
	}()
	stopAndJoin := func() bool {
		cancel()
		select {
		case <-done:
			return true
		case <-time.After(2 * time.Second):
			return false
		}
	}
	// Safely read only metadata the reader goroutine has finished writing.
	recordDial := func(ok bool) {
		result.DialRoute = "PRIVATE_PATH_LISTENKEY"
		select {
		case result.DialElapsedMS = <-dialDuration:
		default:
		}
		if ok {
			result.DialPhase = "CONNECTED"
			return
		}
		select {
		case diag := <-dialReport:
			result.DialPhase = diag.Phase
			result.DialHTTPStatus = diag.HTTPStatus
		default:
			result.DialPhase = "UNCLASSIFIED"
		}
	}
	// WS dial is bounded independently even if the caller provides no deadline.
	dialTimer := time.NewTimer(9 * time.Second)
	defer dialTimer.Stop()
	select {
	case <-connected:
		recordDial(true)
		result.HandshakeOK = true
	case <-done:
		// A valid connection may have been established and closed quickly.
		// Do not re-label an observed successful handshake as a failed dial.
		select {
		case <-connected:
			recordDial(true)
			result.HandshakeOK = true
			return finish("STREAM_INTERRUPTED_OR_INVALID_EVENT")
		default:
		}
		recordDial(false)
		return finish("WS_HANDSHAKE_FAILED")
	case <-dialTimer.C:
		if !stopAndJoin() {
			return finish("OBSERVER_NOT_STOPPED")
		}
		result.DialPhase = "TIMEOUT"
		return finish("WS_HANDSHAKE_TIMEOUT")
	case <-ctx.Done():
		if !stopAndJoin() {
			return finish("OBSERVER_NOT_STOPPED")
		}
		return finish("CANCELLED")
	}
	observation := time.NewTimer(observeWindow)
	defer observation.Stop()
	select {
	case <-done: // includes invalid/expired/unknown events and unexpected close
		return finish("STREAM_INTERRUPTED_OR_INVALID_EVENT")
	case <-ctx.Done():
		if !stopAndJoin() {
			return finish("OBSERVER_NOT_STOPPED")
		}
		return finish("CANCELLED")
	case <-observation.C:
		if !stopAndJoin() {
			return finish("OBSERVER_NOT_STOPPED")
		}
	}
	if rejected.Load() {
		return finish("STREAM_INTERRUPTED_OR_INVALID_EVENT")
	}
	for kind, n := range kinds {
		result.EventCount += n
		result.EventTypes = append(result.EventTypes, kind)
	}
	sort.Strings(result.EventTypes)
	if result.EventCount == 0 {
		return finish("HANDSHAKE_ONLY")
	}
	return finish("EVENTS_OBSERVED")
}
