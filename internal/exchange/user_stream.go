package exchange

// Demo-only user-data WebSocket transport. This is deliberately an UNWIRED
// adapter: no application HTTP handler starts it and it cannot place orders.
// The stream has no sequence number; reconnecting NEVER proves that no account
// events were missed. Consumers MUST invalidate their account baseline and
// reconcile orders, fills, funding and positions independently before trading.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"binance_trader/internal/wsclient"
)

// Official USD-M user-data REST endpoints require X-MBX-APIKEY but no HMAC.
// Never put the API key in a URL. The listen key is an account credential:
// don't log it or include it in diagnostic errors or user-facing status.
const (
	DemoUserStreamWS = "wss://demo-fstream.binance.com/private"
	// Current Binance USD-M user-data streams are path-based under the /private
	// WebSocket base: /ws/<listenKey>. Event payloads remain advisory until
	// signed REST reconciliation succeeds.
)

var listenKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,256}$`)

func (c *Client) userKeyRequest(ctx context.Context, method, apiKey string) (string, error) {
	if err := c.demoOnly(); err != nil {
		return "", err
	}
	if strings.TrimSpace(apiKey) == "" || strings.ContainsAny(apiKey, "\r\n") {
		return "", errors.New("Demo API Key가 필요합니다")
	}
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodDelete {
		return "", errors.New("지원하지 않는 사용자 스트림 명령")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+"/fapi/v1/listenKey", nil)
	if err != nil {
		return "", errors.New("사용자 스트림 요청 생성 실패")
	}
	req.Header.Set("X-MBX-APIKEY", apiKey)
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		return "", errors.New("Demo 사용자 스트림 요청 결과를 확인할 수 없습니다")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	if err != nil {
		return "", errors.New("사용자 스트림 응답 읽기 실패")
	}
	if resp.StatusCode != http.StatusOK { // never include upstream body, URL or keys
		var e struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(raw, &e)
		return "", fmt.Errorf("Demo 사용자 스트림 HTTP %d 코드 %d", resp.StatusCode, e.Code)
	}
	if method == http.MethodDelete {
		return "", nil
	}
	var result struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || !listenKeyPattern.MatchString(result.ListenKey) {
		return "", errors.New("Demo 사용자 스트림 listenKey 응답 누락 또는 오류")
	}
	return result.ListenKey, nil
}

func (c *Client) StartDemoUserStream(ctx context.Context, apiKey string) (string, error) {
	return c.userKeyRequest(ctx, http.MethodPost, apiKey)
}
func (c *Client) KeepaliveDemoUserStream(ctx context.Context, apiKey, oldKey string) (string, error) {
	if !listenKeyPattern.MatchString(oldKey) {
		return "", errors.New("기존 listenKey 오류")
	}
	key, err := c.userKeyRequest(ctx, http.MethodPut, apiKey)
	if err != nil {
		return "", err
	}
	// A rotated key requires a new WebSocket connection and full reconciliation.
	return key, nil
}
func (c *Client) CloseDemoUserStream(ctx context.Context, apiKey, key string) error {
	if !listenKeyPattern.MatchString(key) {
		return errors.New("listenKey 오류")
	}
	_, err := c.userKeyRequest(ctx, http.MethodDelete, apiKey)
	return err
}

// UserEventConn permits mock WebSockets without creating an actual Demo key.
// Only the fixed Binance Demo destination is accepted outside test injection.
type UserEventConn interface {
	ReadText() ([]byte, error)
	Close() error
}
type UserStreamDialer func(context.Context, string) (UserEventConn, error)

func demoWSURL(key string) (string, error) {
	if !listenKeyPattern.MatchString(key) {
		return "", errors.New("listenKey 오류")
	}
	// listenKeyPattern excludes '/', '?', '#', '%' and all other URL syntax,
	// so concatenating it as one path segment cannot alter the destination.
	return DemoUserStreamWS + "/ws/" + key, nil
}

// DialDemoUserStreamURL is a UserStreamDialer: its argument is the *fully
// constructed WebSocket URL*, not the listenKey. StreamDemoUserEvents builds
// and validates this URL before invoking the callback. Keep this signature
// explicit: dev.3.2 accidentally wired a key-taking function here, so every
// real private diagnosis failed before the first network dial.
func DialDemoUserStreamURL(ctx context.Context, streamURL string) (UserEventConn, error) {
	conn, err := wsclient.Dial(ctx, streamURL)
	if err != nil {
		return nil, wsclient.RedactDial(err)
	}
	return conn, nil
}

var _ UserStreamDialer = DialDemoUserStreamURL

// StreamDemoUserEvents executes ONE connection epoch and exits on any error.
// The caller must durably mark the epoch untrusted before retrying, and must
// never replay an ambiguous live operation based only on stream notifications.
// A quiet user stream can legitimately have no events: ReadText() timeout is
// treated as an interrupted epoch, NOT as proof of no account activity.
func StreamDemoUserEvents(ctx context.Context, key string, dial UserStreamDialer, handle func([]byte) error) error {
	if _, err := demoWSURL(key); err != nil {
		return err
	}
	if handle == nil {
		return errors.New("사용자 스트림 이벤트 핸들러가 필요합니다")
	}
	if dial == nil {
		dial = func(ctx context.Context, u string) (UserEventConn, error) { return wsclient.Dial(ctx, u) }
	}
	u, _ := demoWSURL(key)
	conn, err := dial(ctx, u)
	if err != nil {
		return wsclient.RedactDial(err)
	}
	defer conn.Close()
	// Close promptly on shutdown even when server is idle.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer close(done)
	timer := time.NewTimer(23 * time.Hour) // before mandatory 24h rotation
	defer timer.Stop()
	// A private stream may legitimately remain silent for hours. Proactively
	// ping to elicit PONG frames; wsclient resets its read deadline on each
	// control frame. Never treat a quiet stream as confirmation of no changes.
	pingDone := make(chan struct{})
	if p, ok := conn.(interface{ Ping() error }); ok {
		go func() {
			t := time.NewTicker(8 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-pingDone:
					return
				case <-t.C:
					if p.Ping() != nil {
						_ = conn.Close()
						return
					}
				}
			}
		}()
	}
	defer close(pingDone)
	rotationDone := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			_ = conn.Close()
		case <-rotationDone:
		}
	}()
	defer close(rotationDone)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Bounded wire ReadText() (20s). Explicit channel rotation is checked
		// after each event or timeout. A timeout still invalidates the epoch.
		raw, err := conn.ReadText()
		if err != nil {
			return errors.New("사용자 스트림 중단: 전체 계정 재동기화 필요")
		}
		if len(raw) == 0 || len(raw) > 1<<20 {
			return errors.New("사용자 스트림 빈 데이터 또는 과대 데이터")
		}
		if err := handle(raw); err != nil {
			return fmt.Errorf("사용자 스트림 처리 중단: %w", err)
		}
		select {
		case <-timer.C:
			return errors.New("사용자 스트림 예정 교체: 전체 계정 재동기화 필요")
		default:
		}
	}
}
