package trading

// Private stream integration is an UNWIRED, permanently fail-closed observer.
// Creating/keeping a listenKey is non-trading API activity, but the observer
// never unlocks order submission even after a complete-looking event stream.
// The future execution layer must independently reconcile the signed snapshot,
// every journaled order, all relevant fills, all algo orders, and income events.
import (
	"binance_trader/internal/exchange"
	"binance_trader/internal/wsclient"
	"context"
	"errors"
	"time"
)

func RunDemoAccountObservation(ctx context.Context, c *exchange.Client, apiKey string, store *EventStore, dial exchange.UserStreamDialer) error {
	return runDemoAccountObservation(ctx, c, apiKey, store, dial, 30*time.Minute)
}
func runDemoAccountObservation(ctx context.Context, c *exchange.Client, apiKey string, store *EventStore, dial exchange.UserStreamDialer, keepaliveInterval time.Duration) error {
	return runDemoAccountObservationReady(ctx, c, apiKey, store, dial, keepaliveInterval, nil)
}

// runDemoAccountObservationReady signals only after an actual WebSocket dial succeeds.
// A dial success does NOT prove stream completeness and never unlocks trading.
func runDemoAccountObservationReady(ctx context.Context, c *exchange.Client, apiKey string, store *EventStore, dial exchange.UserStreamDialer, keepaliveInterval time.Duration, onConnected func()) error {
	return runDemoAccountObservationReadyWithJoin(ctx, c, apiKey, store, dial, keepaliveInterval, 3*time.Second, onConnected)
}

func runDemoAccountObservationReadyWithJoin(ctx context.Context, c *exchange.Client, apiKey string, store *EventStore, dial exchange.UserStreamDialer, keepaliveInterval, joinTimeout time.Duration, onConnected func()) error {
	if c == nil || store == nil || apiKey == "" || keepaliveInterval <= 0 || joinTimeout <= 0 {
		return errors.New("Demo 사용자 스트림 초기화 매개변수 오류")
	}
	if err := store.RecordDiscontinuity("STARTUP"); err != nil {
		return err
	}
	key, err := c.StartDemoUserStream(ctx, apiKey)
	if err != nil {
		store.Stop("Demo 사용자 스트림 키 생성 실패")
		return err
	}
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		if dial == nil {
			dial = func(ctx context.Context, u string) (exchange.UserEventConn, error) { return wsclient.Dial(ctx, u) }
		}
		// Only a successful socket handshake may wake the REST baseline stage.
		actualDial := func(ctx context.Context, u string) (exchange.UserEventConn, error) {
			conn, err := dial(ctx, u)
			if err == nil && onConnected != nil {
				onConnected()
			}
			return conn, err
		}
		errCh <- exchange.StreamDemoUserEvents(innerCtx, key, actualDial, func(raw []byte) error {
			event, err := ParseAccountEvent(raw)
			if err != nil {
				return err
			}
			if _, err = store.Append(event); err != nil {
				return err
			}
			if event.Kind == "listenKeyExpired" {
				return errors.New("listenKeyExpired: 독립 계정 재검증 필요")
			}
			return nil
		})
	}()
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	joinStream := func(reason string) error {
		cancel()
		timer := time.NewTimer(joinTimeout)
		defer timer.Stop()
		select {
		case <-errCh:
			return nil
		case <-timer.C:
			store.Stop("Demo 사용자 스트림 내부 goroutine 종료 미확인: 수동 재검증 필요")
			return errors.New(reason)
		}
	}
	for {
		select {
		case err := <-errCh:
			cancel()
			if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
				return e
			}
			// NEVER return the listenKey, WebSocket URI, API key or payload.
			if err == nil {
				return errors.New("사용자 스트림이 예기치 않게 종료되었습니다")
			}
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return ctx.Err()
			}
			// Preserve only whitelisted dial phase/status; discard every raw cause.
			d := wsclient.DiagnoseDial(err)
			if d.Phase != "UNCLASSIFIED" {
				return wsclient.RedactDial(err)
			}
			return errors.New("Demo 계정 WebSocket 중단: 수동 계정 전체 재검증 필요")
		case <-ticker.C:
			reqCtx, finish := context.WithTimeout(ctx, 7*time.Second)
			refreshed, e := c.KeepaliveDemoUserStream(reqCtx, apiKey, key)
			finish()
			if e != nil || refreshed != key {
				if writeErr := store.RecordDiscontinuity("KEEPALIVE_FAILED"); writeErr != nil {
					cancel()
					return writeErr
				}
				if joinErr := joinStream("Demo 사용자 스트림 내부 종료 미확인: 계정 재검증 필요"); joinErr != nil {
					return joinErr
				}
				return errors.New("Demo 사용자 스트림 키 갱신 실패/회전: 계정 재검증 필요")
			}
		case <-ctx.Done():
			if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
				cancel()
				return e
			}
			if joinErr := joinStream("Demo 사용자 스트림 내부 종료 미확인: 계정 재검증 필요"); joinErr != nil {
				return joinErr
			}
			return ctx.Err()
		}
	}
}
