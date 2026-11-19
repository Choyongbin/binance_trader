package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

func TestReadOnlySoakAuthenticationAndLockedRoutes(t *testing.T) {
	s := connectedAccountingServer(t)
	defer s.Close()
	h := s.Handler()
	calls := 0
	s.soakRunner = func(context.Context, trading.DemoCredentials, string, int64, time.Duration) (trading.RecoveryProbe, error) {
		calls++
		return trading.RecoveryProbe{}, context.Canceled
	}
	path := "/api/v1/recovery/soak"
	valid := `{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30}`
	for _, v := range []struct {
		body, token string
		want        int
	}{
		{valid, "bad", 403},
		{`{"passphrase":"wrong-password","lookback_hours":24,"duration_seconds":30}`, s.csrfToken, 403},
		{`{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":10}`, s.csrfToken, 400},
		{`{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30,"unknown":1}`, s.csrfToken, 400},
	} {
		resp := request(h, "POST", path, v.token, "", v.body)
		if resp.Code != v.want {
			t.Fatalf("unsafe request status %d wanted %d: %s", resp.Code, v.want, resp.Body)
		}
	}
	if calls != 0 {
		t.Fatal("unauthorized/invalid request reached private runner")
	}
	good := request(h, "POST", path, s.csrfToken, "", valid)
	if good.Code != http.StatusServiceUnavailable {
		t.Fatalf("incomplete fake archive accepted: %d %s", good.Code, good.Body)
	}
	if calls != 1 {
		t.Fatal("runner not executed once", calls)
	}
	status := request(h, "GET", "/api/v1/recovery/soak/status", "", "", "")
	if status.Code != 200 || !strings.Contains(status.Body.String(), `"trading_authorized":false`) || !strings.Contains(status.Body.String(), `"historical_only":true`) || !strings.Contains(status.Body.String(), `"report_count":0`) {
		t.Fatalf("unsafe status %d %s", status.Code, status.Body)
	}
	for _, route := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		x := request(h, "POST", route, s.csrfToken, "", `{}`)
		if x.Code != 423 {
			t.Fatal("order unlocked", route, x.Code)
		}
	}
}
func TestReadOnlySoakRequiresDemoSession(t *testing.T) {
	s, e := newWithFetcher(t.TempDir(), readDemoMock{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	got := request(s.Handler(), "POST", "/api/v1/recovery/soak", s.csrfToken, "", `{"passphrase":"abc","lookback_hours":24,"duration_seconds":30}`)
	if got.Code != 423 {
		t.Fatalf("unconnected account accepted: %d", got.Code)
	}
}

func TestReadOnlySoakMockPositiveDurabilityAndOrderLock(t *testing.T) {
	if testing.Short() {
		t.Skip("30-second bounded HTTP soak mock")
	}
	s := connectedAccountingServer(t)
	defer s.Close()
	s.soakRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64, dwell time.Duration) (trading.RecoveryProbe, error) {
		signed := fakeAccountingWindow(func(_ context.Context, key, secret string, from int64) (exchange.RecoveryBaseline, error) {
			if key != "demo-only-key" || secret != "demo-only-secret" {
				t.Fatal("wrong signed account")
			}
			return minimalSignedWindow(t, from), nil
		})
		observer := func(ctx context.Context, store *trading.EventStore, connected func()) error {
			if e := store.RecordDiscontinuity("STARTUP"); e != nil {
				return e
			}
			connected()
			<-ctx.Done()
			return ctx.Err()
		}
		return trading.ProbeDemoRecoveryWithSoak(ctx, signed, creds, dir, since, observer, dwell)
	}
	h := s.Handler()
	path := "/api/v1/recovery/soak"
	ok := request(h, "POST", path, s.csrfToken, "", `{"passphrase":"strong-passphrase","lookback_hours":24,"duration_seconds":30}`)
	if ok.Code != 200 || !strings.Contains(ok.Body.String(), `"status":"BOUNDED_SOAK_REVIEWED_NOT_CONTINUOUS"`) || !strings.Contains(ok.Body.String(), `"second_rest_reviewed":true`) || !strings.Contains(ok.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("bounded mock not completed %d %s", ok.Code, ok.Body)
	}
	status := request(h, "GET", "/api/v1/recovery/soak/status", "", "", "")
	if status.Code != 200 || !strings.Contains(status.Body.String(), `"report_count":1`) || !strings.Contains(status.Body.String(), `"status":"REVIEWED"`) || !strings.Contains(status.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("archive/status mismatch %d %s", status.Code, status.Body)
	}
	for _, route := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start", "/api/v1/trading/auto/stop"} {
		if x := request(h, "POST", route, s.csrfToken, "", `{}`); x.Code != 423 {
			t.Fatal("order lock opened", route, x.Code)
		}
	}
}
