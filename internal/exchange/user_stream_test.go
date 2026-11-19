package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func mustDemoWSURLForTest(t *testing.T, key string) string {
	t.Helper()
	u, err := demoWSURL(key)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestDemoListenKeyLifecycleAndRedirectNoLeak(t *testing.T) {
	var starts, puts, deletes atomic.Int32
	key := strings.Repeat("a", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/listenKey" || r.URL.RawQuery != "" || r.Header.Get("X-MBX-APIKEY") != "private-demo-test" {
			t.Errorf("leaky request path=%s query=%s", r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(401)
			return
		}
		switch r.Method {
		case "POST":
			starts.Add(1)
			fmt.Fprintf(w, `{"listenKey":"%s"}`, key)
		case "PUT":
			puts.Add(1)
			fmt.Fprintf(w, `{"listenKey":"%s"}`, key)
		case "DELETE":
			deletes.Add(1)
			fmt.Fprint(w, `{}`)
		default:
			t.Error("unexpected method")
		}
	}))
	defer srv.Close()
	c := NewWithBase(srv.URL, srv.Client())
	actual, err := c.StartDemoUserStream(context.Background(), "private-demo-test")
	if err != nil || actual != key {
		t.Fatalf("start %q %v", actual, err)
	}
	got, err := c.KeepaliveDemoUserStream(context.Background(), "private-demo-test", key)
	if err != nil || got != key {
		t.Fatalf("keepalive %q %v", got, err)
	}
	if err := c.CloseDemoUserStream(context.Background(), "private-demo-test", key); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 1 || puts.Load() != 1 || deletes.Load() != 1 {
		t.Fatal("wrong method counts")
	}
	if u, err := demoWSURL(key); err != nil || u != mustDemoWSURLForTest(t, key) {
		t.Fatal("bad private path")
	}
	if _, err := NewWithBase(RealBase, &http.Client{}).StartDemoUserStream(context.Background(), "key"); err == nil {
		t.Fatal("real stream unexpectedly allowed")
	}
	// Never follow a 307 from Demo because the API key would be forwarded.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Error("redirect leaked key") }))
	defer other.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", other.URL)
		w.WriteHeader(307)
	}))
	defer redir.Close()
	if _, err := NewWithBase(redir.URL, redir.Client()).StartDemoUserStream(context.Background(), "private-demo-test"); err == nil {
		t.Fatal("redirect accepted")
	}
}

type scriptedUserConn struct {
	reads  int
	closed chan struct{}
}

func (c *scriptedUserConn) ReadText() ([]byte, error) {
	if c.reads == 0 {
		c.reads++
		return []byte(`{"e":"listenKeyExpired","E":1790525396365}`), nil
	}
	<-c.closed
	return nil, fmt.Errorf("disconnected")
}
func (c *scriptedUserConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}
func TestMockPrivateStreamDisconnectSanitized(t *testing.T) {
	conn := &scriptedUserConn{closed: make(chan struct{})}
	key := strings.Repeat("x", 64)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := StreamDemoUserEvents(ctx, key, func(_ context.Context, u string) (UserEventConn, error) {
		if u != mustDemoWSURLForTest(t, key) {
			t.Fatal("unexpected endpoint")
		}
		return conn, nil
	}, func(raw []byte) error {
		if !strings.Contains(string(raw), "listenKeyExpired") {
			t.Fatal("unexpected event")
		}
		return fmt.Errorf("stop on expiry")
	})
	if err == nil || strings.Contains(err.Error(), key) {
		t.Fatalf("stream error leaked key %v", err)
	}
}
