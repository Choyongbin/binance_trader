package trading

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type fakePrivateConn struct {
	sent   bool
	closed chan struct{}
}

func (c *fakePrivateConn) ReadText() ([]byte, error) {
	if !c.sent {
		c.sent = true
		return []byte(`{"e":"ACCOUNT_UPDATE","E":1790525396365,"T":1790525396364,"a":{"m":"ORDER","B":[],"P":[]}}`), nil
	}
	<-c.closed
	return nil, fmt.Errorf("mock link closed")
}
func (c *fakePrivateConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}
func TestObserverKeepaliveRotationForcesGap(t *testing.T) {
	k := strings.Repeat("q", 64)
	k2 := strings.Repeat("r", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/listenKey" || r.Header.Get("X-MBX-APIKEY") != "demo-mock-key" {
			t.Errorf("bad %s", r.URL)
			w.WriteHeader(400)
			return
		}
		if r.Method == "POST" {
			fmt.Fprintf(w, `{"listenKey":"%s"}`, k)
		} else if r.Method == "PUT" {
			fmt.Fprintf(w, `{"listenKey":"%s"}`, k2)
		} else {
			w.WriteHeader(400)
		}
	}))
	defer srv.Close()
	store, err := OpenEventStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	conn := &fakePrivateConn{closed: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = runDemoAccountObservation(ctx, exchange.NewWithBase(srv.URL, srv.Client()), "demo-mock-key", store, func(_ context.Context, u string) (exchange.UserEventConn, error) {
		parsed, parseErr := url.Parse(u)
		if parseErr != nil || parsed.Path != "/private/ws/"+k || parsed.RawQuery != "" {
			t.Errorf("wrong ws path")
		}
		return conn, nil
	}, 20*time.Millisecond)
	if err == nil || strings.Contains(err.Error(), k) {
		t.Fatalf("rotation failed to halt or leaked key: %v", err)
	}
	records, e := store.ReadAll()
	if e != nil || len(records) != 1 || records[0].Account == nil {
		t.Fatalf("event persistence %+v %v", records, e)
	}
	dir := filepath.Join(filepath.Dir(store.dir), "stream_discontinuities")
	markers, e := os.ReadDir(dir)
	if e != nil || len(markers) < 2 {
		t.Fatalf("missing startup+rotation gap markers %+v %v", markers, e)
	}
	if h, _ := store.Status(); !h {
		t.Fatal("observer made live trading ready")
	}
}

func TestPrivateObserverSignalsOnlyAfterSuccessfulDial(t *testing.T) {
	key := strings.Repeat("d", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/listenKey" || r.Method != http.MethodPost || r.Header.Get("X-MBX-APIKEY") != "fake-demo-key" {
			t.Errorf("unexpected mock request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
			return
		}
		fmt.Fprintf(w, `{"listenKey":"%s"}`, key)
	}))
	defer srv.Close()
	client := exchange.NewWithBase(srv.URL, srv.Client())
	root := t.TempDir()
	s, err := OpenEventStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{}, 1)
	done := make(chan error, 1)
	conn := &fakePrivateConn{closed: make(chan struct{})}
	go func() {
		done <- runDemoAccountObservationReady(ctx, client, "fake-demo-key", s, func(_ context.Context, u string) (exchange.UserEventConn, error) {
			parsed, parseErr := url.Parse(u)
			if parseErr != nil || parsed.Path != "/private/ws/"+key || parsed.RawQuery != "" {
				t.Errorf("wrong private WS address")
			}
			return conn, nil
		}, time.Hour, func() { called <- struct{}{} })
	}()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("successful dial never signaled")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel must halt")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observer failed to stop")
	}
	// Inability to complete a private WebSocket handshake must NOT notify the
	// coordinator that a REST snapshot can begin under an active stream.
	s2, e := OpenEventStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	connected := make(chan struct{}, 1)
	err = runDemoAccountObservationReady(context.Background(), client, "fake-demo-key", s2, func(context.Context, string) (exchange.UserEventConn, error) {
		return nil, errors.New("dial rejected")
	}, time.Hour, func() { connected <- struct{}{} })
	if err == nil {
		t.Fatal("failed dial accepted")
	}
	select {
	case <-connected:
		t.Fatal("failed dial signaled connection")
	default:
	}
}

type stubbornPrivateConn struct {
	release chan struct{}
	started chan struct{}
}

func (c *stubbornPrivateConn) ReadText() ([]byte, error) {
	select {
	case <-c.started:
	default:
		close(c.started)
	}
	<-c.release
	return nil, errors.New("released stubborn read")
}
func (*stubbornPrivateConn) Close() error { return nil }

func TestPrivateObserverCancelRequiresInnerGoroutineJoin(t *testing.T) {
	key := strings.Repeat("j", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/listenKey" || r.Method != http.MethodPost || r.Header.Get("X-MBX-APIKEY") != "join-test-key" {
			t.Errorf("unexpected mock request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, `{"listenKey":"%s"}`, key)
	}))
	defer srv.Close()
	client := exchange.NewWithBase(srv.URL, srv.Client())
	store, err := OpenEventStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	connected := make(chan struct{}, 1)
	conn := &stubbornPrivateConn{release: make(chan struct{}), started: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- runDemoAccountObservationReadyWithJoin(ctx, client, "join-test-key", store, func(context.Context, string) (exchange.UserEventConn, error) {
			return conn, nil
		}, time.Hour, 20*time.Millisecond, func() { connected <- struct{}{} })
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("observer never connected")
	}
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("stream never entered blocking read")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "내부 종료 미확인") {
			t.Fatalf("stubborn stream was silently accepted: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not fail closed after bounded join timeout")
	}
	if halted, reason := store.Status(); !halted || !strings.Contains(reason, "goroutine 종료 미확인") {
		t.Fatalf("store not fail-closed: halted=%v reason=%q", halted, reason)
	}
	close(conn.release) // allow the injected stream goroutine to exit before test teardown
}
