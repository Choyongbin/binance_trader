package trading

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func dev388MockDemoExecutor(t *testing.T, handler func(http.ResponseWriter, *http.Request)) (*DemoExecutor, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			_, _ = w.Write([]byte(exchangeInfoTest))
		case "/fapi/v1/order":
			handler(w, r)
		case "/fapi/v1/time":
			_, _ = fmt.Fprintf(w, `{"serverTime":%d}`, time.Now().UnixMilli())
		default:
			t.Errorf("unexpected test path: %s", r.URL)
			http.Error(w, "unconfigured", http.StatusNotFound)
		}
	}))
	client := exchange.NewWithBase(server.URL, server.Client())
	ex, err := NewDemoExecutor(client,
		func(context.Context) (DemoCredentials, error) { return DemoCredentials{"key", "secret"}, nil },
		func(context.Context, string) (Quote, error) {
			return Quote{Symbol: "BTCUSDT", Price: "84510.6", ReceivedAt: time.Now().UTC()}, nil
		},
		func(context.Context, PlannedOrder, exchange.SymbolRules, Quote, DemoCredentials) error { return nil },
	)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return ex, server.Close
}

func dev388Plan(t *testing.T) PlannedOrder {
	t.Helper()
	i := sampleIntent()
	i.Quantity = "0.004"
	p, err := i.Plan()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// STOP while the second safety gate is evaluating: a durable lease invalidation
// must fail closed before any HTTP POST occurs, even after exchange preflight.
func TestDev388StopDuringFinalCheckBlocksExchangePost(t *testing.T) {
	var posts atomic.Int32
	ex, closeServer := dev388MockDemoExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		http.Error(w, "unexpected post", http.StatusInternalServerError)
	})
	defer closeServer()
	ctl, err := OpenAutoRunControl(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ctl.Start()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	plan := dev388Plan(t)
	go func() {
		_, err := ex.SubmitWithFinalCheck(context.Background(), plan, func(ctx context.Context) error {
			close(entered)
			<-release
			return ctl.Guard(lease)(ctx)
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("final gate not reached")
	}
	if err := ctl.Stop(); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "STOP") {
			t.Fatalf("STOP not observed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("executor not finished")
	}
	if got := posts.Load(); got != 0 {
		t.Fatalf("POST after STOP: %d", got)
	}
}

// Context canceled during signed/final checks must fail before the first POST.
func TestDev388CanceledFinalCheckBlocksExchangePost(t *testing.T) {
	var posts atomic.Int32
	ex, closeServer := dev388MockDemoExecutor(t, func(w http.ResponseWriter, r *http.Request) { posts.Add(1) })
	defer closeServer()
	ctx, cancel := context.WithCancel(context.Background())
	_, err := ex.SubmitWithFinalCheck(ctx, dev388Plan(t), func(ctx context.Context) error {
		cancel()
		return nil // Simulate a guard that incorrectly ignores cancellation.
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request was not stopped: %v", err)
	}
	if got := posts.Load(); got != 0 {
		t.Fatalf("POST after cancellation: %d", got)
	}
}

// If STOP occurs after an HTTP POST has started, it cannot unsend the request.
// Verify the ambiguous result is surfaced, never interpreted as safe to retry.
func TestDev388StopAfterPostBeginsRemainsAmbiguous(t *testing.T) {
	entered := make(chan struct{}, 1)
	releaseServer := make(chan struct{})
	var posts atomic.Int32
	ex, closeServer := dev388MockDemoExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		entered <- struct{}{}
		// A client may abandon a request after sending all bytes while the
		// handler still sees an active context. Model a delayed no-ACK server.
		<-releaseServer
		http.Error(w, "simulated uncertain response", http.StatusInternalServerError)
	})
	defer closeServer()
	ctl, err := OpenAutoRunControl(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ctl.Start()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	plan := dev388Plan(t)
	go func() {
		_, err := ex.SubmitWithFinalCheck(ctx, plan, ctl.Guard(lease))
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no observed POST")
	}
	if err := ctl.Stop(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing ACK must not count as success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled POST did not return")
	}
	close(releaseServer)
	if got := posts.Load(); got != 1 {
		t.Fatalf("unexpected POST retries: %d", got)
	}
	if err := ctl.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunStopped) {
		t.Fatalf("old lease survived STOP: %v", err)
	}
}

// Exercise the actual AutoController.Stop handshake, not just the persistent
// run-control lease. STOP must not return while a pre-POST final gate is still
// active. The model runtime is synthetic and the exchange is localhost-only.
type dev388HeldFinalRuntime struct {
	exec    *DemoExecutor
	control *AutoRunControl
	lease   AutoRunLease
	plan    PlannedOrder
	entered chan struct{}
	release chan struct{}
}

func (r *dev388HeldFinalRuntime) RunOnce(ctx context.Context) (AutoExecutionResult, error) {
	_, err := r.exec.SubmitWithFinalCheck(ctx, r.plan, func(ctx context.Context) error {
		close(r.entered)
		<-r.release
		return r.control.Guard(r.lease)(ctx)
	})
	return AutoExecutionResult{}, err
}
func (r *dev388HeldFinalRuntime) Close() error { return nil }

func TestDev388ControllerStopWaitsBeforeFinalPost(t *testing.T) {
	var posts atomic.Int32
	ex, closeServer := dev388MockDemoExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
	})
	defer closeServer()
	ctl, err := OpenAutoRunControl(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	controller, err := NewAutoController(ctl, func(lease AutoRunLease) (AutoRuntime, error) {
		return &dev388HeldFinalRuntime{exec: ex, control: ctl, lease: lease, plan: dev388Plan(t), entered: entered, release: release}, nil
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("final check did not begin")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- controller.Stop() }()
	// STOP should persist its invalidating generation before acknowledging.
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, err := ctl.Status()
		if err != nil {
			t.Fatal(err)
		}
		if st.State == AutoRunStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("durable STOP was not recorded")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-stopDone:
		t.Fatalf("STOP returned while final check in progress: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("STOP did not wait for guarded runtime")
	}
	if posts.Load() != 0 {
		t.Fatalf("STOP let %d POST(s) escape", posts.Load())
	}
}
