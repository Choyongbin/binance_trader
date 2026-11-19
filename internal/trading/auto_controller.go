package trading

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// AutoRuntime is the minimal long-lived automatic execution contract.
// The server owns exactly one controller; HTTP start/stop remains locked until
// Demo E2E unlock, but the controller is production-wired and testable now.
type AutoRuntime interface {
	RunOnce(context.Context) (AutoExecutionResult, error)
	Close() error
}

type AutoRuntimeFactory func(AutoRunLease) (AutoRuntime, error)

// ErrSupervisedSingleOrderComplete is returned ONLY by the supervised PC
// one-shot wrapper after the Demo executor acknowledged a dispatched order.
// It stops the durable auto-run generation without treating the intentional
// one-order limit as an order-submission failure. A signed position review is
// still required: it does not assert FILLED, flat account or TP/SL coverage.
var ErrSupervisedSingleOrderComplete = errors.New("supervised Demo single-order limit reached: signed account review required")

type AutoControllerStatus struct {
	Running              bool           `json:"running"`
	LastRunAt            string         `json:"last_run_at,omitempty"`
	LastError            string         `json:"last_error,omitempty"`
	LastDecision         string         `json:"last_decision,omitempty"`
	LastOrderID          string         `json:"last_client_order_id,omitempty"`
	FailClosed           bool           `json:"fail_closed"`
	Dispatched           bool           `json:"dispatched"`
	Lifecycle            ExecutionStage `json:"lifecycle_stage,omitempty"`
	StopReason           string         `json:"stop_reason,omitempty"`
	SignedReviewRequired bool           `json:"signed_review_required"`
}

type AutoController struct {
	mu       sync.Mutex
	control  *AutoRunControl
	factory  AutoRuntimeFactory
	interval time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
	status   AutoControllerStatus
	stopping bool
	stopMu   sync.Mutex
}

func NewAutoController(control *AutoRunControl, factory AutoRuntimeFactory, interval time.Duration) (*AutoController, error) {
	if control == nil || factory == nil {
		return nil, errors.New("자동매매 controller 필수 구성요소가 없습니다")
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &AutoController{control: control, factory: factory, interval: interval}, nil
}

func (c *AutoController) Status() AutoControllerStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// Start is intentionally not wired to HTTP while trading is locked.
func (c *AutoController) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping {
		return errors.New("자동매매 controller STOP 진행 중에는 START할 수 없습니다")
	}
	if c.status.SignedReviewRequired || c.status.FailClosed {
		return errors.New("이전 자동매매 사고/주문 확인이 미완료입니다: 현재 controller에서 START 재허용 금지")
	}
	if c.cancel != nil {
		return nil
	}
	lease, err := c.control.Start()
	if err != nil {
		return err
	}
	rt, err := c.factory(lease)
	if err != nil {
		_ = c.control.Stop()
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.cancel, c.done = cancel, done
	c.status = AutoControllerStatus{Running: true}
	go c.loop(ctx, done, rt)
	return nil
}

func (c *AutoController) loop(ctx context.Context, done chan struct{}, rt AutoRuntime) {
	defer func() {
		c.mu.Lock()
		if c.done == done {
			c.cancel, c.done = nil, nil
			c.status.Running = false
		}
		c.mu.Unlock()
		close(done)
	}()
	// Registered before runtime.Close so it also catches a Close panic (LIFO).
	defer func() {
		if recovered := recover(); recovered != nil {
			stopErr := c.control.Stop()
			msg := fmt.Sprintf("자동매매 runtime panic: %v", recovered)
			if stopErr != nil {
				msg += "; durable STOP 실패: " + stopErr.Error()
			}
			c.mu.Lock()
			c.status.LastError = msg
			c.status.FailClosed = true
			c.status.Running = false
			c.mu.Unlock()
		}
	}()
	defer func() {
		if err := rt.Close(); err != nil {
			c.mu.Lock()
			c.status.LastError = "runtime close 실패: " + err.Error()
			c.status.FailClosed = true
			c.status.Running = false
			c.mu.Unlock()
			_ = c.control.Stop()
		}
	}()
	timer := time.NewTicker(c.interval)
	defer timer.Stop()
	run := func() bool {
		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := rt.RunOnce(runCtx)
		cancel()
		c.mu.Lock()
		c.status.LastRunAt = time.Now().UTC().Format(time.RFC3339Nano)
		c.status.LastDecision = result.Decision.Action
		c.status.LastOrderID = result.ClientOrderID
		c.status.Dispatched = result.Dispatched
		c.status.Lifecycle = result.Lifecycle
		// A supervised stop still durably stops the lease and blocks later
		// orders. It is not a failed dispatch and must not be reported as
		// one. The signed account check remains mandatory.
		complete := errors.Is(err, ErrSupervisedSingleOrderComplete) && result.Dispatched && result.ClientOrderID != ""
		if complete {
			c.status.StopReason = "SINGLE_ORDER_DISPATCHED_SIGNED_REVIEW_REQUIRED"
			c.status.SignedReviewRequired = true
			c.status.Running = false
		} else if err != nil {
			c.status.LastError = err.Error()
			c.status.FailClosed = true
			c.status.Running = false
			if result.ClientOrderID != "" {
				c.status.SignedReviewRequired = true
			}
		}
		c.mu.Unlock()
		if err != nil {
			// Any execution/readiness/recovery error stops the run generation.
			// It never retries a possibly-sent order.
			if stopErr := c.control.Stop(); stopErr != nil {
				c.mu.Lock()
				c.status.LastError += "; durable STOP 실패: " + stopErr.Error()
				c.status.FailClosed = true
				c.mu.Unlock()
			}
			return false
		}
		return true
	}
	if !run() {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !run() {
				return
			}
		}
	}
}

func (c *AutoController) Stop() error {
	// Serialize complete STOP operations. In particular, do not expose the
	// controller as startable between clearing the in-memory loop and fsyncing
	// the durable STOP generation.
	c.stopMu.Lock()
	defer c.stopMu.Unlock()

	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.stopping = true
	c.status.Running = false
	c.mu.Unlock()

	// Durable generation changes first so all old leases fail immediately.
	// Even if persistence fails, cancel the local runtime: continuing locally
	// after a failed durable STOP would be less safe than leaving restart-block
	// evidence for the next process.
	durableErr := c.control.Stop()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(35 * time.Second):
			c.mu.Lock()
			c.status.FailClosed = true
			c.status.LastError = "자동매매 controller 종료 확인 시간 초과"
			c.mu.Unlock()
			return errors.New("자동매매 controller 종료 확인 시간 초과")
		}
	}
	if durableErr != nil {
		c.mu.Lock()
		c.status.FailClosed = true
		c.status.LastError = "durable STOP 실패: " + durableErr.Error()
		c.mu.Unlock()
		// Keep stopping=true. START remains blocked in this process, while a
		// fresh process sees any surviving RUNNING evidence as RESTART_BLOCKED.
		return durableErr
	}

	c.mu.Lock()
	if c.done == done {
		c.cancel, c.done = nil, nil
	}
	c.stopping = false
	c.mu.Unlock()
	return nil
}
