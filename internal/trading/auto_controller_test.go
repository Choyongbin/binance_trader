package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"binance_trader/internal/model"
)

type fakeAutoRuntime struct {
	calls *atomic.Int32
	fail  bool
}

func (f *fakeAutoRuntime) RunOnce(context.Context) (AutoExecutionResult, error) {
	f.calls.Add(1)
	if f.fail {
		return AutoExecutionResult{}, errors.New("blocked")
	}
	return AutoExecutionResult{Decision: modelHoldDecision()}, nil
}
func (f *fakeAutoRuntime) Close() error { return nil }

func modelHoldDecision() model.Decision { return model.Decision{Action: model.ActionHold} }

func TestAutoControllerUsesSingleRunControlAndStopsOnFailure(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return &fakeAutoRuntime{calls: &calls, fail: true}, nil }, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for c.Status().Running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Status().Running || !c.Status().FailClosed || calls.Load() != 1 {
		t.Fatalf("status=%+v calls=%d", c.Status(), calls.Load())
	}
	st, err := control.Status()
	if err != nil || st.State != AutoRunStopped {
		t.Fatalf("control=%+v err=%v", st, err)
	}
}

func TestAutoControllerExplicitStopInvalidatesLease(t *testing.T) {
	dir := t.TempDir()
	control, _ := OpenAutoRunControl(dir)
	var calls atomic.Int32
	c, _ := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return &fakeAutoRuntime{calls: &calls}, nil }, 50*time.Millisecond)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if st, _ := control.Status(); st.State != AutoRunRunning {
		t.Fatalf("state=%+v", st)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	if st, _ := control.Status(); st.State != AutoRunStopped {
		t.Fatalf("state=%+v", st)
	}
}

type blockingCloseRuntime struct {
	started chan struct{}
	closed  chan struct{}
	release chan struct{}
}

func (r *blockingCloseRuntime) RunOnce(ctx context.Context) (AutoExecutionResult, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	<-ctx.Done()
	return AutoExecutionResult{}, ctx.Err()
}
func (r *blockingCloseRuntime) Close() error {
	close(r.closed)
	<-r.release
	return nil
}

func TestAutoControllerStartRejectedWhileStopInProgress(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	rt := &blockingCloseRuntime{started: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return rt, nil }, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rt.started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not start")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- c.Stop() }()
	select {
	case <-rt.closed:
	case <-time.After(time.Second):
		t.Fatal("runtime close not reached")
	}
	if err := c.Start(); err == nil || !strings.Contains(err.Error(), "STOP 진행 중") {
		t.Fatalf("START must be rejected while STOP waits for runtime close: %v", err)
	}
	close(rt.release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("STOP did not finish")
	}
	st, err := control.Status()
	if err != nil || st.State != AutoRunStopped {
		t.Fatalf("control=%+v err=%v", st, err)
	}
}

type panicAutoRuntime struct{}

func (*panicAutoRuntime) RunOnce(context.Context) (AutoExecutionResult, error) {
	panic("injected runtime panic")
}
func (*panicAutoRuntime) Close() error { return nil }

func TestAutoControllerRuntimePanicFailsClosedAndDurablyStops(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return &panicAutoRuntime{}, nil }, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := c.Status()
		if st.FailClosed && !st.Running {
			if !strings.Contains(st.LastError, "runtime panic") {
				t.Fatalf("status=%+v", st)
			}
			ctrl, err := control.Status()
			if err != nil || ctrl.State != AutoRunStopped {
				t.Fatalf("control=%+v err=%v", ctrl, err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("controller did not fail closed: %+v", c.Status())
}

type cancelObservedRuntime struct{ canceled chan struct{} }

func (r *cancelObservedRuntime) RunOnce(ctx context.Context) (AutoExecutionResult, error) {
	<-ctx.Done()
	select {
	case <-r.canceled:
	default:
		close(r.canceled)
	}
	return AutoExecutionResult{}, ctx.Err()
}
func (*cancelObservedRuntime) Close() error { return nil }

func TestAutoControllerDurableStopFailureStillCancelsLocalRuntime(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	rt := &cancelObservedRuntime{canceled: make(chan struct{})}
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return rt, nil }, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	// Inject an unknown control file so the durable STOP cannot safely inspect
	// the generation chain. Local execution must still be canceled.
	if err := os.WriteFile(filepath.Join(dir, "trading", "auto_control", "INJECTED"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(); err == nil {
		t.Fatal("expected durable STOP failure")
	}
	select {
	case <-rt.canceled:
	case <-time.After(time.Second):
		t.Fatal("local runtime continued after durable STOP failure")
	}
	if err := c.Start(); err == nil || !strings.Contains(err.Error(), "STOP 진행 중") {
		t.Fatalf("START must remain blocked after durable STOP failure: %v", err)
	}
	st := c.Status()
	if !st.FailClosed || !strings.Contains(st.LastError, "durable STOP") {
		t.Fatalf("status=%+v", st)
	}
}

type closePanicRuntime struct{}

func (*closePanicRuntime) RunOnce(context.Context) (AutoExecutionResult, error) {
	return AutoExecutionResult{}, errors.New("stop loop")
}
func (*closePanicRuntime) Close() error { panic("injected close panic") }

func TestAutoControllerClosePanicAlsoFailsClosed(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return &closePanicRuntime{}, nil }, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := c.Status()
		if st.FailClosed && !st.Running && strings.Contains(st.LastError, "runtime panic") {
			ctrl, err := control.Status()
			if err != nil || ctrl.State != AutoRunStopped {
				t.Fatalf("control=%+v err=%v", ctrl, err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("close panic did not fail closed: %+v", c.Status())
}

// A supervised order was actually submitted, even though the one-order cap
// deliberately halts the controller. Do not lose the dispatch evidence or
// mark this *intentional* stop as a failed exchange submission.
type supervisedOneShotRuntime struct{ calls *atomic.Int32 }

func (r *supervisedOneShotRuntime) RunOnce(context.Context) (AutoExecutionResult, error) {
	r.calls.Add(1)
	return AutoExecutionResult{ClientOrderID: "bt_one_shot_fixture", Dispatched: true, Lifecycle: ExecutionSettled}, ErrSupervisedSingleOrderComplete
}
func (*supervisedOneShotRuntime) Close() error { return nil }
func TestAutoControllerSupervisedStopPreservesDispatchAndStopsLease(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return &supervisedOneShotRuntime{calls: &calls}, nil }, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.Status().Running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err == nil || !strings.Contains(err.Error(), "START 재허용 금지") {
		t.Fatalf("supervised order must prohibit controller restart until external signed review: %v", err)
	}
	st := c.Status()
	if st.Running || st.FailClosed || !st.Dispatched || st.LastOrderID != "bt_one_shot_fixture" || !st.SignedReviewRequired || st.LastError != "" || st.StopReason != "SINGLE_ORDER_DISPATCHED_SIGNED_REVIEW_REQUIRED" || calls.Load() != 1 {
		t.Fatalf("supervised stop lost state: %+v calls=%d", st, calls.Load())
	}
	durable, err := control.Status()
	if err != nil || durable.State != AutoRunStopped {
		t.Fatalf("durable not stopped: %+v %v", durable, err)
	}
}

// Only a trusted dispatched result can use the controlled one-order stop;
// an unknown outcome with an order ID must remain fail-closed.
type ambiguousOneShotRuntime struct{}

func (*ambiguousOneShotRuntime) RunOnce(context.Context) (AutoExecutionResult, error) {
	return AutoExecutionResult{ClientOrderID: "bt_uncertain", Lifecycle: ExecutionMayHaveSent}, ErrSupervisedSingleOrderComplete
}
func (*ambiguousOneShotRuntime) Close() error { return nil }
func TestAutoControllerRejectsFalseSupervisedSuccess(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewAutoController(control, func(AutoRunLease) (AutoRuntime, error) { return &ambiguousOneShotRuntime{}, nil }, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.Status().Running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	st := c.Status()
	if !st.FailClosed || st.Dispatched || !st.SignedReviewRequired || st.StopReason != "" {
		t.Fatalf("ambiguous order falsely accepted: %+v", st)
	}
}
