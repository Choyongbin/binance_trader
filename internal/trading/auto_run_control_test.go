package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAutoRunControlExplicitStartStopInvalidatesLease(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := c.Status(); err != nil || st.State != AutoRunStopped {
		t.Fatalf("initial %+v %v", st, err)
	}
	lease, err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Guard(lease)(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st, _ := c.Status(); st.State != AutoRunRunning || st.Generation != lease.Generation {
		t.Fatalf("running %+v", st)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := c.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunStopped) {
		t.Fatalf("old lease must stop: %v", err)
	}
	if st, _ := c.Status(); st.State != AutoRunStopped || st.Generation <= lease.Generation {
		t.Fatalf("stopped %+v", st)
	}
}

func TestAutoRunControlCrashDoesNotAutoResume(t *testing.T) {
	dir := t.TempDir()
	first, _ := OpenAutoRunControl(dir)
	lease, err := first.Start()
	if err != nil {
		t.Fatal(err)
	}
	// Simulate process restart: a fresh controller has no in-memory ownership.
	reopened, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := reopened.Status()
	if err != nil || st.State != AutoRunRestartBlocked || !st.RestartBlock {
		t.Fatalf("restart status %+v %v", st, err)
	}
	if _, err := reopened.Start(); !errors.Is(err, ErrAutoRunRestartBlocked) {
		t.Fatalf("restart must not auto resume: %v", err)
	}
	if err := reopened.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunRestartBlocked) {
		t.Fatalf("old lease must be rejected: %v", err)
	}
	// Explicit STOP acknowledges the interrupted run; only then can a new run start.
	if err := reopened.Stop(); err != nil {
		t.Fatal(err)
	}
	newLease, err := reopened.Start()
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Generation <= lease.Generation || newLease.RunID == lease.RunID {
		t.Fatalf("new lease not rotated: old=%+v new=%+v", lease, newLease)
	}
}

type stopBeforeFinalExecutor struct {
	control *AutoRunControl
	calls   int
}

func (e *stopBeforeFinalExecutor) durableFinalGuardContract() {}
func (e *stopBeforeFinalExecutor) SubmitWithFinalCheck(ctx context.Context, p PlannedOrder, finalCheck func(context.Context) error) (Submission, error) {
	e.calls++
	if err := e.control.Stop(); err != nil {
		return Submission{}, err
	}
	if err := finalCheck(ctx); err != nil {
		return Submission{}, err
	}
	return Submission{ClientOrderID: p.ClientOrderID, OrderID: 1, Status: "NEW"}, nil
}

func TestAutoRunStopAfterMayHaveSentBlocksFinalSideEffect(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(-time.Second)
	c, _, lifecycle, res := lifecycleReservation(t, root, makeIntent("autostop-final01", Manual, "", Long, Enter, "0.001"), now)
	defer c.Close()
	control, err := OpenAutoRunControl(root)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	base := control.Guard(lease)
	guard := func(ctx context.Context, _ Intent) error { return base(ctx) }
	exec := &stopBeforeFinalExecutor{control: control}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, guard); err == nil || !strings.Contains(err.Error(), "STOP") {
		t.Fatalf("expected STOP final guard failure, got %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("executor envelope calls=%d", exec.calls)
	}
	life, err := lifecycle.Load(res)
	if err != nil {
		t.Fatal(err)
	}
	if life.Stage != ExecutionMayHaveSent {
		t.Fatalf("stage=%s", life.Stage)
	}
}

func TestAutoRunControlRejectsSymlinkAndMissingGeneration(t *testing.T) {
	dir := t.TempDir()
	c, _ := OpenAutoRunControl(dir)
	if _, err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	controlDir := filepath.Join(dir, "trading", "auto_control")
	if err := os.Remove(filepath.Join(controlDir, "state.00000000000000000001.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAutoRunControl(dir); err == nil || !strings.Contains(err.Error(), "누락") {
		t.Fatalf("missing generation must fail: %v", err)
	}

	dir2 := t.TempDir()
	controlDir2 := filepath.Join(dir2, "trading", "auto_control")
	if err := os.MkdirAll(controlDir2, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir2, "target.json")
	if err := os.WriteFile(target, []byte(`{"schema":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(controlDir2, "state.00000000000000000001.json")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if _, err := OpenAutoRunControl(dir2); err == nil {
		t.Fatal("symlink control evidence must fail")
	}
}

func TestAutoRunStopWaitsForLeaseBoundMutationWindow(t *testing.T) {
	dir := t.TempDir()
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- control.withLease(lease, func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("mutation window not entered")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- control.Stop() }()
	select {
	case err := <-stopDone:
		t.Fatalf("STOP crossed lease-bound mutation window: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("STOP did not complete after mutation window")
	}
	if err := control.Guard(lease)(context.Background()); !errors.Is(err, ErrAutoRunStopped) {
		t.Fatalf("old lease survived STOP: %v", err)
	}
}
