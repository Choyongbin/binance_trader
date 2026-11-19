package server

import (
	"context"
	"testing"

	"binance_trader/internal/trading"
)

func TestAccountMutationAlwaysDurablyStopsControllerEvidence(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	// Create a run generation directly through the server-owned control without
	// starting the controller loop. Controller.Status().Running is therefore
	// false, reproducing a transition/restart-like state that the old helper
	// would have skipped.
	lease, err := s.autoControl.Start()
	if err != nil {
		t.Fatal(err)
	}
	if s.autoController.Status().Running {
		t.Fatal("fixture unexpectedly running controller loop")
	}
	if err := s.stopAutoForAccountMutation(); err != nil {
		t.Fatal(err)
	}
	st, err := s.autoControl.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.State != trading.AutoRunStopped {
		t.Fatalf("state=%+v", st)
	}
	if err := s.autoControl.Guard(lease)(context.Background()); err == nil {
		t.Fatal("old lease survived account-mutation STOP")
	}
}
