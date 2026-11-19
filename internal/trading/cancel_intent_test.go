package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"binance_trader/internal/exchange"
)

type mockAlgoCancel struct {
	calls    atomic.Int32
	fail     bool
	mismatch bool
}

func (m *mockAlgoCancel) CancelDemoAlgoOnce(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	m.calls.Add(1)
	if m.fail {
		return exchange.DemoAlgoResult{}, errors.New("transport timeout")
	}
	result := exchange.DemoAlgoResult{AlgoID: id, ClientAlgoID: "btp_groupA", Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", AlgoType: "CONDITIONAL", Type: "STOP_MARKET", WorkingType: "MARK_PRICE", Quantity: "0.01", Status: "CANCELED"}
	if m.mismatch {
		result.ClientAlgoID = "foreign"
	}
	return result, nil
}
func TestCancelIntentPersistThenExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	store, err := NewCancelIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := store.Plan("groupA", 901, "btp_groupA")
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Plan("groupA", 901, "btp_groupA")
	if err != nil || again != intent {
		t.Fatalf("not idempotent %+v %v", again, err)
	}
	if _, err := store.Plan("groupA", 902, "btp_other"); err == nil {
		t.Fatal("conflicting sibling plan accepted")
	}
	api := &mockAlgoCancel{}
	r, err := store.AttemptAlgoCancel(context.Background(), api, "test-key", "secret", intent)
	if err != nil || r.AlgoID != 901 || api.calls.Load() != 1 {
		t.Fatalf("first attempt %+v %v %d", r, err, api.calls.Load())
	}
	if _, err := store.AttemptAlgoCancel(context.Background(), api, "test-key", "secret", intent); !errors.Is(err, ErrCancelAlreadyAttempted) {
		t.Fatalf("duplicate issued %v", err)
	}
	store2, _ := NewCancelIntentStore(dir)
	if _, attempted, err := store2.Inspect("groupA"); err != nil || !attempted {
		t.Fatalf("lost dispatch on restart %v %v", attempted, err)
	}
	if _, err := store2.AttemptAlgoCancel(context.Background(), api, "test-key", "secret", intent); !errors.Is(err, ErrCancelAlreadyAttempted) || api.calls.Load() != 1 {
		t.Fatalf("replay issued %v calls=%d", err, api.calls.Load())
	}
}
func TestCancelIntentCrashWindowAndTamperFailClosed(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewCancelIntentStore(dir)
	c, err := s.Plan("crash1", 33, "btp_crash1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkMayHaveSent(c); err != nil {
		t.Fatal(err)
	}
	// Simulated process crash after durable mark but before HTTP DELETE.
	recovered, _ := NewCancelIntentStore(dir)
	api := &mockAlgoCancel{}
	if _, err := recovered.AttemptAlgoCancel(context.Background(), api, "k", "s", c); !errors.Is(err, ErrCancelAlreadyAttempted) || api.calls.Load() != 0 {
		t.Fatal("unsafe crash replay")
	}
	if err := os.WriteFile(filepath.Join(dir, "trading", "algo_cancel_intents", "crash1.dispatch.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := recovered.Inspect("crash1"); err == nil {
		t.Fatal("corruption accepted")
	}
}
func TestCancelIntentUnknownOutcomeBlocksReplay(t *testing.T) {
	s, _ := NewCancelIntentStore(t.TempDir())
	c, err := s.Plan("groupB", 900, "btp_groupA")
	if err != nil {
		t.Fatal(err)
	}
	api := &mockAlgoCancel{fail: true}
	if _, err := s.AttemptAlgoCancel(context.Background(), api, "k", "s", c); err == nil {
		t.Fatal("ambiguous cancel success")
	}
	api.fail = false
	if _, err := s.AttemptAlgoCancel(context.Background(), api, "k", "s", c); err == nil || api.calls.Load() != 1 {
		t.Fatalf("duplicate after timeout: %v %d", err, api.calls.Load())
	}
}
