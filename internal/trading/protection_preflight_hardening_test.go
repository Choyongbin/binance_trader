package trading

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// The live arming preflight must never interpret a missing signed response
// field as proof of an empty account or an empty order inventory.
func TestProtectionPreflightRejectsIncompleteSignedBracket(t *testing.T) {
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	mock := &armingMock{owner: owner, algos: map[int64]exchange.DemoAlgoResult{}}
	valid, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateProtectionPreflight(valid, owner, map[int64]ProtectionInstruction{}); err != nil {
		t.Fatalf("valid fixture: %v", err)
	}
	cases := []struct {
		name   string
		change func(*exchange.ExecutionPreflightSnapshot)
	}{
		{"missing-open-orders", func(s *exchange.ExecutionPreflightSnapshot) { s.OpenOrders = nil }},
		{"missing-open-algos", func(s *exchange.ExecutionPreflightSnapshot) { s.OpenAlgos = nil }},
		{"missing-start-time", func(s *exchange.ExecutionPreflightSnapshot) { s.StartedAt = time.Time{} }},
		{"inverted-bracket", func(s *exchange.ExecutionPreflightSnapshot) { s.StartedAt = s.FinishedAt.Add(time.Second) }},
		{"excessively-long-bracket", func(s *exchange.ExecutionPreflightSnapshot) { s.StartedAt = s.FinishedAt.Add(-10 * time.Second) }},
		{"missing-account-assets", func(s *exchange.ExecutionPreflightSnapshot) { s.Portfolio.Assets = nil }},
		{"missing-account-positions", func(s *exchange.ExecutionPreflightSnapshot) { s.Portfolio.Positions = nil }},
		{"missing-usdt-asset", func(s *exchange.ExecutionPreflightSnapshot) { s.Portfolio.Assets = []exchange.PortfolioAsset{} }},
		{"duplicate-position", func(s *exchange.ExecutionPreflightSnapshot) {
			s.Portfolio.Positions = append(s.Portfolio.Positions, s.Portfolio.Positions[0])
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid
			tc.change(&s)
			if err := validateProtectionPreflight(s, owner, map[int64]ProtectionInstruction{}); err == nil {
				t.Fatal("incomplete evidence accepted")
			}
		})
	}
}

type cancelAfterPreflightTransport struct {
	snapshot exchange.ExecutionPreflightSnapshot
	cancel   context.CancelFunc
	submits  int
}

func (x *cancelAfterPreflightTransport) FetchDemoExecutionPreflight(context.Context, string, string, string) (exchange.ExecutionPreflightSnapshot, error) {
	x.cancel()
	return x.snapshot, nil
}
func (x *cancelAfterPreflightTransport) SubmitDemoProtection(context.Context, string, string, exchange.NewDemoProtection) (exchange.DemoAlgoResult, error) {
	x.submits++
	return exchange.DemoAlgoResult{}, nil
}
func (x *cancelAfterPreflightTransport) QueryDemoAlgo(context.Context, string, string, int64) (exchange.DemoAlgoResult, error) {
	return exchange.DemoAlgoResult{}, nil
}

func TestProtectionArmingContextCancelledBeforeFirstPOST(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01", AverageEntry: "100000"}
	mock := &armingMock{owner: owner, algos: map[int64]exchange.DemoAlgoResult{}}
	snap, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &cancelAfterPreflightTransport{snapshot: snap, cancel: cancel}
	coord := testRecoveryCoordinator(t, store)
	_, err = ArmDemoProtection(ctx, plan.GroupID, owner, coord, store, transport, DemoCredentials{"k", "s"})
	if err == nil || transport.submits != 0 {
		t.Fatalf("cancelled arming must never POST: submits=%d err=%v", transport.submits, err)
	}
	// Cancellation may occur before or after durable MAY_HAVE_SENT; neither
	// state may be converted into a replacement order automatically.
	tp, err := store.Inspect(plan.GroupID, "tp")
	if err != nil {
		t.Fatal(err)
	}
	if tp.SignedObserved {
		t.Fatal("unexpected signed TP observation")
	}
	if tp.MayHaveSent {
		if err := store.markMayHaveSentLive(plan.GroupID, "tp"); err == nil {
			t.Fatal("ambiguous TP replay allowed")
		}
	}
}

func TestProtectionPreflightRejectsNaNLikeAccountState(t *testing.T) {
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.01"}
	mock := &armingMock{owner: owner, algos: map[int64]exchange.DemoAlgoResult{}}
	snap, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snap.Portfolio)
	text := strings.Replace(string(raw), `"totalMarginBalance":"1000"`, `"totalMarginBalance":"unknown"`, 1)
	if text == string(raw) {
		t.Skip("test fixture uses an unexpected decimal representation")
	}
	_ = json.Unmarshal([]byte(text), &snap.Portfolio)
	if err := validateProtectionPreflight(snap, owner, map[int64]ProtectionInstruction{}); err == nil {
		t.Fatal("invalid signed account decimal accepted")
	}
}
