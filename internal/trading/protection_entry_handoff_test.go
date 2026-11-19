package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"testing"
	"time"
)

func handoffFixture(t *testing.T, side PositionSide) (Reservation, ExecutionLifecycle, OwnerPosition, ProtectionDispatchPlan, time.Time) {
	t.Helper()
	_, p, _, _ := testProtectionGroup(t, side)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: side, Quantity: "0.01", AverageEntry: "100000"}
	intent := Intent{RequestID: "handoff-entry-001", Source: Manual, Symbol: "BTCUSDT", PositionSide: side, Action: Enter, OrderType: Market, Quantity: "0.01"}
	planned, err := intent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := digestIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	r := Reservation{Schema: 1, ClientOrderID: planned.ClientOrderID, Intent: intent, IntentDigest: digest, ReservedQuantity: "0.01", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	r.seal()
	root := ExecutionLifecycleRoot{Schema: 1, ClientOrderID: r.ClientOrderID, ReservationChecksum: r.Checksum, IntentDigest: digest, Plan: planned, Stage: ExecutionReserved, CreatedAt: r.CreatedAt}
	root.seal()
	events := make([]ExecutionLifecycleEvent, 0, 3)
	prev := root.Checksum
	for seq, stage := range []ExecutionStage{ExecutionMayHaveSent, ExecutionSignedObserved, ExecutionFillsImported} {
		e := ExecutionLifecycleEvent{Schema: 1, ClientOrderID: r.ClientOrderID, Sequence: seq + 1,
			PreviousChecksum: prev, Stage: stage, At: r.CreatedAt}
		if stage == ExecutionSignedObserved || stage == ExecutionFillsImported {
			e.ExchangeOrderID = 123
			e.ExchangeStatus = "FILLED"
			e.ExecutedQty = "0.01"
			e.SignedCheckedAt = r.CreatedAt
		}
		if stage == ExecutionFillsImported {
			e.ImportedFillCount = 1
			e.ImportedFillDigest = "123abc"
		}
		e.seal()
		events = append(events, e)
		prev = e.Checksum
	}
	return r, ExecutionLifecycle{Root: root, Events: events, Stage: ExecutionFillsImported}, owner, p, time.Now().UTC()
}

func TestEntryProtectionHandoffAlwaysNonExecutable(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			r, l, o, p, now := handoffFixture(t, side)
			mock := &armingMock{owner: o, plan: p, algos: map[int64]exchange.DemoAlgoResult{}}
			snap, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
			if err != nil {
				t.Fatal(err)
			}
			got := ReviewEntryProtectionHandoff(r, l, o, p, snap, now)
			if got.State != "LOCAL_HANDOFF_SHAPE_REVIEW_NOT_AUTHORIZED" || got.TradingAuthorized || got.ProtectionSubmitAuthorized || got.SettlementAuthorized || !got.ManualReviewRequired || got.ImportedFillCount != 1 || got.RequireTradingAuthorization() == nil {
				t.Fatalf("unexpected: %+v", got)
			}
		})
	}
}
func TestEntryProtectionHandoffBlocksUnsafeStates(t *testing.T) {
	r, l, o, p, now := handoffFixture(t, Long)
	mock := &armingMock{owner: o, plan: p, algos: map[int64]exchange.DemoAlgoResult{}}
	fresh, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]func(*Reservation, *ExecutionLifecycle, *OwnerPosition, *ProtectionDispatchPlan, *exchange.ExecutionPreflightSnapshot){
		"may-have-sent": func(_ *Reservation, l *ExecutionLifecycle, _ *OwnerPosition, _ *ProtectionDispatchPlan, _ *exchange.ExecutionPreflightSnapshot) {
			l.Stage = ExecutionMayHaveSent
		},
		"missing-fills": func(_ *Reservation, l *ExecutionLifecycle, _ *OwnerPosition, _ *ProtectionDispatchPlan, _ *exchange.ExecutionPreflightSnapshot) {
			l.Events[len(l.Events)-1].ImportedFillCount = 0
			l.Events[len(l.Events)-1].seal()
		},
		"tampered-reservation": func(r *Reservation, _ *ExecutionLifecycle, _ *OwnerPosition, _ *ProtectionDispatchPlan, _ *exchange.ExecutionPreflightSnapshot) {
			r.Intent.Quantity = "0.02"
		},
		"wrong-owner": func(_ *Reservation, _ *ExecutionLifecycle, o *OwnerPosition, _ *ProtectionDispatchPlan, _ *exchange.ExecutionPreflightSnapshot) {
			o.Quantity = "0.02"
		},
		"wrong-side": func(_ *Reservation, _ *ExecutionLifecycle, o *OwnerPosition, _ *ProtectionDispatchPlan, _ *exchange.ExecutionPreflightSnapshot) {
			o.Side = Short
		},
		"bad-plan": func(_ *Reservation, _ *ExecutionLifecycle, _ *OwnerPosition, p *ProtectionDispatchPlan, _ *exchange.ExecutionPreflightSnapshot) {
			p.TP.Quantity = "0.02"
		},
		"stale-account": func(_ *Reservation, _ *ExecutionLifecycle, _ *OwnerPosition, _ *ProtectionDispatchPlan, s *exchange.ExecutionPreflightSnapshot) {
			s.FinishedAt = now.Add(-time.Minute)
		},
		"missing-open-algos": func(_ *Reservation, _ *ExecutionLifecycle, _ *OwnerPosition, _ *ProtectionDispatchPlan, s *exchange.ExecutionPreflightSnapshot) {
			s.OpenAlgos = nil
		},
	}
	for name, change := range checks {
		t.Run(name, func(t *testing.T) {
			rr, ll, oo, pp, ss := r, l, o, p, fresh
			ll.Events = append([]ExecutionLifecycleEvent(nil), l.Events...)
			change(&rr, &ll, &oo, &pp, &ss)
			got := ReviewEntryProtectionHandoff(rr, ll, oo, pp, ss, now)
			if got.State != "BLOCKED" || len(got.Blockers) == 0 || got.TradingAuthorized || got.ProtectionSubmitAuthorized {
				t.Fatalf("unsafe state accepted: %+v", got)
			}
		})
	}
}

func TestEntryProtectionHandoffRejectsBrokenEarlierLifecycleLinks(t *testing.T) {
	r, l, o, p, now := handoffFixture(t, Long)
	mock := &armingMock{owner: o, plan: p, algos: map[int64]exchange.DemoAlgoResult{}}
	signed, err := (&armingTransportAdapter{m: mock}).FetchDemoExecutionPreflight(context.Background(), "k", "s", "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*ExecutionLifecycle){
		"missing-may-have-sent":      func(l *ExecutionLifecycle) { l.Events = l.Events[1:] },
		"tampered-early-event":       func(l *ExecutionLifecycle) { l.Events[0].At = "2020-01-01T00:00:00Z" },
		"rehashed-early-event":       func(l *ExecutionLifecycle) { l.Events[0].At = "2020-01-01T00:00:00Z"; l.Events[0].seal() },
		"broken-intermediate-link":   func(l *ExecutionLifecycle) { l.Events[1].PreviousChecksum = "fake"; l.Events[1].seal() },
		"reordered-events":           func(l *ExecutionLifecycle) { l.Events[0], l.Events[1] = l.Events[1], l.Events[0] },
		"missing-signed-observation": func(l *ExecutionLifecycle) { l.Events = append(l.Events[:1], l.Events[2:]...) },
		"invalid-intermediate-shape": func(l *ExecutionLifecycle) { l.Events[1].ImportedFillCount = 2; l.Events[1].seal() },
		"falsified-root-stage":       func(l *ExecutionLifecycle) { l.Root.Stage = ExecutionFillsImported; l.Root.seal() },
		"derived-stage-mismatch":     func(l *ExecutionLifecycle) { l.Stage = ExecutionSignedObserved },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			altered := l
			altered.Events = append([]ExecutionLifecycleEvent(nil), l.Events...)
			mutate(&altered)
			got := ReviewEntryProtectionHandoff(r, altered, o, p, signed, now)
			if got.State != "BLOCKED" || got.ProtectionSubmitAuthorized || got.TradingAuthorized || len(got.Blockers) == 0 {
				t.Fatalf("broken history accepted: %+v", got)
			}
		})
	}
}
