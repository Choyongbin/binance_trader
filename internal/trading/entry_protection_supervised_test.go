package trading

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"binance_trader/internal/exchange"
)

func TestArmImportedEntryProtectionSupervisedMockSuccessAndNoReplay(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			r, life, owner, _, _ := handoffFixture(t, side)
			store, plan, _, _ := testProtectionGroup(t, side)
			mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
			transport := &armingTransportAdapter{m: mock}
			coord, err := OpenCoordinator(filepath.Dir(filepath.Dir(store.dir)))
			if err != nil {
				t.Fatal(err)
			}
			defer coord.Close()
			got, err := ArmImportedEntryProtectionSupervised(context.Background(), plan.GroupID, r, life, owner, coord, store, transport, DemoCredentials{"k", "s"})
			if err != nil || !got.Armed || mock.submitCalls != 2 || got.TradingAuthorized {
				t.Fatalf("got=%+v submits=%d err=%v", got, mock.submitCalls, err)
			}
			_, err = ArmImportedEntryProtectionSupervised(context.Background(), plan.GroupID, r, life, owner, coord, store, transport, DemoCredentials{"k", "s"})
			if err == nil || mock.submitCalls != 2 {
				t.Fatalf("replay allowed: submits=%d err=%v", mock.submitCalls, err)
			}
		})
	}
}

func TestArmImportedEntryProtectionSupervisedRefusesTamperWithoutPost(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			r, life, owner, _, _ := handoffFixture(t, side)
			store, plan, _, _ := testProtectionGroup(t, side)
			mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}}
			coord, err := OpenCoordinator(filepath.Dir(filepath.Dir(store.dir)))
			if err != nil {
				t.Fatal(err)
			}
			defer coord.Close()
			transport := &armingTransportAdapter{m: mock}
			badLife := life
			badLife.Events = append([]ExecutionLifecycleEvent(nil), life.Events...)
			badLife.Events[0].At = "2000-01-01T00:00:00Z"
			for name, candidate := range map[string]struct {
				r Reservation
				l ExecutionLifecycle
				o OwnerPosition
			}{
				"tampered-lifecycle": {r, badLife, owner},
				"missing-import":     {r, ExecutionLifecycle{Root: life.Root, Events: life.Events[:2], Stage: ExecutionSignedObserved}, owner},
				"wrong-owner":        {r, life, func() OwnerPosition { x := owner; x.Quantity = "0.02"; return x }()},
				"wrong-reservation":  {func() Reservation { x := r; x.Intent.Quantity = "0.02"; return x }(), life, owner},
			} {
				t.Run(name, func(t *testing.T) {
					_, err := ArmImportedEntryProtectionSupervised(context.Background(), plan.GroupID, candidate.r, candidate.l, candidate.o, coord, store, transport, DemoCredentials{"k", "s"})
					if err == nil || mock.submitCalls != 0 {
						t.Fatalf("unsafe submit: calls=%d err=%v", mock.submitCalls, err)
					}
				})
			}
		})
	}
}

func TestArmImportedEntryProtectionSupervisedAmbiguousSLNeverRetries(t *testing.T) {
	r, life, owner, _, _ := handoffFixture(t, Short)
	store, plan, _, _ := testProtectionGroup(t, Short)
	mock := &armingMock{owner: owner, plan: plan, algos: map[int64]exchange.DemoAlgoResult{}, failSecond: true}
	transport := &armingTransportAdapter{m: mock}
	coord, err := OpenCoordinator(filepath.Dir(filepath.Dir(store.dir)))
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	_, err = ArmImportedEntryProtectionSupervised(context.Background(), plan.GroupID, r, life, owner, coord, store, transport, DemoCredentials{"k", "s"})
	if err == nil || !strings.Contains(err.Error(), "SL") || mock.submitCalls != 2 {
		t.Fatalf("ambiguous SL: %v calls=%d", err, mock.submitCalls)
	}
	_, err = ArmImportedEntryProtectionSupervised(context.Background(), plan.GroupID, r, life, owner, coord, store, transport, DemoCredentials{"k", "s"})
	if err == nil || mock.submitCalls != 2 {
		t.Fatalf("replayed ambiguous SL: %v calls=%d", err, mock.submitCalls)
	}
}
