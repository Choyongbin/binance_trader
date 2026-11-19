package exchange

import (
	"context"
	"errors"
	"testing"
)

type fakeFetch struct{ fail bool }

func (f *fakeFetch) Fetch(_ context.Context, key, secret string) (Snapshot, error) {
	if f.fail {
		return Snapshot{}, errors.New("test failure")
	}
	if key != "key" || secret != "secret" {
		return Snapshot{}, errors.New("wrong creds")
	}
	return Snapshot{Status: "connected", Environment: "demo", Balances: []Balance{{Asset: "USDT", Wallet: "100"}}}, nil
}
func TestManagerClearAndFailureNoStaleData(t *testing.T) {
	fake := &fakeFetch{}
	m := NewManager(fake)
	defer m.Close()
	if err := m.Connect(context.Background(), "key", "secret"); err != nil {
		t.Fatal(err)
	}
	snap := m.Snapshot()
	snap.Balances[0].Wallet = "tampered"
	if m.Snapshot().Balances[0].Wallet != "100" {
		t.Fatal("mutable snapshot")
	}
	fake.fail = true
	if err := m.Refresh(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	result := m.Snapshot()
	if result.Status != "error" || result.Balances != nil {
		t.Fatalf("stale data leaked: %+v", result)
	}
	if err := m.Refresh(context.Background()); err == nil {
		t.Fatal("should require reconnect")
	}
	m.Clear()
	if m.Snapshot().Status != "locked" {
		t.Fatal("clear failed")
	}
}
