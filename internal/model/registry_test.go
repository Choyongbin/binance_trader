package model

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

type stubStrategy struct{ id string }

func (s stubStrategy) ID() string { return s.id }
func (s stubStrategy) EvaluateLive(_ exchange.LiveMarketSnapshot, _ OwnerState, _ time.Time) (Decision, error) {
	return Decision{ModelID: s.id, Status: StatusReady, Action: ActionHold}, nil
}

func TestRegistryRegistrationFreezeAndKnownDefault(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Resolve("model_v2"); err == nil {
		t.Fatal("must reject unfinished registry")
	}
	if err := r.Register(stubStrategy{"model_v2"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(stubStrategy{"model_v2"}); err == nil {
		t.Fatal("duplicate ID")
	}
	if err := r.Register(stubStrategy{"model/bad"}); err == nil {
		t.Fatal("invalid ID")
	}
	if err := r.Freeze(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve("model_v2"); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(stubStrategy{"new_model"}); err == nil {
		t.Fatal("mutation after freeze")
	}
	if _, err := r.Resolve(TestStrategyID); err == nil {
		t.Fatal("unregistered should fail")
	}
	ids := r.IDs()
	if len(ids) != 1 || ids[0] != "model_v2" {
		t.Fatalf("ids=%v", ids)
	}
	d := DefaultRegistry()
	s, err := d.Resolve(TestStrategyID)
	if err != nil || s.ID() != TestStrategyID {
		t.Fatalf("default %v, %v", s, err)
	}
	if err := d.Register(stubStrategy{"model_v2"}); err == nil {
		t.Fatal("default is mutable")
	}
	if _, err := DefaultRegistry().Resolve("model_v2"); err == nil {
		t.Fatal("custom model accidentally enabled")
	}
}

func TestRegistryNilAndInvalid(t *testing.T) {
	var nilStrategy *pointerStrategy
	r := NewRegistry()
	if err := r.Register(nilStrategy); err == nil {
		t.Fatal("typed nil")
	}
	if err := r.Register(nil); err == nil {
		t.Fatal("nil strategy")
	}
	if err := r.Freeze(); err == nil {
		t.Fatal("empty registry should reject freeze")
	}
	var rNil *Registry
	if _, err := rNil.Resolve("anything"); err == nil {
		t.Fatal("nil registry")
	}
}

type pointerStrategy struct{}

func (*pointerStrategy) ID() string { return "ptr" }
func (*pointerStrategy) EvaluateLive(exchange.LiveMarketSnapshot, OwnerState, time.Time) (Decision, error) {
	return Decision{}, nil
}
