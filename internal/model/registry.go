package model

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

// Strategy is a pure signal calculator. It MUST NOT place, cancel, or query
// private orders. All account/owner checks remain in the trading engine.
// The ID is durable: changing trading semantics requires a new versioned ID.
type Strategy interface {
	ID() string
	EvaluateLive(exchange.LiveMarketSnapshot, OwnerState, time.Time) (Decision, error)
}

type builtinStrategy struct{}

func (builtinStrategy) ID() string { return TestStrategyID }
func (builtinStrategy) EvaluateLive(m exchange.LiveMarketSnapshot, owner OwnerState, now time.Time) (Decision, error) {
	return EvaluateLive(m, owner, now)
}

// BuiltinStrategy preserves all existing ema_rsi_macd_v1 signal semantics.
func BuiltinStrategy() Strategy { return builtinStrategy{} }

// strictBuiltinStrategy is a separate registered model using the SAME
// market, ownership, risk, lifecycle, and signed order engine.
type strictBuiltinStrategy struct{}

func (strictBuiltinStrategy) ID() string { return StrictStrategyID }
func (strictBuiltinStrategy) EvaluateLive(m exchange.LiveMarketSnapshot, owner OwnerState, now time.Time) (Decision, error) {
	return EvaluateStrictLive(m, owner, now)
}

func StrictBuiltinStrategy() Strategy { return strictBuiltinStrategy{} }

var strategyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)

// Registry is explicitly configured at process startup. After Freeze, no
// strategy can be replaced: the same model ID always means the same evaluator
// for the lifetime of this process. Trading still requires signed guards.
type Registry struct {
	mu     sync.RWMutex
	models map[string]Strategy
	frozen bool
}

func NewRegistry() *Registry { return &Registry{models: make(map[string]Strategy)} }

func (r *Registry) Register(s Strategy) error {
	if r == nil || s == nil {
		return errors.New("model registry와 strategy가 필요합니다")
	}
	v := reflect.ValueOf(s)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if v.IsNil() {
			return errors.New("nil strategy를 등록할 수 없습니다")
		}
	}
	id := s.ID()
	if !strategyIDPattern.MatchString(id) {
		return fmt.Errorf("유효하지 않은 model ID: %q", id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return errors.New("model registry는 이미 동결되었습니다")
	}
	if _, exists := r.models[id]; exists {
		return fmt.Errorf("model ID 중복 등록: %s", id)
	}
	r.models[id] = s
	return nil
}

// Freeze seals registration. A model not registered before Freeze cannot be
// selected for an execution runtime, even if a caller supplies its ID.
func (r *Registry) Freeze() error {
	if r == nil {
		return errors.New("model registry가 없습니다")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.models) == 0 {
		return errors.New("등록된 전략이 없습니다")
	}
	r.frozen = true
	return nil
}

func (r *Registry) Resolve(id string) (Strategy, error) {
	if r == nil {
		return nil, errors.New("model registry가 없습니다")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.frozen {
		return nil, errors.New("model registry가 동결되지 않았습니다")
	}
	s, ok := r.models[id]
	if !ok {
		return nil, fmt.Errorf("등록되지 않은 model ID: %q", id)
	}
	return s, nil
}

func (r *Registry) IDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0, len(r.models))
	for id := range r.models {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// DefaultRegistry contains both immutable built-in strategies. Registration
// remains source-controlled; choosing a model NEVER authorizes trading.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	if err := r.Register(BuiltinStrategy()); err != nil {
		panic(err)
	}
	if err := r.Register(StrictBuiltinStrategy()); err != nil {
		panic(err)
	}
	if err := r.Freeze(); err != nil {
		panic(err)
	}
	return r
}
