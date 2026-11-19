package server

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"binance_trader/internal/model"
	"binance_trader/internal/trading"
)

type selectionRuntime struct{}

func (selectionRuntime) RunOnce(_ context.Context) (trading.AutoExecutionResult, error) {
	return trading.AutoExecutionResult{}, nil
}
func (selectionRuntime) Close() error { return nil }

func TestChooseAutoRuntimeWiresSingleAndSharedMultiWithoutAuthorizingOrders(t *testing.T) {
	registry := model.DefaultRegistry()
	for _, tc := range []struct {
		name string
		ids  []string
		want string
		bad  bool
	}{
		{"single", []string{model.TestStrategyID}, "single", false},
		{"multi", []string{model.TestStrategyID, model.StrictStrategyID}, "multi", false},
		{"empty", nil, "", true},
		{"duplicate", []string{model.TestStrategyID, model.TestStrategyID}, "", true},
		{"unknown", []string{"unknown"}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := []string{}
			rt, err := chooseAutoRuntime(tc.ids, registry,
				func(id string) (trading.AutoRuntime, error) {
					calls = append(calls, "single:"+id)
					return selectionRuntime{}, nil
				},
				func(ids []string) (trading.AutoRuntime, error) {
					calls = append(calls, "multi")
					if !reflect.DeepEqual(ids, tc.ids) {
						t.Fatal("ids mismatch")
					}
					ids[0] = "tampered"
					return selectionRuntime{}, nil
				},
			)
			if tc.bad {
				if err == nil || len(calls) != 0 || rt != nil {
					t.Fatalf("invalid selection admitted: %v %v", calls, err)
				}
				return
			}
			if err != nil || rt == nil || len(calls) != 1 {
				t.Fatalf("selection failed: %v %v", calls, err)
			}
			if tc.want == "multi" && (calls[0] != "multi" || tc.ids[0] != model.TestStrategyID) {
				t.Fatalf("multi routing or copy violation: %v %v", calls, tc.ids)
			}
			if tc.want == "single" && calls[0] != "single:"+model.TestStrategyID {
				t.Fatal(calls)
			}
		})
	}
	_, err := chooseAutoRuntime([]string{model.TestStrategyID}, registry, func(string) (trading.AutoRuntime, error) { return nil, errors.New("factory refused") }, func([]string) (trading.AutoRuntime, error) { t.Fatal("wrong factory"); return nil, nil })
	if err == nil {
		t.Fatal("factory error ignored")
	}
}
