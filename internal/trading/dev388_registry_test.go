package trading

import (
	"context"
	"strings"
	"testing"

	"binance_trader/internal/exchange"
)

// An internal constructor must fail closed on misconfiguration rather than
// panicking at registry.Resolve (even if it is not exposed to HTTP).
func TestDev388NilModelRegistryRejected(t *testing.T) {
	control, err := OpenAutoRunControl(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	client := exchange.NewWithBase("http://127.0.0.1:1", nil)
	_, err = NewDemoMultiModelAutoRuntimeWithRegistry(nil, t.TempDir(), []string{"one", "two"}, client,
		func(context.Context) (DemoCredentials, error) { return DemoCredentials{}, nil },
		func(context.Context) (exchange.LiveMarketSnapshot, error) { return exchange.LiveMarketSnapshot{}, nil },
		control, lease)
	if err == nil || !strings.Contains(err.Error(), "필수 구성요소") {
		t.Fatalf("nil registry must reject without panic: %v", err)
	}
}
