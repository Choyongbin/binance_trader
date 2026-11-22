package trading

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

func TestProductProtectionDispatchApprovalDeniesBeforeNetworkOrBaseGuard(t *testing.T) {
	invoked := 0
	guarded := requireProductProtectionDispatchApproval(func(context.Context, Intent) error {
		invoked++
		return nil
	})
	for _, action := range []Action{Enter, Exit, Action("UNKNOWN")} {
		t.Run(string(action), func(t *testing.T) {
			for _, side := range []PositionSide{Long, Short} {
				intent := Intent{Source: Model, Symbol: "BTCUSDT", PositionSide: side, Action: action}
				if err := guarded(context.Background(), intent); !errors.Is(err, ErrProtectionApproval) {
					t.Fatalf("unexpected gate error: %v", err)
				}
			}
		})
	}
	if invoked != 0 {
		t.Fatalf("base guard ran %d times despite denial", invoked)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := guarded(ctx, Intent{Action: Enter}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if err := guarded(nil, Intent{Action: Enter}); err == nil {
		t.Fatal("nil context accepted")
	}
}

func TestProductDemoRuntimeFactoryBindsProtectionDispatchGate(t *testing.T) {
	dir := t.TempDir()
	client := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	creds := func(context.Context) (DemoCredentials, error) {
		t.Fatal("unexpected credential retrieval before product dispatch denial")
		return DemoCredentials{}, nil
	}
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) {
		t.Fatal("unexpected market retrieval before product dispatch denial")
		return exchange.LiveMarketSnapshot{}, nil
	}
	control, err := OpenAutoRunControl(dir)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	single, err := NewDemoAutoExecutionRuntime(dir, model.TestStrategyID, client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []Action{Enter, Exit} {
		if err := single.Path.guard(context.Background(), Intent{Action: action, Symbol: "BTCUSDT"}); !errors.Is(err, ErrProtectionApproval) {
			t.Fatalf("single product runtime permitted action %s: %v", action, err)
		}
	}
	if err := single.Close(); err != nil {
		t.Fatal(err)
	}

	// A fresh coordinator lock is required for the second factory.
	multi, err := NewDemoMultiModelAutoRuntime(dir, model.DefaultRegistry().IDs(), client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Close()
	for _, p := range multi.paths {
		for _, action := range []Action{Enter, Exit} {
			if err := p.guard(context.Background(), Intent{Action: action, Symbol: "BTCUSDT"}); !errors.Is(err, ErrProtectionApproval) {
				t.Fatalf("multi model %s permitted action %s: %v", p.modelID, action, err)
			}
		}
	}
}
