package trading

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// A production coordinator with a mock signed-GET adapter. The live client
// deliberately points at an unreachable address to catch accidental network
// access. The injected reader has POST and DELETE methods too; inspection
// must not invoke them.
func productInspectorFixture(t *testing.T, f *entryLifecycleFixture) *DemoProtectionRuntimeCoordinator {
	t.Helper()
	live := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	product, err := NewDemoProtectionRuntimeCoordinator(f.root, f.coordinator, live, func(context.Context) (DemoCredentials, error) {
		return DemoCredentials{APIKey: "test-only", Secret: "not-real"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	product.reader = f.signed
	return product
}

func TestProductProtectionInspectorReadOnlyAfterStopAndRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			f := newEntryLifecycleFixture(t, side)
			product := productInspectorFixture(t, f)
			control, err := OpenAutoRunControl(f.root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := control.Start(); err != nil {
				t.Fatal(err)
			}
			if err := control.Stop(); err != nil {
				t.Fatal(err)
			}
			before, err := product.InspectEntryProtection(context.Background(), f.reservation)
			if err != nil || before.State != EntryLifecyclePreparedOnly {
				t.Fatalf("STOP inspection: %+v %v", before, err)
			}
			assertReadOnlyLifecycle(t, f, before)
			f.armPair(t)
			// Open stores as a cold product process would. A STOP'd run lease is
			// not needed for post-entry operator inspection.
			product = productInspectorFixture(t, f)
			after, err := product.InspectEntryProtection(context.Background(), f.reservation)
			if err != nil || after.State != EntryLifecyclePairActive {
				t.Fatalf("restart inspection: %+v %v", after, err)
			}
			assertReadOnlyLifecycle(t, f, after)
			if result, err := product.Arm(context.Background(), after.GroupID, OwnerPosition{}); !errors.Is(err, ErrProtectionApproval) || result.Armed {
				t.Fatalf("product Arm enabled: %+v %v", result, err)
			}
			if _, err := product.CancelTriggeredSibling(context.Background(), after.GroupID, ProtectionEvidence{}); !errors.Is(err, ErrProtectionApproval) {
				t.Fatalf("product DELETE enabled: %v", err)
			}
		})
	}
}

func TestProductProtectionInspectorFailClosedOnAmbiguousAndDrift(t *testing.T) {
	f := newEntryLifecycleFixture(t, Long)
	product := productInspectorFixture(t, f)
	if err := f.protection.MarkMayHaveSent(f.plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	got, err := product.InspectEntryProtection(context.Background(), f.reservation)
	if err != nil || got.State != EntryLifecycleUnknownSubmission || got.RetryAuthorized {
		t.Fatalf("ambiguous dispatch: %+v %v", got, err)
	}
	assertReadOnlyLifecycle(t, f, got)
	f.signed.failGET = true
	got, err = product.InspectEntryProtection(context.Background(), f.reservation)
	if err == nil || got.State != EntryLifecycleBlocked || got.TradingAuthorized {
		t.Fatalf("signed GET failure: %+v %v", got, err)
	}
	assertReadOnlyLifecycle(t, f, got)
	f.signed.failGET = false
	f.signed.foreignAlgo = true
	got, err = product.InspectEntryProtection(context.Background(), f.reservation)
	if err == nil || got.State != EntryLifecycleBlocked {
		t.Fatalf("foreign algo: %+v %v", got, err)
	}
	assertReadOnlyLifecycle(t, f, got)
}

func TestProductProtectionInspectorInvalidContextOrDependenciesNeverFetches(t *testing.T) {
	f := newEntryLifecycleFixture(t, Short)
	product := productInspectorFixture(t, f)
	cases := []struct {
		name string
		p    *DemoProtectionRuntimeCoordinator
		ctx  context.Context
	}{
		{"nil_runtime", nil, context.Background()},
		{"nil_ctx", product, nil},
		{"nil_reader", &DemoProtectionRuntimeCoordinator{coordinator: f.coordinator, lifecycle: f.lifecycle, submissions: f.protection, cancels: f.cancels, credentials: product.credentials}, context.Background()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.p.InspectEntryProtection(tc.ctx, f.reservation)
			if err == nil || got.State != EntryLifecycleBlocked || !got.ManualReviewRequired || got.TradingAuthorized || got.RetryAuthorized {
				t.Fatalf("unsafe invalid inspection: %+v %v", got, err)
			}
		})
	}
	if f.signed.getCalls != 0 || f.signed.submitCalls != 0 || f.signed.cancelCalls != 0 {
		t.Fatalf("unexpected I/O GET=%d POST=%d DELETE=%d", f.signed.getCalls, f.signed.submitCalls, f.signed.cancelCalls)
	}
}

func TestProductRuntimeFactoriesBindOperatorProtectionInspection(t *testing.T) {
	root := t.TempDir()
	client := exchange.NewWithBase("http://127.0.0.1:1", &http.Client{Timeout: time.Millisecond})
	creds := func(context.Context) (DemoCredentials, error) {
		return DemoCredentials{APIKey: "test-only", Secret: "dummy"}, nil
	}
	market := func(context.Context) (exchange.LiveMarketSnapshot, error) {
		t.Fatal("GET-only inspection must not consult the market signal feed")
		return exchange.LiveMarketSnapshot{}, nil
	}
	control, err := OpenAutoRunControl(root)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := control.Start()
	if err != nil {
		t.Fatal(err)
	}
	one, err := NewDemoAutoExecutionRuntime(root, "ema_rsi_macd_v1", client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	if one.protection == nil || one.protection.reader == nil || one.protection.lifecycle == nil || one.protection.coordinator != one.coordinator {
		t.Fatal("single product factory did not bind its signed inspection scope")
	}
	bad := Reservation{ClientOrderID: "not-a-sealed-order"}
	got, err := one.InspectEntryProtection(context.Background(), bad)
	if err == nil || got.State != EntryLifecycleBlocked || got.TradingAuthorized {
		t.Fatalf("single product inspector accepted invalid entry: %+v %v", got, err)
	}
	inv, err := one.InspectOutstandingProtections(context.Background())
	if err != nil || inv.State != PendingProtectionInspectionEmpty || inv.TradingAuthorized || !inv.ManualReviewRequired {
		t.Fatalf("single product inventory not bound: %+v %v", inv, err)
	}
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}
	many, err := NewDemoMultiModelAutoRuntime(root, []string{"ema_rsi_macd_v1", "ema_rsi_macd_strict_v1"}, client, creds, market, control, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer many.Close()
	if many.protection == nil || many.protection.reader == nil || many.protection.coordinator == nil {
		t.Fatal("multi product factory did not bind its signed inspection scope")
	}
	got, err = many.InspectEntryProtection(context.Background(), bad)
	if err == nil || got.State != EntryLifecycleBlocked || got.CancelAuthorized {
		t.Fatalf("multi product inspector accepted invalid entry: %+v %v", got, err)
	}
	inv, err = many.InspectOutstandingProtections(context.Background())
	if err != nil || inv.State != PendingProtectionInspectionEmpty || inv.TradingAuthorized || !inv.ManualReviewRequired {
		t.Fatalf("multi product inventory not bound: %+v %v", inv, err)
	}
}
