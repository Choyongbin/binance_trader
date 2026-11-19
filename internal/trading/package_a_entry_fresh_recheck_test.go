package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type testFreshReader struct {
	order  exchange.OrderResult
	fills  []exchange.AccountTrade
	fail   error
	mutate func()
	calls  int
}

func (r *testFreshReader) QueryDemoOrder(_ context.Context, _, _, _, _ string) (exchange.OrderResult, error) {
	r.calls++
	if r.fail != nil {
		return exchange.OrderResult{}, r.fail
	}
	if r.mutate != nil && r.calls == 2 {
		r.mutate()
	}
	return r.order, nil
}
func (r *testFreshReader) FetchDemoOrderTrades(_ context.Context, _, _, _ string, _ int64) ([]exchange.AccountTrade, error) {
	if r.fail != nil {
		return nil, r.fail
	}
	return append([]exchange.AccountTrade(nil), r.fills...), nil
}
func TestPackageAEntryFreshRecheckLongShortAndNoMutation(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, archive := entryRecoveryFixture(t, side, true)
			plan, err := PlanPackageAEntryRecovery(context.Background(), data, archive)
			if err != nil {
				t.Fatal(err)
			}
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			rd := &testFreshReader{order: archive.Order, fills: archive.Fills}
			report, err := RecheckPackageAEntryRecovery(context.Background(), data, archive, plan, rd, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if rd.calls != 2 || report.Stage != PackageAEntryFreshRecheckStage || !report.SignedOrderAndFillsMatch || len(report.MissingLocalTradeIDs) != 1 || report.MissingLocalTradeIDs[0] != 901 || len(report.LocalLifecycleEvents) != 0 || report.LocalLifecycleStage != "MISSING_LIFECYCLE_BLOCKED" || !includesBlocker(report.BlockerCodes, "MISSING_ENTRY_LIFECYCLE") || report.RootCauseProven || report.TradingAuthorized || report.SettlementAuthorized || report.RecoveryApplied || report.LifecycleChanged || report.OwnershipChanged || report.FillJournalChanged || report.Checksum != report.digest() || !includesBlocker(report.BlockerCodes, "ATOMIC_LEDGER_POSTING_UNIMPLEMENTED") {
				t.Fatalf("unsafe or incomplete recheck: %+v", report)
			}
			if !strings.Contains(report.FailureBoundary, "NOT_PROVEN") {
				t.Fatal("incident root cause incorrectly claimed")
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("financial originals changed: %v", err)
			}
			// A new invocation must produce the same durable evidence binding.
			again, err := RecheckPackageAEntryRecovery(context.Background(), data, archive, plan, rd, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
			if err != nil || again.OriginalLocalEvidenceSHA256 != report.OriginalLocalEvidenceSHA256 {
				t.Fatalf("recheck restart failure: %v", err)
			}
		})
	}
}
func TestPackageAEntryFreshRecheckRejectsDriftAndTamper(t *testing.T) {
	for _, kind := range []string{"price", "fee", "order-status", "network", "tamper-plan", "cancel", "source-drift"} {
		t.Run(kind, func(t *testing.T) {
			data, archive := entryRecoveryFixture(t, Short, true)
			plan, err := PlanPackageAEntryRecovery(context.Background(), data, archive)
			if err != nil {
				t.Fatal(err)
			}
			rd := &testFreshReader{order: archive.Order, fills: append([]exchange.AccountTrade(nil), archive.Fills...)}
			ctx := context.Background()
			switch kind {
			case "price":
				rd.order.AveragePrice = "1"
			case "fee":
				rd.fills[0].Commission = "3.00000"
			case "order-status":
				rd.order.Status = "PARTIALLY_FILLED"
			case "network":
				rd.fail = errors.New("offline")
			case "tamper-plan":
				plan.Candidates[0].ProposedStoredFillSHA256 = "deadbeef"
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "source-drift":
				// An unexpected original ledger change invalidates the signed plan.
				if err := os.WriteFile(filepath.Join(data, "trading", "fills", "BTCUSDT_12345.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			report, err := RecheckPackageAEntryRecovery(ctx, data, archive, plan, rd, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
			if err == nil || report.SignedOrderAndFillsMatch || report.SettlementAuthorized || report.TradingAuthorized || report.Checksum != "" {
				t.Fatalf("unsafe recheck accepted %s: %+v", kind, report)
			}
		})
	}
}
func TestPackageAEntryFreshRecheckRejectsNilCredentialsAndReader(t *testing.T) {
	data, archive := entryRecoveryFixture(t, Long, true)
	plan, _ := PlanPackageAEntryRecovery(context.Background(), data, archive)
	if _, err := RecheckPackageAEntryRecovery(context.Background(), data, archive, plan, nil, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now()); err == nil {
		t.Fatal("nil reader accepted")
	}
	if _, err := RecheckPackageAEntryRecovery(context.Background(), data, archive, plan, entryArchiveReader{archive.Order, archive.Fills}, DemoCredentials{}, time.Now()); err == nil {
		t.Fatal("missing credentials accepted")
	}
}

func TestPackageAEntryFreshRecheckReportsLastDurableMayHaveSentWithoutClaimingRootCause(t *testing.T) {
	data, oldArchive := entryRecoveryFixture(t, Short, true)
	rec, err := readRecord(filepath.Join(data, "trading", "requests", oldArchive.Gap.ClientOrderID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-2 * time.Second)
	res := Reservation{Schema: 1, ClientOrderID: rec.Plan.ClientOrderID, Intent: rec.Intent, IntentDigest: rec.IntentDigest, ReservedQuantity: rec.Plan.Quantity, MarginUSDT: "1", CreatedAt: now.Format(time.RFC3339Nano)}
	res.seal()
	dir := filepath.Join(data, "trading", "reservations")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, res.ClientOrderID+".json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := NewExecutionLifecycleStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Begin(res, now); err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	archive, err := CapturePackageAEntryArchive(context.Background(), data, res.ClientOrderID, entryArchiveReader{oldArchive.Order, oldArchive.Fills}, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanPackageAEntryRecovery(context.Background(), data, archive)
	if err != nil {
		t.Fatal(err)
	}
	before, err := snapshotExecutionEvidence(data)
	if err != nil {
		t.Fatal(err)
	}
	report, err := RecheckPackageAEntryRecovery(context.Background(), data, archive, plan, entryArchiveReader{archive.Order, archive.Fills}, DemoCredentials{APIKey: "fake", Secret: "fake"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if report.LocalLifecycleStage != "MAY_HAVE_SENT" || len(report.LocalLifecycleEvents) != 2 || report.LocalLifecycleEvents[1].Stage != ExecutionMayHaveSent || report.RootCauseProven || report.RecoveryApplied || report.TradingAuthorized || includesBlocker(report.BlockerCodes, "MISSING_ENTRY_LIFECYCLE") {
		t.Fatalf("incorrect lifecycle report: %+v", report)
	}
	after, err := snapshotExecutionEvidence(data)
	if err != nil || after != before {
		t.Fatalf("original changed %v", err)
	}
}
