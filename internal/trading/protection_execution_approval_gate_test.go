package trading

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type approvalGateSignedReader struct {
	baselines []exchange.RecoveryBaseline
	reads     int
	failAt    int
	onRead    func(int)
}

func (m *approvalGateSignedReader) FetchDemoRecoveryBaseline(_ context.Context, _, _ string, _ int64) (exchange.RecoveryBaseline, error) {
	m.reads++
	if m.onRead != nil {
		m.onRead(m.reads)
	}
	if m.failAt == m.reads {
		return exchange.RecoveryBaseline{}, errors.New("signed GET failure")
	}
	if m.reads > len(m.baselines) {
		return exchange.RecoveryBaseline{}, errors.New("unexpected third signed GET")
	}
	return m.baselines[m.reads-1], nil
}

func newProtectionGateFixture(t *testing.T, side PositionSide) (string, Reservation, *approvalGateSignedReader, int64) {
	t.Helper()
	engine, store, signedOrders := setupOwnership(t)
	root := filepath.Dir(filepath.Dir(store.fillDir))
	intent := makeIntent("gate-entry-001", Model, "gate_model", side, Enter, "0.01")
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil {
		t.Fatal(err)
	}
	order := orderFromRecord(rec, "FILLED", "0.01")
	// Reconstruct an on-disk reservation and a sealed signed lifecycle, not
	// merely an in-memory clientOrderID. This gate must reject old/incomplete
	// entries even when the signed account quantity happens to match.
	c, err := OpenCoordinator(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	reservation, _, err := c.reservePreview(intent, cleanReservationBasis(time.Now().UTC()), time.Now().UTC())
	if err != nil || reservation.ClientOrderID != rec.Plan.ClientOrderID {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	lifecycle, err := NewExecutionLifecycleStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Begin(reservation, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.MarkMayHaveSent(reservation, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.RecordSignedObservation(reservation, OrderObservation{Order: order, CheckedAt: time.Now().UTC()}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signedOrders.orders[rec.Plan.ClientOrderID] = order
	signedOrders.trades[order.OrderID] = []exchange.AccountTrade{receipt(rec, 9172, "0.01", "100000", "0.1", "0", time.Now().UTC().Add(-1500*time.Millisecond).UnixMilli())}
	if n, err := store.ImportDemoOrder(context.Background(), signedOrders, DemoCredentials{APIKey: "demo", Secret: "secret"}, intent); err != nil || n != 1 {
		t.Fatalf("import: %d %v", n, err)
	}
	if _, err := lifecycle.RecordFillsImported(reservation, store, OrderObservation{Order: order, CheckedAt: time.Now().UTC()}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// The existing owner replay, rather than the review caller, is the
	// starting point for the immutable plan. The new gate independently
	// rebuilds and verifies it against the two authenticated baselines.
	owner := OwnerPosition{Source: rec.Intent.Source, ModelID: rec.Intent.ModelID, Symbol: "BTCUSDT", Side: side, Quantity: "0.01", AverageEntry: "100000"}
	ps, err := OpenProtectionSubmissionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	quote := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	preview, err := PreviewProtection(owner, sizingRules(now), quote, quote, entryProtectionGroupID(reservation), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ps.Prepare(entryProtectionGroupID(reservation), owner, preview); err != nil {
		t.Fatal(err)
	}

	fromMS := now.Add(-time.Hour).UnixMilli()
	baseline := func(start time.Time) exchange.RecoveryBaseline {
		b := recoveryBaseline(start.Add(400 * time.Millisecond))
		b.StartedAt = start
		b.FinishedAt = start.Add(200 * time.Millisecond)
		b.WindowStartMS = fromMS
		b.WindowEndMS = start.UnixMilli()
		b.Portfolio = recoveryPortfolio()
		b.Portfolio.Positions = []exchange.PortfolioPosition{{Symbol: "BTCUSDT", PositionSide: string(side), PositionAmt: "0.01", InitialMargin: "10", Isolated: true}}
		if side == Short {
			b.Portfolio.Positions[0].PositionAmt = "-0.01"
		}
		b.Portfolio.TotalInitialMargin = "10"
		b.Portfolio.TotalPositionInitialMargin = "10"
		for _, remote := range signedOrders.orders {
			b.RecentBTCOrders = append(b.RecentBTCOrders, exchange.Order{OrderID: remote.OrderID, ClientOrderID: remote.ClientOrderID,
				Symbol: remote.Symbol, PositionSide: remote.PositionSide, Side: remote.Side, Type: remote.Type, Status: remote.Status,
				OrigQty: remote.OriginalQty, ExecutedQty: remote.ExecutedQty})
		}
		for _, trades := range signedOrders.trades {
			b.RecentBTCTrades = append(b.RecentBTCTrades, trades...)
		}
		return b
	}
	firstStart := now.Add(-800 * time.Millisecond)
	secondStart := now.Add(-300 * time.Millisecond)
	return root, reservation, &approvalGateSignedReader{baselines: []exchange.RecoveryBaseline{baseline(firstStart), baseline(secondStart)}}, fromMS
}

func testGateFingerprint() string {
	sum := sha256.Sum256([]byte("test-demo"))
	return hex.EncodeToString(sum[:])
}
func gateCall(root string, r Reservation, m *approvalGateSignedReader, since int64) (ProtectionExecutionApprovalReview, error) {
	return InspectProtectionExecutionApprovalGate(context.Background(), root, r, m, DemoCredentials{APIKey: "test-demo", Secret: "not-a-real-secret"}, testGateFingerprint(), since)
}
func assertNeverApproved(t *testing.T, r ProtectionExecutionApprovalReview) {
	t.Helper()
	if r.TradingAuthorized || r.ProtectionSubmitAuthorized || r.SiblingCancelAuthorized || r.FinanciallySettled || r.ContinuousCoverageProven || !r.ManualReviewRequired {
		t.Fatalf("unsafe approval gate %+v", r)
	}
}
func TestProtectionExecutionApprovalGateLongShortBoundedReviewOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			root, r, m, since := newProtectionGateFixture(t, side)
			got, err := gateCall(root, r, m, since)
			if err != nil || got.State != ProtectionGateBoundedReview || got.FirstAccountSHA256 == "" || got.FirstAccountSHA256 != got.SecondAccountSHA256 ||
				got.LocalEvidenceSHA256 == "" || got.LocalOwnerCount != 1 || m.reads != 2 {
				t.Fatalf("gate result %+v err=%v signed GETs=%d", got, err, m.reads)
			}
			assertNeverApproved(t, got)
			if !strings.Contains(strings.Join(got.Reasons, " "), "OCO") {
				t.Fatal("missing non-OCO warning")
			}
			// Restarting the review cannot create an order, mark approval, or
			// access any POST/DELETE interface (mock provides only signed GET).
			m.reads = 0
			again, err := gateCall(root, r, m, since)
			if err != nil || again.State != ProtectionGateBoundedReview {
				t.Fatalf("restart %+v %v", again, err)
			}
			assertNeverApproved(t, again)
		})
	}
}
func TestProtectionExecutionApprovalGateRejectsSignedErrorsAndDrift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*approvalGateSignedReader)
	}{
		{"first signed GET fails", func(m *approvalGateSignedReader) { m.failAt = 1 }},
		{"second signed GET fails", func(m *approvalGateSignedReader) { m.failAt = 2 }},
		{"foreign currency hedge", func(m *approvalGateSignedReader) {
			m.baselines[0].Portfolio.Positions = append(m.baselines[0].Portfolio.Positions, exchange.PortfolioPosition{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "1", Isolated: true})
		}},
		{"other open order", func(m *approvalGateSignedReader) {
			m.baselines[0].OpenOrders = append(m.baselines[0].OpenOrders, exchange.Order{Symbol: "ETHUSDT", OrderID: 321})
		}},
		{"foreign conditional order", func(m *approvalGateSignedReader) {
			m.baselines[0].OpenAlgos = append(m.baselines[0].OpenAlgos, exchange.AlgoOrder{AlgoID: 887, ClientAlgoID: "external", Symbol: "BTCUSDT"})
		}},
		{"signed drift", func(m *approvalGateSignedReader) { m.baselines[1].Portfolio.Positions[0].PositionAmt = "0.02" }},
		{"non-isolated", func(m *approvalGateSignedReader) { m.baselines[0].Portfolio.Positions[0].Isolated = false }},
		{"unaudited external trade", func(m *approvalGateSignedReader) {
			m.baselines[0].RecentBTCTrades = append(m.baselines[0].RecentBTCTrades, exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 999, OrderID: 889, TimeMS: m.baselines[0].WindowEndMS - 100, Price: "100000", Quantity: "0.01", Commission: "0.1", RealizedPnL: "0", CommissionAsset: "USDT", Side: "BUY", PositionSide: "LONG"})
		}},
		{"older-than-window evidence", func(m *approvalGateSignedReader) { m.baselines[0].WindowStartMS = m.baselines[0].WindowStartMS + 1 }},
		{"missing recent trade list", func(m *approvalGateSignedReader) { m.baselines[0].RecentBTCTrades = nil }},
		{"stale signed account", func(m *approvalGateSignedReader) {
			m.baselines[0].FinishedAt = m.baselines[0].FinishedAt.Add(-time.Minute)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, r, m, since := newProtectionGateFixture(t, Long)
			tc.modify(m)
			got, err := gateCall(root, r, m, since)
			if err == nil || got.State != ProtectionGateBlocked {
				t.Fatalf("unsafe accept %+v %v", got, err)
			}
			assertNeverApproved(t, got)
		})
	}
}
func TestProtectionExecutionApprovalGateRejectsLocalMutations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(string, Reservation, *approvalGateSignedReader)
	}{
		{"missing sealed lifecycle fill import", func(root string, r Reservation, m *approvalGateSignedReader) {
			_ = os.Remove(filepath.Join(root, "trading", "execution_lifecycle", r.ClientOrderID+".stage.000003.json"))
		}},
		{"missing immutable reservation", func(root string, r Reservation, m *approvalGateSignedReader) {
			_ = os.Remove(filepath.Join(root, "trading", "reservations", r.ClientOrderID+".json"))
		}},
		{"unknown second protection plan", func(root string, r Reservation, m *approvalGateSignedReader) {
			_ = os.WriteFile(filepath.Join(root, "trading", "protection_submissions", "foreign.json"), []byte(`{}`), 0600)
		}},
		{"cancel intent exists", func(root string, r Reservation, m *approvalGateSignedReader) {
			dir := filepath.Join(root, "trading", "algo_cancel_intents")
			_ = os.MkdirAll(dir, 0700)
			_ = os.WriteFile(filepath.Join(dir, "foreign.json"), []byte(`{}`), 0600)
		}},
		{"local journal mutation during second GET", func(root string, r Reservation, m *approvalGateSignedReader) {
			m.onRead = func(n int) {
				if n == 2 {
					_ = os.WriteFile(filepath.Join(root, "trading", "fills", "BTCUSDT_99999.json"), []byte(`{}`), 0600)
				}
			}
		}},
		{"untracked protection mutation during second GET", func(root string, r Reservation, m *approvalGateSignedReader) {
			m.onRead = func(n int) {
				if n == 2 {
					_ = os.WriteFile(filepath.Join(root, "trading", "protection_submissions", "foreign.json"), []byte(`{}`), 0600)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, r, m, since := newProtectionGateFixture(t, Long)
			tc.change(root, r, m)
			got, err := gateCall(root, r, m, since)
			if err == nil || got.State != ProtectionGateBlocked {
				t.Fatalf("mutation accepted %+v %v", got, err)
			}
			assertNeverApproved(t, got)
		})
	}
}

func TestProtectionExecutionApprovalGateRejectsWrongAccountFingerprint(t *testing.T) {
	root, r, m, since := newProtectionGateFixture(t, Long)
	got, err := InspectProtectionExecutionApprovalGate(context.Background(), root, r, m,
		DemoCredentials{APIKey: "test-demo", Secret: "not-a-real-secret"}, strings.Repeat("0", 64), since)
	if err == nil || got.State != ProtectionGateBlocked || m.reads != 0 {
		t.Fatalf("wrong account accepted %+v %v", got, err)
	}
	assertNeverApproved(t, got)
}
