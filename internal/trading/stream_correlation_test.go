package trading

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func streamCorrelationFixture(t *testing.T) (Record, SignedOrderEvidence, AccountEvent, time.Time) {
	t.Helper()
	now := time.Now().UTC()
	rec, _, _ := manualReviewFixture()
	e := SignedOrderEvidence{ClientOrderID: rec.Plan.ClientOrderID,
		Before: orderFromRecord(rec, "PARTIALLY_FILLED", "0.004"), After: orderFromRecord(rec, "PARTIALLY_FILLED", "0.004"),
		Trades:       []exchange.AccountTrade{receipt(rec, 711, "0.004", "100000", "0.10", "0", now.Add(-time.Second).UnixMilli())},
		BeforeReadAt: now.Add(-100 * time.Millisecond), TradesReadAt: now.Add(-50 * time.Millisecond), AfterReadAt: now,
		SignedOrderReads: true, CompleteSignedTrades: true}
	raw := fmt.Sprintf(`{"e":"ORDER_TRADE_UPDATE","E":%d,"T":%d,"o":{"s":"BTCUSDT","c":"%s","i":%d,"t":711,"S":"BUY","ps":"LONG","x":"TRADE","X":"PARTIALLY_FILLED","z":"0.004","l":"0.004","L":"100000","n":"0.1","N":"USDT","rp":"0"}}`,
		now.Add(-time.Second).UnixMilli(), now.Add(-time.Second).UnixMilli(), rec.Plan.ClientOrderID, rec.ExchangeOrderID)
	event, err := ParseAccountEvent([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return rec, e, event, now
}
func TestStreamCorrelationVerifiedSignalNeverGrantsPermission(t *testing.T) {
	rec, e, ev, now := streamCorrelationFixture(t)
	r := ReviewStreamOrderCorrelation([]AccountEvent{ev}, []Record{rec}, []SignedOrderEvidence{e}, now)
	if r.State != "OBSERVED_SIGNALS_MATCH" || r.MatchedTradeEvents != 1 || r.MatchedOrderCount != 1 || r.TradingAuthorized ||
		r.StreamContinuityProven || !r.RequiresFullAccountReview || !r.ManualReviewRequired || len(r.Blockers) == 0 {
		t.Fatalf("signal incorrectly promotes permissions %+v", r)
	}
	a := AccountEvent{Kind: "ACCOUNT_UPDATE", EventTime: now.Add(-2 * time.Second).UnixMilli(),
		Account: &StreamAccount{Reason: "FUNDING_FEE", Assets: []string{"USDT"}}, Digest: strings.Repeat("a", 64)}
	r = ReviewStreamOrderCorrelation([]AccountEvent{a, ev}, []Record{rec}, []SignedOrderEvidence{e}, now)
	if r.State != "OBSERVED_SIGNALS_MATCH" || r.AccountEventCount != 1 || !r.RequiresFullAccountReview {
		t.Fatalf("sparse account update lost %+v", r)
	}
	r = ReviewStreamOrderCorrelation([]AccountEvent{a}, nil, nil, now)
	if r.State != "NO_ORDER_SIGNALS" || r.TradingAuthorized || len(r.Blockers) == 0 {
		t.Fatalf("empty signal assumed safe %+v", r)
	}
}
func TestStreamCorrelationRejectsForeignStaleDuplicateAndContradictory(t *testing.T) {
	rec, e, ev, now := streamCorrelationFixture(t)
	cases := []struct {
		name   string
		change func(*Record, *SignedOrderEvidence, *AccountEvent)
	}{
		{"foreign order", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.ClientID = "external_client" }},
		{"wrong qty", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.LastQty = "0.005" }},
		{"wrong fee", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.Fee = "0.2" }},
		{"wrong pnl", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.RealizedPnL = "0.1" }},
		{"wrong asset", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.FeeAsset = "BNB" }},
		{"wrong order id", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.OrderID = 33 }},
		{"unknown execution", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.Execution = "TRADE_UNKNOWN" }},
		{"trade with NEW status", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.Status = "NEW" }},
		{"stream behind signed terminal", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { v.Order.Status = "FILLED" }},
		{"signed stale", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { e.AfterReadAt = now.Add(-time.Minute) }},
		{"query changed state", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { e.After.Status = "FILLED" }},
		{"incomplete signed trades", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { e.CompleteSignedTrades = false }},
		{"fake signed trades", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { e.Trades[0].Quantity = "0.003" }},
		{"no signature", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { e.SignedOrderReads = false }},
		{"tampered journal", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) { r.Intent.Quantity = "0.12" }},
		{"future stream", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) {
			v.EventTime = now.Add(2 * time.Second).UnixMilli()
		}},
		{"stream newer than snapshot", func(r *Record, e *SignedOrderEvidence, v *AccountEvent) {
			v.EventTime = now.Add(800 * time.Millisecond).UnixMilli()
			e.AfterReadAt = now.Add(-time.Second)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := rec
			ee := e
			ee.Trades = append([]exchange.AccountTrade{}, e.Trades...)
			vv := ev
			ov := *ev.Order
			vv.Order = &ov
			tc.change(&rr, &ee, &vv)
			result := ReviewStreamOrderCorrelation([]AccountEvent{vv}, []Record{rr}, []SignedOrderEvidence{ee}, now)
			if result.State != "BLOCKED" || result.TradingAuthorized || result.StreamContinuityProven || len(result.Blockers) == 0 {
				t.Fatalf("risk accepted %+v", result)
			}
		})
	}
	same := ev
	altered := ev
	other := *ev.Order
	altered.Order = &other
	altered.Digest = strings.Repeat("b", 64)
	r := ReviewStreamOrderCorrelation([]AccountEvent{same, altered}, []Record{rec}, []SignedOrderEvidence{e}, now)
	if r.State != "BLOCKED" {
		t.Fatalf("conflicting trade id accepted %+v", r)
	}
	r = ReviewStreamOrderCorrelation([]AccountEvent{same, same}, []Record{rec}, []SignedOrderEvidence{e}, now)
	if r.State != "BLOCKED" {
		t.Fatalf("duplicate event accepted %+v", r)
	}
	risk := AccountEvent{Kind: "MARGIN_CALL", EventTime: now.UnixMilli(), Digest: strings.Repeat("f", 64)}
	r = ReviewStreamOrderCorrelation([]AccountEvent{risk}, nil, nil, now)
	if r.State != "BLOCKED" {
		t.Fatalf("margin event improperly accepted %+v", r)
	}
}
func TestFetchSignedOrderEvidenceBracketsReads(t *testing.T) {
	_, reader, rec, _ := setupManualCancel(t)
	now := time.Now().UTC()
	reader.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{receipt(rec, 711, "0.004", "100000", "0.10", "0", now.UnixMilli())}
	evidence, err := FetchSignedOrderEvidence(context.Background(), reader, DemoCredentials{APIKey: "fake-demo", Secret: "fake-secret"}, rec)
	if err != nil || !evidence.SignedOrderReads || len(evidence.Trades) != 1 {
		t.Fatalf("stable signed bracket failed %+v %v", evidence, err)
	}
	reader.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "NEW", "0.004")
	reader.mutate = true
	if _, err = FetchSignedOrderEvidence(context.Background(), reader, DemoCredentials{"fake-demo", "fake-secret"}, rec); err == nil {
		t.Fatal("changed order accepted")
	}
	reader.fail = true
	if _, err = FetchSignedOrderEvidence(context.Background(), reader, DemoCredentials{"fake-demo", "fake-secret"}, rec); err == nil {
		t.Fatal("REST failure accepted")
	}
}
func TestDiagnosePersistedStreamAndCorruption(t *testing.T) {
	_, reader, rec, dir := setupManualCancel(t)
	now := time.Now().UTC()
	reader.trades[rec.ExchangeOrderID] = []exchange.AccountTrade{receipt(rec, 711, "0.004", "100000", "0.10", "0", now.Add(-time.Second).UnixMilli())}
	e, err := FetchSignedOrderEvidence(context.Background(), reader, DemoCredentials{"fake-demo", "fake-secret"}, rec)
	if err != nil {
		t.Fatal(err)
	}
	_, _, ev, _ := streamCorrelationFixture(t)
	// Fixture records have the same deterministic clientOrderID but the signed
	// stored cancel fixture is a different request. Build actual stored event.
	payload := fmt.Sprintf(`{"e":"ORDER_TRADE_UPDATE","E":%d,"o":{"s":"BTCUSDT","c":"%s","i":%d,"t":711,"S":"BUY","ps":"LONG","x":"TRADE","X":"PARTIALLY_FILLED","z":"0.004","l":"0.004","L":"100000","n":"0.10","N":"USDT","rp":"0"}}`,
		now.Add(-time.Second).UnixMilli(), rec.Plan.ClientOrderID, rec.ExchangeOrderID)
	ev, err = ParseAccountEvent([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenEventStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.RecordDiscontinuity("STARTUP"); err != nil {
		t.Fatal(err)
	}
	if fresh, err := store.Append(ev); err != nil || !fresh {
		t.Fatalf("append %v %v", fresh, err)
	}
	result, err := DiagnoseStoredStream(dir, store, []SignedOrderEvidence{e}, time.Now().UTC())
	if err != nil || result.Review.State != "OBSERVED_SIGNALS_MATCH" || result.GapCount != 1 || result.TradingAuthorized || !result.Review.ManualReviewRequired {
		t.Fatalf("diagnosis %+v %v", result, err)
	}
	reports, err := LoadStreamReviews(dir)
	if err != nil || len(reports) != 1 || reports[0].SHA256 != result.SHA256 {
		t.Fatalf("archive %+v %v", reports, err)
	}
	path := filepath.Join(dir, "trading", "stream_reviews", result.ID+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var modified map[string]any
	if err = json.Unmarshal(raw, &modified); err != nil {
		t.Fatal(err)
	}
	modified["trading_authorized"] = true
	changed, _ := json.Marshal(modified)
	if err = os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadStreamReviews(dir); err == nil {
		t.Fatal("tampered review accepted")
	}
	if err = os.WriteFile(filepath.Join(dir, "trading", "requests", "unknown.tmp"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = DiagnoseStoredStream(dir, store, []SignedOrderEvidence{e}, time.Now().UTC()); err == nil {
		t.Fatal("unknown journal file accepted")
	}
}

func TestStreamCorrelationManualAndModelShortSeparated(t *testing.T) {
	manual, manualEvidence, manualEvent, now := streamCorrelationFixture(t)
	intent := makeIntent("model-short-001", Model, "test_ema_rsi", Short, Enter, "0.010")
	plan, err := intent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := digestIntent(intent)
	model := Record{Schema: 1, Intent: intent, IntentDigest: digest, Plan: plan, State: Acknowledged, ExchangeOrderID: 4002}
	modelEvidence := SignedOrderEvidence{ClientOrderID: plan.ClientOrderID,
		Before: orderFromRecord(model, "FILLED", "0.010"), After: orderFromRecord(model, "FILLED", "0.010"),
		Trades:       []exchange.AccountTrade{receipt(model, 712, "0.010", "101000", "0.2", "0", now.Add(-time.Second).UnixMilli())},
		BeforeReadAt: now.Add(-100 * time.Millisecond), TradesReadAt: now.Add(-50 * time.Millisecond), AfterReadAt: now,
		SignedOrderReads: true, CompleteSignedTrades: true}
	raw := fmt.Sprintf(`{"e":"ORDER_TRADE_UPDATE","E":%d,"T":%d,"o":{"s":"BTCUSDT","c":"%s","i":4002,"t":712,"S":"SELL","ps":"SHORT","x":"TRADE","X":"FILLED","z":"0.010","l":"0.010","L":"101000","n":"0.2","N":"USDT","rp":"0"}}`,
		now.Add(-time.Second).UnixMilli(), now.Add(-time.Second).UnixMilli(), plan.ClientOrderID)
	modelEvent, err := ParseAccountEvent([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	r := ReviewStreamOrderCorrelation([]AccountEvent{modelEvent, manualEvent}, []Record{model, manual},
		[]SignedOrderEvidence{modelEvidence, manualEvidence}, now)
	if r.State != "OBSERVED_SIGNALS_MATCH" || r.MatchedOrderCount != 2 || r.MatchedTradeEvents != 2 ||
		r.TradingAuthorized || !r.ManualReviewRequired {
		t.Fatalf("multi owner drift %+v", r)
	}
	modelEvidence.Trades[0].TradeID = 711
	r = ReviewStreamOrderCorrelation([]AccountEvent{modelEvent, manualEvent}, []Record{model, manual},
		[]SignedOrderEvidence{modelEvidence, manualEvidence}, now)
	if r.State != "BLOCKED" {
		t.Fatalf("reused signed trade ID across owners accepted: %+v", r)
	}
}
