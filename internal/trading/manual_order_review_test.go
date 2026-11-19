package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func manualReviewFixture() (Record, SignedManualEvidence, time.Time) {
	now := time.Now().UTC()
	i := makeIntent("manual-review-001", Manual, "", Long, Enter, "0.010")
	p, _ := i.Plan()
	h, _ := digestIntent(i)
	r := Record{Schema: 1, Intent: i, IntentDigest: h, Plan: p, State: Acknowledged, ExchangeOrderID: 4001}
	e := SignedManualEvidence{Order: orderFromRecord(r, "NEW", "0"), OrderReadAt: now, FromSignedGET: true, TradesFetchedAt: now, FullTradesFromSignedGET: true,
		Trades: []exchange.AccountTrade{}}
	return r, e, now
}
func TestManualOrderReviewStateMachineNoPermission(t *testing.T) {
	r, e, now := manualReviewFixture()
	table := []struct {
		name, status, filled string
		fill                 []string
		expected             string
	}{
		{"unfilled", "NEW", "0", nil, "OPEN_UNFILLED"},
		{"partially filled", "PARTIALLY_FILLED", "0.004", []string{"0.004"}, "OPEN_PARTIAL"},
		{"partial then canceled", "CANCELED", "0.004", []string{"0.004"}, "TERMINAL_PARTIAL"},
		{"filled", "FILLED", "0.010", []string{"0.004", "0.006"}, "FILLED_MATCH"},
		{"canceled zero", "CANCELED", "0", nil, "TERMINAL_EMPTY"},
		{"expired partial", "EXPIRED", "0.003", []string{"0.003"}, "TERMINAL_PARTIAL"},
		{"new but execution", "NEW", "0.004", []string{"0.004"}, "BLOCKED"},
		{"partial zero", "PARTIALLY_FILLED", "0", nil, "BLOCKED"},
		{"filled short", "FILLED", "0.009", []string{"0.009"}, "BLOCKED"},
		{"sum mismatch", "PARTIALLY_FILLED", "0.004", []string{"0.003"}, "BLOCKED"},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			o := e
			o.Order.Status = tt.status
			o.Order.ExecutedQty = tt.filled
			o.Trades = nil
			for idx, qty := range tt.fill {
				o.Trades = append(o.Trades, receipt(r, int64(idx+1), qty, "100000", "0.01", "0", now.UnixMilli()))
			}
			review := ReviewManualOrder(r, o, now)
			if review.State != tt.expected || review.MayReleaseReservation || review.MayPlaceProtection || !review.IndependentAccountAuditRequired {
				t.Fatalf("invalid review %+v", review)
			}
		})
	}
}
func TestManualReviewRejectsStaleMalformedAndForgedEvidence(t *testing.T) {
	rec, e, now := manualReviewFixture()
	cases := []struct {
		name  string
		apply func(*Record, *SignedManualEvidence)
	}{
		{"no signature", func(r *Record, e *SignedManualEvidence) { e.FromSignedGET = false }},
		{"incomplete fills", func(r *Record, e *SignedManualEvidence) { e.FullTradesFromSignedGET = false }},
		{"stale account", func(r *Record, e *SignedManualEvidence) { e.OrderReadAt = now.Add(-time.Minute) }},
		{"foreign ID", func(r *Record, e *SignedManualEvidence) { e.Order.ClientOrderID = "bt_unrelated" }},
		{"foreign order", func(r *Record, e *SignedManualEvidence) { e.Order.OrderID = 444 }},
		{"wrong side", func(r *Record, e *SignedManualEvidence) { e.Order.Side = "SELL" }},
		{"wrong quantity", func(r *Record, e *SignedManualEvidence) { e.Order.OriginalQty = "0.02" }},
		{"modified journal", func(r *Record, e *SignedManualEvidence) { r.Intent.Quantity = "0.02" }},
		{"model cannot use manual path", func(r *Record, e *SignedManualEvidence) { r.Intent.Source = Model; r.Intent.ModelID = "m1" }},
		{"future observed", func(r *Record, e *SignedManualEvidence) { e.TradesFetchedAt = now.Add(time.Minute) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r, x := rec, e
			tt.apply(&r, &x)
			review := ReviewManualOrder(r, x, now)
			if review.State != "BLOCKED" || review.MayReleaseReservation || review.MayPlaceProtection {
				t.Fatalf("unsafe accepted %+v", review)
			}
		})
	}
	e.Order.Status = "FILLED"
	e.Order.ExecutedQty = "0.010"
	t1 := receipt(rec, 1, "0.005", "100000", "0.1", "0", now.UnixMilli())
	e.Trades = []exchange.AccountTrade{t1, t1}
	if review := ReviewManualOrder(rec, e, now); review.State != "BLOCKED" {
		t.Fatalf("duplicate trade accepted: %+v", review)
	}
	e.Trades[1].TradeID = 2
	e.Trades[1].OrderID = 4002
	if review := ReviewManualOrder(rec, e, now); review.State != "BLOCKED" {
		t.Fatalf("foreign trade accepted: %+v", review)
	}
	e.Trades[1].OrderID = 4001
	e.Trades[1].TimeMS = now.Add(time.Minute).UnixMilli()
	if review := ReviewManualOrder(rec, e, now); review.State != "BLOCKED" {
		t.Fatalf("future trade accepted: %+v", review)
	}
}
