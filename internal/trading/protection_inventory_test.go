package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func TestMultipleKnownOwnerProtectionsVsExternal(t *testing.T) {
	now := time.Now().UTC()
	a := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: now.Format(time.RFC3339Nano), AlgoOrders: []exchange.AlgoOrder{}}
	owners := []OwnerPosition{{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: Long, Quantity: "0.01"}, {Source: Model, ModelID: "B", Symbol: "BTCUSDT", Side: Long, Quantity: "0.02"}}
	expected := []ExpectedProtection{{Source: Model, ModelID: "A", Side: Long, Quantity: "0.01", TPClientAlgoID: "tp-a", SLClientAlgoID: "sl-a"}, {Source: Model, ModelID: "B", Side: Long, Quantity: "0.02", TPClientAlgoID: "tp-b", SLClientAlgoID: "sl-b"}}
	for i, p := range expected {
		for j, cid := range []string{p.TPClientAlgoID, p.SLClientAlgoID} {
			typ := "TAKE_PROFIT_MARKET"
			work := "CONTRACT_PRICE"
			if j == 1 {
				typ = "STOP_MARKET"
				work = "MARK_PRICE"
			}
			a.AlgoOrders = append(a.AlgoOrders, exchange.AlgoOrder{Symbol: "BTCUSDT", PositionSide: "LONG", Side: "SELL", AlgoID: int64(1 + i*2 + j), ClientAlgoID: cid, Quantity: p.Quantity, TriggerPrice: "100000", OrderType: typ, WorkingType: work, AlgoStatus: "NEW"})
		}
	}
	r := ReviewProtectionInventory(a, owners, expected, now)
	if r.Status != "KNOWN_PROTECTED" || r.TradingAuthorized || r.Entries != 2 {
		t.Fatalf("valid multiple owners %+v", r)
	}
	a.AlgoOrders = append(a.AlgoOrders, exchange.AlgoOrder{AlgoID: 9, ClientAlgoID: "outside", Symbol: "BTCUSDT", Quantity: "0.1", AlgoStatus: "NEW"})
	r = ReviewProtectionInventory(a, owners, expected, now)
	if r.Status != "BLOCKED" {
		t.Fatal("foreign algo accepted")
	}
	a.AlgoOrders = a.AlgoOrders[:4]
	a.AlgoOrders[0].Quantity = "0.02"
	r = ReviewProtectionInventory(a, owners, expected, now)
	if r.Status != "BLOCKED" {
		t.Fatal("overprotected owner")
	}
	a.AlgoOrders[0].Quantity = "0.01"
	a.AlgoOrders[0].ClosePosition = true
	r = ReviewProtectionInventory(a, owners, expected, now)
	if r.Status != "BLOCKED" {
		t.Fatal("close all accepted")
	}
	a.AlgoOrders[0].ClosePosition = false
	a.AlgoOrders[0].TriggerPrice = "0"
	if got := ReviewProtectionInventory(a, owners, expected, now); got.Status != "BLOCKED" {
		t.Fatal("zero trigger accepted")
	}
	a.AlgoOrders[0].TriggerPrice = "100000"
	owners = append(owners, OwnerPosition{Source: Model, ModelID: "unprotected", Symbol: "BTCUSDT", Side: Short, Quantity: "0.01"})
	if got := ReviewProtectionInventory(a, owners, expected, now); got.Status != "BLOCKED" {
		t.Fatal("unprotected owner accepted")
	}
}
