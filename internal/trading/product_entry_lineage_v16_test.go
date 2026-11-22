package trading

import (
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func makeEntryLineageCaseV16(t *testing.T) ([]Record, []StoredFill, []ProtectionDispatchPlan, exchange.RecoveryBaseline) {
	t.Helper()
	now := time.Now().UTC()
	intent := Intent{RequestID: "entry-v16-test-0001", Source: Model, ModelID: "ema_rsi", Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.01"}
	order, err := intent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	plan := ProtectionDispatchPlan{Schema: 1, GroupID: "entry_" + order.ClientOrderID, Source: Model, ModelID: "ema_rsi", PositionSide: Long,
		TP: ProtectionInstruction{ClientAlgoID: "v16_tp_entry_one", Symbol: "BTCUSDT", Side: "SELL", PositionSide: Long, AlgoType: "CONDITIONAL", OrderType: "TAKE_PROFIT_MARKET", WorkingType: "CONTRACT_PRICE", Quantity: "0.01", TriggerPrice: "101000"},
		SL: ProtectionInstruction{ClientAlgoID: "v16_sl_entry_one", Symbol: "BTCUSDT", Side: "SELL", PositionSide: Long, AlgoType: "CONDITIONAL", OrderType: "STOP_MARKET", WorkingType: "MARK_PRICE", Quantity: "0.01", TriggerPrice: "99000"}, CreatedAt: now.Format(time.RFC3339Nano)}
	plan.Digest = plan.digest()
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	record := Record{Intent: intent, Plan: order, ExchangeOrderID: 1001}
	trade := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 9001, OrderID: 1001, Side: "BUY", PositionSide: "LONG", Quantity: "0.01", Price: "100000", Commission: "0.1", RealizedPnL: "0", CommissionAsset: "USDT", TimeMS: now.Add(-time.Minute).UnixMilli()}
	fill := StoredFill{ClientOrderID: order.ClientOrderID, Trade: trade}
	baseline := exchange.RecoveryBaseline{RecentBTCOrders: []exchange.Order{{ClientOrderID: order.ClientOrderID, Symbol: "BTCUSDT", PositionSide: "LONG", Side: "BUY", OrderID: 1001, Type: "MARKET", OrigQty: "0.01", ExecutedQty: "0.01", Status: "FILLED"}},
		RecentBTCTrades: []exchange.AccountTrade{trade}, RecentBTCAlgos: []exchange.AlgoOrder{}}
	return []Record{record}, []StoredFill{fill}, []ProtectionDispatchPlan{plan}, baseline
}
func TestProductEntryLineageV16BuildLinksOnlySignedJournal(t *testing.T) {
	rec, fills, plans, base := makeEntryLineageCaseV16(t)
	groups, err := buildSignedEntryLineageV16(rec, fills, plans, base, false)
	if err != nil || len(groups) != 1 || groups[0].GroupID != "entry_"+rec[0].Plan.ClientOrderID ||
		groups[0].ProtectionPlanSHA256 != plans[0].Digest || groups[0].EntryTrades[0].TradeID != 9001 || groups[0].ExitOrProtectionFillSeen {
		t.Fatalf("group reconstruction %+v %v", groups, err)
	}
	rec, fills, plans, base = makeEntryLineageCaseV16(t)
	groups, err = buildSignedEntryLineageV16(rec, fills, plans, base, true)
	if err != nil || len(groups) != 1 || !groups[0].ExitOrProtectionFillSeen {
		t.Fatalf("exit not marked %+v %v", groups, err)
	}
}
func TestProductEntryLineageV16RefusesUnprovenEvents(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]Record, []StoredFill, []ProtectionDispatchPlan, *exchange.RecoveryBaseline)
	}{
		{"missing signed fill", func(_ []Record, _ []StoredFill, _ []ProtectionDispatchPlan, b *exchange.RecoveryBaseline) {
			b.RecentBTCTrades = []exchange.AccountTrade{}
		}},
		{"unimported foreign trade", func(_ []Record, _ []StoredFill, _ []ProtectionDispatchPlan, b *exchange.RecoveryBaseline) {
			b.RecentBTCTrades = append(b.RecentBTCTrades, exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 8002})
		}},
		{"order partially filled", func(_ []Record, _ []StoredFill, _ []ProtectionDispatchPlan, b *exchange.RecoveryBaseline) {
			b.RecentBTCOrders[0].Status = "PARTIALLY_FILLED"
		}},
		{"different exchange order", func(_ []Record, _ []StoredFill, _ []ProtectionDispatchPlan, b *exchange.RecoveryBaseline) {
			b.RecentBTCOrders[0].OrderID = 333
		}},
		{"different executed quantity", func(_ []Record, _ []StoredFill, _ []ProtectionDispatchPlan, b *exchange.RecoveryBaseline) {
			b.RecentBTCOrders[0].ExecutedQty = "0.009"
		}},
		{"plan digest mismatch", func(_ []Record, _ []StoredFill, p []ProtectionDispatchPlan, _ *exchange.RecoveryBaseline) {
			p[0].Digest = strings.Repeat("f", 64)
		}},
		{"signed trade amount drift", func(_ []Record, _ []StoredFill, _ []ProtectionDispatchPlan, b *exchange.RecoveryBaseline) {
			b.RecentBTCTrades[0].Quantity = "0.02"
		}},
		{"local trade missing", func(_ []Record, f []StoredFill, _ []ProtectionDispatchPlan, _ *exchange.RecoveryBaseline) {
			f[0].Trade.TradeID = 8002
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, f, p, b := makeEntryLineageCaseV16(t)
			tc.mutate(r, f, p, &b)
			if _, err := buildSignedEntryLineageV16(r, f, p, b, false); err == nil {
				t.Fatal("unsafe lineage accepted")
			}
		})
	}
}
