package trading

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func mixedAccountingPropertyFixture(t *testing.T) ([]Record, []StoredFill, []ProtectionStoredFill, AccountingInput, time.Time) {
	t.Helper()
	base := time.Now().UTC().Add(-time.Minute).UnixMilli()
	created := time.UnixMilli(base + 10_000).UTC()
	intent := Intent{RequestID: "acct-mixed-partial-001", Source: Model, ModelID: "A", Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "1"}
	plan, err := intent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := digestIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	record := Record{Schema: 1, Intent: intent, Plan: plan, IntentDigest: digest, State: Reconciled, ExchangeOrderID: 5001}
	fill := func(id int64, qty, price, fee, feeAsset string, at int64) StoredFill {
		f := StoredFill{Schema: 1, ClientOrderID: plan.ClientOrderID, Trade: exchange.AccountTrade{
			Symbol: "BTCUSDT", TradeID: id, OrderID: 5001, Side: "BUY", PositionSide: "LONG",
			Quantity: qty, Price: price, Commission: fee, CommissionAsset: feeAsset, RealizedPnL: "0", TimeMS: at,
		}}
		f.seal()
		return f
	}
	fills := []StoredFill{
		fill(5101, "0.4", "100", "0.04", "USDT", base+1000),
		fill(5102, "0.6", "102", "0.006", "BNB", base+1100),
	}
	protectionFill := func(id, orderID, algoID int64, leg, qty, price, fee, feeAsset, pnl string, at int64) ProtectionStoredFill {
		f := ProtectionStoredFill{Schema: 1, GroupID: "pg_mixed_partial_001", PlanDigest: "plan-digest-mixed-001", Leg: leg,
			Source: Model, ModelID: "A", PositionSide: Long, AlgoID: algoID, ActualOrderID: orderID,
			Trade: exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: id, OrderID: orderID, Side: "SELL", PositionSide: "LONG",
				Quantity: qty, Price: price, Commission: fee, CommissionAsset: feeAsset, RealizedPnL: pnl, TimeMS: at},
		}
		f.Checksum = f.digest()
		return f
	}
	protection := []ProtectionStoredFill{
		protectionFill(5201, 6001, 7001, "tp", "0.25", "110", "0.02", "USDT", "2.1", base+6000),
		protectionFill(5202, 6002, 7002, "sl", "0.75", "95", "-0.001", "BNB", "-4.7", base+7000),
	}
	in := AccountingInput{WindowStartMS: base, WindowEndMS: base + 9000, SignedIncome: []exchange.IncomeRecord{
		incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "-0.5", base+3000, 5301),
		incomeFixture(t, "", "TRANSFER", "USDT", "5", base+4000, 5302),
	}}
	return []Record{record}, fills, protection, in, created
}

func TestAccountingPartialFillProtectionFeeAssetPermutationInvariant(t *testing.T) {
	records, fills, protection, in, created := mixedAccountingPropertyFixture(t)
	want, err := evaluateAccountingOffline(records, fills, protection, in, created, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner := ownerAccounting(t, want, Model, "A", Long)
	if owner.OpenQuantity != "0" || owner.VirtualGrossRealizedUSDT != "-2.45" || owner.ExchangeReportedGrossUSDT != "-2.6" ||
		owner.CommissionByAsset["USDT"] != "0.06" || owner.CommissionByAsset["BNB"] != "0.005" || !owner.NonUSDTCommission ||
		owner.ProvisionalFundingUSDT != "-0.5" || owner.ProvisionalAfterFundingUSDT != "-3.01" || owner.Protection == nil ||
		owner.Protection.FillCount != 2 || owner.Protection.DualTriggeredGroupCount != 1 || owner.Protection.TakeProfitQuantity != "0.25" ||
		owner.Protection.StopLossQuantity != "0.75" || owner.Protection.VirtualGrossRealizedUSDT != "-2.45" || owner.Protection.ExchangeReportedGrossUSDT != "-2.6" {
		t.Fatalf("mixed partial accounting mismatch: %+v", owner)
	}
	if want.OtherIncomeByTypeAsset["TRANSFER:USDT"] != "5" || want.ProtectionFillCount != 2 || want.DualTriggeredGroupCount != 1 {
		t.Fatalf("mixed report mismatch: %+v", want)
	}

	rng := rand.New(rand.NewSource(343))
	for n := 0; n < 250; n++ {
		rr := append([]Record(nil), records...)
		ff := append([]StoredFill(nil), fills...)
		pp := append([]ProtectionStoredFill(nil), protection...)
		ii := append([]exchange.IncomeRecord(nil), in.SignedIncome...)
		rng.Shuffle(len(rr), func(i, j int) { rr[i], rr[j] = rr[j], rr[i] })
		rng.Shuffle(len(ff), func(i, j int) { ff[i], ff[j] = ff[j], ff[i] })
		rng.Shuffle(len(pp), func(i, j int) { pp[i], pp[j] = pp[j], pp[i] })
		rng.Shuffle(len(ii), func(i, j int) { ii[i], ii[j] = ii[j], ii[i] })
		got, err := evaluateAccountingOffline(rr, ff, pp, AccountingInput{WindowStartMS: in.WindowStartMS, WindowEndMS: in.WindowEndMS, SignedIncome: ii}, created, nil)
		if err != nil {
			t.Fatalf("permutation %d failed: %v", n, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("permutation %d changed mixed accounting result", n)
		}
	}
}

func TestProtectionFillPersistenceIgnoresSignedTradeListOrdering(t *testing.T) {
	fixture := newProtectionSettlementFixture(t)
	regular, err := fixture.owners.readFills()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().UnixMilli()
	t1 := protectionTrade(97001, 98001, "SELL", "LONG", "0.004", "101000", "3.8")
	t2 := protectionTrade(97001, 98002, "SELL", "LONG", "0.006", "101500", "5.7")
	t1.TimeMS, t2.TimeMS = at+10, at+20
	readsA := []protectionLegRead{{leg: "tp", algoID: 1101, trades: []exchange.AccountTrade{t1, t2}}, {leg: "sl", algoID: 1102, trades: []exchange.AccountTrade{}}}
	readsB := []protectionLegRead{{leg: "tp", algoID: 1101, trades: []exchange.AccountTrade{t2, t1}}, {leg: "sl", algoID: 1102, trades: []exchange.AccountTrade{}}}

	storeA, err := NewProtectionSettlementStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := NewProtectionSettlementStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := storeA.persistProtectionFillsLocked(fixture.plan, readsA, regular); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storeB.persistProtectionFillsLocked(fixture.plan, readsB, regular); err != nil {
		t.Fatal(err)
	}
	fa, err := storeA.readProtectionFillsLocked()
	if err != nil {
		t.Fatal(err)
	}
	fb, err := storeB.readProtectionFillsLocked()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fa, fb) || protectionFillSetDigest(fa) != protectionFillSetDigest(fb) {
		t.Fatalf("signed trade ordering changed persisted protection evidence\nA=%+v\nB=%+v", fa, fb)
	}
}

func TestStreamCorrelationAccountAndOrderEventOrderingInvariant(t *testing.T) {
	rec, signed, orderEvent, now := streamCorrelationFixture(t)
	accountA := AccountEvent{Kind: "ACCOUNT_UPDATE", EventTime: now.Add(-3 * time.Second).UnixMilli(),
		Account: &StreamAccount{Reason: "FUNDING_FEE", Assets: []string{"USDT"}}, Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	accountB := AccountEvent{Kind: "ACCOUNT_UPDATE", EventTime: now.Add(-2 * time.Second).UnixMilli(),
		Account: &StreamAccount{Reason: "ORDER", Assets: []string{"USDT"}}, Digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	events := []AccountEvent{accountA, accountB, orderEvent}
	want := ReviewStreamOrderCorrelation(events, []Record{rec}, []SignedOrderEvidence{signed}, now)
	if want.State != "OBSERVED_SIGNALS_MATCH" {
		t.Fatalf("fixture not accepted: %+v", want)
	}
	rng := rand.New(rand.NewSource(344))
	for n := 0; n < 100; n++ {
		shuffled := append([]AccountEvent(nil), events...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		got := ReviewStreamOrderCorrelation(shuffled, []Record{rec}, []SignedOrderEvidence{signed}, now)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("event permutation %d changed recovery correlation result\n got=%+v\nwant=%+v", n, got, want)
		}
	}
}
