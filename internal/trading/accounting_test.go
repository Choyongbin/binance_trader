package trading

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func accountFixture(base int64) ([]Record, []StoredFill, map[string]string) {
	records := []Record{}
	fills := []StoredFill{}
	cids := map[string]string{}
	add := func(id string, source Source, model string, side PositionSide, action Action, qty, price, fee, feeAsset, gross string, timestamp, tid int64) {
		intent := Intent{RequestID: id, Source: source, ModelID: model, Symbol: "BTCUSDT", PositionSide: side, Action: action, OrderType: Market, Quantity: qty}
		plan, _ := intent.Plan()
		hash, _ := digestIntent(intent)
		rec := Record{Schema: 1, Intent: intent, Plan: plan, IntentDigest: hash, State: Reconciled, ExchangeOrderID: 1000 + tid}
		records = append(records, rec)
		cids[id] = plan.ClientOrderID
		tr := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: tid, OrderID: rec.ExchangeOrderID, Side: rec.Plan.ExchangeSide, PositionSide: string(side),
			Price: price, Quantity: qty, Commission: fee, CommissionAsset: feeAsset, RealizedPnL: gross, TimeMS: timestamp}
		fill := StoredFill{Schema: 1, ClientOrderID: plan.ClientOrderID, Trade: tr}
		fill.seal()
		fills = append(fills, fill)
	}
	add("account-model-A-1", Model, "A", Long, Enter, "1", "100", "0.10", "USDT", "0", base+1000, 1)
	add("account-model-B-1", Model, "B", Long, Enter, "2", "110", "0.2", "USDT", "0", base+2000, 2)
	add("account-model-A-2", Model, "A", Long, Exit, "0.5", "120", "0.06", "USDT", "9.1", base+4500, 3)
	add("account-manual-short", Manual, "", Short, Enter, "0.5", "102", "0.03", "BNB", "0", base+5000, 4)
	return records, fills, cids
}
func incomeFixture(t *testing.T, symbol, kind, asset, value string, ts, id int64) exchange.IncomeRecord {
	t.Helper()
	raw := `{"symbol":"` + symbol + `","incomeType":"` + kind + `","asset":"` + asset + `","income":"` + value + `","time":` + strconv.FormatInt(ts, 10) + `,"tranId":` + strconv.FormatInt(id, 10) + `}`
	var v exchange.IncomeRecord
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func ownerAccounting(t *testing.T, r AccountingReport, source Source, model string, side PositionSide) OwnerAccounting {
	t.Helper()
	for _, o := range r.Owners {
		if o.Source == source && o.ModelID == model && o.PositionSide == side {
			return o
		}
	}
	t.Fatalf("missing owner: %+v", r.Owners)
	return OwnerAccounting{}
}
func TestExactVirtualAccountingProvisionalFundingAndSlippage(t *testing.T) {
	base := time.Now().Add(-time.Minute).UnixMilli()
	records, fills, cids := accountFixture(base)
	in := AccountingInput{WindowStartMS: base, WindowEndMS: base + 9000,
		SignedIncome: []exchange.IncomeRecord{
			incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "-3", base+3000, 30),
			incomeFixture(t, "ETHUSDT", "FUNDING_FEE", "USDT", "-7", base+3500, 31),
			incomeFixture(t, "", "TRANSFER", "USDT", "50", base+4000, 32),
			incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "BNB", "-0.01", base+6000, 33),
		}, Quotes: []BackendQuote{
			{ClientOrderID: cids["account-model-A-1"], Symbol: "BTCUSDT", Price: "99", ObservedMS: base + 990, Source: "BACKEND_WS_TRADE"},
			{ClientOrderID: cids["account-model-B-1"], Symbol: "BTCUSDT", Price: "111", ObservedMS: base + 1900, Source: "BACKEND_WS_TRADE"},
			{ClientOrderID: cids["account-model-A-2"], Symbol: "BTCUSDT", Price: "121", ObservedMS: base + 4400, Source: "BACKEND_WS_TRADE"},
		}}
	r, err := EvaluateAccountingOffline(records, fills, in, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	a := ownerAccounting(t, r, Model, "A", Long)
	if a.OpenQuantity != "0.5" || a.AverageEntry != "100" || a.VirtualGrossRealizedUSDT != "10" || a.ExchangeReportedGrossUSDT != "9.1" ||
		a.VirtualAfterUSDTFeesExFunding != "9.84" || a.ProvisionalFundingUSDT != "-1" || a.ProvisionalAfterFundingUSDT != "8.84" ||
		a.SignedUnfavorableSlippageUSDT != "1.5" || a.SlippageCoverage != "MARKET_FILL_REFERENCE_ESTIMATE_ONLY" {
		t.Fatalf("A unexpected: %+v", a)
	}
	b := ownerAccounting(t, r, Model, "B", Long)
	if b.ProvisionalFundingUSDT != "-2" || b.SignedUnfavorableSlippageUSDT != "-2" || b.ProvisionalAfterFundingUSDT != "-2.2" {
		t.Fatalf("B unexpected: %+v", b)
	}
	manual := ownerAccounting(t, r, Manual, "", Short)
	if manual.NonUSDTCommission != true || manual.SlippageCoverage != "INCOMPLETE_OR_STALE_BACKEND_QUOTE" || manual.ProvisionalFundingUSDT != "0" {
		t.Fatalf("manual unexpected: %+v", manual)
	}
	if r.AccountFundingByAsset["USDT"] != "-10" || r.UnallocatedFundingByAsset["USDT"] != "-7" ||
		r.UnallocatedFundingByAsset["BNB"] != "-0.01" || r.OtherIncomeByTypeAsset["TRANSFER:USDT"] != "50" ||
		r.TradingAuthorized || r.ActualNetVerified || a.ActualNetVerified {
		t.Fatalf("account result unexpected %+v", r)
	}
}
func TestFundingBothDirectionsCannotAttributeAggregate(t *testing.T) {
	base := time.Now().Add(-time.Minute).UnixMilli()
	records, fills, _ := accountFixture(base)
	r, err := EvaluateAccountingOffline(records, fills, AccountingInput{
		WindowStartMS: base, WindowEndMS: base + 9000, SignedIncome: []exchange.IncomeRecord{
			incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "1.25", base+5500, 55),
		},
	}, time.Now())
	if err != nil || r.UnallocatedFundingByAsset["USDT"] != "1.25" || ownerAccounting(t, r, Manual, "", Short).ProvisionalFundingUSDT != "0" {
		t.Fatalf("mixed directions incorrectly allocated %+v: %v", r, err)
	}
}
func TestAccountingFailsClosedOnCorruptionDuplicateAndTiming(t *testing.T) {
	base := time.Now().Add(-time.Minute).UnixMilli()
	records, fills, cids := accountFixture(base)
	in := AccountingInput{WindowStartMS: base, WindowEndMS: base + 9000, SignedIncome: []exchange.IncomeRecord{}}
	altered := append([]StoredFill(nil), fills...)
	altered[0].Trade.Price = "99"
	if _, err := EvaluateAccountingOffline(records, altered, in, time.Now()); err == nil {
		t.Fatal("accepted changed fill digest")
	}
	dup := incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "0.01", base+3000, 44)
	in.SignedIncome = []exchange.IncomeRecord{dup, dup}
	if _, err := EvaluateAccountingOffline(records, fills, in, time.Now()); err == nil {
		t.Fatal("accepted duplicate signed income")
	}
	in.SignedIncome = []exchange.IncomeRecord{}
	in.Quotes = []BackendQuote{{ClientOrderID: cids["account-model-A-1"], Symbol: "BTCUSDT", Price: "98", ObservedMS: base + 5000, Source: "BACKEND_WS_TRADE"}}
	if r, err := EvaluateAccountingOffline(records, fills, in, time.Now()); err != nil || ownerAccounting(t, r, Model, "A", Long).SlippageCoverage != "INCOMPLETE_OR_STALE_BACKEND_QUOTE" {
		t.Fatalf("future quote not marked unknown %+v %v", r, err)
	}
	in.WindowStartMS = 0
	if _, err := EvaluateAccountingOffline(records, fills, in, time.Now()); err == nil {
		t.Fatal("accepted absent coverage")
	}
}
func TestArchiveAccountingImmutableAndReadOnlyStatus(t *testing.T) {
	base := time.Now().Add(-time.Minute).UnixMilli()
	records, fills, _ := accountFixture(base)
	r, err := EvaluateAccountingOffline(records, fills, AccountingInput{
		WindowStartMS: base, WindowEndMS: base + 9000, SignedIncome: []exchange.IncomeRecord{},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if got, e := LoadAccountingReports(dir); e != nil || len(got) != 0 {
		t.Fatalf("empty %+v %v", got, e)
	}
	if err := ArchiveAccounting(dir, &r); err != nil {
		t.Fatal(err)
	}
	reports, err := LoadAccountingReports(dir)
	if err != nil || len(reports) != 1 || reports[0].SHA256 != r.SHA256 || reports[0].ActualNetVerified {
		t.Fatalf("archive failure %+v %v", reports, err)
	}
	if err := ArchiveAccounting(dir, &r); err != nil {
		t.Fatal(err)
	}
	reports, err = LoadAccountingReports(dir)
	if err != nil || len(reports) != 2 || reports[0].ID == reports[1].ID {
		t.Fatalf("append expected %v %+v", err, reports)
	}
	p := filepath.Join(dir, "trading", "accounting_reports", reports[0].ID+".json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(raw), `"trading_authorized": false`, `"trading_authorized": true`, 1)
	if err := os.WriteFile(p, []byte(changed), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAccountingReports(dir); err == nil {
		t.Fatal("accepted modified archive")
	}
}

func TestSignedDemoBaselineValidationBeforeAccounting(t *testing.T) {
	_, store, _ := setupOwnership(t)
	now := time.Now().UTC()
	start := now.Add(-time.Minute).UnixMilli()
	b := exchange.RecoveryBaseline{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS",
		StartedAt: now.Add(-time.Second), FinishedAt: now, WindowStartMS: start, WindowEndMS: now.UnixMilli(),
		RecentIncome: []exchange.IncomeRecord{}, OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{}}
	result, err := store.AnalyzeFromSignedDemoBaseline(b, nil, now)
	if err != nil || result.TradingAuthorized || result.ActualNetVerified {
		t.Fatalf("baseline %+v %v", result, err)
	}
	b.FinishedAt = now.Add(-10 * time.Second)
	if _, err = store.AnalyzeFromSignedDemoBaseline(b, nil, now); err == nil {
		t.Fatal("accepted stale baseline")
	}
	b.FinishedAt = now
	b.RecentIncome = nil
	if _, err = store.AnalyzeFromSignedDemoBaseline(b, nil, now); err == nil {
		t.Fatal("accepted absent signed income")
	}
}

func TestShortPartialClosePreservesWeightedEntryAndSeparatesNonUSDFees(t *testing.T) {
	base := time.Now().Add(-time.Minute).UnixMilli()
	records, fills, cids := accountFixture(base)
	intent := Intent{RequestID: "manual-short-out1", Source: Manual, Symbol: "BTCUSDT", PositionSide: Short, Action: Exit, OrderType: Market, Quantity: "0.25"}
	plan, _ := intent.Plan()
	digest, _ := digestIntent(intent)
	records = append(records, Record{Schema: 1, Intent: intent, Plan: plan, IntentDigest: digest, State: Reconciled, ExchangeOrderID: 2200})
	f := StoredFill{Schema: 1, ClientOrderID: plan.ClientOrderID, Trade: exchange.AccountTrade{
		Symbol: "BTCUSDT", TradeID: 99, OrderID: 2200, Side: "BUY", PositionSide: "SHORT", Price: "100", Quantity: "0.25",
		Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: "0.45", TimeMS: base + 8000}}
	f.seal()
	fills = append(fills, f)
	r, err := EvaluateAccountingOffline(records, fills, AccountingInput{
		WindowStartMS: base, WindowEndMS: base + 9000, SignedIncome: []exchange.IncomeRecord{}, Quotes: []BackendQuote{
			{ClientOrderID: cids["account-manual-short"], Symbol: "BTCUSDT", Price: "103", ObservedMS: base + 4900, Source: "BACKEND_WS_TRADE"},
			{ClientOrderID: plan.ClientOrderID, Symbol: "BTCUSDT", Price: "99", ObservedMS: base + 7990, Source: "BACKEND_WS_TRADE"},
		}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	o := ownerAccounting(t, r, Manual, "", Short)
	if o.OpenQuantity != "0.25" || o.AverageEntry != "102" || o.VirtualGrossRealizedUSDT != "0.5" ||
		o.ExchangeReportedGrossUSDT != "0.45" || o.VirtualAfterUSDTFeesExFunding != "0.49" ||
		o.CommissionByAsset["BNB"] != "0.03" || o.NonUSDTCommission != true ||
		o.SignedUnfavorableSlippageUSDT != "0.75" { // SELL 102 vs 103: +0.5; BUY 100 vs 99: +0.25
		t.Fatalf("short book %+v", o)
	}
}
