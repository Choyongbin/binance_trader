package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func TestFirstDemoMarketPreflight(t *testing.T) {
	now := time.Now().UTC()
	intent := sampleIntent()
	intent.Quantity = "0.004"
	plan, err := intent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	r := sizingRules(now)
	q := Quote{Symbol: "BTCUSDT", Price: "84510.6", ReceivedAt: now}
	conf := exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", AsOf: now.Format(time.RFC3339Nano), HedgeMode: true, MarginType: "ISOLATED", Leverage: 15, AutoAddMargin: false}
	account := exchange.Snapshot{Environment: "demo", Status: "connected", AsOf: now.Format(time.RFC3339Nano), Balances: []exchange.Balance{{Asset: "USDT", Available: "5000.0713"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, account, conf, 10, now); err != nil {
		t.Fatal(err)
	}
	x := account
	x.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.001"}}
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, x, conf, 10, now); err == nil {
		t.Fatal("existing position accepted")
	}
	x = account
	x.Orders = []exchange.Order{{OrderID: 15}}
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, x, conf, 10, now); err == nil {
		t.Fatal("open orders accepted")
	}
	x = account
	x.AlgoOrders = nil
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, x, conf, 10, now); err == nil {
		t.Fatal("incomplete account accepted")
	}
	x = account
	x.AsOf = now.Add(-10 * time.Second).Format(time.RFC3339Nano)
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, x, conf, 10, now); err == nil {
		t.Fatal("stale account accepted")
	}
	c := conf
	c.HedgeMode = false
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, account, c, 10, now); err == nil {
		t.Fatal("wrong hedge setting accepted")
	}
	c = conf
	c.AsOf = now.Add(-11 * time.Second).Format(time.RFC3339Nano)
	if _, err := ValidateFirstDemoMarketEntry(intent, plan, r, q, account, c, 10, now); err == nil {
		t.Fatal("stale config accepted")
	}
	huge := intent
	huge.Quantity = "0.090"
	p, _ := huge.Plan()
	if _, err := ValidateFirstDemoMarketEntry(huge, p, r, q, account, conf, 10, now); err == nil {
		t.Fatal("over-budget order accepted")
	}
	model := intent
	model.Source = Model
	model.ModelID = "first"
	p, _ = model.Plan()
	if _, err := ValidateFirstDemoMarketEntry(model, p, r, q, account, conf, 10, now); err == nil {
		t.Fatal("model before ledger accepted")
	}
	exit := intent
	exit.Action = Exit
	p, _ = exit.Plan()
	if _, err := ValidateFirstDemoMarketEntry(exit, p, r, q, account, conf, 10, now); err == nil {
		t.Fatal("exit on entry gate accepted")
	}
}
