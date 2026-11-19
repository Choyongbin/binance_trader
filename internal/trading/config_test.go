package trading

import (
	"binance_trader/internal/exchange"
	"testing"
	"time"
)

func TestConfigSafetyPlan(t *testing.T) {
	now := time.Now().UTC()
	conf := exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", AsOf: now.Format(time.RFC3339Nano), HedgeMode: true, MarginType: "ISOLATED", Leverage: 15}
	acct := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: now.Format(time.RFC3339), Balances: []exchange.Balance{}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	got, err := ValidateConfigPlan(conf, acct, now)
	if err != nil || got.Blocked || got.NeedsManualHedgeSwitch || got.NeedsIsolated || got.NeedsLeverage15 {
		t.Fatalf("ready config: %+v %v", got, err)
	}
	conf.HedgeMode = false
	got, err = ValidateConfigPlan(conf, acct, now)
	if err != nil || !got.Blocked || !got.NeedsManualHedgeSwitch {
		t.Fatal("account-wide one-way mismatch not flagged")
	}
	conf.HedgeMode = true
	conf.MarginType = "CROSSED"
	conf.Leverage = 8
	acct.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.003"}}
	got, err = ValidateConfigPlan(conf, acct, now)
	if err != nil || !got.Blocked || len(got.Reasons) == 0 {
		t.Fatal("existing position not flagged")
	}
	acct.Positions = []exchange.Position{}
	acct.Orders = nil
	if _, err = ValidateConfigPlan(conf, acct, now); err == nil {
		t.Fatal("missing open-orders response accepted")
	}
	acct.Orders = []exchange.Order{}
	acct.AsOf = now.Add(-30 * time.Second).Format(time.RFC3339)
	if _, err = ValidateConfigPlan(conf, acct, now); err == nil {
		t.Fatal("stale account snapshot accepted")
	}
	acct.AsOf = now.Format(time.RFC3339)
	conf.AsOf = now.Add(-30 * time.Second).Format(time.RFC3339Nano)
	if _, err = ValidateConfigPlan(conf, acct, now); err == nil {
		t.Fatal("stale config accepted")
	}
}
