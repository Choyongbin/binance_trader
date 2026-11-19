package trading

import (
	"context"
	"errors"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type fakeConfigBackend struct {
	cur                exchange.TradingConfiguration
	reads, margin, lev int
	setErr             error
	ignoreMargin       bool
}

func (f *fakeConfigBackend) Current(context.Context) (exchange.TradingConfiguration, error) {
	f.reads++
	f.cur.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
	return f.cur, nil
}
func (f *fakeConfigBackend) SetIsolated(context.Context) error {
	f.margin++
	if f.setErr != nil {
		return f.setErr
	}
	if !f.ignoreMargin {
		f.cur.MarginType = "ISOLATED"
	}
	return nil
}
func (f *fakeConfigBackend) SetLeverage(_ context.Context, l int) error {
	f.lev++
	f.cur.Leverage = l
	return nil
}
func TestConfigurationRequiresExplicitConsentAndFreshEmptyAccount(t *testing.T) {
	cur := exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "CROSSED", Leverage: 5}
	acct := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: time.Now().UTC().Format(time.RFC3339), Balances: []exchange.Balance{}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	backend := &fakeConfigBackend{cur: cur}
	if _, err := ApplyDemoConfig(context.Background(), backend, acct, ""); err == nil || backend.reads != 0 {
		t.Fatal("unconfirmed mutation proceeded")
	}
	acct.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.001"}}
	if _, err := ApplyDemoConfig(context.Background(), backend, acct, "DEMO BTCUSDT 격리 15배 설정"); err == nil || backend.margin != 0 {
		t.Fatal("configured with open position")
	}
	acct.Positions = []exchange.Position{}
	backend.cur.HedgeMode = false
	if _, err := ApplyDemoConfig(context.Background(), backend, acct, "DEMO BTCUSDT 격리 15배 설정"); err == nil || backend.margin != 0 {
		t.Fatal("automatically attempted global Hedge switch")
	}
	backend.cur.HedgeMode = true
	config, err := ApplyDemoConfig(context.Background(), backend, acct, "DEMO BTCUSDT 격리 15배 설정")
	if err != nil || config.MarginType != "ISOLATED" || config.Leverage != 15 || backend.margin != 1 || backend.lev != 1 {
		t.Fatalf("failed mock config %+v %v", config, err)
	}
}
func TestAmbiguousConfigurationStopsSequence(t *testing.T) {
	acct := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: time.Now().UTC().Format(time.RFC3339), Balances: []exchange.Balance{}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	backend := &fakeConfigBackend{cur: exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "CROSSED", Leverage: 8}, setErr: errors.New("unknown result")}
	if _, err := ApplyDemoConfig(context.Background(), backend, acct, "DEMO BTCUSDT 격리 15배 설정"); err == nil || backend.margin != 1 || backend.lev != 0 {
		t.Fatal("continued after ambiguous change")
	}
	backend = &fakeConfigBackend{cur: exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", HedgeMode: true, MarginType: "CROSSED", Leverage: 8}, ignoreMargin: true}
	if _, err := ApplyDemoConfig(context.Background(), backend, acct, "DEMO BTCUSDT 격리 15배 설정"); err == nil || backend.lev != 0 {
		t.Fatal("continued after failed margin readback")
	}
}
