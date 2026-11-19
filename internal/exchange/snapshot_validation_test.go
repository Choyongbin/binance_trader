package exchange

import "testing"

func validDisplay() Snapshot {
	return Snapshot{Balances: []Balance{{Asset: "USDT", Wallet: "100", Available: "80"}}, Positions: []Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.1", Entry: "100"}}, Orders: []Order{}, AlgoOrders: []AlgoOrder{}}
}
func TestStrictDisplayAccount(t *testing.T) {
	if err := validateDisplaySnapshot(validDisplay(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	// A profitable cross-margin account can show more available than wallet.
	profitable := validDisplay()
	profitable.Balances[0].Available = "105"
	profitable.Balances[0].CrossUnPnl = "10"
	if err := validateDisplaySnapshot(profitable, "BTCUSDT"); err != nil {
		t.Fatalf("valid positive unrealized PnL rejected: %v", err)
	}
	cases := []func(*Snapshot){
		func(s *Snapshot) { s.Balances[0].Available = "broken" },
		func(s *Snapshot) { s.Positions[0].Qty = "broken" },
		func(s *Snapshot) { s.Positions[0].Symbol = "ETHUSDT" },
		func(s *Snapshot) { s.Positions[0].Qty = "-0.1" },
		func(s *Snapshot) { s.Balances[0].Available = "101" },
		func(s *Snapshot) { s.Positions[0].PnL = "NaN" },
		func(s *Snapshot) {
			s.AlgoOrders = append(s.AlgoOrders, AlgoOrder{Symbol: "BTCUSDT", AlgoID: 1, Quantity: "broken"})
		},
	}
	for i, f := range cases {
		s := validDisplay()
		f(&s)
		if err := validateDisplaySnapshot(s, "BTCUSDT"); err == nil {
			t.Errorf("case %d silently accepted", i)
		}
	}
}
