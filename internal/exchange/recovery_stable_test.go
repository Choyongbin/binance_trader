package exchange

import "testing"

func TestStableAlgosCannotSwapClientIDsOrCloseAll(t *testing.T) {
	original := []AlgoOrder{{AlgoID: 1, ClientAlgoID: "owner-tp", Symbol: "BTCUSDT", PositionSide: "LONG", Side: "SELL", AlgoStatus: "NEW", Quantity: "0.01", TriggerPrice: "101", OrderType: "TAKE_PROFIT_MARKET", WorkingType: "CONTRACT_PRICE", ClosePosition: false}}
	changed := append([]AlgoOrder{}, original...)
	if !stableAlgos(original, changed) {
		t.Fatal("identical rejected")
	}
	changed[0].ClientAlgoID = "other-owner"
	if stableAlgos(original, changed) {
		t.Fatal("client ID tamper undetected")
	}
	changed[0] = original[0]
	changed[0].ClosePosition = true
	if stableAlgos(original, changed) {
		t.Fatal("close-all mutation undetected")
	}
	changed = append([]AlgoOrder{}, original[0], original[0])
	if stableAlgos(changed, changed) {
		t.Fatal("duplicate algo ID accepted")
	}
}
