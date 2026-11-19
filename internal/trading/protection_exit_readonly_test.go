package trading

import (
	"context"
	"encoding/json"
	"testing"

	"binance_trader/internal/exchange"
)

func signedExitFixture(t *testing.T) (*ProtectionSubmissionStore, *protectionSettlementMock, int64, int64) {
	t.Helper()
	subs, _, _, _, _ := siblingContinuityFixture(t)
	plan, err := subs.read("group-p-001")
	if err != nil {
		t.Fatal(err)
	}
	tp := algoFromInstruction(1101, plan.TP, "FINISHED")
	sl := algoFromInstruction(1102, plan.SL, "CANCELED")
	tp.ActualOrderID = "91001"
	_ = json.Unmarshal([]byte(`"0.010"`), &tp.ActualQty)
	m := &protectionSettlementMock{
		algos:  map[int64]exchange.DemoAlgoResult{1101: tp, 1102: sl},
		trades: map[int64][]exchange.AccountTrade{91001: {protectionTrade(91001, 8001, plan.TP.Side, string(plan.PositionSide), "0.010", "101000", "9.8")}},
	}
	return subs, m, 1101, 1102
}

func TestSignedExitFillsReviewExactAndReadOnly(t *testing.T) {
	subs, m, tp, sl := signedExitFixture(t)
	creds := DemoCredentials{APIKey: "mock", Secret: "mock"}
	out, err := InspectSignedExitFills(context.Background(), subs, m, creds, "group-p-001", tp, sl)
	if err != nil || out.State != "EXIT_FILLS_MATCH_NOT_SETTLED" || out.TotalFilledQty != "0.01" || out.TradeCount != 1 || out.TradingAuthorized || out.FinanciallySettled || !out.ManualReviewRequired {
		t.Fatalf("review %+v %v", out, err)
	}
	out, err = InspectSignedExitFills(context.Background(), subs, m, creds, "group-p-001", tp, sl)
	if err != nil || out.TradeCount != 1 {
		t.Fatalf("repeat %+v %v", out, err)
	}
}
func TestSignedExitFillsReviewPartialAndDoubleTrigger(t *testing.T) {
	subs, m, tp, sl := signedExitFixture(t)
	a := m.algos[tp]
	_ = json.Unmarshal([]byte(`"0.004"`), &a.ActualQty)
	m.algos[tp] = a
	m.trades[91001][0].Quantity = "0.004"
	c := DemoCredentials{APIKey: "mock", Secret: "mock"}
	out, err := InspectSignedExitFills(context.Background(), subs, m, c, "group-p-001", tp, sl)
	if err != nil || out.State != "PARTIAL_OR_UNFILLED_MANUAL_RECONCILIATION" {
		t.Fatalf("partial %+v %v", out, err)
	}
	b := m.algos[sl]
	b.Status = "FINISHED"
	b.ActualOrderID = "91002"
	_ = json.Unmarshal([]byte(`"0.006"`), &b.ActualQty)
	m.algos[sl] = b
	plan, err := subs.read("group-p-001")
	if err != nil {
		t.Fatal(err)
	}
	m.trades[91002] = []exchange.AccountTrade{protectionTrade(91002, 8002, plan.SL.Side, string(plan.PositionSide), "0.006", "99000", "-3")}
	out, err = InspectSignedExitFills(context.Background(), subs, m, c, "group-p-001", tp, sl)
	if err != nil || out.State != "DOUBLE_TRIGGER_MANUAL_RECONCILIATION" || out.TotalFilledQty != "0.01" {
		t.Fatalf("both %+v %v", out, err)
	}
	m.trades[91002][0].Quantity = "0.009"
	_ = json.Unmarshal([]byte(`"0.009"`), &b.ActualQty)
	m.algos[sl] = b
	if out, err = InspectSignedExitFills(context.Background(), subs, m, c, "group-p-001", tp, sl); err == nil || out.State != "HALT" {
		t.Fatalf("overfill %+v %v", out, err)
	}
}
func TestSignedExitFillsReviewFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		change func(*protectionSettlementMock)
	}{
		{"missing trade", func(m *protectionSettlementMock) { m.trades[91001] = nil }},
		{"wrong side", func(m *protectionSettlementMock) { m.trades[91001][0].Side = "BUY" }},
		{"duplicate trade", func(m *protectionSettlementMock) { m.trades[91001] = append(m.trades[91001], m.trades[91001][0]) }},
		{"changed signed observation", func(m *protectionSettlementMock) { m.mutateID = 1101 }},
		{"unknown algo", func(m *protectionSettlementMock) { delete(m.algos, 1102) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subs, m, tp, sl := signedExitFixture(t)
			tt.change(m)
			out, err := InspectSignedExitFills(context.Background(), subs, m, DemoCredentials{APIKey: "mock", Secret: "mock"}, "group-p-001", tp, sl)
			if err == nil || out.State != "HALT" || out.TradingAuthorized || out.FinanciallySettled {
				t.Fatalf("accepted %+v %v", out, err)
			}
		})
	}
}
