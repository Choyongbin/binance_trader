package trading

import (
	"testing"
	"time"
)

func TestProtectionPreviewHedgeOwnerIsolatedSizing(t *testing.T) {
	now := time.Now().UTC()
	rules := sizingRules(now)
	contract := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	mark := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	owner := OwnerPosition{Source: Model, ModelID: "a", Symbol: "BTCUSDT", Side: Long, Quantity: "0.010", AverageEntry: "100000"}
	p, err := PreviewProtection(owner, rules, contract, mark, "group-A-0001", now)
	if err != nil {
		t.Fatal(err)
	}
	if p.TP.TriggerPrice != "100333.4" || p.SL.TriggerPrice != "99666.7" || p.TP.WorkingType != "CONTRACT_PRICE" || p.SL.WorkingType != "MARK_PRICE" || p.TP.Quantity != "0.010" || p.TP.Side != "SELL" || p.LiveSubmissionEnabled {
		t.Fatalf("unexpected long plan %+v", p)
	}
	owner.Side = Short
	p, err = PreviewProtection(owner, rules, contract, mark, "group-A-0001", now)
	if err != nil {
		t.Fatal(err)
	}
	if p.TP.TriggerPrice != "99666.6" || p.SL.TriggerPrice != "100333.3" || p.TP.Side != "BUY" || p.TP.ClientAlgoID == p.SL.ClientAlgoID {
		t.Fatalf("unexpected short plan %+v", p)
	}
	owner.Source = Manual
	owner.ModelID = ""
	owner.Side = Long
	if _, err := PreviewProtection(owner, rules, contract, mark, "manual-group1", now); err != nil {
		t.Fatal(err)
	}
}
func TestProtectionPreviewMustRejectStaleOrImmediateTriggers(t *testing.T) {
	now := time.Now().UTC()
	rules := sizingRules(now)
	owner := OwnerPosition{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: Long, Quantity: "0.010", AverageEntry: "100000"}
	contract := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	mark := contract
	if _, err := PreviewProtection(owner, rules, Quote{Symbol: "BTCUSDT", Price: "100400", ReceivedAt: now}, mark, "group-a-0001", now); err == nil {
		t.Fatal("immediate take profit not rejected")
	}
	if _, err := PreviewProtection(owner, rules, contract, Quote{Symbol: "BTCUSDT", Price: "99600", ReceivedAt: now}, "group-a-0001", now); err == nil {
		t.Fatal("immediate stop not rejected")
	}
	mark.ReceivedAt = now.Add(-10 * time.Second)
	if _, err := PreviewProtection(owner, rules, contract, mark, "group-a-0001", now); err == nil {
		t.Fatal("stale Mark Price accepted")
	}
	mark = contract
	owner.Quantity = "0.0001"
	if _, err := PreviewProtection(owner, rules, contract, mark, "group-a-0001", now); err == nil {
		t.Fatal("off-grid qty accepted")
	}
	owner.Quantity = "0.010"
	rules.FetchedAt = now.Add(-time.Minute * 5)
	if _, err := PreviewProtection(owner, rules, contract, mark, "group-a-0001", now); err == nil {
		t.Fatal("stale filters accepted")
	}
}
