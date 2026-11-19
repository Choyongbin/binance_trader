package main

import (
	"binance_trader/internal/trading"
	"testing"
)

func TestVerifyIncidentOnlyKnownAmbiguousShort(t *testing.T) {
	valid := trading.ExecutionLifecycleAudit{
		State: trading.ExecutionAuditPending,
		Total: 1, Pending: 1, Settled: 0,
		Items: []trading.ExecutionLifecycleAuditItem{{
			ClientOrderID: incidentClientID,
			PositionSide:  trading.PositionSide("SHORT"),
			Action:        trading.Action("ENTER"),
			Stage:         trading.ExecutionMayHaveSent,
		}},
		TradingAuthorized: false, ManualReviewRequired: true,
	}
	if err := verifyIncident(valid); err != nil {
		t.Fatalf("must match incident: %v", err)
	}
	broken := valid
	broken.State = trading.ExecutionAuditEmpty
	if err := verifyIncident(broken); err == nil {
		t.Fatal("EMPTY must not query")
	}
	broken = valid
	broken.Items = append([]trading.ExecutionLifecycleAuditItem(nil), valid.Items...)
	broken.Items[0].Stage = trading.ExecutionReserved
	if err := verifyIncident(broken); err == nil {
		t.Fatal("RESERVED must not query")
	}
	broken = valid
	broken.Items = append([]trading.ExecutionLifecycleAuditItem(nil), valid.Items...)
	broken.Items[0].ClientOrderID = "bt_other"
	if err := verifyIncident(broken); err == nil {
		t.Fatal("another ID must not query")
	}
	broken = valid
	broken.Pending = 0
	if err := verifyIncident(broken); err == nil {
		t.Fatal("pending must be exactly one")
	}
}
