package main

import (
	"binance_trader/internal/trading"
	"testing"
)

func diagnosticFixture(t *testing.T, side string, missing bool) (packageADiscoveryArchive, trading.ExecutionLifecycleAudit) {
	t.Helper()
	b := bundleFixture(t, side)
	b.Incident.Entry.Gap.ClientOrderID = b.ClientOrderID
	b.Incident.Entry.Gap.ModelID = "ema_rsi_macd_v1"
	b.Incident.Entry.Gap.PositionSide = trading.PositionSide(side)
	b.Incident.Entry.Gap.SignedTradeIDs = []int64{11}
	if missing {
		b.Incident.Entry.Gap.MissingLocalTradeIDs = []int64{11}
	}
	b.Incident.Close.ExitTradeIDs = []int64{12}
	b.Incident.Close.BoundedFundingNetUSDT = "0"
	b.Incident.OriginalEvidenceSHA256 = "test-only-original-evidence"
	b.Checksum = b.digest()
	audit := trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditPending, Total: 1, Pending: 1, ManualReviewRequired: true, Items: []trading.ExecutionLifecycleAuditItem{{ClientOrderID: b.ClientOrderID, PositionSide: trading.PositionSide(side), Action: trading.Enter, Stage: trading.ExecutionMayHaveSent}}}
	return b, audit
}
func hasBlocker(d packageADiscoveryDiagnostic, code string) bool {
	for _, c := range d.BlockerCodes {
		if c == code {
			return true
		}
	}
	return false
}
func TestDiagnosticLongShortAndMissingEntry(t *testing.T) {
	for _, side := range []string{"LONG", "SHORT"} {
		for _, missing := range []bool{false, true} {
			b, audit := diagnosticFixture(t, side, missing)
			d, e := createDiscoveryDiagnostic(b, audit)
			if e != nil {
				t.Fatal(e)
			}
			if e = validateDiscoveryDiagnostic(d); e != nil {
				t.Fatal(e)
			}
			if !d.ArchiveIntegrityVerified || d.FullHistoryVerified || d.FundingAttributionVerified || d.PostingAuthorized || d.SettlementAuthorized || d.TradingAuthorized || d.LifecycleChanged || d.OwnershipChanged {
				t.Fatalf("unsafe claim: %+v", d)
			}
			if d.PositionSide != trading.PositionSide(side) || d.LocalLifecycleStage != string(trading.ExecutionMayHaveSent) || !hasBlocker(d, "HISTORICAL_COVERAGE") || !hasBlocker(d, "FUNDING_MODEL_ATTRIBUTION") || !hasBlocker(d, "ATOMIC_LEDGER_POSTING") {
				t.Fatal("missing hard blockers")
			}
			if got := hasBlocker(d, "MISSING_OWNED_ENTRY_FILLS"); got != missing {
				t.Fatalf("missing entry=%v, blocker=%v", missing, got)
			}
		}
	}
}
func TestDiagnosticRejectsAccountPermissionForgeryAndStaleOriginal(t *testing.T) {
	b, audit := diagnosticFixture(t, "SHORT", true)
	d, e := createDiscoveryDiagnostic(b, audit)
	if e != nil {
		t.Fatal(e)
	}
	d.TradingAuthorized = true
	d.Checksum = d.digest()
	if e = validateDiscoveryDiagnostic(d); e == nil {
		t.Fatal("forged trade permission accepted")
	}
	d.TradingAuthorized = false
	d.FullHistoryVerified = true
	d.Checksum = d.digest()
	if e = validateDiscoveryDiagnostic(d); e == nil {
		t.Fatal("false historical coverage accepted")
	}
	d.FullHistoryVerified = false
	for i := range d.Checks {
		if d.Checks[i].Code == "ATOMIC_LEDGER_POSTING" {
			d.Checks[i].Status = "EVIDENCED_BOUNDED"
		}
	}
	d.BlockerCodes = d.BlockerCodes[:len(d.BlockerCodes)-1]
	d.Checksum = d.digest()
	if e = validateDiscoveryDiagnostic(d); e == nil {
		t.Fatal("required posting gate forged clear")
	}
	b, audit = diagnosticFixture(t, "SHORT", true)
	d, e = createDiscoveryDiagnostic(b, audit)
	if e != nil {
		t.Fatal(e)
	}
	d.SettlementAuthorized = true
	d.Checksum = d.digest()
	if e = validateDiscoveryDiagnostic(d); e == nil {
		t.Fatal("false settlement accepted")
	}
	audit.Items[0].PositionSide = trading.Long
	if _, e = createDiscoveryDiagnostic(b, audit); e == nil {
		t.Fatal("wrong side accepted")
	}
	audit.Items[0].PositionSide = trading.Short
	audit.Items[0].Stage = trading.ExecutionSettled
	if _, e = createDiscoveryDiagnostic(b, audit); e == nil {
		t.Fatal("settled lifecycle accepted")
	}
	b, audit = diagnosticFixture(t, "SHORT", false)
	b.Discovery.Candidates[0].OrderID++
	b.Checksum = b.digest()
	if _, e = createDiscoveryDiagnostic(b, audit); e == nil {
		t.Fatal("mismatched archived candidate accepted")
	}
}
func TestDiagnosticMissingLifecycleExplicitlyBlocked(t *testing.T) {
	b, _ := diagnosticFixture(t, "LONG", false)
	audit := trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditEmpty, Items: []trading.ExecutionLifecycleAuditItem{}, ManualReviewRequired: true}
	d, e := createDiscoveryDiagnostic(b, audit)
	if e != nil {
		t.Fatal(e)
	}
	if d.LocalLifecycleStage != "MISSING_LIFECYCLE_BLOCKED" || !hasBlocker(d, "UNSETTLED_LIFECYCLE") || d.SettlementAuthorized {
		t.Fatal("missing lifecycle improperly inferred")
	}
}
