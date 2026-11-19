package trading

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExternalCloseShadowRehearsalLongAndShortNeverMutates(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			d, m, _ := externalCloseFixture(t, side)
			before, e := snapshotExecutionEvidence(d)
			if e != nil {
				t.Fatal(e)
			}
			r := reviewFixture(d, m)
			after, e := snapshotExecutionEvidence(d)
			if e != nil || before != after {
				t.Fatal("source evidence changed", e)
			}
			if len(r.ExternalCloseRehearsals) != 1 {
				t.Fatalf("rehearsal missing: %+v", r)
			}
			p := r.ExternalCloseRehearsals[0]
			if p.Status != ExternalCloseRehearsed || p.PositionSide != side || p.EntryOrderID != 701 || p.ExitOrderID != 899 ||
				p.EntryQuantity != "0.001" || p.ExitQuantity != "0.001" || p.SimulatedRemainingQty != "0" ||
				p.SignedEntryCommission != "0.03" || p.SignedExitCommission != "0.025" ||
				p.SignedRealizedPnL != "1.25" || p.ProvisionalNetExcludingFunding != "1.195" ||
				len(p.EntryTradeIDs) != 1 || len(p.ExitTradeIDs) != 1 || p.EntryTradeIDs[0] != 901 || p.ExitTradeIDs[0] != 902 ||
				p.LocalEvidenceSHA256 != before || len(p.SignedReadDigestSHA256) != 64 || len(p.PreviewSHA256) != 64 ||
				!VerifyExternalCloseRehearsalChecksum(p) || !p.ManualReviewRequired || p.SettlementAuthorized || p.TradingAuthorized ||
				p.LocalLifecycleChanged || p.LocalOwnershipChanged || r.TradingAuthorized || r.State != "BLOCKED_REVIEW_REQUIRED" {
				t.Fatalf("invalid preview %+v", p)
			}
			// Read again with identical signed fixture and original source state.
			// Hashes bind evidence, not any runtime ID or JSON output path.
			r2 := reviewFixture(d, m)
			if len(r2.ExternalCloseRehearsals) != 1 || r2.ExternalCloseRehearsals[0].PreviewSHA256 != p.PreviewSHA256 {
				t.Fatalf("not idempotent or deterministic: %+v %+v", p, r2.ExternalCloseRehearsals)
			}
			modified := p
			modified.ProvisionalNetExcludingFunding = "999"
			if VerifyExternalCloseRehearsalChecksum(modified) {
				t.Fatal("tamper escaped checksum")
			}
			modified = p
			modified.TradingAuthorized = true
			modified.PreviewSHA256 = fingerprintRehearsal(modified)
			if VerifyExternalCloseRehearsalChecksum(modified) {
				t.Fatal("forged authorization escaped fail closed")
			}
		})
	}
}

func TestExternalCloseRehearsalMissingEntryEvidenceBlocked(t *testing.T) {
	d, m, _ := externalCloseFixture(t, Short)
	files, e := os.ReadDir(filepath.Join(d, "trading", "fills"))
	if e != nil || len(files) != 1 {
		t.Fatal(files, e)
	}
	if e := os.Remove(filepath.Join(d, "trading", "fills", files[0].Name())); e != nil {
		t.Fatal(e)
	}
	r := reviewFixture(d, m)
	if len(r.ExternalCloseRehearsals) != 1 {
		t.Fatal(r)
	}
	p := r.ExternalCloseRehearsals[0]
	if p.Status != ExternalCloseRehearsalBlocked || p.SimulatedRemainingQty != "" || p.SettlementAuthorized || p.LocalOwnershipChanged ||
		!strings.Contains(strings.Join(p.Blockers, " "), "idempotent signed order path") {
		t.Fatalf("missing entry fill should block replay: %+v", p)
	}
}

func TestExternalCloseRehearsalRejectsIncompleteDriftAndTamper(t *testing.T) {
	for _, scenario := range []string{"partial", "stale", "history", "tampered-during-audit", "entry-fee-asset", "entry-realized-pnl"} {
		t.Run(scenario, func(t *testing.T) {
			d, m, _ := externalCloseFixture(t, Short)
			switch scenario {
			case "partial":
				m.history[1].Quantity = "0.0005"
				v := m.externalOrders[899]
				v.ExecutedQty, v.OriginalQty = "0.0005", "0.0005"
				m.externalOrders[899] = v
			case "stale":
				m.snapshot.AsOf = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
			case "history":
				m.historyErr = os.ErrPermission
			case "tampered-during-audit":
				m.tamperDir = d
			case "entry-fee-asset":
				t0 := m.readerOrders.trades[701][0]
				t0.CommissionAsset = "BNB"
				m.readerOrders.trades[701][0] = t0
				m.history[0] = t0
				// local imported trade remains USDT, so entry must be rejected.
			case "entry-realized-pnl":
				t0 := m.readerOrders.trades[701][0]
				t0.RealizedPnL = "1"
				m.readerOrders.trades[701][0] = t0
				m.history[0] = t0
			}
			r := reviewFixture(d, m)
			if len(r.ExternalCloseRehearsals) != 1 {
				t.Fatal(r)
			}
			p := r.ExternalCloseRehearsals[0]
			if p.Status != ExternalCloseRehearsalBlocked || p.TradingAuthorized || p.SettlementAuthorized || p.LocalOwnershipChanged {
				t.Fatalf("invalid scenario %q: %+v", scenario, p)
			}
			if scenario == "tampered-during-audit" && (p.LocalEvidenceSHA256 != "" || p.SignedReadDigestSHA256 != "" || !strings.Contains(strings.Join(p.Blockers, " "), "changed during")) {
				t.Fatalf("tamper fence not applied: %+v", p)
			}
		})
	}
}
