package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"binance_trader/internal/exchange"
)

func makeContinuityManifest(t *testing.T, incident PackageAIncidentArchive) ExternalSignedHistoryManifest {
	t.Helper()
	e := incident.Close
	from, to := e.HistoryFromMS, e.HistoryToMS
	if to-from < 2 {
		t.Fatalf("insufficient fixture chronology %d..%d", from, to)
	}
	split := from + (to-from)/2
	windows := []ExternalSignedHistoryWindow{{FromMS: from, ToMS: split}, {FromMS: split + 1, ToMS: to}}
	for i := range windows {
		w := &windows[i]
		w.Trades = make([]exchange.AccountTrade, 0)
		w.Funding = make([]exchange.FundingIncome, 0)
		w.TradePageLimit = 1000
		w.FundingPageLimit = 1000
		for _, v := range e.HistoricalTrades {
			if v.TimeMS >= w.FromMS && v.TimeMS <= w.ToMS {
				w.Trades = append(w.Trades, v)
			}
		}
		for _, v := range e.FundingRows {
			if v.TimeMS >= w.FromMS && v.TimeMS <= w.ToMS {
				w.Funding = append(w.Funding, v)
			}
		}
		w.TradeRowsSHA256 = externalCloseHash(w.Trades)
		w.FundingRowsSHA256 = externalCloseHash(w.Funding)
	}
	out := ExternalSignedHistoryManifest{Schema: 1, ClientOrderID: incident.ClientOrderID, IncidentArchiveSHA256: incident.Checksum, Scope: ExternalSignedHistoryScope, Windows: windows}
	out.Checksum = out.digest()
	return out
}
func copyHistoryManifest(t *testing.T, source ExternalSignedHistoryManifest) ExternalSignedHistoryManifest {
	t.Helper()
	b, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var out ExternalSignedHistoryManifest
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func fixHistoryManifestHashes(m *ExternalSignedHistoryManifest) {
	for i := range m.Windows {
		m.Windows[i].TradeRowsSHA256 = externalCloseHash(m.Windows[i].Trades)
		m.Windows[i].FundingRowsSHA256 = externalCloseHash(m.Windows[i].Funding)
	}
	m.Checksum = m.digest()
}

func TestExternalSignedHistoryContinuityLongShortReadOnly(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		for _, funds := range []string{"", "-0.125", "0.375"} {
			t.Run(string(side)+"_"+funds, func(t *testing.T) {
				data, id, incident, _ := boundedFinancialFixtureWithFunding(t, side, funds)
				manifest := makeContinuityManifest(t, incident)
				before, err := snapshotExecutionEvidence(data)
				if err != nil {
					t.Fatal(err)
				}
				result, err := InspectExternalSignedHistoryContinuity(context.Background(), data, id, incident, manifest)
				if err != nil {
					t.Fatal(err)
				}
				if result.Stage != ExternalSignedHistoryStage || result.WindowCount != 2 || !result.ObservedWindowSequenceContiguous ||
					!result.CapturedRowsMatchIncident || result.TradeCount != len(incident.Close.HistoricalTrades) ||
					result.FundingCount != len(incident.Close.FundingRows) || result.WholeAccountHistoryProven ||
					result.ExchangeSourceAttested || result.PrivateStreamContinuityProven || result.TradingAuthorized ||
					result.SettlementAuthorized || result.PostingApplied || result.ModelFundingAllocated || result.OtherSymbolsCovered {
					t.Fatalf("unexpected or authorizing report: %+v", result)
				}
				if err := ValidateExternalSignedHistoryContinuity(result); err != nil {
					t.Fatal(err)
				}
				again, err := InspectExternalSignedHistoryContinuity(context.Background(), data, id, incident, manifest)
				if err != nil || !reflect.DeepEqual(result, again) {
					t.Fatalf("restart report differs: %v", err)
				}
				after, err := snapshotExecutionEvidence(data)
				if err != nil || before != after {
					t.Fatal("continuity audit modified source files")
				}
				b, _ := json.Marshal(result)
				if !strings.Contains(string(b), `"whole_account_history_proven":false`) || !strings.Contains(string(b), `"settlement_authorized":false`) {
					t.Fatal("report leaked authorizing flags")
				}
			})
		}
	}
}

func TestExternalSignedHistoryContinuityRejectsMissingDuplicateDriftAndTampering(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixtureWithFunding(t, Short, "-0.125")
	good := makeContinuityManifest(t, incident)
	for _, kind := range []string{
		"missing_first", "missing_last", "gap", "overlap", "reversed", "overlong", "null_trades", "null_funding",
		"missing_trade", "missing_funding", "wrong_trade_value", "wrong_funding_value", "extra_unowned_trade",
		"duplicate_trade", "duplicate_funding", "page_at_limit", "wrong_page_limit", "broken_row_hash",
		"wrong_manifest_hash", "wrong_incident", "forged_full_scope", "missing_id", "wrong_version",
		"invalid_trade_side", "invalid_fee", "invalid_funding_asset", "outside_row_time",
	} {
		t.Run(kind, func(t *testing.T) {
			m := copyHistoryManifest(t, good)
			switch kind {
			case "missing_first":
				m.Windows = m.Windows[1:]
			case "missing_last":
				m.Windows = m.Windows[:1]
			case "gap":
				m.Windows[1].FromMS++
			case "overlap":
				m.Windows[1].FromMS--
			case "reversed":
				m.Windows[0], m.Windows[1] = m.Windows[1], m.Windows[0]
			case "overlong":
				m.Windows[0].ToMS += int64(48 * 60 * 60 * 1000)
			case "null_trades":
				m.Windows[0].Trades = nil
			case "null_funding":
				m.Windows[0].Funding = nil
			case "missing_trade":
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						m.Windows[i].Trades = m.Windows[i].Trades[:0]
						break
					}
				}
			case "missing_funding":
				for i := range m.Windows {
					if len(m.Windows[i].Funding) > 0 {
						m.Windows[i].Funding = m.Windows[i].Funding[:0]
						break
					}
				}
			case "wrong_trade_value":
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						m.Windows[i].Trades[0].Commission = "9"
						break
					}
				}
			case "wrong_funding_value":
				for i := range m.Windows {
					if len(m.Windows[i].Funding) > 0 {
						m.Windows[i].Funding[0].Income = "9"
						break
					}
				}
			case "extra_unowned_trade":
				idx := 0
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						idx = i
						break
					}
				}
				x := m.Windows[idx].Trades[0]
				x.TradeID = 999999
				m.Windows[idx].Trades = append(m.Windows[idx].Trades, x)
			case "duplicate_trade":
				idx := 0
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						idx = i
						break
					}
				}
				m.Windows[idx].Trades = append(m.Windows[idx].Trades, m.Windows[idx].Trades[0])
			case "duplicate_funding":
				for i := range m.Windows {
					if len(m.Windows[i].Funding) > 0 {
						m.Windows[i].Funding = append(m.Windows[i].Funding, m.Windows[i].Funding[0])
						break
					}
				}
			case "page_at_limit":
				for len(m.Windows[0].Trades) < 1000 {
					m.Windows[0].Trades = append(m.Windows[0].Trades, exchange.AccountTrade{})
				}
			case "wrong_page_limit":
				m.Windows[0].TradePageLimit = 500
			case "broken_row_hash":
				m.Windows[0].TradeRowsSHA256 = strings.Repeat("0", 64)
			case "wrong_manifest_hash":
				m.Checksum = strings.Repeat("0", 64)
			case "wrong_incident":
				m.IncidentArchiveSHA256 = strings.Repeat("0", 64)
			case "forged_full_scope":
				m.Scope = "FULL_ACCOUNT_HISTORY_PROVEN"
			case "missing_id":
				m.ClientOrderID = ""
			case "wrong_version":
				m.Schema = 2
			case "invalid_trade_side":
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						m.Windows[i].Trades[0].Side = "NONE"
						break
					}
				}
			case "invalid_fee":
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						m.Windows[i].Trades[0].Commission = "-0.5"
						break
					}
				}
			case "invalid_funding_asset":
				for i := range m.Windows {
					if len(m.Windows[i].Funding) > 0 {
						m.Windows[i].Funding[0].Asset = "BTC"
						break
					}
				}
			case "outside_row_time":
				for i := range m.Windows {
					if len(m.Windows[i].Trades) > 0 {
						m.Windows[i].Trades[0].TimeMS = 0
						break
					}
				}
			}
			if kind != "broken_row_hash" && kind != "wrong_manifest_hash" {
				fixHistoryManifestHashes(&m)
			} else if kind == "broken_row_hash" {
				m.Checksum = m.digest()
			}
			if _, err := InspectExternalSignedHistoryContinuity(context.Background(), data, id, incident, m); err == nil {
				t.Fatal("invalid/forged signed window sequence accepted")
			}
		})
	}
}
func TestExternalSignedHistoryContinuityRejectsTamperedOriginalAndCancellation(t *testing.T) {
	for _, kind := range []string{"original", "archive", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			data, id, incident, _ := boundedFinancialFixture(t, Long)
			manifest := makeContinuityManifest(t, incident)
			ctx := context.Background()
			switch kind {
			case "original":
				if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
					t.Fatal(err)
				}
			case "archive":
				incident.Checksum = strings.Repeat("0", 64)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := InspectExternalSignedHistoryContinuity(ctx, data, id, incident, manifest); err == nil {
				t.Fatal("untrusted evidence accepted")
			}
		})
	}
}
func TestExternalSignedHistoryContinuityConcurrentReads(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixtureWithFunding(t, Long, "0.375")
	m := makeContinuityManifest(t, incident)
	var wg sync.WaitGroup
	errors := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := InspectExternalSignedHistoryContinuity(context.Background(), data, id, incident, m)
			if err == nil {
				err = ValidateExternalSignedHistoryContinuity(r)
			}
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestExternalSignedHistoryContinuityRejectsAuthorizingRehashedReport(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixture(t, Short)
	m := makeContinuityManifest(t, incident)
	report, err := InspectExternalSignedHistoryContinuity(context.Background(), data, id, incident, m)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"posting", "account_proven", "private_stream", "scope", "blocker", "hash"} {
		t.Run(kind, func(t *testing.T) {
			x := report
			switch kind {
			case "posting":
				x.SettlementAuthorized = true
			case "account_proven":
				x.WholeAccountHistoryProven = true
			case "private_stream":
				x.PrivateStreamContinuityProven = true
			case "scope":
				x.Scope = "FULL_ACCOUNT"
			case "blocker":
				x.BlockerCodes = append([]string(nil), x.BlockerCodes...)
				x.BlockerCodes[0] = ""
			case "hash":
				x.DigestSHA256 = strings.Repeat("f", 64)
			}
			if kind != "hash" {
				x.DigestSHA256 = x.digest()
			}
			if err := ValidateExternalSignedHistoryContinuity(x); err == nil {
				t.Fatal("forged report accepted")
			}
		})
	}
}
