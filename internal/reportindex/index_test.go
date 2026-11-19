package reportindex

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/sqlitebridge"
	"binance_trader/internal/trading"
)

func reportFixture(t *testing.T, data string, model string) {
	t.Helper()
	now := time.Now().UTC()
	r := trading.AccountingReport{Schema: 1, CreatedAt: now, WindowStartMS: now.Add(-time.Minute).UnixMilli(), WindowEndMS: now.Add(-time.Second).UnixMilli(), KnownOrders: 2, KnownFills: 2, IncomeRows: 0, ProtectionFillCount: 2, ProtectionGroupCount: 1, DualTriggeredGroupCount: 1,
		Owners: []trading.OwnerAccounting{
			{Source: trading.Model, ModelID: model, Symbol: "BTCUSDT", PositionSide: trading.Long, OpenQuantity: "0.12345678", AverageEntry: "123456.12345678", VirtualGrossRealizedUSDT: "-0.00000001", ExchangeReportedGrossUSDT: "0", CommissionByAsset: map[string]string{"USDT": "0.01000001", "BNB": "0.00123456"}, VirtualAfterUSDTFeesExFunding: "-0.01000002", ProvisionalFundingUSDT: "0", ProvisionalAfterFundingUSDT: "-0.01000002", SlippageCoverage: "INCOMPLETE_OR_STALE_BACKEND_QUOTE", Protection: &trading.ProtectionAccountingAttribution{FillCount: 2, GroupCount: 1, DualTriggeredGroupCount: 1, TakeProfitFillCount: 1, StopLossFillCount: 1, TakeProfitQuantity: "0.05", StopLossQuantity: "0.07345678", VirtualGrossRealizedUSDT: "1.2", ExchangeReportedGrossUSDT: "1.1", CommissionByAsset: map[string]string{"USDT": "0.02"}}},
			{Source: trading.Manual, Symbol: "BTCUSDT", PositionSide: trading.Short, OpenQuantity: "0", AverageEntry: "0", VirtualGrossRealizedUSDT: "2", ExchangeReportedGrossUSDT: "2", CommissionByAsset: map[string]string{"USDT": "0.2"}, VirtualAfterUSDTFeesExFunding: "1.8", ProvisionalFundingUSDT: "0", ProvisionalAfterFundingUSDT: "1.8", SlippageCoverage: "INCOMPLETE_OR_STALE_BACKEND_QUOTE"},
		}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{"USDT": "-1"}, OtherIncomeByTypeAsset: map[string]string{"TRANSFER:USDT": "50"},
		FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS", Warnings: []string{"잠정 손익입니다", "실거래 검증이 완료되지 않았습니다"}}
	if e := trading.ArchiveAccounting(data, &r); e != nil {
		t.Fatal(e)
	}
}
func TestBuildSQLiteReadProjectionExactDecAndModels(t *testing.T) {
	if !sqlitebridge.Available() {
		t.Skip("SQLite 없음: 실 DB 검증 생략")
	}
	dir := t.TempDir()
	reportFixture(t, dir, "signal-A")
	if _, e := ReadHistory(dir, 20, 0, Filter{}); !errors.Is(e, ErrNotBuilt) {
		t.Fatalf("unexpected initial read: %v", e)
	}
	status, e := Build(dir)
	if e != nil || status.State != "CURRENT" || status.ReportCount != 1 {
		t.Fatalf("build %#v: %v", status, e)
	}
	again, e := Build(dir)
	if e != nil || again.SourceFingerprint != status.SourceFingerprint {
		t.Fatalf("idempotency: %v", e)
	}
	page, e := ReadHistory(dir, 20, 0, Filter{Source: "model", ModelID: "signal-A"})
	if e != nil {
		t.Fatal(e)
	}
	if page.Total != 1 || len(page.Reports) != 1 || len(page.Reports[0].Owners) != 1 {
		t.Fatalf("filter unexpected: %#v", page)
	}
	owner := page.Reports[0].Owners[0]
	if owner.OpenQuantity != "0.12345678" || owner.CommissionByAsset["BNB"] != "0.00123456" || owner.VirtualGrossRealizedUSDT != "-0.00000001" || owner.Protection == nil || owner.Protection.DualTriggeredGroupCount != 1 || owner.Protection.TakeProfitQuantity != "0.05" || page.Status.ActualNetVerified || page.Status.TradingAuthorized {
		t.Fatalf("amount loss or unsafe label: %#v", page)
	}
	if _, e = ReadHistory(dir, 20, 0, Filter{Source: "model", ModelID: "not-found"}); e != nil {
		t.Fatal(e)
	}
	filtered, e := ReadHistory(dir, 20, 0, Filter{Source: "manual"})
	if e != nil || filtered.Total != 1 || len(filtered.Reports[0].Owners) != 1 || filtered.Reports[0].Owners[0].Source != trading.Manual {
		t.Fatalf("manual %#v %v", filtered, e)
	}
	empty, e := ReadHistory(dir, 20, 99, Filter{})
	if e != nil || empty.Total != 1 || len(empty.Reports) != 0 {
		t.Fatalf("pagination %#v: %v", empty, e)
	}
	for _, filter := range []Filter{{Source: "model';DROP TABLE reports;--"}, {ModelID: "signal-A"}, {Source: "manual", ModelID: "A"}} {
		if _, e := ReadHistory(dir, 20, 0, filter); e == nil {
			t.Fatalf("accepted invalid filter %+v", filter)
		}
	}
	reportFixture(t, dir, "signal-B")
	stale, e := StatusOnly(dir)
	if e != nil || stale.State != "STALE" || stale.ReportCount != 2 {
		t.Fatalf("expected stale %#v %v", stale, e)
	}
	if _, e := ReadHistory(dir, 10, 0, Filter{}); !errors.Is(e, ErrStale) {
		t.Fatalf("stale should fail: %v", e)
	}
	status, e = Build(dir)
	if e != nil || status.ReportCount != 2 {
		t.Fatalf("incremental rebuild %#v %v", status, e)
	}
	all, e := ReadHistory(dir, 20, 0, Filter{})
	if e != nil || all.Total != 2 {
		t.Fatalf("second build %#v %v", all, e)
	}
	if all.Reports[0].CreatedAt.Before(all.Reports[1].CreatedAt) {
		t.Fatal("not newest-first")
	}
}
func TestProjectionTamperingAndInterruptedBuildFailClosed(t *testing.T) {
	if !sqlitebridge.Available() {
		t.Skip("SQLite 없음: 실 DB 검증 생략")
	}
	dir := t.TempDir()
	reportFixture(t, dir, "model-X")
	status, e := Build(dir)
	if e != nil {
		t.Fatal(e)
	}
	dbPath := filepath.Join(dir, "trading", "report_index", status.SourceFingerprint+".sqlite3")
	f, e := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write([]byte("evil")); e != nil {
		t.Fatal(e)
	}
	f.Close()
	if _, e := StatusOnly(dir); !errors.Is(e, ErrUnsafe) {
		t.Fatalf("expected corruption, got %v", e)
	}
	if _, e := Build(dir); !errors.Is(e, ErrUnsafe) {
		t.Fatalf("should not repair tamper silently: %v", e)
	}
}
func TestInterruptedProjectionBuildNeedsReview(t *testing.T) {
	if !sqlitebridge.Available() {
		t.Skip("SQLite 없음: 실 DB 검증 생략")
	}
	dir := t.TempDir()
	reportFixture(t, dir, "model-Y")
	p := dirOf(dir)
	if e := os.MkdirAll(filepath.Join(p, "build.lock"), 0700); e != nil {
		t.Fatal(e)
	}
	if _, e := Build(dir); !errors.Is(e, ErrUnsafe) {
		t.Fatalf("should block dirty build: %v", e)
	}
	if _, e := StatusOnly(dir); !errors.Is(e, ErrUnsafe) {
		t.Fatalf("should block read with builder lock: %v", e)
	}
}
func TestImmutableSourceTamperingFailsBeforeSQLiteRead(t *testing.T) {
	if !sqlitebridge.Available() {
		t.Skip("SQLite 없음: 실 DB 검증 생략")
	}
	dir := t.TempDir()
	reportFixture(t, dir, "model-Z")
	if _, e := Build(dir); e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(dir, "trading", "accounting_reports")
	entries, e := os.ReadDir(p)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(p, entries[0].Name())
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	b = []byte(strings.Replace(string(b), "signal-foo", "signal-bar", 1))
	b = append(b, byte('!'))
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadHistory(dir, 20, 0, Filter{}); e == nil {
		t.Fatal("accepted damaged immutable source")
	}
}
