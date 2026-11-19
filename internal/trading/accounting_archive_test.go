package trading

import (
	"testing"
	"time"
)

func TestAccountingArchiveRejectsMalformedProtectionAttribution(t *testing.T) {
	now := time.Now().UTC()
	r := AccountingReport{Schema: 1, CreatedAt: now, WindowStartMS: now.Add(-time.Hour).UnixMilli(), WindowEndMS: now.Add(-time.Second).UnixMilli(),
		Owners:                []OwnerAccounting{{Source: Model, ModelID: "A", Symbol: "BTCUSDT", PositionSide: Long, OpenQuantity: "0", AverageEntry: "0", VirtualGrossRealizedUSDT: "0", ExchangeReportedGrossUSDT: "0", CommissionByAsset: map[string]string{}, VirtualAfterUSDTFeesExFunding: "0", ProvisionalFundingUSDT: "0", ProvisionalAfterFundingUSDT: "0", SlippageCoverage: "NO_MARKET_EXECUTIONS", Protection: &ProtectionAccountingAttribution{FillCount: 2, GroupCount: 1, DualTriggeredGroupCount: 2, TakeProfitFillCount: 1, StopLossFillCount: 1, TakeProfitQuantity: "0.1", StopLossQuantity: "0.1", VirtualGrossRealizedUSDT: "0", ExchangeReportedGrossUSDT: "0", CommissionByAsset: map[string]string{}}}},
		AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{}, FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS", Warnings: []string{"a", "b"}}
	if err := ArchiveAccounting(t.TempDir(), &r); err == nil {
		t.Fatal("malformed protection attribution was archived")
	}
}

func TestAccountingArchiveAcceptsUnattestedOpeningLotCoverage(t *testing.T) {
	now := time.Now().UTC()
	r := AccountingReport{
		Schema: 1, CreatedAt: now, WindowStartMS: now.Add(-2 * time.Hour).UnixMilli(), WindowEndMS: now.Add(-time.Second).UnixMilli(),
		Owners: []OwnerAccounting{}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{},
		FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS",
		Warnings: []string{"a", "b"}, EvidenceCoverage: "BOUNDED_DEMO_REST_MATCHED_LOCAL_WITH_UNATTESTED_OPENING_LOTS_NOT_CONTINUOUS",
		EvidenceSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	if err := ArchiveAccounting(t.TempDir(), &r); err != nil {
		t.Fatalf("valid opening-lot coverage could not be archived: %v", err)
	}
}

func TestAccountingArchiveRejectsInvalidEvidenceDigestAndNegativeProtectionQuantity(t *testing.T) {
	now := time.Now().UTC()
	base := AccountingReport{
		Schema: 1, CreatedAt: now, WindowStartMS: now.Add(-time.Hour).UnixMilli(), WindowEndMS: now.Add(-time.Second).UnixMilli(),
		Owners: []OwnerAccounting{}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{},
		FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS", Warnings: []string{"a", "b"},
		EvidenceCoverage: "BOUNDED_DEMO_REST_MATCHED_LOCAL_NOT_CONTINUOUS", EvidenceSHA256: "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
	}
	if err := ArchiveAccounting(t.TempDir(), &base); err == nil {
		t.Fatal("non-hex evidence digest was archived")
	}

	base.EvidenceSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	base.Owners = []OwnerAccounting{{Source: Model, ModelID: "A", Symbol: "BTCUSDT", PositionSide: Long, OpenQuantity: "0", AverageEntry: "0", VirtualGrossRealizedUSDT: "0", ExchangeReportedGrossUSDT: "0", CommissionByAsset: map[string]string{}, VirtualAfterUSDTFeesExFunding: "0", ProvisionalFundingUSDT: "0", ProvisionalAfterFundingUSDT: "0", SlippageCoverage: "NO_MARKET_EXECUTIONS", Protection: &ProtectionAccountingAttribution{FillCount: 1, GroupCount: 1, TakeProfitFillCount: 1, TakeProfitQuantity: "-0.1", StopLossQuantity: "0", VirtualGrossRealizedUSDT: "0", ExchangeReportedGrossUSDT: "0", CommissionByAsset: map[string]string{}}}}
	base.ProtectionFillCount = 1
	base.ProtectionGroupCount = 1
	if err := ArchiveAccounting(t.TempDir(), &base); err == nil {
		t.Fatal("negative protection quantity was archived")
	}
}
