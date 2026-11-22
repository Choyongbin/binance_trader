package protectionfence

import (
	"strings"
	"testing"
	"time"
)

func signedFrameFixtureV15(t *testing.T) (Fence, SignedAccountFrameV15, SignedAccountFrameV15, []OwnerShare, time.Time) {
	t.Helper()
	_, f, s, now := freshExposureFixture(t)
	a := SignedAccountFrameV15{Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS",
		AccountFingerprintSHA256: f.Intent.AccountFingerprintSHA256,
		StartedAt:                now.Add(-1300 * time.Millisecond), FinishedAt: now.Add(-1050 * time.Millisecond),
		WindowStartMS: now.Add(-time.Hour).UnixMilli(), AccountEvidenceSHA256: strings.Repeat("d", 64),
		LongQtyBTC: s.LongQtyBTC, ShortQtyBTC: s.ShortQtyBTC, CanTrade: true}
	a.WindowEndMS = a.StartedAt.UnixMilli()
	b := a
	b.StartedAt = now.Add(-500 * time.Millisecond)
	b.FinishedAt = now.Add(-250 * time.Millisecond)
	b.WindowEndMS = b.StartedAt.UnixMilli()
	return f, a, b, s.Owners, now
}
func mustNeverAuthorizeV15(t *testing.T, g SignedAccountExposureReviewV15) {
	t.Helper()
	if !g.ManualReviewRequired || g.TradingAuthorized || g.SubmitAuthorized || g.CancelAuthorized || g.RetryAuthorized || g.SettlementAuthorized {
		t.Fatalf("unsafe approval: %+v", g)
	}
}
func TestSignedAccountPairV15TwoFreshIndependentWindowsNotApproval(t *testing.T) {
	f, a, b, owners, now := signedFrameFixtureV15(t)
	got := InspectSignedAccountFramePairV15(f, a, b, now)
	if got.State != "SIGNED_BOUNDED_ACCOUNT_STABLE_OWNER_UNPROVEN" {
		t.Fatalf("pair %+v", got)
	}
	mustNeverAuthorizeV15(t, got)
	full := ReviewSignedAccountFramesV15(f, a, b, owners, now)
	if full.State != "SIGNED_BOUNDED_SINGLE_OWNER_NOT_AUTHORIZED" || full.SameSideOwnerCount != 1 {
		t.Fatalf("full %+v", full)
	}
	mustNeverAuthorizeV15(t, full)
}
func TestSignedAccountPairV15PooledSameSideStillUnproven(t *testing.T) {
	f, a, b, owners, now := signedFrameFixtureV15(t)
	owners = append(owners, OwnerShare{GroupID: "entry_other", Source: "model", ModelID: "other", Symbol: "BTCUSDT", Side: "LONG", QuantityBTC: "0.02"})
	a.LongQtyBTC = "0.03"
	b.LongQtyBTC = "0.03000000"
	out := ReviewSignedAccountFramesV15(f, a, b, owners, now)
	if out.State != "SIGNED_BOUNDED_POOLED_SIDE_MANUAL_RECONCILIATION" || out.SameSideOwnerCount != 2 {
		t.Fatalf("pooled %+v", out)
	}
	mustNeverAuthorizeV15(t, out)
}
func TestSignedAccountV15FailClosedMatrix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SignedAccountFrameV15, *SignedAccountFrameV15)
	}{
		{"account fingerprint mismatch", func(a, b *SignedAccountFrameV15) { b.AccountFingerprintSHA256 = strings.Repeat("f", 64) }},
		{"snapshot digest drift", func(a, b *SignedAccountFrameV15) { b.AccountEvidenceSHA256 = strings.Repeat("e", 64) }},
		{"foreign exposure", func(a, b *SignedAccountFrameV15) { a.ForeignExposure = true; b.ForeignExposure = true }},
		{"unverified collateral", func(a, b *SignedAccountFrameV15) { a.UnverifiedCollateral = true; b.UnverifiedCollateral = true }},
		{"open normal order", func(a, b *SignedAccountFrameV15) { a.OpenNormalOrders = 1; b.OpenNormalOrders = 1 }},
		{"open algo order", func(a, b *SignedAccountFrameV15) { a.OpenAlgoOrders = 2; b.OpenAlgoOrders = 2 }},
		{"market order race", func(a, b *SignedAccountFrameV15) { b.OpenNormalOrders = 1 }},
		{"invalid capability", func(a, b *SignedAccountFrameV15) { a.CanTrade = false; b.CanTrade = false }},
		{"multi asset margin", func(a, b *SignedAccountFrameV15) { a.MultiAssetsMargin = true; b.MultiAssetsMargin = true }},
		{"traded between windows", func(a, b *SignedAccountFrameV15) { b.LongQtyBTC = "0.02" }},
		{"stale", func(a, b *SignedAccountFrameV15) { a.FinishedAt = a.FinishedAt.Add(-time.Minute) }},
		{"overlapping windows", func(a, b *SignedAccountFrameV15) {
			b.StartedAt = a.FinishedAt.Add(-time.Millisecond)
			b.WindowEndMS = b.StartedAt.UnixMilli()
		}},
		{"missing full history", func(a, b *SignedAccountFrameV15) { b.Coverage = "PARTIAL" }},
		{"wrong environment", func(a, b *SignedAccountFrameV15) { a.Environment = "real" }},
		{"bad signed account amount", func(a, b *SignedAccountFrameV15) { a.LongQtyBTC = "1e2" }},
		{"incomplete digest", func(a, b *SignedAccountFrameV15) { a.AccountEvidenceSHA256 = "" }},
		{"window changed", func(a, b *SignedAccountFrameV15) { b.WindowStartMS = b.WindowStartMS - 1000 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, a, b, owners, now := signedFrameFixtureV15(t)
			tc.mutate(&a, &b)
			got := ReviewSignedAccountFramesV15(f, a, b, owners, now)
			if got.State != "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION" {
				t.Fatalf("did not fail closed: %+v", got)
			}
			mustNeverAuthorizeV15(t, got)
		})
	}
}
func TestSignedAccountV15CorruptFenceAndBadOwnerFailClosed(t *testing.T) {
	f, a, b, owners, now := signedFrameFixtureV15(t)
	bad := f
	bad.Checksum = strings.Repeat("0", 64)
	got := ReviewSignedAccountFramesV15(bad, a, b, owners, now)
	if got.State != "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION" {
		t.Fatalf("tampered fence %+v", got)
	}
	mustNeverAuthorizeV15(t, got)
	owners[0].GroupID = "other"
	got = ReviewSignedAccountFramesV15(f, a, b, owners, now)
	if got.State != "BLOCKED_SIGNED_ACCOUNT_RECONCILIATION" {
		t.Fatalf("invalid owner %+v", got)
	}
	mustNeverAuthorizeV15(t, got)
}
func TestSignedTotalBTCV15PreservesDecimalAndSideSign(t *testing.T) {
	cases := []struct {
		qty   string
		short bool
		want  bool
	}{
		{"0.00001000", false, true}, {"-0.01000000", true, true}, {"0", true, true}, {"0", false, true},
		{"0.01", true, false}, {"-0.01", false, false}, {"-0", false, false}, {"1e-6", false, false},
		{"+0.01", false, false}, {"0.000000001", false, false}, {"01", false, false},
	}
	for _, tc := range cases {
		_, ok := SignedTotalBTCV15(tc.qty, tc.short)
		if ok != tc.want {
			t.Errorf("qty=%q short=%v ok=%v want=%v", tc.qty, tc.short, ok, tc.want)
		}
	}
}
