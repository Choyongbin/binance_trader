package protectionfence

import (
	"strings"
	"testing"
	"time"
)

func freshExposureFixture(t *testing.T) (*Store, Fence, ExposureSnapshot, time.Time) {
	t.Helper()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	fence, err := store.SealBeforePossibleSend(testIntent(SubmitTP), now)
	if err != nil {
		t.Fatal(err)
	}
	snap := ExposureSnapshot{AccountFingerprintSHA256: fence.Intent.AccountFingerprintSHA256,
		ObservedAt: now, LongQtyBTC: "0.01000000", ShortQtyBTC: "0",
		Owners: []OwnerShare{{GroupID: fence.Intent.GroupID, Source: "model", ModelID: "ema_rsi",
			Symbol: "BTCUSDT", Side: "LONG", QuantityBTC: fence.Intent.QuantityBTC}}}
	return store, fence, snap, now
}

func assertExposureNonAuthorizing(t *testing.T, e ExposureReview) {
	t.Helper()
	if !e.ManualReviewRequired || e.TradingAuthorized || e.SubmitAuthorized || e.CancelAuthorized || e.RetryAuthorized || e.SettlementAuthorized {
		t.Fatalf("unsafe exposure review: %+v", e)
	}
}
func TestExposureSingleOwnerIsNotTradingApproval(t *testing.T) {
	_, fence, snapshot, now := freshExposureFixture(t)
	got := AssessExposure(fence, snapshot, now)
	if got.State != ExposureOneOwner || got.SameSideOwnerCount != 1 {
		t.Fatalf("review: %+v", got)
	}
	assertExposureNonAuthorizing(t, got)
}
func TestExposurePooledSideWithMatchingAccountMustBlock(t *testing.T) {
	_, fence, snapshot, now := freshExposureFixture(t)
	snapshot.Owners = append(snapshot.Owners, OwnerShare{GroupID: "entry_other", Source: "model", ModelID: "macd", Symbol: "BTCUSDT", Side: "LONG", QuantityBTC: "0.02"})
	snapshot.LongQtyBTC = "0.03000000"
	got := AssessExposure(fence, snapshot, now)
	if got.State != ExposurePooled || got.SameSideOwnerCount != 2 {
		t.Fatalf("pooled not detected: %+v", got)
	}
	assertExposureNonAuthorizing(t, got)
}
func TestExposureFailsClosedMatrix(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*ExposureSnapshot)
	}{
		{"missing owner", func(s *ExposureSnapshot) { s.Owners[0].GroupID = "entry_wrong" }},
		{"hidden same side qty", func(s *ExposureSnapshot) { s.LongQtyBTC = "0.02" }},
		{"hidden short qty", func(s *ExposureSnapshot) { s.ShortQtyBTC = "0.01" }},
		{"wrong account", func(s *ExposureSnapshot) { s.AccountFingerprintSHA256 = strings.Repeat("f", 64) }},
		{"stale", func(s *ExposureSnapshot) { s.ObservedAt = s.ObservedAt.Add(-time.Minute) }},
		{"future", func(s *ExposureSnapshot) { s.ObservedAt = s.ObservedAt.Add(time.Minute) }},
		{"model without ID", func(s *ExposureSnapshot) { s.Owners[0].ModelID = "" }},
		{"manual with model ID", func(s *ExposureSnapshot) { s.Owners[0].Source = "manual" }},
		{"wrong target side", func(s *ExposureSnapshot) { s.Owners[0].Side = "SHORT" }},
		{"negative quantity", func(s *ExposureSnapshot) { s.Owners[0].QuantityBTC = "-0.01" }},
		{"rounded quantity", func(s *ExposureSnapshot) { s.Owners[0].QuantityBTC = "0.010001" }},
		{"foreign symbol", func(s *ExposureSnapshot) { s.Owners[0].Symbol = "ETHUSDT" }},
		{"empty owners", func(s *ExposureSnapshot) { s.Owners = nil }},
		{"duplicate owner", func(s *ExposureSnapshot) { s.Owners = append(s.Owners, s.Owners[0]) }},
		{"invalid account amount", func(s *ExposureSnapshot) { s.LongQtyBTC = "1e20" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, f, s, now := freshExposureFixture(t)
			tc.modify(&s)
			got := AssessExposure(f, s, now)
			if got.State != ExposureBlocked {
				t.Fatalf("unexpected state %+v", got)
			}
			assertExposureNonAuthorizing(t, got)
		})
	}
}
func TestExposureOppositeHedgeOwnerIsNotHidden(t *testing.T) {
	_, f, s, now := freshExposureFixture(t)
	s.Owners = append(s.Owners, OwnerShare{GroupID: "entry_short", Source: "manual", Symbol: "BTCUSDT", Side: "SHORT", QuantityBTC: "0.03"})
	s.ShortQtyBTC = "0.03"
	got := AssessExposure(f, s, now)
	if got.State != ExposureOneOwner {
		t.Fatalf("opposite-side state: %+v", got)
	}
	assertExposureNonAuthorizing(t, got)
}
