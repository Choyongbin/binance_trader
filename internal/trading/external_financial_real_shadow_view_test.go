package trading

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRealLedgerAndShadowSeparateReaderLongShort(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, archive, seal, shadow := financialShadowFixture(t, side)
			ctx := context.Background()
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			live, err := InspectRealLedgerObservation(data, id)
			if err != nil {
				t.Fatal(err)
			}
			if live.EvidenceSHA256 != before || live.ClientOrderID != id || live.JournalCount == 0 ||
				!live.ManualReviewRequired || live.PostingApplied || live.SettlementAuthorized || live.TradingAuthorized {
				t.Fatalf("bad real observation %+v", live)
			}
			if _, err := shadow.ReadSeparatedRealAndShadow(ctx, archive, seal, before); err == nil {
				t.Fatal("shadow not yet published; mixed read accepted")
			}
			if _, err := shadow.PrepareShadow(ctx, archive, seal); err != nil {
				t.Fatal(err)
			}
			if _, err := shadow.ReadSeparatedRealAndShadow(ctx, archive, seal, before); err == nil {
				t.Fatal("prepared-only shadow accepted")
			}
			if _, err := shadow.PublishShadow(ctx, archive, seal); err != nil {
				t.Fatal(err)
			}
			view, err := shadow.ReadSeparatedRealAndShadow(ctx, archive, seal, before)
			if err != nil {
				t.Fatal(err)
			}
			if view.Stage != ExternalRealShadowSeparatedNotPosted || view.ClientOrderID != id ||
				!reflect.DeepEqual(view.Real, live) || view.Shadow.CommitSHA256 != seal ||
				view.Shadow.Lifecycle.CommitSHA256 != view.Shadow.Ownership.CommitSHA256 ||
				view.ShadowPublishSHA256 == "" || view.PostingApplied ||
				view.SettlementAuthorized || view.TradingAuthorized {
				t.Fatalf("unsafe merged view: %+v", view)
			}
			restarted, err := NewExternalShadowCommitStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			again, err := restarted.ReadSeparatedRealAndShadow(ctx, archive, seal, before)
			if err != nil || !reflect.DeepEqual(again, view) {
				t.Fatalf("restart mismatch %v %+v", err, again)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("real financial ledger modified %v", err)
			}
		})
	}
}

func TestRealShadowReaderFailsClosedOnStalePinsOrSourceDrift(t *testing.T) {
	data, id, archive, seal, shadow := financialShadowFixture(t, Short)
	ctx := context.Background()
	live, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PrepareShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PublishShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	for _, pin := range []string{"", strings.Repeat("f", 64)} {
		if _, err := shadow.ReadSeparatedRealAndShadow(ctx, archive, seal, pin); err == nil {
			t.Fatal("foreign real pin accepted")
		}
	}
	if _, err := shadow.ReadSeparatedRealAndShadow(ctx, archive, strings.Repeat("a", 64), live.EvidenceSHA256); err == nil {
		t.Fatal("foreign shadow seal accepted")
	}
	if _, err := shadow.ReadSeparatedRealAndShadow(nil, archive, seal, live.EvidenceSHA256); err == nil {
		t.Fatal("nil context accepted")
	}
	// A valid checksum on the shadow does not repair a changed local ledger.
	fillPath := filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")
	if err := os.WriteFile(fillPath, []byte("{\"malformed\":true}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.ReadSeparatedRealAndShadow(ctx, archive, seal, live.EvidenceSHA256); err == nil {
		t.Fatal("changed real fill accepted")
	}
}

func TestRealShadowReaderDetectsMidReadMutationAndUnauthorizedPreview(t *testing.T) {
	data, id, archive, seal, shadow := financialShadowFixture(t, Long)
	ctx := context.Background()
	live, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PrepareShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PublishShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	// Deterministically change the real ledger between the first observation
	// and the last one. No mixed ledger result may escape.
	_, err = shadow.readSeparatedRealAndShadow(ctx, archive, seal, live.EvidenceSHA256,
		func() (ExternalAtomicLedgerPair, error) {
			pair, err := shadow.ReadShadowPair(ctx, archive, seal)
			if err != nil {
				return pair, err
			}
			path := filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")
			if err := os.WriteFile(path, []byte("changed mid-read"), 0600); err != nil {
				t.Fatal(err)
			}
			return pair, nil
		})
	if err == nil {
		t.Fatal("torn real/shadow read accepted")
	}
}

func TestRealShadowReaderRejectsAppliedPreview(t *testing.T) {
	data, id, archive, seal, shadow := financialShadowFixture(t, Long)
	ctx := context.Background()
	real, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PrepareShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PublishShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	_, err = shadow.readSeparatedRealAndShadow(ctx, archive, seal, real.EvidenceSHA256,
		func() (ExternalAtomicLedgerPair, error) {
			got, err := shadow.ReadShadowPair(ctx, archive, seal)
			got.Ownership.Applied = true // a poisoned reader must not gain authority
			return got, err
		})
	if err == nil {
		t.Fatal("authorizing shadow proposal accepted")
	}
}

func TestRealObservationMissingLocalEntryIsNotFinancialSettlement(t *testing.T) {
	data, id, _, _, _ := financialShadowFixture(t, Short)
	// This models the real incident class: the source request exists but the
	// exchange-confirmed entry was not imported to the local immutable fills.
	err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json"))
	if err != nil {
		t.Fatal(err)
	}
	real, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if real.EntryFillCount != 0 || real.EntryQuantityBTC != "0" || !real.ManualReviewRequired ||
		real.PostingApplied || real.TradingAuthorized || real.SettlementAuthorized {
		t.Fatalf("missing entry fill incorrectly counted as posted: %+v", real)
	}
	if _, err := InspectRealLedgerObservation(data, "bt_ffffffffffffffffffffffffffff"); err == nil {
		t.Fatal("foreign client order ID accepted")
	}
}

func TestRealObservationDoesNotCreateAnyDirectories(t *testing.T) {
	data := t.TempDir()
	_, err := InspectRealLedgerObservation(data, "bt_ffffffffffffffffffffffffffff")
	if err == nil {
		t.Fatal("empty data directory should fail closed")
	}
	if _, err := os.Lstat(filepath.Join(data, "trading")); !os.IsNotExist(err) {
		t.Fatalf("real ledger baseline wrote a trading directory: %v", err)
	}
}
