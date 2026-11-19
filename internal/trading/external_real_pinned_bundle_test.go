package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPinnedRealBundleLongShortRestartAndSameGeneration(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, _, _, _ := financialShadowFixture(t, side)
			baseline, err := InspectRealLedgerObservation(data, id)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadPinnedRealLedgerBundle(context.Background(), data, id, baseline.EvidenceSHA256)
			if err != nil {
				t.Fatal(err)
			}
			if got.Stage != ExternalRealPinnedReadOnly || got.Schema != 1 || got.ClientOrderID != id ||
				got.EvidenceSHA256 != baseline.EvidenceSHA256 || got.Ownership != baseline ||
				!got.ManualReviewRequired || got.PostingApplied || got.SettlementAuthorized || got.TradingAuthorized ||
				got.Lifecycle.Total != len(got.Lifecycle.Items) || got.Lifecycle.TradingAuthorized ||
				got.Lifecycle.State != got.Ownership.LifecycleAuditState {
				t.Fatalf("unexpected bundle %+v", got)
			}
			replay, err := ReadPinnedRealLedgerBundle(context.Background(), data, id, baseline.EvidenceSHA256)
			if err != nil || !reflect.DeepEqual(replay, got) {
				t.Fatalf("restarted read mismatch: %v %+v", err, replay)
			}
			if _, err := ReadPinnedRealLedgerBundle(context.Background(), data, id, strings.Repeat("0", 64)); err == nil {
				t.Fatal("foreign version accepted")
			}
		})
	}
}

func TestPinnedRealBundleRejectsMidReadChangeAndStaleToken(t *testing.T) {
	data, id, _, _, _ := financialShadowFixture(t, Short)
	baseline, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")
	changed := false
	_, err = readPinnedRealLedgerBundle(context.Background(), data, id, baseline.EvidenceSHA256, func() error {
		changed = true
		return os.WriteFile(path, []byte("changed between two readers"), 0600)
	})
	if !changed || err == nil {
		t.Fatalf("torn read accepted: %v", err)
	}
	if _, err := ReadPinnedRealLedgerBundle(context.Background(), data, id, baseline.EvidenceSHA256); err == nil {
		t.Fatal("stale baseline accepted")
	}
}

func TestPinnedRealBundleRejectsContextAndDoesNotCreateData(t *testing.T) {
	data, id, _, _, _ := financialShadowFixture(t, Long)
	real, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadPinnedRealLedgerBundle(ctx, data, id, real.EvidenceSHA256); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context not rejected: %v", err)
	}
	if _, err := ReadPinnedRealLedgerBundle(nil, data, id, real.EvidenceSHA256); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := ReadPinnedRealLedgerBundle(context.Background(), data, id, ""); err == nil {
		t.Fatal("empty pin accepted")
	}
	missing := t.TempDir()
	if _, err := ReadPinnedRealLedgerBundle(context.Background(), missing, id, real.EvidenceSHA256); err == nil {
		t.Fatal("missing data accepted")
	}
	if _, err := os.Lstat(filepath.Join(missing, "trading")); !os.IsNotExist(err) {
		t.Fatalf("reader created directory: %v", err)
	}
}

func TestPinnedRealBundleMissingFillNeverSettles(t *testing.T) {
	data, id, _, _, _ := financialShadowFixture(t, Long)
	if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
		t.Fatal(err)
	}
	baseline, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := ReadPinnedRealLedgerBundle(context.Background(), data, id, baseline.EvidenceSHA256)
	if err != nil || bundle.Ownership.EntryFillCount != 0 || bundle.Ownership.EntryQuantityBTC != "0" ||
		bundle.PostingApplied || bundle.SettlementAuthorized || bundle.TradingAuthorized {
		t.Fatalf("missing entry was treated as settlement: %v %+v", err, bundle)
	}
}
