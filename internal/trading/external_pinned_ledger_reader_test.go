package trading

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestExternalPinnedLedgerSourceLongShortAndRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, archive, seal, shadow := financialShadowFixture(t, side)
			ctx := context.Background()
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			realPin := ExternalLedgerReadPin{RealEvidenceSHA256: before}
			real, err := ReadExternalLedgerSnapshot(ctx, data, id, ExternalLedgerSourceReal, realPin, nil)
			if err != nil {
				t.Fatal(err)
			}
			if real.Source != ExternalLedgerSourceReal || real.Real == nil || real.Shadow != nil ||
				real.SourceSHA256 != before || real.PublicationSHA256 != "" ||
				!real.ManualReviewRequired || real.TradingAuthorized || real.SettlementAuthorized || real.PostingApplied ||
				real.Real.Ownership.ClientOrderID != id || real.Real.Lifecycle.Total != len(real.Real.Lifecycle.Items) {
				t.Fatalf("unsafe real reader: %+v", real)
			}
			if _, err := ReadExternalLedgerSnapshot(ctx, data, id, ExternalLedgerSourceShadow, ExternalLedgerReadPin{ShadowSealSHA256: seal, ShadowPublicationSHA256: strings.Repeat("f", 64)}, &archive); err == nil {
				t.Fatal("unpublished shadow accepted")
			}
			if _, err := shadow.PrepareShadow(ctx, archive, seal); err != nil {
				t.Fatal(err)
			}
			if _, err := shadow.PublishShadow(ctx, archive, seal); err != nil {
				t.Fatal(err)
			}
			published, err := shadow.Inspect(ctx, archive, seal)
			if err != nil {
				t.Fatal(err)
			}
			pin := ExternalLedgerReadPin{ShadowSealSHA256: seal, ShadowPublicationSHA256: published.ShadowPublishSHA256}
			got, err := ReadExternalLedgerSnapshot(ctx, data, id, ExternalLedgerSourceShadow, pin, &archive)
			if err != nil {
				t.Fatal(err)
			}
			if got.Source != ExternalLedgerSourceShadow || got.Real != nil || got.Shadow == nil ||
				got.SourceSHA256 != seal || got.PublicationSHA256 != published.ShadowPublishSHA256 ||
				got.Shadow.Lifecycle.Proposal == "" || got.Shadow.Ownership.Proposal == "" ||
				got.PostingApplied || got.SettlementAuthorized || got.TradingAuthorized ||
				got.Shadow.Lifecycle.Applied || got.Shadow.Ownership.Applied || !got.ManualReviewRequired {
				t.Fatalf("unsafe shadow reader: %+v", got)
			}
			restarted, err := NewExternalShadowCommitStore(data, id)
			if err != nil {
				t.Fatal(err)
			}
			port, err := NewExternalShadowReadPort(restarted, archive)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := port.ReadPinned(ctx, pin)
			if err != nil || !reflect.DeepEqual(replay, got) {
				t.Fatalf("restart read mismatch: %v", err)
			}
			realAgain, err := ReadExternalLedgerSnapshot(ctx, data, id, ExternalLedgerSourceReal, realPin, nil)
			if err != nil || !reflect.DeepEqual(realAgain, real) {
				t.Fatalf("real read mismatch: %v", err)
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("financial originals changed: %v", err)
			}
			// A reusable common read-only interface can represent either
			// projection, but each port accepts only its own typed pins.
			rport, err := NewExternalRealReadPort(data, id)
			if err != nil {
				t.Fatal(err)
			}
			var readers []struct {
				port ExternalLedgerReadPort
				pin  ExternalLedgerReadPin
			} = []struct {
				port ExternalLedgerReadPort
				pin  ExternalLedgerReadPin
			}{{rport, realPin}, {port, pin}}
			for _, reader := range readers {
				if _, err := reader.port.ReadPinned(ctx, reader.pin); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExternalPinnedLedgerRejectsCrossSourcePinsAndStaleEvidence(t *testing.T) {
	data, id, archive, seal, shadow := financialShadowFixture(t, Short)
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
	published, err := shadow.Inspect(ctx, archive, seal)
	if err != nil {
		t.Fatal(err)
	}
	goodShadow := ExternalLedgerReadPin{ShadowSealSHA256: seal, ShadowPublicationSHA256: published.ShadowPublishSHA256}
	goodReal := ExternalLedgerReadPin{RealEvidenceSHA256: real.EvidenceSHA256}
	cases := []struct {
		name    string
		kind    ExternalLedgerSource
		pin     ExternalLedgerReadPin
		archive *PackageAEntryArchive
	}{
		{"unspecified", "", goodReal, nil},
		{"unknown", "FINANCIAL_COMMIT", goodReal, nil},
		{"real without token", ExternalLedgerSourceReal, ExternalLedgerReadPin{}, nil},
		{"real with foreign token", ExternalLedgerSourceReal, ExternalLedgerReadPin{RealEvidenceSHA256: strings.Repeat("a", 64)}, nil},
		{"real with shadow pin", ExternalLedgerSourceReal, ExternalLedgerReadPin{RealEvidenceSHA256: real.EvidenceSHA256, ShadowSealSHA256: seal}, nil},
		{"real with archive", ExternalLedgerSourceReal, goodReal, &archive},
		{"shadow without archive", ExternalLedgerSourceShadow, goodShadow, nil},
		{"shadow with real token", ExternalLedgerSourceShadow, ExternalLedgerReadPin{RealEvidenceSHA256: real.EvidenceSHA256, ShadowSealSHA256: seal, ShadowPublicationSHA256: published.ShadowPublishSHA256}, &archive},
		{"shadow with only seal", ExternalLedgerSourceShadow, ExternalLedgerReadPin{ShadowSealSHA256: seal}, &archive},
		{"shadow with foreign publication", ExternalLedgerSourceShadow, ExternalLedgerReadPin{ShadowSealSHA256: seal, ShadowPublicationSHA256: strings.Repeat("f", 64)}, &archive},
		{"shadow with foreign seal", ExternalLedgerSourceShadow, ExternalLedgerReadPin{ShadowSealSHA256: strings.Repeat("a", 64), ShadowPublicationSHA256: published.ShadowPublishSHA256}, &archive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ReadExternalLedgerSnapshot(ctx, data, id, tc.kind, tc.pin, tc.archive); err == nil {
				t.Fatal("unsafe ledger read accepted")
			}
		})
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ReadExternalLedgerSnapshot(canceled, data, id, ExternalLedgerSourceReal, goodReal, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel ignored: %v", err)
	}
	if _, err := ReadExternalLedgerSnapshot(nil, data, id, ExternalLedgerSourceShadow, goodShadow, &archive); err == nil {
		t.Fatal("nil context accepted")
	}
	// Simulate actual source modification after pinning: the SHADOW proposal
	// must not silently substitute for stale original execution evidence.
	if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadExternalLedgerSnapshot(ctx, data, id, ExternalLedgerSourceReal, goodReal, nil); err == nil {
		t.Fatal("stale real pin accepted")
	}
	if _, err := ReadExternalLedgerSnapshot(ctx, data, id, ExternalLedgerSourceShadow, goodShadow, &archive); err == nil {
		t.Fatal("stale shadow evidence accepted")
	}
}

func TestExternalPinnedLedgerReadersConcurrentReadOnly(t *testing.T) {
	data, id, archive, seal, shadow := financialShadowFixture(t, Long)
	ctx := context.Background()
	baseline, err := InspectRealLedgerObservation(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PrepareShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.PublishShadow(ctx, archive, seal); err != nil {
		t.Fatal(err)
	}
	status, err := shadow.Inspect(ctx, archive, seal)
	if err != nil {
		t.Fatal(err)
	}
	pins := []struct {
		kind    ExternalLedgerSource
		pin     ExternalLedgerReadPin
		archive *PackageAEntryArchive
	}{
		{ExternalLedgerSourceReal, ExternalLedgerReadPin{RealEvidenceSHA256: baseline.EvidenceSHA256}, nil},
		{ExternalLedgerSourceShadow, ExternalLedgerReadPin{ShadowSealSHA256: seal, ShadowPublicationSHA256: status.ShadowPublishSHA256}, &archive},
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := pins[index%len(pins)]
			r, err := ReadExternalLedgerSnapshot(ctx, data, id, p.kind, p.pin, p.archive)
			if err == nil && (r.PostingApplied || r.TradingAuthorized || r.SettlementAuthorized) {
				err = errors.New("reader authorized posting")
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
