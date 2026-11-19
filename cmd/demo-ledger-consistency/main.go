package main

// Offline read-only comparison of REAL local ledger observations and a
// SHADOW-only financial candidate. No Binance credentials/network, trading,
// cancellation, financial posting, or changes to existing trading files.
import (
	"binance_trader/internal/trading"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	data := flag.String("data", "", "existing local data directory (offline, stopped copy)")
	id := flag.String("client", "", "original entry clientOrderID")
	action := flag.String("action", "baseline", "baseline|bundle|compare|source")
	source := flag.String("source", "", "source action only: real|shadow (explicit origin)")
	archivePath := flag.String("archive", "", "entry evidence archive (shadow or compare only)")
	seal := flag.String("seal", "", "sealed proposal SHA-256 (shadow or compare only)")
	publishPin := flag.String("publish-pin", "", "published shadow checkpoint SHA-256 (shadow source only)")
	realPin := flag.String("real-pin", "", "previous baseline real_evidence_sha256")
	flag.Parse()
	if *data == "" || *id == "" || flag.NArg() != 0 ||
		(*action != "baseline" && *action != "bundle" && *action != "compare" && *action != "source") {
		fail(errors.New("-data -client -action baseline|bundle|compare|source required"))
	}
	ctx := context.Background()
	if *action == "baseline" {
		if *archivePath != "" || *seal != "" || *realPin != "" || *publishPin != "" || *source != "" {
			fail(errors.New("baseline accepts no archive, source or pin tokens"))
		}
		observation, err := trading.InspectRealLedgerObservation(*data, *id)
		if err != nil {
			fail(err)
		}
		output(observation)
		return
	}
	if *action == "bundle" {
		if *archivePath != "" || *seal != "" || *realPin == "" || *publishPin != "" || *source != "" {
			fail(errors.New("bundle requires only -real-pin from an earlier baseline"))
		}
		view, err := trading.ReadExternalLedgerSnapshot(ctx, *data, *id, trading.ExternalLedgerSourceReal,
			trading.ExternalLedgerReadPin{RealEvidenceSHA256: *realPin}, nil)
		if err != nil {
			fail(err)
		}
		output(view.Real) // preserve the existing bundle JSON contract
		return
	}
	if *action == "source" {
		if *source == "real" {
			if *realPin == "" || *archivePath != "" || *seal != "" || *publishPin != "" {
				fail(errors.New("source real requires only -real-pin"))
			}
			view, err := trading.ReadExternalLedgerSnapshot(ctx, *data, *id, trading.ExternalLedgerSourceReal,
				trading.ExternalLedgerReadPin{RealEvidenceSHA256: *realPin}, nil)
			if err != nil {
				fail(err)
			}
			output(view)
			return
		}
		if *source != "shadow" || *realPin != "" || *archivePath == "" || *seal == "" || *publishPin == "" {
			fail(errors.New("source shadow requires -archive -seal -publish-pin and no -real-pin"))
		}
		archive := loadArchive(*archivePath)
		view, err := trading.ReadExternalLedgerSnapshot(ctx, *data, *id, trading.ExternalLedgerSourceShadow,
			trading.ExternalLedgerReadPin{ShadowSealSHA256: *seal, ShadowPublicationSHA256: *publishPin}, &archive)
		if err != nil {
			fail(err)
		}
		output(view)
		return
	}
	if *archivePath == "" || *seal == "" || *realPin == "" || *publishPin != "" || *source != "" {
		fail(errors.New("compare requires -archive -seal -real-pin and no -source or -publish-pin"))
	}
	archive := loadArchive(*archivePath)
	store, err := trading.NewExternalShadowCommitStore(*data, *id)
	if err != nil {
		fail(err)
	}
	result, err := store.ReadSeparatedRealAndShadow(ctx, archive, *seal, *realPin)
	if err != nil {
		fail(err)
	}
	output(result)
}

func loadArchive(path string) trading.PackageAEntryArchive {
	f, err := os.Open(path)
	if err != nil {
		fail(err)
	}
	defer f.Close()
	var archive trading.PackageAEntryArchive
	dec := json.NewDecoder(io.LimitReader(f, 2<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&archive); err != nil {
		fail(err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		fail(errors.New("trailing or malformed archive data"))
	}
	return archive
}

func output(v any) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
