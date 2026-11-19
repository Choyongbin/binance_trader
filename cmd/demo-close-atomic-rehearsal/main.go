package main

// Local-only cross-ledger atomicity rehearsal. NOT a financial posting tool.
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
	data := flag.String("data", "", "existing local data directory")
	id := flag.String("client", "", "existing entry clientOrderID")
	archivePath := flag.String("archive", "", "existing signed entry archive")
	action := flag.String("action", "inspect", "inspect|pair|continuity|commit-gate|prepare|lifecycle|ownership|seal")
	confirm := flag.String("confirm", "", "required on writes: SHADOW_ONLY_NO_FINANCIAL_POSTING")
	commit := flag.String("commit", "", "required for pair: seal SHA-256 from a prior inspect")
	legacy := flag.Bool("legacy-read-only", false, "explicitly inspect 3.106/3.107 legacy single-directory checkpoints; NEVER write")
	linked := flag.Bool("linked-event", false, "opt-in 3.110: bind atomic event to the same scoped settlement journal")
	flag.Parse()
	readOnly := *action == "inspect" || *action == "pair" || *action == "continuity" || *action == "commit-gate"
	valid := readOnly || *action == "prepare" || *action == "lifecycle" || *action == "ownership" || *action == "seal"
	if *data == "" || *id == "" || *archivePath == "" || !valid || (*legacy && !readOnly) ||
		((*action == "pair" || *action == "continuity" || *action == "commit-gate") && *commit == "") || ((*action != "pair" && *action != "continuity" && *action != "commit-gate") && *commit != "") ||
		((*action == "continuity" || *action == "commit-gate") && !*linked) || (*linked && *legacy) ||
		(readOnly && *confirm != "") || (!readOnly && *confirm != "SHADOW_ONLY_NO_FINANCIAL_POSTING") {
		fail(errors.New("-data -client -archive required; write stages require SHADOW_ONLY_NO_FINANCIAL_POSTING"))
	}
	f, err := os.Open(*archivePath)
	if err != nil {
		fail(err)
	}
	defer f.Close()
	var archive trading.PackageAEntryArchive
	dec := json.NewDecoder(io.LimitReader(f, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&archive); err != nil {
		fail(err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		fail(errors.New("malformed or trailing archive data"))
	}
	var store *trading.ExternalAtomicRehearsalStore
	if *legacy {
		store, err = trading.NewExternalAtomicRehearsalStore(*data)
	} else if *linked {
		store, err = trading.NewExternalAtomicLinkedEventStore(*data, *id)
	} else {
		store, err = trading.NewExternalAtomicEventStore(*data, *id)
	}
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	if *action == "commit-gate" {
		result, err := store.InspectFinancialCommitPreflight(ctx, *id, archive, *commit)
		if err != nil {
			fail(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fail(err)
		}
		return
	}
	if *action == "continuity" {
		result, err := store.InspectScopedSettlementContinuity(ctx, *id, archive, *commit)
		if err != nil {
			fail(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fail(err)
		}
		return
	}
	if *action == "pair" {
		result, err := store.ReadLedgerPair(ctx, *id, archive, *commit)
		if err != nil {
			fail(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fail(err)
		}
		return
	}
	var result trading.ExternalAtomicRehearsalInspection
	switch *action {
	case "inspect":
		result, err = store.Inspect(ctx, *id, archive)
	case "prepare":
		result, err = store.Prepare(ctx, *id, archive)
	case "lifecycle":
		result, err = store.StageLifecycle(ctx, *id, archive)
	case "ownership":
		result, err = store.StageOwnership(ctx, *id, archive)
	case "seal":
		result, err = store.Seal(ctx, *id, archive)
	}
	if err != nil {
		fail(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
