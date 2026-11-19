package main

// OFFLINE ONLY: immutable cross-ledger dependency journal, never financial posting.
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
	data := flag.String("data", "", "existing application data directory")
	id := flag.String("client", "", "existing entry clientOrderID")
	archivePath := flag.String("archive", "", "existing signed entry archive JSON")
	action := flag.String("action", "inspect", "inspect|inspect-recovery|inspect-evidence|prepare|fence")
	confirm := flag.String("confirm", "", "required on writes: JOURNAL_ONLY_NO_SETTLEMENT")
	legacy := flag.Bool("legacy-read-only", false, "explicitly inspect pre-3.109 single-directory checkpoints; never write")
	flag.Parse()
	if *data == "" || *id == "" || *archivePath == "" || (*action != "inspect" && *action != "inspect-recovery" && *action != "inspect-evidence" && *action != "prepare" && *action != "fence") ||
		((*action == "inspect" || *action == "inspect-recovery" || *action == "inspect-evidence") && *confirm != "") || ((*action == "prepare" || *action == "fence") && *confirm != "JOURNAL_ONLY_NO_SETTLEMENT") ||
		(*legacy && (*action == "prepare" || *action == "fence")) {
		fmt.Fprintln(os.Stderr, "-data -client -archive -action inspect|inspect-recovery|inspect-evidence|prepare|fence required; writes require JOURNAL_ONLY_NO_SETTLEMENT")
		os.Exit(2)
	}
	f, err := os.Open(*archivePath)
	if err != nil {
		fail(err)
	}
	defer f.Close()
	var archive trading.PackageAEntryArchive
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err = dec.Decode(&archive); err != nil {
		fail(err)
	}
	var leftover any
	if err = dec.Decode(&leftover); err == nil {
		fail(fmt.Errorf("extra JSON after entry archive"))
	} else if !errors.Is(err, io.EOF) {
		fail(err)
	}
	var store *trading.ExternalSettlementJournalStore
	if *legacy {
		store, err = trading.NewExternalSettlementJournalStore(*data)
	} else {
		store, err = trading.NewExternalSettlementEventJournalStore(*data, *id)
	}
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	var result any
	switch *action {
	case "inspect-evidence":
		result, err = store.InspectEvidenceMatrix(ctx, *id, archive)
	case "inspect-recovery":
		result, err = store.InspectRecovery(ctx, *id, archive)
	case "inspect":
		var root trading.ExternalSettlementJournalRecord
		var fenced *trading.ExternalSettlementJournalRecord
		root, fenced, err = store.Load(ctx, *id, archive)
		result = struct {
			Prepared trading.ExternalSettlementJournalRecord  `json:"prepared"`
			Fenced   *trading.ExternalSettlementJournalRecord `json:"fenced,omitempty"`
		}{root, fenced}
	case "prepare":
		result, err = store.Prepare(ctx, *id, archive)
	case "fence":
		result, err = store.Fence(ctx, *id, archive)
	}
	if err != nil {
		fail(err)
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
