// demo-close-financial-review performs an OFFLINE, READ-ONLY review of a
// bounded historical external close. It cannot settle funds or place orders.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"binance_trader/internal/trading"
)

func main() {
	data := flag.String("data", "", "stopped Binance Trader data directory")
	id := flag.String("id", "", "existing entry clientOrderID")
	source := flag.String("incident", "", "captured PackageA incident archive JSON")
	modelEvidence := flag.Bool("model-evidence", false, "emit bounded per-trade fee + unallocated funding contract (read-only)")
	modelAssertion := flag.String("model-assertion", "", "optional unposted candidate assertion JSON; requires -model-evidence")
	historyWindows := flag.String("history-windows", "", "optional offline signed GET capture manifest; requires -model-evidence and cannot combine with -model-assertion")
	flag.Parse()
	if *data == "" || *id == "" || *source == "" || flag.NArg() != 0 || (*modelAssertion != "" && !*modelEvidence) || (*historyWindows != "" && (!*modelEvidence || *modelAssertion != "")) {
		fail(errors.New("usage: demo-close-financial-review -data <data> -id <clientOrderID> -incident <package-a-incident.json>"))
	}
	fi, err := os.Lstat(*source)
	if err != nil {
		fail(err)
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > 8<<20 {
		fail(errors.New("incident must be a regular JSON file under 8 MiB"))
	}
	f, err := os.Open(*source)
	if err != nil {
		fail(err)
	}
	defer f.Close()
	var incident trading.PackageAIncidentArchive
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&incident); err != nil {
		fail(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		fail(errors.New("trailing or malformed incident JSON"))
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if *modelEvidence {
		result, err := trading.InspectExternalModelFinancialEvidence(context.Background(), *data, *id, incident)
		if err != nil {
			fail(err)
		}
		if *historyWindows != "" {
			var manifest trading.ExternalSignedHistoryManifest
			if err := readStrictFinancialJSONLimited(*historyWindows, &manifest, 8<<20); err != nil {
				fail(err)
			}
			history, err := trading.InspectExternalSignedHistoryContinuity(context.Background(), *data, *id, incident, manifest)
			if err != nil {
				fail(err)
			}
			if err := encoder.Encode(history); err != nil {
				fail(err)
			}
			return
		}
		if *modelAssertion != "" {
			var claim trading.ExternalModelFinancialAssertion
			if err := readStrictFinancialJSON(*modelAssertion, &claim); err != nil {
				fail(err)
			}
			if err := trading.ValidateExternalModelFinancialAssertion(result, claim); err != nil {
				fail(err)
			}
		}
		if err := encoder.Encode(result); err != nil {
			fail(err)
		}
		return
	}
	result, err := trading.InspectExternalCloseBoundedFinancials(context.Background(), *data, *id, incident)
	if err != nil {
		fail(err)
	}
	if err := encoder.Encode(result); err != nil {
		fail(err)
	}
}

func readStrictFinancialJSON(path string, out any) error {
	return readStrictFinancialJSONLimited(path, out, 1<<20)
}

func readStrictFinancialJSONLimited(path string, out any, limit int64) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > limit {
		return errors.New("evidence must be a regular JSON file within size limit")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing or malformed assertion JSON")
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "BLOCKED:", err)
	os.Exit(1)
}
