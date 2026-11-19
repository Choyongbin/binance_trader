// demo-close-cross-event-audit compares independently reviewed, OFFLINE
// snapshots from separate incidents. It NEVER contacts Binance, posts a
// ledger entry, allocates funding, or authorizes trading. Conflicts exit 2.
package main

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

type manifest struct {
	Schema int `json:"schema"`
	Events []struct {
		Data         string `json:"data"`
		ID           string `json:"id"`
		IncidentFile string `json:"incident"`
	} `json:"events"`
}

func readStrictJSON(path string, sizeLimit int64, out any) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > sizeLimit {
		return errors.New("input is not a regular, bounded-size JSON file")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, sizeLimit+1))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing or malformed JSON")
	}
	return nil
}

func run(ctx context.Context, manifestFile string, out io.Writer) (bool, error) {
	return runWithMode(ctx, manifestFile, out, false)
}

func runWithMode(ctx context.Context, manifestFile string, out io.Writer, modelPolicy bool) (bool, error) {
	var input manifest
	if err := readStrictJSON(manifestFile, 1<<20, &input); err != nil {
		return false, fmt.Errorf("manifest: %w", err)
	}
	if input.Schema != 1 || len(input.Events) < 2 || len(input.Events) > 64 {
		return false, errors.New("manifest schema=1 and 2..64 events required")
	}
	events := make([]trading.ExternalFinancialEventSource, 0, len(input.Events))
	for _, e := range input.Events {
		if e.Data == "" || e.ID == "" || e.IncidentFile == "" {
			return false, errors.New("data, id, incident required for every event")
		}
		var archive trading.PackageAIncidentArchive
		if err := readStrictJSON(e.IncidentFile, 8<<20, &archive); err != nil {
			return false, fmt.Errorf("incident %s: %w", e.ID, err)
		}
		events = append(events, trading.ExternalFinancialEventSource{DataDir: e.Data, ClientOrderID: e.ID, Incident: archive})
	}
	if modelPolicy {
		report, err := trading.InspectExternalModelAttributionPolicy(ctx, events)
		if err != nil {
			return false, err
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return false, err
		}
		return false, nil
	}
	report, err := trading.InspectExternalCrossEventFinancials(ctx, events)
	if err != nil {
		return false, err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return false, err
	}
	return len(report.Conflicts) > 0, nil
}

func main() {
	file := flag.String("manifest", "", "JSON with schema=1 and events [{data,id,incident},...]")
	modelPolicy := flag.Bool("model-policy", false, "independently reconstruct bounded model fee grouping; funding NEVER allocated (read-only)")
	flag.Parse()
	if *file == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "BLOCKED: usage: demo-close-cross-event-audit -manifest <offline-events.json>")
		os.Exit(1)
	}
	conflicts, err := runWithMode(context.Background(), *file, os.Stdout, *modelPolicy)
	if err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
	if conflicts {
		fmt.Fprintln(os.Stderr, "BLOCKED: conflicting incident evidence; NO settlement permitted")
		os.Exit(2)
	}
}
