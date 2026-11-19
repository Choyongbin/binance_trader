// Offline NON-POSTED evidence identity reservation. No Binance network calls,
// trading permission, actual posting, or original-data modifications.
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

func readBounded(path string, max int64, out any) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > max {
		return errors.New("invalid regular bounded JSON input")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, max+1))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return errors.New("trailing or malformed JSON")
	}
	return nil
}

func readManifestEvents(path string) ([]trading.ExternalFinancialEventSource, error) {
	var input manifest
	if err := readBounded(path, 1<<20, &input); err != nil {
		return nil, err
	}
	if input.Schema != 1 || len(input.Events) < 2 || len(input.Events) > 64 {
		return nil, errors.New("schema=1 and 2..64 events required")
	}
	events := make([]trading.ExternalFinancialEventSource, 0, len(input.Events))
	for _, ev := range input.Events {
		if ev.Data == "" || ev.ID == "" || ev.IncidentFile == "" {
			return nil, errors.New("data,id,incident required")
		}
		var incident trading.PackageAIncidentArchive
		if err := readBounded(ev.IncidentFile, 8<<20, &incident); err != nil {
			return nil, fmt.Errorf("incident %s: %w", ev.ID, err)
		}
		events = append(events, trading.ExternalFinancialEventSource{DataDir: ev.Data, ClientOrderID: ev.ID, Incident: incident})
	}
	return events, nil
}

func run(ctx context.Context, root, action, manifestPath, ack string, out io.Writer) error {
	store, err := trading.NewExternalGlobalEvidenceReservationStore(root)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	switch action {
	case "inspect":
		if manifestPath != "" || ack != "" {
			return errors.New("inspect must not include manifest/ack")
		}
		report, err := store.Inspect(ctx)
		if err != nil {
			return err
		}
		return enc.Encode(report)
	case "reserve", "consistency":
		if manifestPath == "" {
			return errors.New("manifest required")
		}
		if action == "reserve" && ack != "REVIEW_ONLY_NOT_SETTLEMENT" {
			return errors.New("reserve requires REVIEW_ONLY_NOT_SETTLEMENT acknowledgment")
		}
		if action == "consistency" && ack != "" {
			return errors.New("consistency is read-only and must not include an acknowledgment")
		}
		events, err := readManifestEvents(manifestPath)
		if err != nil {
			return err
		}
		if action == "reserve" {
			report, err := store.ReserveBatch(ctx, events)
			if err != nil {
				return err
			}
			return enc.Encode(report)
		}
		report, err := trading.InspectExternalGlobalAtomicConsistency(ctx, store, events)
		if err != nil {
			return err
		}
		return enc.Encode(report)
	default:
		return errors.New("action must be inspect, reserve or consistency")
	}
}

func main() {
	root := flag.String("registry", "", "existing independent offline registry root")
	action := flag.String("action", "inspect", "inspect|reserve|consistency (NON-POSTING only)")
	file := flag.String("manifest", "", "same schema=1 manifest as demo-close-cross-event-audit")
	ack := flag.String("ack", "", "explicit REVIEW_ONLY_NOT_SETTLEMENT for reserve")
	flag.Parse()
	if *root == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "BLOCKED: -registry <existing-root> required")
		os.Exit(1)
	}
	if err := run(context.Background(), *root, *action, *file, *ack, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
}
