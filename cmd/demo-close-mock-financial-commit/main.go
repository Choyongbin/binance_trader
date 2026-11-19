// Standalone OFFLINE multi-ledger commit-boundary SIMULATOR (dev.3.132).
// No Binance client, API keys, live posting, or production runtime imports.
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

func boundedJSON(path string, max int64, out any) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() < 2 || st.Size() > max {
		return errors.New("invalid bounded regular JSON source")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, max+1))
	d.DisallowUnknownFields()
	if err = d.Decode(out); err != nil {
		return err
	}
	var leftover any
	if d.Decode(&leftover) != io.EOF {
		return errors.New("extra trailing JSON content")
	}
	return nil
}

func readEvents(path string) ([]trading.ExternalFinancialEventSource, error) {
	var m manifest
	if err := boundedJSON(path, 1<<20, &m); err != nil {
		return nil, err
	}
	if m.Schema != 1 || len(m.Events) < 2 || len(m.Events) > 64 {
		return nil, errors.New("manifest requires schema=1 and 2..64 events")
	}
	result := make([]trading.ExternalFinancialEventSource, 0, len(m.Events))
	for _, e := range m.Events {
		if e.Data == "" || e.ID == "" || e.IncidentFile == "" {
			return nil, errors.New("event data/id/incident required")
		}
		var incident trading.PackageAIncidentArchive
		if err := boundedJSON(e.IncidentFile, 8<<20, &incident); err != nil {
			return nil, fmt.Errorf("%s: %w", e.ID, err)
		}
		result = append(result, trading.ExternalFinancialEventSource{DataDir: e.Data, ClientOrderID: e.ID, Incident: incident})
	}
	return result, nil
}

func run(ctx context.Context, root, registryRoot, manifestPath, action, step, ack string, expectedSteps int, expectedSHA256 string, out io.Writer) error {
	if root == "" || registryRoot == "" || manifestPath == "" {
		return errors.New("existing separate -root, -registry and -manifest required")
	}
	store, err := trading.NewExternalMockFinancialCommitStore(root)
	if err != nil {
		return err
	}
	registry, err := trading.NewExternalGlobalEvidenceReservationStore(registryRoot)
	if err != nil {
		return err
	}
	events, err := readEvents(manifestPath)
	if err != nil {
		return err
	}
	var report any
	switch action {
	case "inspect":
		if step != "" || ack != "" || expectedSteps != -1 || expectedSHA256 != "" {
			return errors.New("inspect is read-only; no step, ack or recovery pins")
		}
		report, err = store.Inspect(ctx, registry, events)
	case "recover":
		if step != "" || ack != "" || expectedSteps != -1 || expectedSHA256 != "" {
			return errors.New("recover is read-only; no step, ack or recovery pins")
		}
		report, err = store.InspectRecovery(ctx, registry, events)
	case "resume-next":
		if step != "" || ack != trading.ExternalMockCommitAcknowledgement || expectedSteps < 0 || expectedSHA256 == "" {
			return errors.New("resume-next requires no stage, explicit simulation-only ack, and pinned -expected-steps/-expected-sha256")
		}
		report, err = store.ResumeNext(ctx, registry, events, expectedSteps, expectedSHA256, ack)
	case "advance":
		if step == "" || ack != trading.ExternalMockCommitAcknowledgement || expectedSteps != -1 || expectedSHA256 != "" {
			return errors.New("advance requires explicit stage and SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT ack")
		}
		report, err = store.Advance(ctx, registry, events, step, ack)
	default:
		return errors.New("action must be inspect, advance, recover, or resume-next")
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func main() {
	root := flag.String("root", "", "pre-existing SEPARATE mock ledger directory")
	reg := flag.String("registry", "", "pre-existing global reservation directory")
	manifestFile := flag.String("manifest", "", "existing 2..64 event manifest")
	action := flag.String("action", "inspect", "inspect|advance|recover|resume-next")
	step := flag.String("step", "", "INTENT|LIFECYCLE|OWNERSHIP|FINANCIAL|COMMIT")
	ack := flag.String("ack", "", "SIMULATION_ONLY_NOT_FINANCIAL_SETTLEMENT (advance or resume-next only)")
	expectedSteps := flag.Int("expected-steps", -1, "pinned completed_steps from recover (resume-next only)")
	expectedSHA256 := flag.String("expected-sha256", "", "pinned consistency_sha256 from recover (resume-next only)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "BLOCKED: unexpected positional arguments")
		os.Exit(1)
	}
	if err := run(context.Background(), *root, *reg, *manifestFile, *action, *step, *ack, *expectedSteps, *expectedSHA256, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
}
