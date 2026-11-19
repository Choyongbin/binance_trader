// Offline dev.3.128 capture/restart REHEARSAL. Never performs a signed GET,
// exchange POST/DELETE, settlement or any changes to the trading data tree.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

type offlineSource struct {
	Schema         int                      `json:"schema"`
	ClientOrderID  string                   `json:"client_order_id"`
	IncidentSHA256 string                   `json:"incident_archive_sha256"`
	Scope          string                   `json:"scope"`
	Trades         []exchange.AccountTrade  `json:"offline_trade_rows"`
	Funding        []exchange.FundingIncome `json:"offline_funding_rows"`
}

func strictFile(path string, limit int64, dest any) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > limit {
		return errors.New("input must be a regular bounded JSON file")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, limit+1))
	d.DisallowUnknownFields()
	if err := d.Decode(dest); err != nil {
		return err
	}
	var next any
	if err := d.Decode(&next); err != io.EOF {
		return errors.New("unexpected JSON trailing content")
	}
	return nil
}
func sourceHash(v offlineSource) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func run(ctx context.Context, action, data, id, incidentPath, sourcePath, root string, w io.Writer) error {
	if action != "capture" && action != "inspect" {
		return errors.New("action must be capture or inspect")
	}
	if data == "" || id == "" || incidentPath == "" || sourcePath == "" || root == "" {
		return errors.New("-data -id -incident -source -checkpoint required")
	}
	var incident trading.PackageAIncidentArchive
	if err := strictFile(incidentPath, 8<<20, &incident); err != nil {
		return err
	}
	var source offlineSource
	if err := strictFile(sourcePath, 8<<20, &source); err != nil {
		return err
	}
	if source.Schema != 1 || source.ClientOrderID != id || source.IncidentSHA256 != incident.Checksum ||
		source.Scope != trading.ExternalSignedHistoryScope || source.Trades == nil || source.Funding == nil {
		return errors.New("offline mock source identity/rows invalid")
	}
	store, err := trading.NewExternalHistoryCaptureStore(root)
	if err != nil {
		return err
	}
	hash := sourceHash(source)
	var result trading.ExternalHistoryCaptureInspection
	if action == "inspect" {
		result, err = store.Inspect(ctx, data, id, incident, hash)
	} else {
		reader := func(ctx context.Context, from, to int64) ([]exchange.AccountTrade, []exchange.FundingIncome, error) {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			trades := make([]exchange.AccountTrade, 0)
			funds := make([]exchange.FundingIncome, 0)
			for _, row := range source.Trades {
				if row.TimeMS >= from && row.TimeMS <= to {
					trades = append(trades, row)
				}
			}
			for _, row := range source.Funding {
				if row.TimeMS >= from && row.TimeMS <= to {
					funds = append(funds, row)
				}
			}
			return trades, funds, nil
		}
		result, err = store.Capture(ctx, data, id, incident, hash, reader)
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// runBaseline is a strictly read-only scope/baseline review. "claim" emits
// the only permissible bounded expectation; "baseline" checks the separately
// saved expectation against the immutable checkpoint and original ledgers.
func runBaseline(ctx context.Context, action, data, id, incidentPath, sourcePath, root, claimPath string, w io.Writer) error {
	if action != "claim" && action != "baseline" {
		return errors.New("invalid baseline action")
	}
	if data == "" || id == "" || incidentPath == "" || sourcePath == "" || root == "" {
		return errors.New("-data -id -incident -source -checkpoint required")
	}
	if (action == "baseline") != (claimPath != "") {
		return errors.New("-claim required only with -action baseline")
	}
	var incident trading.PackageAIncidentArchive
	if err := strictFile(incidentPath, 8<<20, &incident); err != nil {
		return err
	}
	var source offlineSource
	if err := strictFile(sourcePath, 8<<20, &source); err != nil {
		return err
	}
	if source.Schema != 1 || source.ClientOrderID != id || source.IncidentSHA256 != incident.Checksum ||
		source.Scope != trading.ExternalSignedHistoryScope || source.Trades == nil || source.Funding == nil {
		return errors.New("invalid offline mock source identity")
	}
	hash := sourceHash(source)
	store, err := trading.NewExternalHistoryCaptureStore(root)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if action == "claim" {
		claim, err := trading.PrepareExternalHistoryBaselineClaim(id, incident, hash)
		if err != nil {
			return err
		}
		return encoder.Encode(claim)
	}
	var claim trading.ExternalHistoryBaselineClaim
	if err := strictFile(claimPath, 1<<20, &claim); err != nil {
		return err
	}
	report, err := trading.InspectExternalHistoryBaseline(ctx, store, data, id, incident, hash, claim)
	if err != nil {
		return err
	}
	return encoder.Encode(report)
}

func main() {
	action := flag.String("action", "inspect", "inspect|capture|claim|baseline; OFFLINE MOCK ONLY")
	data := flag.String("data", "", "original stopped trader data directory (read-only)")
	id := flag.String("id", "", "existing entry client order ID")
	incident := flag.String("incident", "", "Package A captured incident JSON")
	source := flag.String("source", "", "offline mock response rows JSON (NOT signed Binance proof)")
	checkpoint := flag.String("checkpoint", "", "existing separate offline checkpoint root")
	claim := flag.String("claim", "", "operator bounded baseline expectation JSON; required for baseline only")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "BLOCKED: unexpected positional arguments")
		os.Exit(1)
	}
	if *action == "claim" || *action == "baseline" {
		if err := runBaseline(context.Background(), *action, *data, *id, *incident, *source, *checkpoint, *claim, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "BLOCKED:", err)
			os.Exit(1)
		}
		return
	}
	if *claim != "" {
		fmt.Fprintln(os.Stderr, "BLOCKED: -claim requires -action baseline")
		os.Exit(1)
	}
	if err := run(context.Background(), *action, *data, *id, *incident, *source, *checkpoint, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
}
