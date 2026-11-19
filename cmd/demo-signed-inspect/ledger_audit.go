package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

// runLedgerAudit is a separate read-only mode of the legacy incident inspector.
// It does not assume the original incident is the only local order. All
// credentials remain in memory; report is exclusively created under pc-results.
func runLedgerAudit(root, output string) error {
	target, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "pc-results")
	if filepath.Dir(target) != dir {
		return errors.New("-out must be a direct child of pc-results")
	}
	if filepath.Ext(target) != ".json" {
		return errors.New("-out requires .json")
	}
	if fi, err := os.Lstat(dir); err == nil && (!fi.IsDir() || fi.Mode()&os.ModeSymlink != 0) {
		return errors.New("unsafe output directory")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	creds, err := readCredentials(root)
	if err != nil {
		return fmt.Errorf("invalid local Demo credential file: %w", err)
	}
	// Production uses a hard-coded, Demo-only signed GET adapter. Never pass
	// user-defined URLs or enable any mutation method in this tool.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	result := trading.ReviewSignedLedgerHistory(ctx, filepath.Join(root, "data"), exchange.NewDemoClient(), creds, time.Now().UTC())
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	fmt.Printf("READ-ONLY LEDGER AUDIT: %s\nState: %s; orders: %d; unattributed trades: %d; external close cases: %d; Trading authorized: false\n", target, result.State, len(result.Orders), len(result.UnattributedTrades), len(result.ExternalCloseCases))
	return nil
}
