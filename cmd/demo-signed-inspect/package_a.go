package main

// Package A integrated signed preflight: exact fee/quantity/realized-PnL
// comparison plus source-ledger drift and bounded funding reconciliation.
// All outputs and append-only receipts are NOT_POSTED; this is not settlement.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

type packageAResult struct {
	Schema            int                             `json:"schema"`
	Stage             string                          `json:"stage"`
	Receipt           *trading.PackageAPreflight      `json:"receipt,omitempty"`
	EntryGap          *trading.PackageAEntryGapReport `json:"entry_gap,omitempty"`
	ExitDiscovery     *exitDiscoveryResult            `json:"exit_discovery,omitempty"`
	ArchiveSHA256     string                          `json:"archive_sha256,omitempty"`
	PostingAuthorized bool                            `json:"posting_authorized"`
	TradingAuthorized bool                            `json:"trading_authorized"`
}

func runSignedPackageA(root, out, action, id, confirm string, exitOrderID int64) error {
	if action != "probe" && action != "entry-probe" && action != "entry-archive" && action != "verify-entry-archive" && action != "incident-archive" && action != "verify-incident-archive" && action != "exit-discovery" && action != "discovery-archive" && action != "verify-discovery-archive" && action != "diagnose-discovery-archive" && action != "plan-entry-recovery" && action != "recheck-entry-recovery" && action != "stage" && action != "recheck" {
		return errors.New("Package A action must be entry-probe|entry-archive|verify-entry-archive|incident-archive|verify-incident-archive|exit-discovery|discovery-archive|verify-discovery-archive|diagnose-discovery-archive|plan-entry-recovery|recheck-entry-recovery|probe|stage|recheck")
	}
	if action != "probe" && action != "entry-probe" && action != "entry-archive" && action != "verify-entry-archive" && action != "incident-archive" && action != "verify-incident-archive" && action != "exit-discovery" && action != "discovery-archive" && action != "verify-discovery-archive" && action != "diagnose-discovery-archive" && action != "plan-entry-recovery" && action != "recheck-entry-recovery" && confirm != "PACKAGE_A_NO_POSTING" {
		return errors.New("explicit non-posting consent required")
	}
	if (action == "probe" || action == "entry-probe" || action == "entry-archive" || action == "verify-entry-archive" || action == "incident-archive" || action == "verify-incident-archive" || action == "exit-discovery" || action == "discovery-archive" || action == "verify-discovery-archive" || action == "diagnose-discovery-archive" || action == "plan-entry-recovery" || action == "recheck-entry-recovery") && confirm != "" {
		return errors.New("no consent token expected for read-only probe")
	}
	abs, e := filepath.Abs(out)
	if e != nil {
		return e
	}
	if filepath.Dir(abs) != filepath.Join(root, "pc-results") || filepath.Ext(abs) != ".json" {
		return errors.New("report must be a JSON file directly in pc-results")
	}
	dir := filepath.Dir(abs)
	if fi, err := os.Lstat(dir); err == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return errors.New("unsafe results directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if (action == "incident-archive" && exitOrderID <= 0) || (action != "incident-archive" && exitOrderID != 0) {
		return errors.New("external exit order ID must be positive and supplied ONLY for incident-archive")
	}
	if action == "verify-entry-archive" || action == "verify-incident-archive" || action == "verify-discovery-archive" || action == "diagnose-discovery-archive" || action == "plan-entry-recovery" || action == "recheck-entry-recovery" {
		// id is a file basename, not an arbitrary path. The archive must be a
		// regular, non-symlink file within the operator's results directory.
		if filepath.Base(id) != id || id == "." || id == ".." || filepath.Ext(id) != ".json" {
			return errors.New("archive filename must be a JSON basename in pc-results")
		}
		path := filepath.Join(root, "pc-results", id)
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() || fi.Size() > 16<<20 {
			return errors.New("archive must be a regular bounded file")
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(f, 16<<20+1))
		f.Close()
		if err != nil || len(raw) > 16<<20 {
			return errors.New("archive file exceeds size limit or cannot be read")
		}
		if action == "verify-discovery-archive" || action == "diagnose-discovery-archive" {
			var b packageADiscoveryArchive
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&b); err != nil {
				return err
			}
			var trailing interface{}
			if err := decoder.Decode(&trailing); err != io.EOF {
				return errors.New("archive JSON has trailing content")
			}
			if err := verifyPackageADiscoveryArchive(context.Background(), filepath.Join(root, "data"), b); err != nil {
				return err
			}
			if action == "diagnose-discovery-archive" {
				triage, err := verifyThenDiagnoseDiscoveryArchive(context.Background(), filepath.Join(root, "data"), b)
				if err != nil {
					return err
				}
				return writePackageAJSON(out, dir, triage)
			}
			return savePackageAResult(out, dir, packageAResult{Schema: 1, Stage: "DISCOVERY_ARCHIVE_INTEGRITY_ONLY_NOT_FRESH_SIGNED", ArchiveSHA256: b.Checksum})
		}
		if action == "verify-incident-archive" {
			var archive trading.PackageAIncidentArchive
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&archive); err != nil {
				return err
			}
			var trailing interface{}
			if err := decoder.Decode(&trailing); err != io.EOF {
				return errors.New("archive JSON has trailing content")
			}
			if err := trading.VerifyPackageAIncidentArchive(context.Background(), filepath.Join(root, "data"), archive); err != nil {
				return err
			}
			return savePackageAResult(out, dir, packageAResult{Schema: 1, Stage: "INCIDENT_ARCHIVE_INTEGRITY_ONLY_NOT_FRESH_SIGNED", ArchiveSHA256: archive.Checksum})
		}
		var archive trading.PackageAEntryArchive
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&archive); err != nil {
			return err
		}
		var trailing interface{}
		if err := decoder.Decode(&trailing); err != io.EOF {
			return errors.New("archive JSON contains unexpected trailing content")
		}
		if err := trading.VerifyPackageAEntryArchive(context.Background(), filepath.Join(root, "data"), archive); err != nil {
			return err
		}
		if action == "plan-entry-recovery" {
			plan, err := trading.PlanPackageAEntryRecovery(context.Background(), filepath.Join(root, "data"), archive)
			if err != nil {
				return err
			}
			if err := trading.VerifyPackageAEntryRecoveryPlan(context.Background(), filepath.Join(root, "data"), archive, plan); err != nil {
				return err
			}
			return writePackageAJSON(out, dir, plan)
		}
		if action == "recheck-entry-recovery" {
			// Unlike the offline archive verification, this action makes
			// ONLY signed GETs. It never calls the order or ledger writer.
			plan, err := trading.PlanPackageAEntryRecovery(context.Background(), filepath.Join(root, "data"), archive)
			if err != nil {
				return err
			}
			creds, err := readCredentials(root)
			if err != nil {
				return errors.New("local Demo credentials unavailable or invalid")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
			defer cancel()
			report, err := trading.RecheckPackageAEntryRecovery(ctx, filepath.Join(root, "data"), archive, plan, exchange.NewDemoClient(), trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret}, time.Now().UTC())
			if err != nil {
				return err
			}
			return writePackageAJSON(out, dir, report)
		}
		return savePackageAResult(out, dir, packageAResult{Schema: 1, Stage: "ARCHIVE_INTEGRITY_ONLY_NOT_FRESH_SIGNED", ArchiveSHA256: archive.Checksum})
	}
	creds, err := readCredentials(root)
	if err != nil {
		return errors.New("local Demo credential file unavailable or invalid")
	}
	reader := trading.DemoSignedCloseReader{Client: exchange.NewDemoClient(), Key: creds.APIKey, Secret: creds.Secret, Now: time.Now}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	dataDir := filepath.Join(root, "data")
	var receipt trading.PackageAPreflight
	now := time.Now().UTC()
	if action == "discovery-archive" {
		b, e := capturePackageADiscoveryArchive(ctx, dataDir, id, reader.Client, reader.Client, reader, trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret}, now)
		if e != nil {
			return e
		}
		if e := writePackageAJSON(out, dir, b); e != nil {
			return e
		}
		fmt.Printf("PACKAGE A DISCOVERY+INCIDENT ARCHIVED (OPERATOR REVIEW ONLY; NO SETTLEMENT): %s\n", out)
		return nil
	}
	if action == "exit-discovery" {
		entry, e := trading.CapturePackageAEntryArchive(ctx, dataDir, id, reader.Client, trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret}, now)
		if e != nil {
			return e
		}
		result := discoverExitOrder(ctx, reader.Client, trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret}, entry, now)
		if e := trading.VerifyPackageAEntryArchive(ctx, dataDir, entry); e != nil {
			return fmt.Errorf("original journal changed during exit discovery: %w", e)
		}
		return savePackageAResult(out, dir, packageAResult{Schema: 1, Stage: "EXIT_ORDER_DISCOVERY_REVIEW_ONLY", ExitDiscovery: &result})
	}
	if action == "incident-archive" {
		incident, e := trading.CapturePackageAIncidentArchive(ctx, dataDir, id, exitOrderID, reader.Client, reader, trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret}, now)
		if e != nil {
			return e
		}
		if e := writePackageAJSON(out, dir, incident); e != nil {
			return e
		}
		fmt.Printf("PACKAGE A INCIDENT ARCHIVE saved (READ ONLY, NOT SETTLED): %s\n", out)
		return nil
	}
	if action == "entry-archive" {
		archive, e := trading.CapturePackageAEntryArchive(ctx, dataDir, id, reader.Client, trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret}, now)
		if e != nil {
			return e
		}
		if e := writePackageAJSON(out, dir, archive); e != nil {
			return e
		}
		fmt.Printf("PACKAGE A ENTRY ARCHIVE saved (READ ONLY, NOT SETTLED): %s\n", out)
		return nil
	}
	if action == "entry-probe" {
		gap, e := trading.AuditPackageAEntryGap(ctx, dataDir, id, reader.Client, trading.DemoCredentials{APIKey: creds.APIKey, Secret: creds.Secret})
		if e != nil {
			return e
		}
		return savePackageAResult(out, dir, packageAResult{Schema: 1, Stage: "PACKAGE_A_ENTRY_GAP_REVIEW_ONLY", EntryGap: &gap})
	}
	if action == "probe" {
		receipt, err = trading.PreparePackageAPreflight(ctx, dataDir, id, reader, now)
	} else {
		store, e := trading.NewPackageAPreflightStore(dataDir)
		if e != nil {
			return e
		}
		if action == "stage" {
			receipt, err = store.Stage(ctx, id, reader, now)
		} else {
			receipt, err = store.Recheck(ctx, id, reader, now)
		}
	}
	if err != nil {
		return err
	}
	result := packageAResult{Schema: 1, Stage: "PACKAGE_A_EVIDENCE_STILL_BLOCKED", Receipt: &receipt}
	return savePackageAResult(out, dir, result)
}
func savePackageAResult(out, dir string, result packageAResult) error {
	if err := writePackageAJSON(out, dir, result); err != nil {
		return err
	}
	fmt.Printf("PACKAGE A NON-POSTED audit: %s; signed GET or offline archive check; trading locked\n", out)
	return nil
}
func writePackageAJSON(out, dir string, result interface{}) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if len(b) > 16<<20 {
		return errors.New("Package A evidence exceeds 16 MiB limit")
	}
	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return nil
}
