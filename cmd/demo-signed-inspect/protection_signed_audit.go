package main

// Standalone operator-triggered diagnostic, deliberately unconnected to live
// order submission/cancel routines. Read-only signed Binance Demo GETs only.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"binance_trader/internal/trading"
	"binance_trader/internal/version"
)

var protectionClientPattern = regexp.MustCompile(`^bt_[0-9a-f]{28}$`)

type pcProtectionReport struct {
	Schema      int                              `json:"schema"`
	Release     string                           `json:"release"`
	Environment string                           `json:"environment"`
	Mode        string                           `json:"mode"`
	WindowHours int                              `json:"window_hours"`
	Review      trading.ProtectionSignedPCReview `json:"review"`
	SHA256      string                           `json:"sha256"`
}

func (r pcProtectionReport) checksum() (string, error) {
	r.SHA256 = ""
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

// Only the read-only operation is injectable for deterministic offline tests.
// The caller's credential loader and local ledger inspection stay unchanged.
type protectionSignedReader interface{ trading.WindowDemoReader }

func runSignedProtectionAudit(root, output, clientID string, hours int, reader protectionSignedReader) error {
	if reader == nil || hours < 1 || hours > 120 || !protectionClientPattern.MatchString(clientID) {
		return errors.New("invalid protection review arguments; no signed GET")
	}
	target, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "pc-results")
	if filepath.Clean(filepath.Dir(target)) != filepath.Clean(dir) || filepath.Ext(target) != ".json" {
		return errors.New("-out must be a .json file directly within pc-results")
	}
	info, err := os.Lstat(dir)
	if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("unsafe output directory")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	// Reserve a unique output *before* contacting Binance. Failed calls are
	// also recorded; a repeated manual review uses a new report filename.
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	creds, credErr := readCredentials(root)
	checked := time.Now().UTC()
	fromMS := checked.Add(-time.Duration(hours) * time.Hour).UnixMilli()
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	review, gateErr := trading.ReviewPreparedProtectionSignedPC(ctx, filepath.Join(root, "data"), clientID, reader, creds, fromMS)
	if credErr != nil {
		review.ErrorCode = "CREDENTIALS_UNAVAILABLE"
		review.Gate.State = trading.ProtectionGateBlocked
		review.Gate.Reasons = []string{"CREDENTIALS_UNAVAILABLE"}
		gateErr = errors.New("Demo credentials missing or invalid")
	}
	// An integrity checksum only detects accidental changes; it is not an
	// exchange signature, notarization, or approval to execute a trade.
	report := pcProtectionReport{Schema: 1, Release: version.Release, Environment: "binance_futures_demo", Mode: "SIGNED_GET_ONLY_NO_ORDER", WindowHours: hours, Review: review}
	report.SHA256, err = report.checksum()
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if _, err = f.Write(append(raw, '\n')); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	fmt.Printf("SIGNED TP/SL READ-ONLY REVIEW: %s\nstate=%s; signed_get_attempted=%v; trading_authorized=false\n", target, review.Gate.State, review.SignedGETAttempted)
	if gateErr != nil {
		// Deliberately do not print raw remote HTTP errors, credentials or
		// response bodies, while preserving a BLOCKED report for manual triage.
		return fmt.Errorf("review blocked (%s); inspect JSON; no orders sent", review.ErrorCode)
	}
	return nil
}
