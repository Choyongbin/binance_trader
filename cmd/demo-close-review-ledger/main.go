// demo-close-review-ledger stores HUMAN REVIEW RECEIPTS about previously saved
// signed audit previews. It does NOT contact Binance, authenticate a JSON file,
// import trades, mutate legacy lifecycle/ownership, or authorize settlement.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/trading"
	"binance_trader/internal/version"
)

const confirmation = "REVIEW_ONLY_NOT_SETTLEMENT"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "OPERATOR REVIEW DENIED:", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("root", ".", "installed project root")
	action := flag.String("action", "show", "stage | acknowledge | reject | show")
	report := flag.String("report", "", "saved signed-ledger-audit JSON in pc-results; stage only")
	clientID := flag.String("client-id", "", "existing entry clientOrderID")
	reviewer := flag.String("reviewer", "", "local operator label (NOT authenticated)")
	note := flag.String("note", "", "reviewer's text explanation")
	confirm := flag.String("confirm", "", "explicit REVIEW_ONLY_NOT_SETTLEMENT consent for writes")
	flag.Parse()
	abs, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(abs, "VERSION"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(b)) != version.Release {
		return fmt.Errorf("release mismatch; require %s", version.Release)
	}
	// This tool is offline even for reads, to avoid knowingly racing an active UI.
	if c, err := net.DialTimeout("tcp", "127.0.0.1:8080", 250*time.Millisecond); err == nil {
		c.Close()
		return errors.New("stop local trader (8080) before review")
	}
	ledger, err := trading.NewExternalCloseReviewLedger(filepath.Join(abs, "data"))
	if err != nil {
		return err
	}
	var state trading.ExternalCloseReviewState
	switch *action {
	case "show":
		state, err = ledger.Load(*clientID)
	case "stage":
		if *confirm != confirmation || *clientID == "" || *report == "" {
			return errors.New("stage requires client-id, pc-results report, explicit review-only confirmation")
		}
		reportPath, err := filepath.Abs(*report)
		if err != nil {
			return err
		}
		if filepath.Clean(filepath.Dir(reportPath)) != filepath.Join(abs, "pc-results") || !strings.HasPrefix(filepath.Base(reportPath), "signed-ledger-audit-") || filepath.Ext(reportPath) != ".json" {
			return errors.New("report must be a signed-ledger-audit-*.json directly under pc-results")
		}
		fi, err := os.Lstat(reportPath)
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() || fi.Size() > 8<<20 || fi.Mode()&os.ModeSymlink != 0 {
			return errors.New("unsafe signed audit report file")
		}
		f, err := os.Open(reportPath)
		if err != nil {
			return err
		}
		var audit trading.SignedLedgerHistoryReport
		decoder := json.NewDecoder(io.LimitReader(f, 8<<20))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&audit)
		if err == nil {
			if e := decoder.Decode(new(any)); !errors.Is(e, io.EOF) {
				err = errors.New("unexpected extra JSON")
			}
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if audit.Environment != "demo" || audit.TradingAuthorized || audit.LocalLifecycleChanged || len(audit.ExternalCloseRehearsals) != 1 || audit.ExternalCloseRehearsals[0].EntryClientOrderID != *clientID {
			return errors.New("report does not contain exactly one matching Demo review-only rehearsal")
		}
		// This is an untrusted report copy, so staging is human review ONLY.
		state, err = ledger.Stage(audit.ExternalCloseRehearsals[0], time.Now().UTC())
	case "acknowledge", "reject":
		if *confirm != confirmation {
			return errors.New("explicit review-only confirmation required")
		}
		stage := trading.ExternalReviewAcknowledged
		if *action == "reject" {
			stage = trading.ExternalReviewRejected
		}
		state, err = ledger.Decide(*clientID, stage, *reviewer, *note, time.Now().UTC())
	default:
		return errors.New("unknown action")
	}
	if err != nil {
		return err
	}
	out, err := json.MarshalIndent(struct {
		ClientOrderID        string `json:"client_order_id"`
		Stage                string `json:"stage"`
		EventCount           int    `json:"event_count"`
		ReviewOnly           bool   `json:"review_only"`
		SettlementAuthorized bool   `json:"settlement_authorized"`
		TradingAuthorized    bool   `json:"trading_authorized"`
		Warning              string `json:"warning"`
	}{state.Root.ClientOrderID, state.Stage, len(state.Events), true, false, false, "Saved report is NOT independently authenticated by this command; settlement and trading remain locked"}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
