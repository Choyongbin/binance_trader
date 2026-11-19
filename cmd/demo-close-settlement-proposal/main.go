// Offline, unauthenticated operator-side POSTING PROPOSAL ONLY. No signed GET,
// no Binance orders and no mutation of original accounting/lifecycle journals.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/trading"
	"binance_trader/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "EXTERNAL CLOSE PROPOSAL DENIED:", err)
		os.Exit(1)
	}
}
func run() error {
	root := flag.String("root", ".", "installed project root")
	action := flag.String("action", "show", "show | prepare")
	clientID := flag.String("client-id", "", "existing entry clientOrderID")
	confirm := flag.String("confirm", "", "explicit PROPOSAL_ONLY_NOT_SETTLEMENT acknowledgment")
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
		return errors.New("release mismatch")
	}
	if conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 250*time.Millisecond); err == nil {
		conn.Close()
		return errors.New("stop trader server before offline review")
	}
	s, err := trading.NewExternalCloseSettlementProposalStore(filepath.Join(abs, "data"))
	if err != nil {
		return err
	}
	var proposal trading.ExternalCloseSettlementProposal
	switch *action {
	case "show":
		proposal, err = s.Load(*clientID)
	case "prepare":
		if *confirm != "PROPOSAL_ONLY_NOT_SETTLEMENT" {
			return errors.New("prepare requires explicit proposal-only confirmation")
		}
		proposal, err = s.Prepare(*clientID, time.Now().UTC())
	default:
		return errors.New("unknown action")
	}
	if err != nil {
		return err
	}
	out, err := json.MarshalIndent(proposal, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
