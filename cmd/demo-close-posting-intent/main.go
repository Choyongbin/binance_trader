package main

// Offline forensic command: inspect or reserve a globally exclusive, immutable
// NON-POSTED transaction identity. This executable has no Binance keys,
// no exchange client, and no code path capable of modifying the trading ledger.
import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"binance_trader/internal/trading"
)

func main() {
	data := flag.String("data", "", "existing application data/ directory")
	id := flag.String("client", "", "existing entry clientOrderId")
	action := flag.String("action", "inspect", "inspect|reserve")
	confirm := flag.String("confirm", "", "required for reserve: RESERVE_ONLY_NOT_SETTLEMENT")
	flag.Parse()
	if *data == "" || *id == "" {
		fmt.Fprintln(os.Stderr, "-data and -client required")
		os.Exit(2)
	}
	if *action != "inspect" && *action != "reserve" {
		fmt.Fprintln(os.Stderr, "unknown action")
		os.Exit(2)
	}
	if *action == "reserve" && *confirm != "RESERVE_ONLY_NOT_SETTLEMENT" {
		fmt.Fprintln(os.Stderr, "explicit NON-POSTED confirmation required")
		os.Exit(2)
	}
	if *action == "inspect" && *confirm != "" {
		fmt.Fprintln(os.Stderr, "inspect does not accept confirmation")
		os.Exit(2)
	}
	s, err := trading.NewExternalClosePostingIntentStore(*data)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var receipt trading.ExternalClosePostingIntent
	if *action == "inspect" {
		receipt, err = s.Load(*id)
	} else {
		receipt, err = s.Reserve(*id)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(receipt); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
