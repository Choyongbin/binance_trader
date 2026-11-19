package main

// OFFLINE NON-POSTED dual-ledger transaction preview. No credentials, signed
// REST calls, financial posting, source ledger modification or order routes.
import (
	"binance_trader/internal/trading"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	data := flag.String("data", "", "existing data directory")
	id := flag.String("client", "", "original entry clientOrderID")
	action := flag.String("action", "inspect", "inspect|prepare|verify")
	confirm := flag.String("confirm", "", "for sidecar writes: SIMULATE_ONLY_NO_SETTLEMENT")
	flag.Parse()
	if *data == "" || *id == "" || (*action != "inspect" && *action != "prepare" && *action != "verify") ||
		(*action == "inspect" && *confirm != "") ||
		(*action != "inspect" && *confirm != "SIMULATE_ONLY_NO_SETTLEMENT") {
		fmt.Fprintln(os.Stderr, "expected -data -client -action inspect|prepare|verify; writes require SIMULATE_ONLY_NO_SETTLEMENT")
		os.Exit(2)
	}
	store, err := trading.NewExternalCloseDualLedgerPlanStore(*data)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var result any
	switch *action {
	case "inspect":
		var root trading.ExternalCloseDualLedgerEnvelope
		var verified *trading.ExternalCloseDualLedgerEnvelope
		root, verified, err = store.Load(*id)
		result = struct {
			Prepared trading.ExternalCloseDualLedgerEnvelope  `json:"prepared"`
			Verified *trading.ExternalCloseDualLedgerEnvelope `json:"verified,omitempty"`
		}{root, verified}
	case "prepare":
		result, err = store.Prepare(*id)
	case "verify":
		result, err = store.Verify(*id)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
