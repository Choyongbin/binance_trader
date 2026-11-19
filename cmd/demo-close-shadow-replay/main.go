package main

// Offline simulated close accounting only. The command intentionally has no
// exchange credentials, order submission or real settlement API.
import (
	"binance_trader/internal/trading"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	data := flag.String("data", "", "existing application data/ directory")
	id := flag.String("client", "", "entry clientOrderId")
	action := flag.String("action", "inspect", "inspect|prepare|replay")
	confirm := flag.String("confirm", "", "for writes: SIMULATE_ONLY_NO_SETTLEMENT")
	flag.Parse()
	if *data == "" || *id == "" {
		fmt.Fprintln(os.Stderr, "-data and -client required")
		os.Exit(2)
	}
	if (*action != "inspect" && *action != "prepare" && *action != "replay") ||
		(*action == "inspect" && *confirm != "") ||
		(*action != "inspect" && *confirm != "SIMULATE_ONLY_NO_SETTLEMENT") {
		fmt.Fprintln(os.Stderr, "invalid action or missing NON-POSTED simulation confirmation")
		os.Exit(2)
	}
	s, err := trading.NewExternalCloseShadowLedger(*data)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var result any
	switch *action {
	case "inspect":
		var root trading.ExternalCloseShadowRecord
		var replay *trading.ExternalCloseShadowRecord
		root, replay, err = s.Load(*id)
		result = struct {
			Prepared trading.ExternalCloseShadowRecord  `json:"prepared"`
			Replayed *trading.ExternalCloseShadowRecord `json:"replayed,omitempty"`
		}{root, replay}
	case "prepare":
		result, err = s.Prepare(*id)
	case "replay":
		result, err = s.Replay(*id)
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
