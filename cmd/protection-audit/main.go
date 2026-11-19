// protection-audit is a READ-ONLY local protection-plan review. It performs no
// Binance request and never grants trading permission.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"binance_trader/internal/trading"
)

func main() {
	data := flag.String("data", "", "stopped-server or offline-copy data directory")
	flag.Parse()
	if *data == "" || flag.NArg() != 0 {
		fail("usage: protection-audit -data <DATA_DIR>", 2)
	}
	review, err := trading.ReviewProtectionSubmissionDirectory(*data)
	if err != nil {
		fail("protection plan archive invalid or changed; no files altered and trading remains locked", 1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(review); err != nil {
		fail("protection audit output unavailable", 1)
	}
}

func fail(message string, code int) {
	fmt.Fprintln(os.Stderr, "STOP:", message)
	os.Exit(code)
}
