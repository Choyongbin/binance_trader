// operator-status is a READ-ONLY local unified evidence summary for a stopped
// server or offline copy. It performs no Binance request and never grants
// trading permission.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"binance_trader/internal/trading"
)

func main() {
	data := flag.String("data", "", "stopped-server or offline-copy data directory")
	flag.Parse()
	if *data == "" || flag.NArg() != 0 {
		fail("usage: operator-status -data <DATA_DIR>", 2)
	}
	status, err := trading.AnalyzeOperatorUnifiedStatus(*data, time.Now().UTC())
	if err != nil {
		fail("unified operator evidence invalid, incomplete, or changing; no files altered and trading remains locked", 1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(status); err != nil {
		fail("operator status output unavailable", 1)
	}
}

func fail(message string, code int) {
	fmt.Fprintln(os.Stderr, "STOP:", message)
	os.Exit(code)
}
