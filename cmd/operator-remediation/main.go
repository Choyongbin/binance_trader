// operator-remediation prints a READ-ONLY ordered recovery guide derived from
// immutable local evidence. It performs no Binance request and no repair.
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
		fail("usage: operator-remediation -data <DATA_DIR>", 2)
	}
	plan, err := trading.AnalyzeOperatorRemediation(*data, time.Now().UTC())
	if err != nil {
		fail("operator remediation evidence invalid, incomplete, or changing; no files altered and trading remains locked", 1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(plan); err != nil {
		fail("operator remediation output unavailable", 1)
	}
}
func fail(message string, code int) { fmt.Fprintln(os.Stderr, "STOP:", message); os.Exit(code) }
