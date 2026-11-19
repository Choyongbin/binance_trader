// operator-incident-packets prints sealed READ-ONLY forensic packets for every
// CRITICAL remediation blocker. It performs no Binance request and writes no
// file into the inspected data tree.
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
		fail("usage: operator-incident-packets -data <DATA_DIR>", 2)
	}
	packets, err := trading.BuildOperatorIncidentPackets(*data, time.Now().UTC())
	if err != nil {
		fail("incident packet evidence invalid, incomplete, or changing; no files altered and trading remains locked", 1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(packets); err != nil {
		fail("incident packet output unavailable", 1)
	}
}

func fail(message string, code int) { fmt.Fprintln(os.Stderr, "STOP:", message); os.Exit(code) }
