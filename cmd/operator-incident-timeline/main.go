package main

import (
	"binance_trader/internal/trading"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	data := flag.String("data", "", "stopped-server or offline-copy data directory")
	flag.Parse()
	if *data == "" || flag.NArg() != 0 {
		fail("usage: operator-incident-timeline -data <DATA_DIR>", 2)
	}
	v, err := trading.BuildOperatorIncidentTimeline(*data, time.Now().UTC())
	if err != nil {
		fail("incident timeline evidence invalid, incomplete, or changing; no files altered and trading remains locked", 1)
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	if e.Encode(v) != nil {
		fail("incident timeline output unavailable", 1)
	}
}
func fail(m string, c int) { fmt.Fprintln(os.Stderr, "STOP:", m); os.Exit(c) }
