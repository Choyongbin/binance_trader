// pooled-exit-audit is a local READ-ONLY reservation/fence inspection tool.
// It never contacts Binance and never creates, settles or deletes a trade.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"binance_trader/internal/trading"
)

func main() {
	data := flag.String("data", "", "offline data directory to inspect")
	flag.Parse()
	if *data == "" {
		fmt.Fprintln(os.Stderr, "usage: pooled-exit-audit -data <DATA_DIR>")
		os.Exit(2)
	}
	r, err := trading.ReviewPooledExitSerialization(*data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
