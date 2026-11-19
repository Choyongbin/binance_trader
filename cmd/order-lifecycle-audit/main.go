// order-lifecycle-audit is a local READ-ONLY inspection tool for the durable
// order execution state machine. It never contacts Binance and never creates,
// advances, settles, deletes or authorizes an order.
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
		fmt.Fprintln(os.Stderr, "usage: order-lifecycle-audit -data <DATA_DIR>")
		os.Exit(2)
	}
	report, err := trading.ReviewExecutionLifecycles(*data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "BLOCKED:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
