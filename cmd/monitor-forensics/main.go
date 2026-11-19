// monitor-forensics only inspects an explicitly supplied local data directory.
// Prefer a stopped-server copy: live writers cause a fail-closed scan error.
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
	data := flag.String("data", "", "data directory, preferably a stopped-server forensic copy")
	flag.Parse()
	if *data == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: monitor-forensics -data <offline-data-directory>")
		os.Exit(2)
	}
	r, err := trading.AnalyzeRestartForensics(*data, time.Now().UTC())
	if err != nil {
		// Never echo raw paths or potentially sensitive underlying errors.
		fmt.Fprintln(os.Stderr, "FAIL: local evidence inaccessible, inconsistent, or changed; leave trading locked")
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if enc.Encode(r) != nil {
		os.Exit(1)
	}
}
