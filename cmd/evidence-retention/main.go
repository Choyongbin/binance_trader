// evidence-retention compacts only historical diagnostic summaries into a
// sealed cold archive. It never touches execution/request/fill/protection
// source-of-truth evidence and never authorizes trading.
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
	data := flag.String("data", "", "data directory")
	keep := flag.Int("keep-recent", 20, "active files to keep per diagnostic category")
	apply := flag.Bool("apply", false, "seal cold archive and remove compacted diagnostic source files")
	flag.Parse()
	if *data == "" || *keep < 1 {
		fmt.Fprintln(os.Stderr, "usage: evidence-retention -data <DATA_DIR> [-keep-recent N] [-apply]")
		os.Exit(2)
	}
	if !*apply {
		fmt.Printf("DRY RUN ONLY: no files changed. data=%s keep_recent=%d\n", *data, *keep)
		fmt.Println("Only recovery_reviews, integrated_reviews and accounting_reports are eligible; execution/request/fill/protection evidence is never compacted.")
		return
	}
	out, err := trading.CompactDiagnosticArchives(*data, *keep, time.Now().UTC())
	if err != nil {
		fmt.Fprintln(os.Stderr, "retention failed:", err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}
