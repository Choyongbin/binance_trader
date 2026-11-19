// incident-review records a two-phase human acknowledgement of an OFFLINE
// forensic COPY. It is NOT a stale-lock cleaner or a trading authorization.
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
	action := flag.String("action", "", "prepare, acknowledge, or verify")
	data := flag.String("data", "", "stopped-server OFFLINE data copy (never original live data)")
	reviews := flag.String("reviews", "", "PREEXISTING external review record directory; NOT inside data")
	id := flag.String("id", "", "previously prepared review plan id")
	challenge := flag.String("challenge", "", "exact confirmation from the locally generated plan")
	stopped := flag.Bool("stopped-writers", false, "operator self-attests no writers use the original or offline copy")
	backup := flag.Bool("external-backup", false, "operator self-attests a separately verified full original-data backup exists")
	flag.Parse()
	if *reviews == "" || flag.NArg() != 0 {
		fail("usage: -action prepare|acknowledge|verify -reviews <EXTERNAL_DIR> [ -data <OFFLINE_COPY> ... ]", 2)
	}
	var output any
	var err error
	switch *action {
	case "prepare":
		if *data == "" || *id != "" || *challenge != "" || *stopped || *backup {
			fail("prepare requires only offline data copy and external review dir", 2)
		}
		output, err = trading.PrepareIncidentReview(*data, *reviews, time.Now().UTC())
	case "acknowledge":
		if *data == "" || *id == "" || *challenge == "" || !*stopped || !*backup {
			fail("acknowledge requires offline data, plan id, challenge and both self-attestations", 2)
		}
		output, err = trading.AcknowledgeIncidentReview(*data, *reviews, *id, *challenge, *stopped, *backup, time.Now().UTC())
	case "verify":
		if *data != "" || *id != "" || *challenge != "" || *stopped || *backup {
			fail("verify only accepts external review dir", 2)
		}
		var plans []trading.IncidentReviewPlan
		var receipts []trading.IncidentReviewReceipt
		plans, receipts, err = trading.ReadIncidentReviews(*reviews)
		output = struct {
			Schema                   int  `json:"schema"`
			VerifiedPlans            int  `json:"verified_plans"`
			VerifiedAcknowledgements int  `json:"verified_acknowledgements"`
			TradingAuthorized        bool `json:"trading_authorized"`
			AutoRearmAllowed         bool `json:"auto_rearm_allowed"`
			StreamContinuityProven   bool `json:"stream_continuity_proven"`
			ManualReviewRequired     bool `json:"manual_review_required"`
		}{1, len(plans), len(receipts), false, false, false, true}
	default:
		fail("action must be prepare, acknowledge, or verify", 2)
	}
	if err != nil {
		fail("offline evidence/review invalid or changed; no locks altered and trading remains locked", 1)
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	if e.Encode(output) != nil {
		fail("review output unavailable", 1)
	}
}
func fail(message string, code int) { fmt.Fprintln(os.Stderr, "STOP:", message); os.Exit(code) }
