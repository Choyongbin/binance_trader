// demo-signed-inspect is a READ-ONLY incident inspector for a single ambiguous
// Binance Futures Demo order. It NEVER submits, cancels, settles or reconciles
// lifecycle records. The report is evidence for HUMAN review, not authorization.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

const incidentClientID = "bt_06893a0c50b6928dede7365cd2f0"

type signedOrder struct {
	Lookup       string `json:"lookup"`
	Status       string `json:"status,omitempty"`
	OrderID      int64  `json:"order_id,omitempty"`
	Side         string `json:"side,omitempty"`
	PositionSide string `json:"position_side,omitempty"`
	Type         string `json:"type,omitempty"`
	OriginalQty  string `json:"original_qty,omitempty"`
	ExecutedQty  string `json:"executed_qty,omitempty"`
	AveragePrice string `json:"average_price,omitempty"`
	TradeCount   int    `json:"trade_count,omitempty"`
	TradeRead    string `json:"trade_read,omitempty"`
	Message      string `json:"message,omitempty"`
}

type position struct {
	PositionSide string `json:"position_side"`
	Qty          string `json:"qty"`
	EntryPrice   string `json:"entry_price"`
}
type report struct {
	CheckedAt            string      `json:"checked_at_utc"`
	Environment          string      `json:"environment"`
	ClientOrderID        string      `json:"client_order_id"`
	LocalAuditState      string      `json:"local_audit_state"`
	LocalStage           string      `json:"local_stage"`
	SignedOrder          signedOrder `json:"signed_order"`
	AccountRead          string      `json:"account_read"`
	OpenOrderCount       int         `json:"open_order_count"`
	OpenAlgoCount        int         `json:"open_algo_count"`
	OpenOrderClientIDs   []string    `json:"open_order_client_ids,omitempty"`
	Positions            []position  `json:"nonzero_btc_positions,omitempty"`
	Classification       string      `json:"classification"`
	Blockers             []string    `json:"blockers"`
	TradingAuthorized    bool        `json:"trading_authorized"`
	ManualReviewRequired bool        `json:"manual_review_required"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "READ-ONLY SIGNED REVIEW FAILED:", err)
		os.Exit(1)
	}
}
func run() error {
	root := flag.String("root", ".", "dev.3.55 project root")
	output := flag.String("out", "", "JSON report path under pc-results")
	flag.Parse()
	if *output == "" {
		return errors.New("-out report path required")
	}
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	dataPath := filepath.Join(absRoot, "data")
	if b, err := os.ReadFile(filepath.Join(absRoot, "VERSION")); err != nil || strings.TrimSpace(string(b)) != "v0.4.11-dev.3.56" {
		return errors.New("requires v0.4.11-dev.3.56; do not modify existing source")
	}
	// Do not race any running trading server or another inspector.
	if conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 300*time.Millisecond); err == nil {
		conn.Close()
		return errors.New("server port 8080 still listening: run stop.ps1")
	}
	audit, err := trading.ReviewExecutionLifecycles(dataPath)
	if err != nil {
		return fmt.Errorf("local evidence invalid: %w", err)
	}
	if err := verifyIncident(audit); err != nil {
		return err
	}
	r := report{CheckedAt: time.Now().UTC().Format(time.RFC3339Nano), Environment: "demo", ClientOrderID: incidentClientID,
		LocalAuditState: audit.State, LocalStage: string(audit.Items[0].Stage), AccountRead: "NOT_RUN",
		SignedOrder: signedOrder{Lookup: "NOT_RUN"}, Classification: "SIGNED_REVIEW_REQUIRED", TradingAuthorized: false, ManualReviewRequired: true,
		Blockers: []string{"MAY_HAVE_SENT remains unresolved; do not replay, cancel, settle, clear evidence, or start automatic trading from this report"},
	}
	creds, err := readCredentials(absRoot)
	if err != nil {
		return fmt.Errorf("cannot load Demo credentials (not shown): %w", err)
	}
	// All network calls below use exchange.NewDemoClient(), whose base URL is
	// hard-coded to the Demo host. The only methods invoked are signed GETs.
	client := exchange.NewDemoClient()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first, firstErr := client.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", incidentClientID)
	if firstErr == nil {
		r.SignedOrder = summarizeOrder(first)
		if first.Side != "SELL" || first.PositionSide != "SHORT" {
			r.Blockers = append(r.Blockers, "SIGNED ORDER SIDE MISMATCH (expected SELL/SHORT)")
		}
	} else if errors.Is(firstErr, exchange.ErrOrderNotFound) {
		r.SignedOrder.Lookup = "NOT_FOUND_UNPROVEN"
		r.Blockers = append(r.Blockers, "Binance returned order-not-found; this is NOT evidence that POST was never sent")
	} else {
		r.SignedOrder.Lookup = "ERROR"
		r.SignedOrder.Message = firstErr.Error() // errors sanitize Binance response bodies
		r.Blockers = append(r.Blockers, "signed order query failed")
	}
	snapshot, accountErr := client.Fetch(ctx, creds.APIKey, creds.Secret)
	if accountErr != nil {
		r.AccountRead = "ERROR"
		r.Blockers = append(r.Blockers, "signed account snapshot failed: "+accountErr.Error())
	} else {
		r.AccountRead = "OK"
		r.OpenOrderCount = len(snapshot.Orders)
		r.OpenAlgoCount = len(snapshot.AlgoOrders)
		for _, v := range snapshot.Orders {
			r.OpenOrderClientIDs = append(r.OpenOrderClientIDs, v.ClientOrderID)
		}
		for _, p := range snapshot.Positions {
			if p.Symbol == "BTCUSDT" {
				r.Positions = append(r.Positions, position{PositionSide: p.Side, Qty: p.Qty, EntryPrice: p.Entry})
			}
		}
		if len(r.Positions) > 0 {
			r.Blockers = append(r.Blockers, "nonzero BTCUSDT position detected; operator must review on Binance Demo")
		}
		if len(snapshot.Orders) > 0 || len(snapshot.AlgoOrders) > 0 {
			r.Blockers = append(r.Blockers, "open orders or protective algo orders exist; operator review required")
		}
	}
	if firstErr == nil {
		trades, tradeErr := client.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, "BTCUSDT", first.OrderID)
		if tradeErr != nil {
			r.SignedOrder.TradeRead = "ERROR"
			r.Blockers = append(r.Blockers, "signed userTrades read failed: "+tradeErr.Error())
		} else {
			r.SignedOrder.TradeRead = "OK"
			r.SignedOrder.TradeCount = len(trades)
		}
		second, secondErr := client.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", incidentClientID)
		if secondErr != nil {
			r.Blockers = append(r.Blockers, "second signed order query failed: "+secondErr.Error())
		} else if second.OrderID != first.OrderID || second.Side != first.Side || second.PositionSide != first.PositionSide || second.OriginalQty != first.OriginalQty {
			r.Blockers = append(r.Blockers, "inconsistent signed order identity between independent reads")
		} else {
			r.SignedOrder = summarizeOrder(second)
			r.SignedOrder.TradeRead = "OK"
			if tradeErr != nil {
				r.SignedOrder.TradeRead = "ERROR"
			} else {
				r.SignedOrder.TradeCount = len(trades)
			}
		}
	}
	// There is NO conditional path that changes r.TradingAuthorized from false.
	resultPath, err := filepath.Abs(*output)
	if err != nil {
		return err
	}
	dir := filepath.Join(absRoot, "pc-results")
	if filepath.Clean(filepath.Dir(resultPath)) != dir {
		return errors.New("report must be directly within project pc-results directory")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	// Exclusive create prevents accidental overwrite of earlier signed evidence.
	f, err := os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(append(raw, '\n')); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	fmt.Printf("READ-ONLY REVIEW saved: %s\nOrder lookup: %s\nAccount read: %s\nOrders sent: 0; trading authorization: false\n", resultPath, r.SignedOrder.Lookup, r.AccountRead)
	return nil
}
func verifyIncident(a trading.ExecutionLifecycleAudit) error {
	if a.TradingAuthorized || a.State != trading.ExecutionAuditPending || a.Total != 1 || a.Pending != 1 || a.Settled != 0 || len(a.Items) != 1 || a.Items[0].ClientOrderID != incidentClientID || a.Items[0].PositionSide != trading.PositionSide("SHORT") || a.Items[0].Action != trading.Action("ENTER") || a.Items[0].Stage != trading.ExecutionMayHaveSent {
		return errors.New("live incident evidence differs from uploaded audit: no requests made; preserve all data and contact reviewer")
	}
	return nil
}
func summarizeOrder(v exchange.OrderResult) signedOrder {
	return signedOrder{Lookup: "FOUND", Status: v.Status, OrderID: v.OrderID, Side: v.Side, PositionSide: v.PositionSide, Type: v.Type, OriginalQty: v.OriginalQty, ExecutedQty: v.ExecutedQty, AveragePrice: v.AveragePrice}
}

// The two-line plaintext .enc format is the same legacy test convention used
// by cmd/demo-auto-pc. Credentials are never printed or written to the report.
func readCredentials(root string) (trading.DemoCredentials, error) {
	matches, err := filepath.Glob(filepath.Join(root, "*.enc"))
	if err != nil || len(matches) != 1 {
		return trading.DemoCredentials{}, errors.New("exactly one *.enc expected under project root")
	}
	st, err := os.Lstat(matches[0])
	if err != nil || !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 || st.Size() < 10 || st.Size() > 2048 {
		return trading.DemoCredentials{}, errors.New("invalid credential file")
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		return trading.DemoCredentials{}, errors.New("cannot read credential file")
	}
	defer func() {
		for i := range data {
			data[i] = 0
		}
	}()
	var contents string
	if bytes.HasPrefix(data, []byte{0xff, 0xfe}) {
		if (len(data)-2)%2 != 0 {
			return trading.DemoCredentials{}, errors.New("malformed UTF-16LE file")
		}
		units := make([]uint16, (len(data)-2)/2)
		for i := range units {
			units[i] = binary.LittleEndian.Uint16(data[2+i*2:])
		}
		contents = string(utf16.Decode(units))
	} else {
		b := bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
		if !utf8.Valid(b) {
			return trading.DemoCredentials{}, errors.New("invalid UTF-8 credential file")
		}
		contents = string(b)
	}
	contents = strings.ReplaceAll(contents, "\r\n", "\n")
	lines := strings.Split(strings.TrimRight(contents, "\n"), "\n")
	if len(lines) != 2 || len(lines[0]) < 8 || len(lines[1]) < 8 || strings.ContainsAny(contents, "\x00\r") || lines[0] != strings.TrimSpace(lines[0]) || lines[1] != strings.TrimSpace(lines[1]) {
		return trading.DemoCredentials{}, errors.New("invalid two-line credential format")
	}
	return trading.DemoCredentials{APIKey: lines[0], Secret: lines[1]}, nil
}
