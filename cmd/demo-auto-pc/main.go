// demo-auto-pc is a deliberately supervised, Demo-only PC test harness.
// It NEVER unlocks the UI/server routes and will not submit an order unless
// the operator passes explicit opt-in switches. The single-order mode is NOT
// a replacement for TP/SL protection and MUST NOT be left unattended.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	"binance_trader/internal/trading"
	"binance_trader/internal/wallet"
)

const maxAssignedUSDT = 60 // leverage 15x * 10% allocation => <= 90 USDT notional, before buffers

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "STOP (fail closed):", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("root", ".", "project folder (contains data/ and .enc)")
	mode := flag.String("mode", "market", "market (read-only) or demo-single (at most one automatic Demo order)")
	confirmation := flag.String("confirm", "", "demo-single only: exact value DEMO_ONE_ORDER_NO_TPSL")
	duration := flag.Duration("duration", 3*time.Minute, "demo-single maximum watch duration (up to 10m)")
	flag.Parse()
	if *mode != "market" && *mode != "demo-single" {
		return errors.New("mode must be market or demo-single")
	}
	if *mode == "demo-single" && (*confirmation != "DEMO_ONE_ORDER_NO_TPSL" || *duration < 30*time.Second || *duration > 10*time.Minute) {
		return errors.New("demo-single requires explicit confirmation and duration 30s..10m")
	}
	// A separate stopped application is important: don't allow two runtimes
	// to compete with the HTTP server over dataDir or the demo account.
	if c, err := net.DialTimeout("tcp", "127.0.0.1:8080", 250*time.Millisecond); err == nil {
		_ = c.Close()
		return errors.New("PC trader server appears to be running on 127.0.0.1:8080; stop.ps1 first")
	}
	absolute, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(absolute, "VERSION")); err != nil {
		return errors.New("project VERSION not found")
	}
	dataDir := filepath.Join(absolute, "data")
	stream := exchange.NewDemoStream(exchange.NewDemoMarketService())
	stream.Start()
	defer stream.Close()
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	fmt.Println("Binance Futures Demo BTCUSDT market WS; trading routes unchanged (HTTP 423)")
	if err := waitForMarket(ctx, stream, 70*time.Second); err != nil {
		return err
	}
	if *mode == "market" {
		fmt.Println("PASS: market ready, no API keys read, NO ORDERS")
		return nil
	}
	// All order modes are local, explicitly opted-in, Demo only. There is
	// deliberately no flag to select or construct a Real/Mainnet client.
	file, err := findCredentialFile(absolute)
	if err != nil {
		return err
	}
	creds, err := readTwoLineCredentials(file)
	if err != nil {
		return err
	}
	// Never log the file, key, secret or their hash.
	signedClient := exchange.NewDemoClient()
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	signed, err := signedClient.FetchDemoExecutionPreflight(checkCtx, creds.APIKey, creds.Secret, "BTCUSDT")
	cancel()
	if err != nil {
		return errors.New("Demo signed account preflight failed; NO ORDER")
	}
	if err := checkFlatDemo(signed); err != nil {
		return err
	}
	budget, err := wallet.Open(dataDir)
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256([]byte(creds.APIKey))
	allocation, err := budget.SnapshotFor(hex.EncodeToString(fingerprint[:]))
	if err != nil || !allocation.Verified {
		return errors.New("Demo wallet account binding not verified")
	}
	if err := enforceBudgetCap(allocation.AssignedUSDT); err != nil {
		return err
	}
	// Reject an impossible test before running a model for minutes: with 10%
	// margin at 15x, the absolute optimistic maximum is only 1.5x the
	// assigned wallet (fees/price buffer make the actual maximum smaller).
	// Some Binance BTCUSDT rules require a larger minimum order notional.
	ruleCtx, ruleCancel := context.WithTimeout(ctx, 12*time.Second)
	rules, rulesErr := signedClient.FetchSymbolRules(ruleCtx, "BTCUSDT")
	ruleCancel()
	if rulesErr != nil {
		return errors.New("cannot verify signed Demo BTCUSDT minimum notional; NO ORDER")
	}
	if err := checkNotionalFeasibility(allocation.AssignedUSDT, rules.MinNotional); err != nil {
		return err
	}
	control, err := trading.OpenAutoRunControl(dataDir)
	if err != nil {
		return err
	}
	status, err := control.Status()
	if err != nil || status.State != trading.AutoRunStopped {
		return errors.New("auto run control is not STOPPED (restart block preserved)")
	}
	audit, err := trading.ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return fmt.Errorf("execution lifecycle audit failed; NO ORDER: %w", err)
	}
	if err := requireCleanExecutionAudit(audit); err != nil {
		return err
	}
	fmt.Printf("Execution lifecycle audit PASS: state=%s total=%d pending=%d (NOT trading authorization)\n", audit.State, audit.Total, audit.Pending)
	// Do not claim a pending TP/SL is managed by this test harness.
	unsettled, _, err := trading.ReviewProtectionReadiness(dataDir)
	if err != nil || unsettled != 0 {
		return errors.New("open protection lifecycle must be reconciled first")
	}
	fmt.Println("Signed Demo flat-account check PASS; strict wallet cap PASS; entering supervised mode")
	fmt.Println("WARNING: This harness DOES NOT attach TP/SL. A filled order may leave a Demo position OPEN.")
	fmt.Println("Observe Demo account; manually review/close positions before repeating any run.")
	credentials := func(ctx context.Context) (trading.DemoCredentials, error) {
		if err := ctx.Err(); err != nil {
			return trading.DemoCredentials{}, err
		}
		return creds, nil
	}
	market := func(ctx context.Context) (exchange.LiveMarketSnapshot, error) {
		if err := ctx.Err(); err != nil {
			return exchange.LiveMarketSnapshot{}, err
		}
		return stream.Get()
	}
	controller, err := trading.NewAutoController(control, func(lease trading.AutoRunLease) (trading.AutoRuntime, error) {
		actual, err := trading.NewDemoAutoExecutionRuntime(dataDir, model.TestStrategyID, signedClient, credentials, market, control, lease)
		if err != nil {
			return nil, err
		}
		return &singleDispatch{inner: actual, signedRecovery: func(ctx context.Context, orderID string) (trading.ExecutionRecoveryResult, error) {
			return actual.ReconcileDispatched(ctx, orderID, signedClient, creds)
		}}, nil
	}, 2*time.Second)
	if err != nil {
		return err
	}
	if err := controller.Start(); err != nil {
		return fmt.Errorf("Demo controller start blocked: %w", err)
	}
	defer func() { _ = controller.Stop() }()
	watch := time.NewTimer(*duration)
	defer watch.Stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.New("operator interrupted session; signed order/position review required")
		case <-watch.C:
			fmt.Println("Duration elapsed; controller STOPPED; signed position review required")
			return nil
		case <-tick.C:
			st := controller.Status()
			if !st.Running {
				fmt.Printf("Controller stopped: reason=%s dispatched=%t lifecycle=%s clientOrderID=%s signedReviewRequired=%t\n", st.StopReason, st.Dispatched, st.Lifecycle, st.LastOrderID, st.SignedReviewRequired)
				if st.StopReason == "SINGLE_ORDER_DISPATCHED_SIGNED_REVIEW_REQUIRED" && !st.FailClosed {
					fmt.Println("DEMO_ORDER_SUBMITTED_REVIEW_REQUIRED: executor dispatched at most one order; signed account/position review is mandatory; NO TP/SL")
					return nil
				}
				if st.FailClosed {
					return fmt.Errorf("controller safety stop (signed review=%t, dispatched=%t, lifecycle=%s): %s", st.SignedReviewRequired, st.Dispatched, st.Lifecycle, st.LastError)
				}
				return nil
			}
			if st.LastRunAt == "" {
				fmt.Println("Demo model evaluation in progress; no completed decision or dispatch result yet")
				continue
			}
			fmt.Printf("Last completed Demo decision: %s; dispatched: %t; lifecycle: %s\n", st.LastDecision, st.Dispatched, st.Lifecycle)
		}
	}
}

// requireCleanExecutionAudit permits a genuinely empty execution ledger or a
// completely settled one. The shared TradingReadiness gate already supports
// both states. Any pending, inconsistent, or unrecognized state remains locked.
// It does not grant trading permission; signed account and runtime gates still
// run independently before any order side effect.
func requireCleanExecutionAudit(a trading.ExecutionLifecycleAudit) error {
	if a.TradingAuthorized || a.Pending != 0 || a.Total != len(a.Items) || a.Settled > a.Total || a.Settled < 0 || a.Total < 0 {
		return errors.New("execution lifecycle inconsistent/pending: signed reconciliation required; NO ORDER")
	}
	switch a.State {
	case trading.ExecutionAuditEmpty:
		if a.Total == 0 && a.Settled == 0 {
			return nil
		}
	case trading.ExecutionAuditClear:
		if a.Total > 0 && a.Total == a.Settled {
			for _, item := range a.Items {
				if item.Stage != trading.ExecutionSettled {
					return errors.New("execution lifecycle not fully settled: signed reconciliation required; NO ORDER")
				}
			}
			return nil
		}
	}
	return fmt.Errorf("execution lifecycle state=%s total=%d settled=%d pending=%d: signed reconciliation required; NO ORDER", a.State, a.Total, a.Settled, a.Pending)
}

// singleDispatch makes any second order impossible within this supervised
// session even when the next 1m candle also has a strong entry signal.
type singleDispatch struct {
	inner trading.AutoRuntime
	seen  bool
	// Signed recovery runs after a confirmed executor submission. It performs
	// no new orders; incomplete evidence stops the controller fail-closed.
	signedRecovery func(context.Context, string) (trading.ExecutionRecoveryResult, error)
}

func (s *singleDispatch) RunOnce(ctx context.Context) (trading.AutoExecutionResult, error) {
	if s.seen {
		return trading.AutoExecutionResult{}, errors.New("one-order test cap reached")
	}
	result, err := s.inner.RunOnce(ctx)
	if result.ClientOrderID != "" || result.Dispatched {
		s.seen = true
	}
	if err != nil {
		return result, err
	}
	if result.Dispatched {
		if result.ClientOrderID == "" {
			return result, errors.New("dispatched result missing clientOrderID: signed review required")
		}
		if s.signedRecovery == nil {
			return result, errors.New("signed recovery not configured; order may have been submitted")
		}
		recovered, recoverErr := s.signedRecovery(ctx, result.ClientOrderID)
		if recovered.FinalStage != "" {
			result.Lifecycle = recovered.FinalStage
		}
		if recoverErr != nil {
			return result, fmt.Errorf("Demo order dispatched; signed lifecycle recovery blocked (clientOrderID=%s, stage=%s): %w", result.ClientOrderID, recovered.FinalStage, recoverErr)
		}
		result.Lifecycle = recovered.FinalStage
		if recovered.FinalStage != trading.ExecutionSettled || recovered.PendingReason != "" {
			return result, fmt.Errorf("Demo order dispatched; signed lifecycle still pending (stage=%s, reason=%s); NO REPLAY", recovered.FinalStage, recovered.PendingReason)
		}
		return result, trading.ErrSupervisedSingleOrderComplete
	}
	if result.ClientOrderID != "" {
		return result, errors.New("reserved order without trusted dispatch outcome; signed review required, NO REPLAY")
	}
	return result, nil
}
func (s *singleDispatch) Close() error { return s.inner.Close() }

func waitForMarket(ctx context.Context, s *exchange.StreamService, limit time.Duration) error {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last uint64
	var progress int
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			st := s.Status()
			return fmt.Errorf("market not ready (%s, endpoint=%s, reconnects=%d, last_error=%s)", st.State, st.Endpoint, st.ReconnectCount, st.LastError)
		case <-tick.C:
			st := s.Status()
			if st.Ready && st.MessageCount > last && st.State == "live" {
				progress++
			} else if !st.Ready {
				progress = 0
			}
			last = st.MessageCount
			if progress >= 3 {
				return nil
			}
			if st.ReconnectCount > 0 || st.LastError != "" {
				fmt.Printf("market=%s messages=%d reconnects=%d endpoint=%s reason=%s\n", st.State, st.MessageCount, st.ReconnectCount, st.Endpoint, st.LastError)
			}
		}
	}
}

func findCredentialFile(root string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(root, "*.enc"))
	if err != nil || len(matches) != 1 {
		return "", errors.New("exactly one *.enc (two-line key/secret) is required under project root")
	}
	return matches[0], nil
}
func readTwoLineCredentials(path string) (trading.DemoCredentials, error) {
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Size() < 10 || fi.Size() > 2048 {
		return trading.DemoCredentials{}, errors.New("invalid .enc file type/size")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return trading.DemoCredentials{}, errors.New("cannot read .enc file")
	}
	defer func() {
		for i := range data {
			data[i] = 0
		}
	}()
	var normalized string
	if bytes.HasPrefix(data, []byte{0xff, 0xfe}) {
		// Windows PowerShell 5.1 Set-Content historically writes UTF-16LE.
		// Decode without treating NUL bytes as part of the credentials.
		if (len(data)-2)%2 != 0 {
			return trading.DemoCredentials{}, errors.New("malformed UTF-16LE credential file")
		}
		units := make([]uint16, (len(data)-2)/2)
		for i := range units {
			units[i] = binary.LittleEndian.Uint16(data[2+i*2:])
		}
		normalized = string(utf16.Decode(units))
	} else {
		text := bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
		if !utf8.Valid(text) {
			return trading.DemoCredentials{}, errors.New("credential text encoding must be UTF-8 or UTF-16LE")
		}
		normalized = string(text)
	}
	normalized = strings.ReplaceAll(normalized, "\r\n", "\n")
	lines := strings.Split(strings.TrimRight(normalized, "\n"), "\n")
	if len(lines) != 2 || len(lines[0]) < 8 || len(lines[1]) < 8 || strings.ContainsAny(normalized, "\x00\r") || lines[0] != strings.TrimSpace(lines[0]) || lines[1] != strings.TrimSpace(lines[1]) {
		return trading.DemoCredentials{}, errors.New(".enc must contain exactly two lines: API key then secret (never printed)")
	}
	return trading.DemoCredentials{APIKey: lines[0], Secret: lines[1]}, nil
}
func checkFlatDemo(s exchange.ExecutionPreflightSnapshot) error {
	if s.Environment != "demo" || !s.Portfolio.CanTrade || !s.Configuration.SettingsMatchV1 || !s.Configuration.HedgeMode || s.Configuration.MarginType != "ISOLATED" || s.Configuration.Leverage != 15 || s.Configuration.AutoAddMargin || s.FinishedAt.IsZero() || time.Since(s.FinishedAt) > 5*time.Second || s.Portfolio.Assets == nil || s.Portfolio.Positions == nil || s.OpenOrders == nil || s.OpenAlgos == nil {
		return errors.New("Demo signed capabilities, freshness, or portfolio completeness NOT verified")
	}
	if len(s.OpenOrders) > 0 || len(s.OpenAlgos) > 0 {
		return errors.New("other orders/algo orders exist: NO ORDER")
	}
	for _, p := range s.Portfolio.Positions {
		q, ok := new(big.Rat).SetString(string(p.PositionAmt))
		if !ok || q.Sign() != 0 {
			return errors.New("account has a position or invalid position quantity: NO ORDER")
		}
	}
	return nil
}
func enforceBudgetCap(amount string) error {
	q, err := wallet.Decimal(amount, false)
	if err != nil || q.Cmp(big.NewRat(maxAssignedUSDT, 1)) > 0 {
		return errors.New("Demo allocation must be >0 and <=60 USDT for this one-order test")
	}
	return nil
}

// This optimistic feasibility test is not permission to order. It only
// detects the guaranteed-impossible case under the immutable sizing policy.
func checkNotionalFeasibility(assigned, minNotional string) error {
	walletMargin, err := wallet.Decimal(assigned, false)
	if err != nil {
		return errors.New("invalid assigned wallet; NO ORDER")
	}
	minimum, err := wallet.Decimal(minNotional, false)
	if err != nil {
		return errors.New("invalid exchange minimum notional; NO ORDER")
	}
	theoreticalMaximum := new(big.Rat).Mul(walletMargin, big.NewRat(3, 2)) // 10% x 15x
	if minimum.Cmp(theoreticalMaximum) > 0 {
		return fmt.Errorf("BTCUSDT Demo minimum notional %s USDT exceeds the strict %.0f-USDT wallet test cap's theoretical maximum %s USDT: no order can pass; NO ORDER", minNotional, float64(maxAssignedUSDT), theoreticalMaximum.FloatString(2))
	}
	return nil
}
