package trading

// This inspector compares immutable local ownership fills with independent
// Demo signed order / userTrades GETs and bounded complete account history.
// It is FORENSIC ONLY: it never creates/repairs local records, accepts orders,
// assigns external trades to models, or authorizes trading.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"time"

	"binance_trader/internal/exchange"
)

const signedLedgerMaxHistory = 14 * 24 * time.Hour
const maxLedgerSignedOrders = 40

type SignedLedgerReader interface {
	DemoTradeReader
	FetchDemoTradeWindow(context.Context, string, string, string, int64, int64) ([]exchange.AccountTrade, error)
	QueryDemoOrderByID(context.Context, string, string, string, int64) (exchange.OrderResult, error)
	Fetch(context.Context, string, string) (exchange.Snapshot, error)
}

type SignedLedgerOrder struct {
	ClientOrderID     string       `json:"client_order_id"`
	ModelID           string       `json:"model_id,omitempty"`
	PositionSide      PositionSide `json:"position_side"`
	Action            Action       `json:"action"`
	SignedOrderID     int64        `json:"signed_order_id,omitempty"`
	Status            string       `json:"signed_status,omitempty"`
	SignedTradeCount  int          `json:"signed_trade_count"`
	LocalTradeCount   int          `json:"local_trade_count"`
	SignedQty         string       `json:"signed_quantity,omitempty"`
	LocalQty          string       `json:"local_quantity,omitempty"`
	ExactFillsMatched bool         `json:"exact_fills_matched"`
	Blockers          []string     `json:"blockers"`
}

type SignedExternalOrder struct {
	OrderID               int64    `json:"order_id"`
	Side                  string   `json:"side,omitempty"`
	PositionSide          string   `json:"position_side,omitempty"`
	Status                string   `json:"status,omitempty"`
	TradeCount            int      `json:"trade_count"`
	SignedQuantity        string   `json:"signed_quantity,omitempty"`
	VerifiedOrderIdentity bool     `json:"verified_order_identity"`
	Blockers              []string `json:"blockers"`
}

type SignedUnattributedTrade struct {
	TradeID      int64  `json:"trade_id"`
	OrderID      int64  `json:"order_id"`
	Side         string `json:"side"`
	PositionSide string `json:"position_side"`
	Quantity     string `json:"quantity"`
	TimeMS       int64  `json:"time_ms"`
}

type SignedLedgerHistoryReport struct {
	State                   string                      `json:"state"`
	CheckedAtUTC            string                      `json:"checked_at_utc"`
	Environment             string                      `json:"environment"`
	Orders                  []SignedLedgerOrder         `json:"orders"`
	UnattributedTrades      []SignedUnattributedTrade   `json:"unattributed_trades"`
	ExternalOrders          []SignedExternalOrder       `json:"external_orders"`
	ExternalCloseCases      []ExternalCloseRecoveryCase `json:"external_close_recovery_cases"`
	ExternalCloseRehearsals []ExternalCloseRehearsal    `json:"external_close_rehearsals"`
	HistoryStartUTC         string                      `json:"history_start_utc,omitempty"`
	HistoryEndUTC           string                      `json:"history_end_utc,omitempty"`
	HistoryWindowComplete   bool                        `json:"history_window_complete"`
	LocalFillsMatchSigned   bool                        `json:"local_fills_match_signed"`
	SignedAccountFresh      bool                        `json:"signed_account_fresh"`
	LifecycleState          string                      `json:"lifecycle_state,omitempty"`
	LifecyclePending        int                         `json:"lifecycle_pending"`
	AccountSnapshotState    string                      `json:"account_snapshot_state,omitempty"`
	Blockers                []string                    `json:"blockers"`
	TradingAuthorized       bool                        `json:"trading_authorized"`      // ALWAYS false
	LocalLifecycleChanged   bool                        `json:"local_lifecycle_changed"` // ALWAYS false
}

// ReviewSignedLedgerHistory requires a STOPPED, offline local data directory.
// The caller owns credentials and MUST construct a Demo-only, signed GET reader.
// A window only proves completeness within its bounded interval, NEVER earlier
// historical account provenance (or funding and other instruments).
func ReviewSignedLedgerHistory(ctx context.Context, dataDir string, reader SignedLedgerReader, creds DemoCredentials, now time.Time) (out SignedLedgerHistoryReport) {
	out = SignedLedgerHistoryReport{State: "BLOCKED_REVIEW_REQUIRED", CheckedAtUTC: now.UTC().Format(time.RFC3339Nano), Environment: "demo", Orders: []SignedLedgerOrder{}, UnattributedTrades: []SignedUnattributedTrade{}, ExternalOrders: []SignedExternalOrder{}, ExternalCloseCases: []ExternalCloseRecoveryCase{}, ExternalCloseRehearsals: []ExternalCloseRehearsal{}, Blockers: []string{"독립 signed 조회 결과는 정산/소유권 확정 또는 거래 허가가 아닙니다. 미해결 증거와 주문 잠금을 유지하세요."}}
	hasIssue := false
	block := func(s string) { hasIssue = true; out.Blockers = append(out.Blockers, s) }
	if reader == nil || creds.APIKey == "" || creds.Secret == "" || dataDir == "" || now.IsZero() {
		block("signed Demo reader, credentials, directory or time unavailable")
		return out
	}
	if err := ctx.Err(); err != nil {
		block("context canceled")
		return out
	}
	for _, rel := range []string{"", "trading", "trading/requests", "trading/fills", "trading/reservations", "trading/execution_lifecycle"} {
		p := filepath.Join(dataDir, filepath.FromSlash(rel))
		fi, err := os.Lstat(p)
		if errors.Is(err, os.ErrNotExist) && rel == "trading/execution_lifecycle" {
			continue
		}
		if err != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			block("unsafe or absent evidence path: " + rel)
			return out
		}
	}
	for _, rel := range []string{"trading/coordinator.active", "trading/mutation_gate.active"} {
		_, err := os.Lstat(filepath.Join(dataDir, filepath.FromSlash(rel)))
		if err == nil {
			block("active mutation/coordinator lock: " + rel)
			return out
		}
		if !errors.Is(err, os.ErrNotExist) {
			block("cannot inspect lock: " + rel)
			return out
		}
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		block("pre-audit evidence snapshot invalid")
		return out
	}
	defer func() {
		after, e := snapshotExecutionEvidence(dataDir)
		if e != nil || before != after {
			block("local evidence changed during signed audit; report invalid")
			out.State = "BLOCKED_REVIEW_REQUIRED"
			out.LocalFillsMatchSigned = false
			out.HistoryWindowComplete = false
			for i := range out.ExternalCloseRehearsals {
				out.ExternalCloseRehearsals[i].Status = ExternalCloseRehearsalBlocked
				out.ExternalCloseRehearsals[i].Blockers = append(out.ExternalCloseRehearsals[i].Blockers, "local evidence changed during signed audit; preview invalid")
				out.ExternalCloseRehearsals[i].LocalEvidenceSHA256 = ""
				out.ExternalCloseRehearsals[i].SignedReadDigestSHA256 = ""
				out.ExternalCloseRehearsals[i].PreviewSHA256 = fingerprintRehearsal(out.ExternalCloseRehearsals[i])
			}
			for i := range out.ExternalCloseCases {
				out.ExternalCloseCases[i].Status = ExternalCloseAmbiguous
				out.ExternalCloseCases[i].Blockers = append(out.ExternalCloseCases[i].Blockers, "local evidence changed during signed audit; candidate invalid")
			}
		}
	}()
	lc, e := ReviewExecutionLifecycles(dataDir)
	if e != nil {
		block("local lifecycle audit invalid: " + e.Error())
		return out
	}
	out.LifecycleState, out.LifecyclePending = lc.State, lc.Pending
	if lc.State != ExecutionAuditClear && lc.State != ExecutionAuditEmpty {
		block("lifecycle state not clear: " + lc.State)
	}
	if lc.Pending != 0 {
		block(fmt.Sprintf("pending lifecycle=%d (never settle automatically)", lc.Pending))
	}
	store := &OwnershipStore{journalDir: filepath.Join(dataDir, "trading", "requests"), fillDir: filepath.Join(dataDir, "trading", "fills")}
	records, e := store.ScanJournal()
	if e != nil {
		block("local journal invalid: " + e.Error())
		return out
	}
	fills, e := store.readFills()
	if e != nil {
		block("local fills invalid: " + e.Error())
		return out
	}
	// Preserve local protection evidence integrity. We do not claim protection
	// trade coverage under this standard-order history inspection.
	if _, e = readValidatedProtectionFillsForStore(filepath.Join(dataDir, "trading"), fills); e != nil {
		block("local protection evidence invalid: " + e.Error())
		return out
	}
	if len(records) > maxLedgerSignedOrders {
		block("owned signed order count exceeds bounded audit limit")
		return out
	}
	owned := make(map[string][]exchange.AccountTrade, len(records))
	recordIDs := make(map[string]bool, len(records))
	for _, r := range records {
		recordIDs[r.Plan.ClientOrderID] = true
	}
	for _, f := range fills {
		if !recordIDs[f.ClientOrderID] {
			block("orphan local fill: " + f.ClientOrderID)
			return out
		}
		owned[f.ClientOrderID] = append(owned[f.ClientOrderID], f.Trade)
	}
	for _, rel := range []string{"trading/reservations"} {
		co := &Coordinator{reserveDir: filepath.Join(dataDir, filepath.FromSlash(rel))}
		reservations, scanErr := co.scan()
		if scanErr != nil {
			block("reservation evidence invalid: " + scanErr.Error())
			return out
		}
		for _, r := range reservations {
			if !co.isDone(r) {
				block("unsettled reservation: " + r.ClientOrderID)
			}
		}
	}
	if len(records) == 0 {
		block("no owned order baseline; cannot attest account history")
	}
	remoteTrades := map[int64]exchange.AccountTrade{}
	externalByOrder := map[int64][]exchange.AccountTrade{}
	earliestMS := int64(0)
	allMatch := len(records) > 0
	for _, rec := range records {
		entry := SignedLedgerOrder{ClientOrderID: rec.Plan.ClientOrderID, ModelID: rec.Intent.ModelID, PositionSide: rec.Plan.PositionSide, Action: rec.Intent.Action, Blockers: []string{}}
		fail := func(reason string) { entry.Blockers = append(entry.Blockers, reason); allMatch = false }
		local := owned[rec.Plan.ClientOrderID]
		entry.LocalTradeCount = len(local)
		localSum := new(big.Rat)
		localByID := map[int64]exchange.AccountTrade{}
		for _, t := range local {
			qty, qe := positiveRat(t.Quantity)
			if qe != nil || t.TradeID <= 0 || localByID[t.TradeID].TradeID != 0 {
				fail("invalid/duplicate locally recorded fill")
				continue
			}
			localByID[t.TradeID] = t
			localSum.Add(localSum, qty)
		}
		entry.LocalQty = formatOwnershipDecimal(localSum)
		first, e := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", rec.Plan.ClientOrderID)
		if e != nil {
			fail("first signed order GET failed: " + e.Error())
			out.Orders = append(out.Orders, entry)
			continue
		}
		entry.SignedOrderID, entry.Status = first.OrderID, first.Status
		want, e := verifyRemoteOrder(rec, first)
		if e != nil {
			fail("signed order does not match immutable plan: " + e.Error())
			out.Orders = append(out.Orders, entry)
			continue
		}
		trades, e := reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, "BTCUSDT", first.OrderID)
		if e != nil {
			fail("signed per-order fills incomplete: " + e.Error())
			out.Orders = append(out.Orders, entry)
			continue
		}
		second, e := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, "BTCUSDT", rec.Plan.ClientOrderID)
		if e != nil || second.OrderID != first.OrderID || second.Status != first.Status || second.ExecutedQty != first.ExecutedQty || second.OriginalQty != first.OriginalQty || second.Side != first.Side || second.PositionSide != first.PositionSide || second.Type != first.Type || second.ClientOrderID != first.ClientOrderID {
			fail("second signed order identity/status/quantity drift")
			out.Orders = append(out.Orders, entry)
			continue
		}
		sum := new(big.Rat)
		seen := map[int64]bool{}
		matched := true
		for _, t := range trades {
			if err := validateFill(rec, t); err != nil || t.OrderID != first.OrderID || seen[t.TradeID] {
				matched = false
				fail("signed fill identity/quantity invalid or duplicate")
				break
			}
			seen[t.TradeID] = true
			qty, _ := positiveRat(t.Quantity)
			sum.Add(sum, qty)
			if t.TimeMS > now.Add(time.Second).UnixMilli() {
				matched = false
				fail("future signed fill timestamp")
				break
			}
			if v, ok := remoteTrades[t.TradeID]; ok && v.OrderID != t.OrderID {
				matched = false
				fail("signed trade ID reused across owned orders")
				break
			}
			remoteTrades[t.TradeID] = t
			if earliestMS == 0 || t.TimeMS < earliestMS {
				earliestMS = t.TimeMS
			}
			f, ok := localByID[t.TradeID]
			if !ok || !equalSignedFill(f, t) {
				matched = false
			}
		}
		if sum.Cmp(want) != 0 {
			fail("signed fill quantity does not equal executedQty")
			matched = false
		}
		if len(trades) != len(local) || sum.Cmp(localSum) != 0 || !matched {
			fail("signed fills do not match immutable local fill records exactly")
		}
		entry.SignedQty = formatOwnershipDecimal(sum)
		entry.SignedTradeCount = len(trades)
		entry.ExactFillsMatched = len(entry.Blockers) == 0
		out.Orders = append(out.Orders, entry)
	}
	out.LocalFillsMatchSigned = allMatch
	// Empty signed fill lists and unknown order identities NEVER establish a
	// complete historical baseline. A bounded explicit interval is essential.
	if earliestMS == 0 {
		block("no verified signed trade timestamp to establish bounded history baseline")
	} else {
		start := time.UnixMilli(earliestMS).UTC().Add(-time.Minute)
		if start.After(now) || now.Sub(start) > signedLedgerMaxHistory {
			block("signed history older than supported 14-day review horizon")
		} else {
			out.HistoryStartUTC = start.Format(time.RFC3339Nano)
			out.HistoryEndUTC = now.UTC().Format(time.RFC3339Nano)
			seenHistory := map[int64]bool{}
			complete := true
			for from := start.UnixMilli(); from <= now.UnixMilli(); {
				if ctx.Err() != nil {
					block("signed history canceled")
					complete = false
					break
				}
				to := from + int64((24*time.Hour)/time.Millisecond) - 1
				if to > now.UnixMilli() {
					to = now.UnixMilli()
				}
				events, err := reader.FetchDemoTradeWindow(ctx, creds.APIKey, creds.Secret, "BTCUSDT", from, to)
				if err != nil {
					block("signed account history window incomplete: " + err.Error())
					complete = false
					break
				}
				for _, t := range events {
					if seenHistory[t.TradeID] || t.TradeID <= 0 || t.TimeMS < from || t.TimeMS > to || t.OrderID <= 0 || t.Symbol != "BTCUSDT" {
						block("overlapping/invalid signed history row")
						complete = false
						break
					}
					seenHistory[t.TradeID] = true
					if own, ok := remoteTrades[t.TradeID]; ok {
						if !equalSignedFill(own, t) {
							block("signed history contradicts per-order signed fill")
							complete = false
						}
					} else {
						externalByOrder[t.OrderID] = append(externalByOrder[t.OrderID], t)
						out.UnattributedTrades = append(out.UnattributedTrades, SignedUnattributedTrade{TradeID: t.TradeID, OrderID: t.OrderID, Side: t.Side, PositionSide: t.PositionSide, Quantity: t.Quantity, TimeMS: t.TimeMS})
					}
				}
				if !complete {
					break
				}
				if to == now.UnixMilli() {
					break
				}
				from = to + 1
			}
			if complete {
				for id, t := range remoteTrades {
					if !seenHistory[id] && t.TimeMS >= start.UnixMilli() {
						block(fmt.Sprintf("owned signed trade %d absent from bounded account history", id))
						complete = false
					}
				}
			}
			out.HistoryWindowComplete = complete
		}
	}
	// External account trades are NOT model-owned. Independently verify
	// their signed order ID/status and quantity; never infer close ownership.
	externalIDs := make([]int64, 0, len(externalByOrder))
	for id := range externalByOrder {
		externalIDs = append(externalIDs, id)
	}
	sort.Slice(externalIDs, func(i, j int) bool { return externalIDs[i] < externalIDs[j] })
	if len(externalIDs) > maxLedgerSignedOrders {
		block("external signed order count exceeds bounded review limit")
	} else if out.HistoryWindowComplete {
		for _, id := range externalIDs {
			trades := externalByOrder[id]
			e := SignedExternalOrder{OrderID: id, TradeCount: len(trades), Blockers: []string{}}
			fail := func(reason string) {
				e.Blockers = append(e.Blockers, reason)
				block("external order unverifiable: " + reason)
			}
			first, err := reader.QueryDemoOrderByID(ctx, creds.APIKey, creds.Secret, "BTCUSDT", id)
			if err != nil {
				fail("signed orderId first GET failed: " + err.Error())
				out.ExternalOrders = append(out.ExternalOrders, e)
				continue
			}
			e.Side, e.PositionSide, e.Status = first.Side, first.PositionSide, first.Status
			qty := new(big.Rat)
			for _, t := range trades {
				q, qerr := positiveRat(t.Quantity)
				if qerr != nil || t.Side != first.Side || t.PositionSide != first.PositionSide || t.OrderID != id {
					fail("external fills side/quantity mismatch")
					break
				}
				qty.Add(qty, q)
			}
			e.SignedQuantity = formatOwnershipDecimal(qty)
			executed, exErr := decimalRat(first.ExecutedQty, true)
			if exErr != nil || first.OrderID != id || first.Symbol != "BTCUSDT" || (first.Side != "BUY" && first.Side != "SELL") || (first.PositionSide != "LONG" && first.PositionSide != "SHORT") || qty.Cmp(executed) != 0 || first.Status != "FILLED" {
				fail("external signed order/fill identity or executed quantity mismatch")
			}
			second, err := reader.QueryDemoOrderByID(ctx, creds.APIKey, creds.Secret, "BTCUSDT", id)
			if err != nil || second.OrderID != first.OrderID || second.Symbol != first.Symbol || second.Status != first.Status || second.ExecutedQty != first.ExecutedQty || second.Side != first.Side || second.PositionSide != first.PositionSide || second.OriginalQty != first.OriginalQty || second.Type != first.Type {
				fail("external signed order changed between independent reads")
			}
			e.VerifiedOrderIdentity = len(e.Blockers) == 0
			out.ExternalOrders = append(out.ExternalOrders, e)
		}
	}
	sort.Slice(out.UnattributedTrades, func(i, j int) bool { return out.UnattributedTrades[i].TradeID < out.UnattributedTrades[j].TradeID })
	if len(out.UnattributedTrades) > 0 {
		block(fmt.Sprintf("%d unowned/external BTCUSDT trades found; DO NOT assign to a model", len(out.UnattributedTrades)))
	}
	snapshot, e := reader.Fetch(ctx, creds.APIKey, creds.Secret)
	if e != nil {
		block("last signed account GET failed: " + e.Error())
	} else {
		evidence := InspectMultiModelEvidence(dataDir, snapshot, time.Now().UTC())
		out.SignedAccountFresh = evidence.SignedAccountFresh
		out.AccountSnapshotState = evidence.State
		if !evidence.LocalSnapshotConsistent {
			block("current signed account and local ownership still not consistent")
		}
		out.ExternalCloseCases = buildExternalCloseCases(out, records, remoteTrades, externalByOrder, snapshot, now)
		out.ExternalCloseRehearsals = buildExternalCloseRehearsals(out, before, remoteTrades, externalByOrder, snapshot)
	}
	if !out.LocalFillsMatchSigned {
		block("local fill/remote order audit incomplete or contradictory")
	}
	if !out.HistoryWindowComplete {
		block("full bounded account history not proven")
	}
	// REVIEW_ONLY even with zero blockers: funding, complete earlier history,
	// protection and virtual owner accounting are outside this tool's scope.
	out.State = "SIGNED_LEDGER_FORENSICS_REVIEW_ONLY"
	if hasIssue || !out.HistoryWindowComplete || !out.LocalFillsMatchSigned || !out.SignedAccountFresh || len(out.UnattributedTrades) > 0 || out.LifecyclePending > 0 {
		out.State = "BLOCKED_REVIEW_REQUIRED"
	}
	return out
}

func equalSignedFill(a, b exchange.AccountTrade) bool {
	if a.Symbol != b.Symbol || a.TradeID != b.TradeID || a.OrderID != b.OrderID || a.Side != b.Side || a.PositionSide != b.PositionSide || a.TimeMS != b.TimeMS || a.CommissionAsset != b.CommissionAsset {
		return false
	}
	for _, pair := range [][2]string{{a.Price, b.Price}, {a.Quantity, b.Quantity}, {a.Commission, b.Commission}, {a.RealizedPnL, b.RealizedPnL}} {
		x, ex := signedRat(pair[0])
		y, ey := signedRat(pair[1])
		if ex != nil || ey != nil || x.Cmp(y) != 0 {
			return false
		}
	}
	return true
}
