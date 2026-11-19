package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type protectionSettlementMock struct {
	orders        map[string]exchange.OrderResult
	trades        map[int64][]exchange.AccountTrade
	algos         map[int64]exchange.DemoAlgoResult
	snapshot      exchange.Snapshot
	fetches       int
	algoCalls     int
	mutateID      int64
	changeAfter   int
	changeID      int64
	changedResult exchange.DemoAlgoResult
}

func (m *protectionSettlementMock) QueryDemoOrder(_ context.Context, _, _, _, cid string) (exchange.OrderResult, error) {
	o, ok := m.orders[cid]
	if !ok {
		return exchange.OrderResult{}, errors.New("missing order")
	}
	return o, nil
}
func (m *protectionSettlementMock) FetchDemoOrderTrades(_ context.Context, _, _, _ string, id int64) ([]exchange.AccountTrade, error) {
	out, ok := m.trades[id]
	if !ok {
		return []exchange.AccountTrade{}, nil
	}
	return append([]exchange.AccountTrade(nil), out...), nil
}
func (m *protectionSettlementMock) QueryDemoAlgo(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	m.algoCalls++
	o, ok := m.algos[id]
	if !ok {
		return exchange.DemoAlgoResult{}, errors.New("missing algo")
	}
	if m.mutateID == id && m.algoCalls%2 == 0 {
		o.Status = "TRIGGERED"
	}
	if m.changeAfter > 0 && m.algoCalls >= m.changeAfter && m.changeID == id {
		o = m.changedResult
	}
	return o, nil
}
func (m *protectionSettlementMock) Fetch(_ context.Context, _, _ string) (exchange.Snapshot, error) {
	m.fetches++
	s := m.snapshot
	s.AsOf = time.Now().UTC().Format(time.RFC3339Nano)
	return s, nil
}

func algoFromInstruction(id int64, in ProtectionInstruction, status string) exchange.DemoAlgoResult {
	payload := map[string]any{
		"algoId": id, "clientAlgoId": in.ClientAlgoID, "symbol": in.Symbol, "side": in.Side,
		"positionSide": string(in.PositionSide), "algoStatus": status, "orderType": in.OrderType,
		"algoType": in.AlgoType, "triggerPrice": in.TriggerPrice, "workingType": in.WorkingType,
		"closePosition": false, "reduceOnly": false, "quantity": in.Quantity, "actualQty": "0",
	}
	raw, _ := json.Marshal(payload)
	var out exchange.DemoAlgoResult
	_ = json.Unmarshal(raw, &out)
	return out
}

func protectionTrade(orderID, tradeID int64, side, positionSide, qty, price, pnl string) exchange.AccountTrade {
	return exchange.AccountTrade{Symbol: "BTCUSDT", OrderID: orderID, TradeID: tradeID, Side: side, PositionSide: positionSide,
		Quantity: qty, Price: price, Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: pnl, TimeMS: time.Now().UTC().UnixMilli() + tradeID}
}

type protectionSettlementFixture struct {
	dataDir     string
	owners      *OwnershipStore
	submissions *ProtectionSubmissionStore
	settlements *ProtectionSettlementStore
	mock        *protectionSettlementMock
	plan        ProtectionDispatchPlan
	owner       OwnerPosition
}

func newProtectionSettlementFixture(t *testing.T) protectionSettlementFixture {
	t.Helper()
	dataDir := t.TempDir()
	engine, err := NewEngine(dataDir, &uniqueOrders{next: 500})
	if err != nil {
		t.Fatal(err)
	}
	owners, err := NewOwnershipStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	mock := &protectionSettlementMock{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}, algos: map[int64]exchange.DemoAlgoResult{}}
	intent := makeIntent("protect-enter-001", Model, "A", Long, Enter, "0.010")
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil {
		t.Fatal(err)
	}
	remote := orderFromRecord(rec, "FILLED", "0.010")
	mock.orders[rec.Plan.ClientOrderID] = remote
	mock.trades[remote.OrderID] = []exchange.AccountTrade{receipt(rec, 7001, "0.010", "100000", "0.02", "0", time.Now().UTC().UnixMilli()-1000)}
	if n, err := owners.ImportDemoOrder(context.Background(), mock, DemoCredentials{APIKey: "k", Secret: "s"}, intent); err != nil || n != 1 {
		t.Fatalf("entry import %d %v", n, err)
	}
	owner := OwnerPosition{Source: Model, ModelID: "A", Symbol: "BTCUSDT", Side: Long, Quantity: "0.010", AverageEntry: "100000"}
	now := time.Now().UTC()
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	preview, err := PreviewProtection(owner, sizingRules(now), q, q, "protect-group-001", now)
	if err != nil {
		t.Fatal(err)
	}
	submissions, err := OpenProtectionSubmissionStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := submissions.Prepare("protect-group-001", owner, preview)
	if err != nil {
		t.Fatal(err)
	}
	mock.algos[1101] = algoFromInstruction(1101, plan.TP, "NEW")
	mock.algos[1102] = algoFromInstruction(1102, plan.SL, "NEW")
	if err := submissions.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	if err := submissions.RecordObserved(context.Background(), plan.GroupID, "tp", 1101, mock, "k", "s"); err != nil {
		t.Fatal(err)
	}
	if err := submissions.MarkMayHaveSent(plan.GroupID, "sl", true); err != nil {
		t.Fatal(err)
	}
	if err := submissions.RecordObserved(context.Background(), plan.GroupID, "sl", 1102, mock, "k", "s"); err != nil {
		t.Fatal(err)
	}
	settlements, err := NewProtectionSettlementStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	mock.snapshot = accountSnapshot(now, "0", "0")
	return protectionSettlementFixture{dataDir: dataDir, owners: owners, submissions: submissions, settlements: settlements, mock: mock, plan: plan, owner: owner}
}

func TestProtectionSettlementFinishedAndCanceledReconcilesOwnership(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID = "91001"
	tp.ActualQty = "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[91001] = []exchange.AccountTrade{protectionTrade(91001, 8001, "SELL", "LONG", "0.010", "101000", "9.8")}

	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalStage != ProtectionSettlementSettled || !result.Progressed || result.ImportedFillCount != 1 || result.OwnerRemainingQty != "0" || result.ExchangeSideQty != "0" || result.TradingAuthorized {
		t.Fatalf("bad settlement result: %+v", result)
	}

	// General ownership audit must now understand the protection fill rather
	// than permanently reporting a virtual/exchange position mismatch.
	now := time.Now().UTC()
	report, err := f.owners.Audit(accountSnapshot(now, "0", "0"), observations(&readerOrders{orders: f.mock.orders}, now), now)
	if err != nil || !report.ReadyForAssessment || report.ObservedProtectionFills != 1 {
		t.Fatalf("post-protection ownership audit failed: %+v %v", report, err)
	}
	p := owned(t, report, Model, "A", Long)
	if p.Quantity != "0" || p.ExchangeGrossRealizedUSDT != "9.8" {
		t.Fatalf("protection fill not reflected in owner: %+v", p)
	}

	// Restart/retry is idempotent and performs no exchange mutation.
	result2, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result2.InitialStage != ProtectionSettlementSettled || result2.FinalStage != ProtectionSettlementSettled || result2.ImportedFillCount != 0 {
		t.Fatalf("settled retry not idempotent: %+v %v", result2, err)
	}
}

func TestProtectionSettlementTwoTriggeredLegsCanOnlySettleExactCombinedOwnerQty(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "92001", "0.004"
	sl := algoFromInstruction(1102, f.plan.SL, "FINISHED")
	sl.ActualOrderID, sl.ActualQty = "92002", "0.006"
	f.mock.algos[1101], f.mock.algos[1102] = tp, sl
	f.mock.trades[92001] = []exchange.AccountTrade{protectionTrade(92001, 8101, "SELL", "LONG", "0.004", "101000", "3.9")}
	f.mock.trades[92002] = []exchange.AccountTrade{protectionTrade(92002, 8102, "SELL", "LONG", "0.006", "99000", "-6.1")}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementSettled || result.ImportedFillCount != 2 {
		t.Fatalf("exact simultaneous reconciliation failed: %+v %v", result, err)
	}
}

func TestProtectionSettlementPartialCombinedFillFailsClosed(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "93001", "0.004"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[93001] = []exchange.AccountTrade{protectionTrade(93001, 8201, "SELL", "LONG", "0.004", "101000", "4")}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err == nil || result.FinalStage == ProtectionSettlementSettled || result.TradingAuthorized {
		t.Fatalf("partial protection falsely settled: %+v %v", result, err)
	}
}

func TestProtectionSettlementActiveLegRemainsPendingWithoutLedgerMutation(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	f.mock.algos[1101] = algoFromInstruction(1101, f.plan.TP, "TRIGGERED")
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementPending || result.PendingReason == "" || result.Progressed {
		t.Fatalf("active protection did not remain pending: %+v %v", result, err)
	}
	entries, err := os.ReadDir(filepath.Join(f.dataDir, "trading", "protection_fills"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("pending flow mutated fill ledger: %d %v", len(entries), err)
	}
}

func TestProtectionSettlementTamperedFillBlocksGeneralOwnershipAudit(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "94001", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[94001] = []exchange.AccountTrade{protectionTrade(94001, 8301, "SELL", "LONG", "0.010", "101000", "9")}
	if _, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.dataDir, "trading", "protection_fills", protectionFillName(8301))
	if err := os.WriteFile(path, []byte(`{"schema":1,"group_id":"tampered"}`), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := f.owners.Audit(accountSnapshot(now, "0", "0"), observations(&readerOrders{orders: f.mock.orders}, now), now); err == nil {
		t.Fatal("tampered protection fill accepted by general ownership audit")
	}
}

func TestProtectionSettlementFillImportedStageBlocksPublicOwnershipReadiness(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "95001", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[95001] = []exchange.AccountTrade{protectionTrade(95001, 8401, "SELL", "LONG", "0.010", "101000", "9")}
	reads, pending, err := f.settlements.collectStableLegEvidence(context.Background(), f.plan, f.submissions, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || pending != "" {
		t.Fatalf("stable evidence: %q %v", pending, err)
	}
	regular, err := f.owners.readFills()
	if err != nil {
		t.Fatal(err)
	}
	f.settlements.mu.Lock()
	_, all, err := f.settlements.persistProtectionFillsLocked(f.plan, reads, regular)
	if err == nil {
		groupFills := []ProtectionStoredFill{}
		for _, pf := range all {
			if pf.GroupID == f.plan.GroupID {
				groupFills = append(groupFills, pf)
			}
		}
		_, err = f.settlements.writeMarkerLocked(protectionSettlementMarker{GroupID: f.plan.GroupID, PlanDigest: f.plan.Digest,
			Stage: ProtectionSettlementFillsImported, FillCount: len(groupFills), FillDigest: protectionFillSetDigest(groupFills)})
	}
	f.settlements.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	report, err := f.owners.Audit(accountSnapshot(now, "0", "0"), observations(&readerOrders{orders: f.mock.orders}, now), now)
	if err != nil {
		t.Fatal(err)
	}
	if report.ReadyForAssessment {
		t.Fatalf("incomplete protection settlement became ready: %+v", report)
	}
	found := false
	for _, b := range report.Blockers {
		if strings.Contains(b, "미완료 protection settlement") {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing settlement blocker: %+v", report.Blockers)
	}
}

func TestProtectionSettlementOvercloseAcrossBothLegsFailsClosed(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "96001", "0.006"
	sl := algoFromInstruction(1102, f.plan.SL, "FINISHED")
	sl.ActualOrderID, sl.ActualQty = "96002", "0.006"
	f.mock.algos[1101], f.mock.algos[1102] = tp, sl
	f.mock.trades[96001] = []exchange.AccountTrade{protectionTrade(96001, 8501, "SELL", "LONG", "0.006", "101000", "5")}
	f.mock.trades[96002] = []exchange.AccountTrade{protectionTrade(96002, 8502, "SELL", "LONG", "0.006", "99000", "-6")}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err == nil || result.FinalStage == ProtectionSettlementSettled {
		t.Fatalf("overclose falsely settled: %+v %v", result, err)
	}
}

func TestProtectionSettlementChangingSignedEvidenceDoesNotPersist(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "97001", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[97001] = []exchange.AccountTrade{protectionTrade(97001, 8601, "SELL", "LONG", "0.010", "101000", "9")}
	f.mock.mutateID = 1101
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.PendingReason == "" || result.Progressed {
		t.Fatalf("changing evidence not held pending: %+v %v", result, err)
	}
	entries, err := os.ReadDir(filepath.Join(f.dataDir, "trading", "protection_fills"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("unstable evidence persisted fills: %d %v", len(entries), err)
	}
}

func TestSettledProtectionGroupNoLongerBlocksNewSameSidePlan(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "98001", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[98001] = []exchange.AccountTrade{protectionTrade(98001, 8701, "SELL", "LONG", "0.010", "101000", "9")}
	if _, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	nextPreview, err := PreviewProtection(f.owner, sizingRules(now), q, q, "protect-group-002", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.submissions.Prepare("protect-group-002", f.owner, nextPreview); err != nil {
		t.Fatalf("settled historical plan still blocked same-side staging: %v", err)
	}
	review, err := f.submissions.ReviewAllPlans()
	if err != nil || review.State != ProtectionArmingSingleOwnerPairOnly || review.PlanCount != 1 {
		t.Fatalf("active plan review still counts settled history: %+v %v", review, err)
	}
	if _, err := f.submissions.Prepare(f.plan.GroupID, f.owner, ProtectionPreview{TP: f.plan.TP, SL: f.plan.SL, Leverage: 15, TargetGrossROI: "0.05", DoesNotIncludeCosts: true}); err == nil {
		t.Fatal("settled group ID was reusable")
	}
}

func TestProtectionSettlementShortSideSignAndOwnership(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := NewEngine(dataDir, &uniqueOrders{next: 600})
	if err != nil {
		t.Fatal(err)
	}
	owners, err := NewOwnershipStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	mock := &protectionSettlementMock{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}, algos: map[int64]exchange.DemoAlgoResult{}}
	intent := makeIntent("protect-short-enter", Model, "B", Short, Enter, "0.010")
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil {
		t.Fatal(err)
	}
	remote := orderFromRecord(rec, "FILLED", "0.010")
	mock.orders[rec.Plan.ClientOrderID] = remote
	mock.trades[remote.OrderID] = []exchange.AccountTrade{receipt(rec, 8801, "0.010", "100000", "0.02", "0", time.Now().UTC().UnixMilli()-1000)}
	if n, err := owners.ImportDemoOrder(context.Background(), mock, DemoCredentials{APIKey: "k", Secret: "s"}, intent); err != nil || n != 1 {
		t.Fatalf("entry %d %v", n, err)
	}
	owner := OwnerPosition{Source: Model, ModelID: "B", Symbol: "BTCUSDT", Side: Short, Quantity: "0.010", AverageEntry: "100000"}
	now := time.Now().UTC()
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	preview, err := PreviewProtection(owner, sizingRules(now), q, q, "protect-short-001", now)
	if err != nil {
		t.Fatal(err)
	}
	submissions, err := OpenProtectionSubmissionStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := submissions.Prepare("protect-short-001", owner, preview)
	if err != nil {
		t.Fatal(err)
	}
	mock.algos[1201] = algoFromInstruction(1201, plan.TP, "NEW")
	mock.algos[1202] = algoFromInstruction(1202, plan.SL, "NEW")
	if err := submissions.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	if err := submissions.RecordObserved(context.Background(), plan.GroupID, "tp", 1201, mock, "k", "s"); err != nil {
		t.Fatal(err)
	}
	if err := submissions.MarkMayHaveSent(plan.GroupID, "sl", true); err != nil {
		t.Fatal(err)
	}
	if err := submissions.RecordObserved(context.Background(), plan.GroupID, "sl", 1202, mock, "k", "s"); err != nil {
		t.Fatal(err)
	}
	tp := algoFromInstruction(1201, plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99001", "0.010"
	mock.algos[1201] = tp
	mock.algos[1202] = algoFromInstruction(1202, plan.SL, "CANCELED")
	mock.trades[99001] = []exchange.AccountTrade{protectionTrade(99001, 8901, "BUY", "SHORT", "0.010", "99000", "9.8")}
	mock.snapshot = accountSnapshot(now, "0", "0")
	settlements, err := NewProtectionSettlementStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	result, err := settlements.RecoverProtectionSettlement(context.Background(), plan.GroupID, submissions, owners, mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementSettled {
		t.Fatalf("short settle %+v %v", result, err)
	}
	now = time.Now().UTC()
	report, err := owners.Audit(accountSnapshot(now, "0", "0"), observations(&readerOrders{orders: mock.orders}, now), now)
	if err != nil || !report.ReadyForAssessment {
		t.Fatalf("short audit %+v %v", report, err)
	}
	p := owned(t, report, Model, "B", Short)
	if p.Quantity != "0" || p.ExchangeGrossRealizedUSDT != "9.8" {
		t.Fatalf("short owner %+v", p)
	}
}

func TestProtectionSettlementFinalSignedRoundMustMatchImportedEvidence(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99101", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[99101] = []exchange.AccountTrade{protectionTrade(99101, 8951, "SELL", "LONG", "0.010", "101000", "9")}
	changed := tp
	changed.Status = "CANCELED"
	// Fixture creation consumed two algo GETs. The first reconciliation consumes
	// four more, so call 7 is the first GET of the independent final round.
	f.mock.changeAfter = 7
	f.mock.changeID = 1101
	f.mock.changedResult = changed
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err == nil || result.FinalStage == ProtectionSettlementSettled {
		t.Fatalf("changed terminal evidence falsely settled: %+v %v", result, err)
	}
}

func TestProtectionReplayKeepsPostSettlementReentryChronological(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99201", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	protectTime := time.Now().UTC().UnixMilli() + 1000
	f.mock.trades[99201] = []exchange.AccountTrade{{Symbol: "BTCUSDT", OrderID: 99201, TradeID: 8961, Side: "SELL", PositionSide: "LONG",
		Quantity: "0.010", Price: "101000", Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: "9", TimeMS: protectTime}}
	if _, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"}); err != nil {
		t.Fatal(err)
	}

	// A later regular entry for the same owner must remain open. Replaying all
	// regular fills first and subtracting historical protection fills last would
	// incorrectly erase this new position.
	engine, err := NewEngine(f.dataDir, &uniqueOrders{next: 2000})
	if err != nil {
		t.Fatal(err)
	}
	intent := makeIntent("protect-reenter-01", Model, "A", Long, Enter, "0.005")
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil {
		t.Fatal(err)
	}
	remote := orderFromRecord(rec, "FILLED", "0.005")
	f.mock.orders[rec.Plan.ClientOrderID] = remote
	f.mock.trades[remote.OrderID] = []exchange.AccountTrade{receipt(rec, 8962, "0.005", "102000", "0.01", "0", protectTime+1000)}
	if n, err := f.owners.ImportDemoOrder(context.Background(), f.mock, DemoCredentials{APIKey: "k", Secret: "s"}, intent); err != nil || n != 1 {
		t.Fatalf("reentry import %d %v", n, err)
	}
	now := time.Now().UTC()
	report, err := f.owners.Audit(accountSnapshot(now, "0.005", "0"), observations(&readerOrders{orders: f.mock.orders}, now), now)
	if err != nil || !report.ReadyForAssessment {
		t.Fatalf("chronological audit failed: %+v %v", report, err)
	}
	p := owned(t, report, Model, "A", Long)
	if p.Quantity != "0.005" || p.AverageEntry != "102000" {
		t.Fatalf("historical protection fill consumed reentry: %+v", p)
	}
}

func TestSettledProtectionAndCancelHistoryDoNotPermanentlyLockWalletEdit(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	cancelStore, err := NewCancelIntentStore(f.dataDir)
	if err != nil {
		t.Fatal(err)
	}
	cancelIntent, err := cancelStore.Plan(f.plan.GroupID, 1102, f.plan.SL.ClientAlgoID)
	if err != nil {
		t.Fatal(err)
	}
	if err := cancelStore.MarkMayHaveSent(cancelIntent); err != nil {
		t.Fatal(err)
	}

	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99101", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[99101] = []exchange.AccountTrade{protectionTrade(99101, 8911, "SELL", "LONG", "0.010", "101000", "9.8")}
	if _, err := cancelStore.RecoverAttemptedCancel(context.Background(), f.mock, "k", "s", f.plan.GroupID); err != nil {
		t.Fatal(err)
	}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementSettled {
		t.Fatalf("settlement failed: %+v %v", result, err)
	}

	fingerprint, historical, err := CheckWalletEditHistory(f.dataDir)
	if err != nil || !historical || fingerprint == "" {
		t.Fatalf("settled protection history permanently locked wallet edit: fingerprint=%q historical=%v err=%v", fingerprint, historical, err)
	}
}

func TestAccountingIncludesSettledProtectionFills(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99201", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[99201] = []exchange.AccountTrade{protectionTrade(99201, 8921, "SELL", "LONG", "0.010", "101000", "9.8")}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementSettled {
		t.Fatalf("settlement failed: %+v %v", result, err)
	}
	if _, err := LocalAccountingFingerprint(f.dataDir); err != nil {
		t.Fatalf("valid protection evidence should be fingerprintable: %v", err)
	}
	created := time.Now().UTC().Add(30 * time.Second)
	start := created.Add(-time.Hour).UnixMilli()
	report, err := f.owners.AnalyzeOfflineFromLocalStore(AccountingInput{WindowStartMS: start, WindowEndMS: created.Add(-time.Second).UnixMilli(), SignedIncome: []exchange.IncomeRecord{}}, created)
	if err != nil {
		t.Fatalf("protection-aware accounting failed: %v", err)
	}
	owner := ownerAccounting(t, report, Model, "A", Long)
	if report.KnownFills != 2 || owner.OpenQuantity != "0" || owner.VirtualGrossRealizedUSDT != "10" || owner.ExchangeReportedGrossUSDT != "9.8" ||
		owner.CommissionByAsset["USDT"] != "0.03" || owner.VirtualAfterUSDTFeesExFunding != "9.97" {
		t.Fatalf("protection fill was not attributed to owner accounting: report=%+v owner=%+v", report, owner)
	}
}

func TestAttestedAccountingMatchesSettledProtectionActualOrderAndAlgoHistory(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99301", "0.010"
	f.mock.algos[1101] = tp
	f.mock.algos[1102] = algoFromInstruction(1102, f.plan.SL, "CANCELED")
	f.mock.trades[99301] = []exchange.AccountTrade{protectionTrade(99301, 8931, "SELL", "LONG", "0.010", "101000", "9.8")}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementSettled {
		t.Fatalf("settlement failed: %+v %v", result, err)
	}

	f.owners.mu.Lock()
	records, err := f.owners.ScanJournal()
	if err != nil {
		f.owners.mu.Unlock()
		t.Fatal(err)
	}
	regular, err := f.owners.readFills()
	f.owners.mu.Unlock()
	if err != nil || len(records) != 1 || len(regular) != 1 {
		t.Fatalf("local evidence: records=%d fills=%d err=%v", len(records), len(regular), err)
	}
	protection, err := readValidatedProtectionFillsForStore(filepath.Join(f.dataDir, "trading"), regular)
	if err != nil || len(protection) != 1 {
		t.Fatalf("protection evidence: %d %v", len(protection), err)
	}
	knownAlgos, err := loadProtectionAccountingAlgos(filepath.Join(f.dataDir, "trading"), protection)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Add(30 * time.Second)
	start := now.Add(-time.Hour).UnixMilli()
	started := now.Add(-time.Second)
	baseline := exchange.RecoveryBaseline{
		Environment: "demo", Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS", StartedAt: started, FinishedAt: now,
		WindowStartMS: start, WindowEndMS: started.UnixMilli(), OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{},
		RecentBTCOrders: []exchange.Order{
			{Symbol: "BTCUSDT", OrderID: records[0].ExchangeOrderID, ClientOrderID: records[0].Plan.ClientOrderID, Side: records[0].Plan.ExchangeSide, PositionSide: string(records[0].Plan.PositionSide), Type: string(records[0].Plan.Type), Status: "FILLED", OrigQty: records[0].Plan.Quantity, ExecutedQty: records[0].Plan.Quantity},
			{Symbol: "BTCUSDT", OrderID: 99301, ClientOrderID: "binance-protection-child-99301", Side: "SELL", PositionSide: "LONG", Type: "MARKET", Status: "FILLED", OrigQty: "0.010", ExecutedQty: "0.010"},
		},
		RecentBTCAlgos: []exchange.AlgoOrder{
			{AlgoID: 1101, ClientAlgoID: f.plan.TP.ClientAlgoID, Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", OrderType: f.plan.TP.OrderType, AlgoStatus: "FINISHED", Quantity: f.plan.TP.Quantity, TriggerPrice: f.plan.TP.TriggerPrice, WorkingType: f.plan.TP.WorkingType},
			{AlgoID: 1102, ClientAlgoID: f.plan.SL.ClientAlgoID, Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", OrderType: f.plan.SL.OrderType, AlgoStatus: "CANCELED", Quantity: f.plan.SL.Quantity, TriggerPrice: f.plan.SL.TriggerPrice, WorkingType: f.plan.SL.WorkingType},
		},
		RecentBTCTrades: []exchange.AccountTrade{regular[0].Trade, protection[0].Trade}, RecentIncome: []exchange.IncomeRecord{},
	}
	if err := json.Unmarshal([]byte(`{"canTrade":true,"totalWalletBalance":"4000","totalUnrealizedProfit":"0","totalMarginBalance":"4000","totalInitialMargin":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","availableBalance":"4000","assets":[{"asset":"USDT","walletBalance":"4000","availableBalance":"4000","marginBalance":"4000"}],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0","initialMargin":"0","isolated":true},{"symbol":"BTCUSDT","positionSide":"SHORT","positionAmt":"0","initialMargin":"0","isolated":true}]}`), &baseline.Portfolio); err != nil {
		t.Fatal(err)
	}
	report, err := buildAttestedAccounting(records, regular, protection, knownAlgos, baseline, now)
	if err != nil {
		t.Fatalf("attested protection accounting failed: %v", err)
	}
	owner := ownerAccounting(t, report, Model, "A", Long)
	if report.KnownFills != 2 || owner.OpenQuantity != "0" || owner.VirtualGrossRealizedUSDT != "10" || owner.ExchangeReportedGrossUSDT != "9.8" || len(report.EvidenceSHA256) != 64 {
		t.Fatalf("unexpected attested protection accounting: %+v owner=%+v", report, owner)
	}

	bad := baseline
	bad.RecentBTCTrades = append([]exchange.AccountTrade(nil), baseline.RecentBTCTrades...)
	bad.RecentBTCTrades[1].Commission = "0.02"
	if _, err := buildAttestedAccounting(records, regular, protection, knownAlgos, bad, now); err == nil {
		t.Fatal("accepted protection fill fee mismatch")
	}
	missingSibling := baseline
	missingSibling.RecentBTCAlgos = append([]exchange.AlgoOrder(nil), baseline.RecentBTCAlgos[:1]...)
	if _, err := buildAttestedAccounting(records, regular, protection, knownAlgos, missingSibling, now); err == nil {
		t.Fatal("accepted missing sibling algo history")
	}
}

func TestAccountingProtectionAttributionDistinguishesDualTriggerLegs(t *testing.T) {
	f := newProtectionSettlementFixture(t)
	tp := algoFromInstruction(1101, f.plan.TP, "FINISHED")
	tp.ActualOrderID, tp.ActualQty = "99401", "0.004"
	sl := algoFromInstruction(1102, f.plan.SL, "FINISHED")
	sl.ActualOrderID, sl.ActualQty = "99402", "0.006"
	f.mock.algos[1101], f.mock.algos[1102] = tp, sl
	f.mock.trades[99401] = []exchange.AccountTrade{protectionTrade(99401, 8941, "SELL", "LONG", "0.004", "101000", "3.9")}
	f.mock.trades[99402] = []exchange.AccountTrade{protectionTrade(99402, 8942, "SELL", "LONG", "0.006", "99000", "-6.1")}
	result, err := f.settlements.RecoverProtectionSettlement(context.Background(), f.plan.GroupID, f.submissions, f.owners, f.mock, DemoCredentials{APIKey: "k", Secret: "s"})
	if err != nil || result.FinalStage != ProtectionSettlementSettled {
		t.Fatalf("settlement failed: %+v %v", result, err)
	}
	created := time.Now().UTC().Add(30 * time.Second)
	report, err := f.owners.AnalyzeOfflineFromLocalStore(AccountingInput{WindowStartMS: created.Add(-time.Hour).UnixMilli(), WindowEndMS: created.Add(-time.Second).UnixMilli(), SignedIncome: []exchange.IncomeRecord{}}, created)
	if err != nil {
		t.Fatal(err)
	}
	owner := ownerAccounting(t, report, Model, "A", Long)
	p := owner.Protection
	if report.ProtectionFillCount != 2 || report.ProtectionGroupCount != 1 || report.DualTriggeredGroupCount != 1 || p == nil {
		t.Fatalf("missing report attribution: %+v owner=%+v", report, owner)
	}
	if p.FillCount != 2 || p.GroupCount != 1 || p.DualTriggeredGroupCount != 1 || p.TakeProfitFillCount != 1 || p.StopLossFillCount != 1 ||
		p.TakeProfitQuantity != "0.004" || p.StopLossQuantity != "0.006" || p.VirtualGrossRealizedUSDT != "-2" ||
		p.ExchangeReportedGrossUSDT != "-2.2" || p.CommissionByAsset["USDT"] != "0.02" {
		t.Fatalf("wrong dual-trigger attribution: %+v", p)
	}
}
