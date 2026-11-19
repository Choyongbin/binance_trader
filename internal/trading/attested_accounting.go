package trading

// Operator-triggered READ-ONLY bounded accounting bridge. This deliberately
// does not claim uninterrupted account-stream coverage, final P&L, safe OCO,
// or any authorization to trade. A signed REST result is only trusted when
// produced by the caller's Demo-only authenticated reader. Historical trades
// outside this short bounded window cannot be reconstructed here.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"time"

	"binance_trader/internal/exchange"
)

// AccountingWindowReader is read-only, limited to the existing authenticated
// Demo recovery implementation. An injected mock is used in unit tests.
type AccountingWindowReader interface {
	FetchDemoRecoveryBaseline(context.Context, string, string, int64) (exchange.RecoveryBaseline, error)
}

// LocalAccountingFingerprint includes EVERY byte in the immutable order/fill
// directories, including protection-fill settlement evidence. A missing base
// request/fill directory is created by NewOwnershipStore; optional protection
// directories are hashed when present. Corrupted protection evidence is
// rejected before calculating the hash. This is a local change detector, NOT
// an authentication signature.
func LocalAccountingFingerprint(dataDir string) (string, error) {
	store, err := NewOwnershipStore(dataDir)
	if err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := store.ScanJournal(); err != nil {
		return "", err
	}
	regular, err := store.readFills()
	if err != nil {
		return "", err
	}
	tradingRoot := filepath.Dir(store.fillDir)
	if _, err := readValidatedProtectionFillsForStore(tradingRoot, regular); err != nil {
		return "", err
	}
	h := sha256.New()
	for _, dir := range []string{store.journalDir, store.fillDir, filepath.Join(tradingRoot, "protection_fills"), filepath.Join(tradingRoot, "protection_settlements"), filepath.Join(tradingRoot, "protection_submissions")} {
		files, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if len(files) > 100000 {
			return "", errors.New("주문 또는 체결 파일 개수가 너무 많습니다")
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
		for _, file := range files {
			if !file.Type().IsRegular() {
				return "", errors.New("정상 파일이 아닌 거래 증적 발견")
			}
			raw, err := os.ReadFile(filepath.Join(dir, file.Name()))
			if err != nil {
				return "", err
			}
			if len(raw) > 2<<20 {
				return "", errors.New("거래 증적 크기가 허용치를 초과합니다")
			}
			part := sha256.Sum256(raw)
			// dir tag avoids accidental same-name collision between folders.
			fmt.Fprintf(h, "%s:%s:%x\n", filepath.Base(dir), file.Name(), part)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func loadProtectionAccountingAlgos(tradingRoot string, protection []ProtectionStoredFill) (map[int64]protectionAccountingAlgo, error) {
	known := make(map[int64]protectionAccountingAlgo)
	if len(protection) == 0 {
		return known, nil
	}
	if blockers, err := protectionSettlementBlockers(tradingRoot, protection, ""); err != nil {
		return nil, err
	} else if len(blockers) != 0 {
		return nil, errors.New("미완료 protection settlement이 있어 기간 회계를 생성할 수 없습니다")
	}
	ps := &ProtectionSubmissionStore{dir: filepath.Join(tradingRoot, "protection_submissions")}
	groups := make(map[string]bool)
	for _, f := range protection {
		groups[f.GroupID] = true
	}
	for group := range groups {
		plan, err := ps.read(group)
		if err != nil {
			return nil, err
		}
		for _, leg := range []string{"tp", "sl"} {
			state, err := ps.inspectLocked(group, leg)
			if err != nil {
				return nil, err
			}
			if !state.SignedObserved || state.ObservedAlgoID <= 0 {
				return nil, errors.New("protection algo의 signed-observed 증적이 완전하지 않습니다")
			}
			instruction := plan.TP
			if leg == "sl" {
				instruction = plan.SL
			}
			if _, exists := known[state.ObservedAlgoID]; exists {
				return nil, errors.New("protection algoId 중복")
			}
			known[state.ObservedAlgoID] = protectionAccountingAlgo{GroupID: group, Leg: leg, Instruction: instruction}
		}
	}
	return known, nil
}

func sameSignedDecimal(a, b string, positive bool) bool {
	if positive {
		av, ae := positiveRat(a)
		bv, be := positiveRat(b)
		return ae == nil && be == nil && av.Cmp(bv) == 0
	}
	av, ae := signedRat(a)
	bv, be := signedRat(b)
	return ae == nil && be == nil && av.Cmp(bv) == 0
}

// verifyBoundedTrades requires a one-to-one match between the complete
// signed BTC trade window and EVERY locally owned immutable fill. An external
// trade, stale history, fee discrepancy or an earlier local fill invalidates
// this *particular* accounting snapshot rather than silently attributing it.
type protectionAccountingAlgo struct {
	GroupID     string
	Leg         string
	Instruction ProtectionInstruction
}

func verifyBoundedTrades(records []Record, fills []StoredFill, b exchange.RecoveryBaseline) error {
	return verifyBoundedTradesWithProtection(records, fills, nil, nil, b)
}

func verifyBoundedTradesWithProtection(records []Record, fills []StoredFill, protection []ProtectionStoredFill, knownAlgos map[int64]protectionAccountingAlgo, b exchange.RecoveryBaseline) error {
	if b.RecentBTCOrders == nil || b.RecentBTCAlgos == nil || b.RecentBTCTrades == nil || b.RecentIncome == nil ||
		b.OpenOrders == nil || b.OpenAlgos == nil {
		return errors.New("서명된 거래소 주문·체결·소득 목록이 누락되었습니다")
	}
	if len(b.OpenOrders) != 0 || len(b.OpenAlgos) != 0 {
		return errors.New("미체결 주문 또는 조건부 주문이 존재합니다")
	}
	seenRemoteAlgos := make(map[int64]bool, len(b.RecentBTCAlgos))
	for _, a := range b.RecentBTCAlgos {
		known, ok := knownAlgos[a.AlgoID]
		if !ok || seenRemoteAlgos[a.AlgoID] {
			return errors.New("로컬 보호 장부에 없는 과거 조건부 주문이 존재합니다")
		}
		seenRemoteAlgos[a.AlgoID] = true
		i := known.Instruction
		if a.Symbol != i.Symbol || a.ClientAlgoID != i.ClientAlgoID || a.Side != i.Side || a.PositionSide != string(i.PositionSide) ||
			a.OrderType != i.OrderType || a.WorkingType != i.WorkingType || a.ClosePosition || !sameSignedDecimal(a.Quantity, i.Quantity, true) ||
			!sameSignedDecimal(a.TriggerPrice, i.TriggerPrice, true) {
			return errors.New("서명된 과거 조건부 주문이 로컬 protection plan과 다릅니다")
		}
	}
	windowProtectionGroups := make(map[string]bool)
	for _, f := range protection {
		windowProtectionGroups[f.GroupID] = true
	}
	for algoID, known := range knownAlgos {
		if windowProtectionGroups[known.GroupID] && !seenRemoteAlgos[algoID] {
			return errors.New("체결된 protection group의 TP/SL 서명 이력이 완전하지 않습니다")
		}
	}
	orders := make(map[string]Record, len(records))
	for _, r := range records {
		if r.Plan.ClientOrderID == "" {
			return errors.New("불완전한 로컬 주문")
		}
		orders[r.Plan.ClientOrderID] = r
	}
	// A record not present in bounded history may have been created before the
	// requested window. Do not infer absence of outside activity from it.
	remoteOrders := make(map[string]exchange.Order, len(b.RecentBTCOrders))
	remoteOrderIDs := make(map[int64]bool, len(b.RecentBTCOrders))
	protectionOrderQty := make(map[int64]*big.Rat)
	protectionByOrder := make(map[int64]ProtectionStoredFill)
	for _, f := range protection {
		if protectionOrderQty[f.ActualOrderID] == nil {
			protectionOrderQty[f.ActualOrderID] = new(big.Rat)
			protectionByOrder[f.ActualOrderID] = f
		}
		q, err := positiveRat(f.Trade.Quantity)
		if err != nil {
			return errors.New("protection 체결 수량 오류")
		}
		protectionOrderQty[f.ActualOrderID].Add(protectionOrderQty[f.ActualOrderID], q)
	}
	for _, o := range b.RecentBTCOrders {
		if o.Symbol != "BTCUSDT" || o.OrderID <= 0 || o.ClientOrderID == "" {
			return errors.New("잘못된 서명 주문 내역")
		}
		rec, ok := orders[o.ClientOrderID]
		if ok {
			if rec.Plan.Symbol != o.Symbol || rec.Plan.ExchangeSide != o.Side || string(rec.Plan.PositionSide) != o.PositionSide || string(rec.Plan.Type) != o.Type {
				return errors.New("서명된 일반 주문이 로컬 원본과 다릅니다")
			}
		} else {
			pf, protectionOrder := protectionByOrder[o.OrderID]
			if !protectionOrder || o.Side != pf.Trade.Side || o.PositionSide != pf.Trade.PositionSide {
				return errors.New("로컬 장부에서 소유자를 찾을 수 없는 Demo 주문이 있습니다")
			}
		}
		if _, duplicate := remoteOrders[o.ClientOrderID]; duplicate || remoteOrderIDs[o.OrderID] {
			return errors.New("서명 주문 ID 중복")
		}
		switch o.Status {
		case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED":
		default:
			return errors.New("서명 조회 시 주문이 미완료 상태입니다")
		}
		orig, oe := positiveRat(o.OrigQty)
		executed, ee := decimalRat(o.ExecutedQty, true)
		if oe != nil || ee != nil || executed.Cmp(orig) > 0 {
			return errors.New("서명 주문 수량이 잘못되었습니다")
		}
		if ok {
			planned, pe := positiveRat(rec.Plan.Quantity)
			if pe != nil || planned.Cmp(orig) != 0 {
				return errors.New("서명 주문 원래 수량이 로컬 원본과 일치하지 않습니다")
			}
		} else if executed.Cmp(protectionOrderQty[o.OrderID]) != 0 {
			return errors.New("보호 actual order의 서명 누적 체결량이 로컬 protection fill과 다릅니다")
		}
		remoteOrderIDs[o.OrderID] = true
		remoteOrders[o.ClientOrderID] = o
	}
	type localTradeEvidence struct {
		trade exchange.AccountTrade
		order int64
	}
	local := make(map[int64]localTradeEvidence, len(fills)+len(protection))
	for _, f := range fills {
		if f.Schema != 1 || f.Checksum == "" || f.Checksum != f.digest() ||
			f.Trade.TimeMS < b.WindowStartMS || f.Trade.TimeMS > b.WindowEndMS {
			return errors.New("현재 조회 범위 밖의 로컬 체결 또는 손상된 체결이 있습니다: 기간을 조정하거나 수동 검토하세요")
		}
		rec, ok := orders[f.ClientOrderID]
		if !ok || validateFill(rec, f.Trade) != nil {
			return errors.New("로컬 체결과 주문 소유자 연결이 올바르지 않습니다")
		}
		ro, ok := remoteOrders[f.ClientOrderID]
		if !ok || ro.OrderID != f.Trade.OrderID {
			return errors.New("로컬 체결의 거래소 주문 원본이 조회되지 않았습니다")
		}
		if _, duplicate := local[f.Trade.TradeID]; duplicate {
			return errors.New("로컬 체결 ID 중복")
		}
		local[f.Trade.TradeID] = localTradeEvidence{trade: f.Trade, order: f.Trade.OrderID}
	}
	for _, f := range protection {
		if f.Schema != 1 || f.Checksum == "" || f.Checksum != f.digest() || f.Trade.TimeMS < b.WindowStartMS || f.Trade.TimeMS > b.WindowEndMS {
			return errors.New("현재 조회 범위 밖의 protection 체결 또는 손상된 체결이 있습니다")
		}
		if _, duplicate := local[f.Trade.TradeID]; duplicate {
			return errors.New("일반/protection 체결 ID 중복")
		}
		if !remoteOrderIDs[f.ActualOrderID] {
			return errors.New("protection actual order가 서명 주문 이력에서 조회되지 않았습니다")
		}
		local[f.Trade.TradeID] = localTradeEvidence{trade: f.Trade, order: f.ActualOrderID}
	}
	signed := make(map[int64]bool, len(b.RecentBTCTrades))
	signedQty := make(map[int64]*big.Rat)
	for _, t := range b.RecentBTCTrades {
		f, ok := local[t.TradeID]
		if !ok || signed[t.TradeID] {
			return errors.New("로컬 장부에 없는 외부 체결 또는 서명 체결 ID 중복")
		}
		l := f.trade
		if t.Symbol != l.Symbol || t.OrderID != l.OrderID || t.Side != l.Side ||
			t.PositionSide != l.PositionSide || t.TimeMS != l.TimeMS ||
			t.CommissionAsset != l.CommissionAsset ||
			!sameSignedDecimal(t.Price, l.Price, true) || !sameSignedDecimal(t.Quantity, l.Quantity, true) ||
			!sameSignedDecimal(t.Commission, l.Commission, false) || !sameSignedDecimal(t.RealizedPnL, l.RealizedPnL, false) {
			return errors.New("서명된 Demo 체결과 로컬 원본의 가격·수량·수수료·손익이 다릅니다")
		}
		signed[t.TradeID] = true
		q, _ := positiveRat(t.Quantity)
		if signedQty[t.OrderID] == nil {
			signedQty[t.OrderID] = new(big.Rat)
		}
		signedQty[t.OrderID].Add(signedQty[t.OrderID], q)
	}
	if len(local) != len(signed) {
		return errors.New("서명된 Demo 체결 목록에 포함되지 않은 로컬 체결이 있습니다")
	}
	for _, o := range remoteOrders {
		want, _ := decimalRat(o.ExecutedQty, true)
		observed := signedQty[o.OrderID]
		if observed == nil {
			observed = new(big.Rat)
		}
		if observed.Cmp(want) != 0 {
			return errors.New("서명된 주문 누적 체결량과 독립적인 체결 목록의 합계가 다릅니다")
		}
	}
	return nil
}

func canonicalAccountingEvidenceJSON(b exchange.RecoveryBaseline, fills []StoredFill, protection []ProtectionStoredFill, records []Record) ([]byte, error) {
	portfolio := b.Portfolio
	portfolio.Assets = append([]exchange.PortfolioAsset(nil), portfolio.Assets...)
	portfolio.Positions = append([]exchange.PortfolioPosition(nil), portfolio.Positions...)
	sort.Slice(portfolio.Assets, func(i, j int) bool { return portfolio.Assets[i].Asset < portfolio.Assets[j].Asset })
	sort.Slice(portfolio.Positions, func(i, j int) bool {
		if portfolio.Positions[i].Symbol != portfolio.Positions[j].Symbol {
			return portfolio.Positions[i].Symbol < portfolio.Positions[j].Symbol
		}
		return portfolio.Positions[i].PositionSide < portfolio.Positions[j].PositionSide
	})

	openOrders := append([]exchange.Order(nil), b.OpenOrders...)
	openAlgos := append([]exchange.AlgoOrder(nil), b.OpenAlgos...)
	orders := append([]exchange.Order(nil), b.RecentBTCOrders...)
	algos := append([]exchange.AlgoOrder(nil), b.RecentBTCAlgos...)
	trades := append([]exchange.AccountTrade(nil), b.RecentBTCTrades...)
	income := append([]exchange.IncomeRecord(nil), b.RecentIncome...)
	fills = append([]StoredFill(nil), fills...)
	protection = append([]ProtectionStoredFill(nil), protection...)
	records = append([]Record(nil), records...)

	sortOrders := func(items []exchange.Order) {
		sort.Slice(items, func(i, j int) bool {
			if items[i].OrderID != items[j].OrderID {
				return items[i].OrderID < items[j].OrderID
			}
			return items[i].ClientOrderID < items[j].ClientOrderID
		})
	}
	sortAlgos := func(items []exchange.AlgoOrder) {
		sort.Slice(items, func(i, j int) bool {
			if items[i].AlgoID != items[j].AlgoID {
				return items[i].AlgoID < items[j].AlgoID
			}
			return items[i].ClientAlgoID < items[j].ClientAlgoID
		})
	}
	sortOrders(openOrders)
	sortAlgos(openAlgos)
	sortOrders(orders)
	sortAlgos(algos)
	sort.Slice(trades, func(i, j int) bool {
		if trades[i].TradeID != trades[j].TradeID {
			return trades[i].TradeID < trades[j].TradeID
		}
		return trades[i].OrderID < trades[j].OrderID
	})
	sort.Slice(income, func(i, j int) bool {
		if income[i].TransactionID != income[j].TransactionID {
			return income[i].TransactionID < income[j].TransactionID
		}
		if income[i].TimeMS != income[j].TimeMS {
			return income[i].TimeMS < income[j].TimeMS
		}
		if income[i].Type != income[j].Type {
			return income[i].Type < income[j].Type
		}
		if income[i].Asset != income[j].Asset {
			return income[i].Asset < income[j].Asset
		}
		return income[i].Symbol < income[j].Symbol
	})
	sort.Slice(fills, func(i, j int) bool {
		if fills[i].Trade.TradeID != fills[j].Trade.TradeID {
			return fills[i].Trade.TradeID < fills[j].Trade.TradeID
		}
		return fills[i].ClientOrderID < fills[j].ClientOrderID
	})
	sort.Slice(protection, func(i, j int) bool {
		if protection[i].Trade.TradeID != protection[j].Trade.TradeID {
			return protection[i].Trade.TradeID < protection[j].Trade.TradeID
		}
		if protection[i].GroupID != protection[j].GroupID {
			return protection[i].GroupID < protection[j].GroupID
		}
		return protection[i].Leg < protection[j].Leg
	})
	sort.Slice(records, func(i, j int) bool {
		return records[i].Plan.ClientOrderID < records[j].Plan.ClientOrderID
	})

	return json.Marshal(struct {
		WindowStart     int64
		WindowEnd       int64
		Portfolio       exchange.AccountPortfolio
		OpenOrders      []exchange.Order
		OpenAlgos       []exchange.AlgoOrder
		SignedOrders    []exchange.Order
		SignedTrades    []exchange.AccountTrade
		SignedAlgos     []exchange.AlgoOrder
		SignedIncome    []exchange.IncomeRecord
		LocalFills      []StoredFill
		ProtectionFills []ProtectionStoredFill
		LocalOrders     []Record
	}{b.WindowStartMS, b.WindowEndMS, portfolio, openOrders, openAlgos, orders, trades, algos, income, fills, protection, records})
}

// BuildAttestedAccounting computes an historical report only when bounded
// authenticated order/trade evidence exactly covers local evidence. It does
// NOT write files or manage SQL; callers must separately verify session and
// local fingerprint again before explicitly archiving the result.
func BuildAttestedAccounting(records []Record, fills []StoredFill, b exchange.RecoveryBaseline, now time.Time) (AccountingReport, error) {
	return buildAttestedAccounting(records, fills, nil, nil, b, now)
}

func buildAttestedAccounting(records []Record, fills []StoredFill, protection []ProtectionStoredFill, knownAlgos map[int64]protectionAccountingAlgo, b exchange.RecoveryBaseline, now time.Time) (AccountingReport, error) {
	if b.Environment != "demo" || b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		b.StartedAt.IsZero() || b.FinishedAt.IsZero() || b.FinishedAt.Before(b.StartedAt) ||
		b.FinishedAt.After(now.Add(time.Second)) || now.Sub(b.FinishedAt) > 20*time.Second ||
		b.WindowStartMS <= 0 || b.WindowEndMS != b.StartedAt.UnixMilli() ||
		b.WindowEndMS <= b.WindowStartMS || b.WindowEndMS-b.WindowStartMS > int64(6*24*time.Hour/time.Millisecond) {
		return AccountingReport{}, errors.New("신선하고 완전한 Demo 서명 이력 조회 범위가 필요합니다")
	}
	if err := exchange.ValidatePortfolioSnapshot(b.Portfolio); err != nil {
		return AccountingReport{}, errors.New("서명된 계정 스냅샷이 불완전합니다")
	}
	// This pure calculation must validate ALL immutable local inputs, not only
	// those inside the signed interval. Historical opening-cost corruption is
	// otherwise invisible to bounded trade reconciliation.
	seenRecords := make(map[string]bool, len(records))
	for _, r := range records {
		plan, pe := r.Intent.Plan()
		digest, de := digestIntent(r.Intent)
		if pe != nil || de != nil || !supportedJournalSchema(r.Schema) || r.Plan != plan || r.IntentDigest != digest || seenRecords[r.Plan.ClientOrderID] {
			return AccountingReport{}, errors.New("과거 주문 원본 또는 소유권 체크섬 손상")
		}
		seenRecords[r.Plan.ClientOrderID] = true
	}
	seenAllTrades := make(map[int64]bool, len(fills)+len(protection))
	for _, f := range fills {
		if f.Schema != 1 || f.Checksum == "" || f.Checksum != f.digest() || !seenRecords[f.ClientOrderID] || seenAllTrades[f.Trade.TradeID] {
			return AccountingReport{}, errors.New("기간 이전을 포함한 로컬 체결 손상 또는 중복")
		}
		seenAllTrades[f.Trade.TradeID] = true
	}
	for _, f := range protection {
		if f.Schema != 1 || f.Checksum == "" || f.Checksum != f.digest() || seenAllTrades[f.Trade.TradeID] {
			return AccountingReport{}, errors.New("기간 이전을 포함한 protection 체결 손상 또는 중복")
		}
		seenAllTrades[f.Trade.TradeID] = true
	}
	// Historical fills outside the signed window cannot be attributed to its
	// income. Require their replayed positions to be fully closed before the
	// window, then keep them out of this window's report.
	var historical, windowFills []StoredFill
	var historicalProtection, windowProtection []ProtectionStoredFill
	for _, f := range fills {
		if f.Trade.TimeMS < b.WindowStartMS {
			historical = append(historical, f)
		} else {
			windowFills = append(windowFills, f)
		}
	}
	for _, f := range protection {
		if f.Trade.TimeMS < b.WindowStartMS {
			historicalProtection = append(historicalProtection, f)
		} else {
			windowProtection = append(windowProtection, f)
		}
	}
	// Replay all old immutable fills to reconstruct the opening cost basis,
	// but do NOT mix their historical gross/fees into the selected window.
	// This is provisional LOCAL carry-forward, not historical signed proof.
	var opening map[ownerKey]*lot
	if len(historical) > 0 || len(historicalProtection) > 0 {
		prior, err := replayWithProtection(records, historical, historicalProtection)
		if err != nil {
			return AccountingReport{}, err
		}
		for key, priorLot := range prior {
			if priorLot.qty.Sign() == 0 {
				continue
			}
			if opening == nil {
				opening = make(map[ownerKey]*lot)
			}
			opening[key] = priorLot
		}
	}
	windowOrders := make(map[string]bool, len(b.RecentBTCOrders))
	for _, o := range b.RecentBTCOrders {
		windowOrders[o.ClientOrderID] = true
	}
	var windowRecords []Record
	for _, rec := range records {
		if windowOrders[rec.Plan.ClientOrderID] {
			windowRecords = append(windowRecords, rec)
		}
	}
	if err := verifyBoundedTradesWithProtection(windowRecords, windowFills, windowProtection, knownAlgos, b); err != nil {
		return AccountingReport{}, err
	}
	// The exchange pools same-side positions. Even a fully matched *bounded*
	// trade list cannot account for a position opened before that window. A
	// current actual-vs-virtual quantity mismatch is direct evidence that
	// attributing funding or open quantity to owners would be misleading.
	lots, err := replayWithProtection(records, fills, protection)
	if err != nil {
		return AccountingReport{}, err
	}
	actual := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, p := range b.Portfolio.Positions {
		q, qe := signedRat(string(p.PositionAmt))
		if qe != nil {
			return AccountingReport{}, errors.New("계정 포지션 수량 읽기 실패")
		}
		if p.Symbol != "BTCUSDT" {
			if q.Sign() != 0 {
				return AccountingReport{}, errors.New("다른 종목의 실제 포지션이 존재하여 소유자 귀속이 불완전합니다")
			}
			continue
		}
		if p.PositionSide == "BOTH" {
			if q.Sign() != 0 {
				return AccountingReport{}, errors.New("단방향 모드의 잔여 포지션이 있습니다")
			}
			continue
		}
		side := PositionSide(p.PositionSide)
		if side == Long && q.Sign() < 0 || side == Short && q.Sign() > 0 {
			return AccountingReport{}, errors.New("거래소 Hedge 방향의 수량 부호가 잘못되었습니다")
		}
		actual[side].Add(actual[side], new(big.Rat).Abs(q))
	}
	owned := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for k, l := range lots {
		if l.qty.Sign() != 0 {
			owned[k.side].Add(owned[k.side], l.qty)
		}
	}
	if actual[Long].Cmp(owned[Long]) != 0 || actual[Short].Cmp(owned[Short]) != 0 {
		return AccountingReport{}, errors.New("현재 BTC 방향별 거래소 포지션과 로컬 소유 수량이 불일치합니다")
	}
	report, err := evaluateAccountingOffline(windowRecords, windowFills, windowProtection, AccountingInput{
		WindowStartMS: b.WindowStartMS, WindowEndMS: b.WindowEndMS, SignedIncome: b.RecentIncome,
	}, now, opening)
	if err != nil {
		return AccountingReport{}, err
	}
	// A local digest of precisely compared evidence is useful for audits, but
	// cannot turn a REST sample into an unbroken or cryptographically signed
	// stream. Old (unattested) report schema remains readable.
	raw, err := canonicalAccountingEvidenceJSON(b, windowFills, windowProtection, windowRecords)
	if err != nil {
		return AccountingReport{}, err
	}
	digest := sha256.Sum256(raw)
	report.EvidenceCoverage = "BOUNDED_DEMO_REST_MATCHED_LOCAL_NOT_CONTINUOUS"
	if len(opening) > 0 {
		report.EvidenceCoverage = "BOUNDED_DEMO_REST_MATCHED_LOCAL_WITH_UNATTESTED_OPENING_LOTS_NOT_CONTINUOUS"
	}
	report.EvidenceSHA256 = hex.EncodeToString(digest[:])
	report.Warnings = append(report.Warnings, "이 보고서는 지정된 최대 6일 구간의 Demo 서명 REST 체결과 로컬 장부만 일치합니다. 구간 밖 거래 및 계정 이벤트 누락은 검증하지 않습니다.")
	report.seal()
	return report, nil
}

// PrepareAttestedAccounting wraps the signed read in full raw local directory
// before/after checks. It returns a report only; no order or archive mutation.
func PrepareAttestedAccounting(ctx context.Context, dataDir string, reader AccountingWindowReader, creds DemoCredentials,
	fromMS int64, clock func() time.Time) (AccountingReport, string, error) {
	if dataDir == "" || reader == nil || creds.APIKey == "" || creds.Secret == "" || fromMS <= 0 {
		return AccountingReport{}, "", errors.New("Demo 서명 조회와 명시적인 조회 기간이 필요합니다")
	}
	if clock == nil {
		clock = time.Now
	}
	before, err := LocalAccountingFingerprint(dataDir)
	if err != nil {
		return AccountingReport{}, "", err
	}
	b, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return AccountingReport{}, "", err
	}
	if err := ctx.Err(); err != nil {
		return AccountingReport{}, "", err
	}
	after, err := LocalAccountingFingerprint(dataDir)
	if err != nil || after != before {
		return AccountingReport{}, "", errors.New("서명 조회 중 주문·체결 로컬 증적이 바뀌었거나 손상되었습니다")
	}
	store, err := NewOwnershipStore(dataDir)
	if err != nil {
		return AccountingReport{}, "", err
	}
	store.mu.Lock()
	records, err := store.ScanJournal()
	if err != nil {
		store.mu.Unlock()
		return AccountingReport{}, "", err
	}
	fills, err := store.readFills()
	protection, protectionErr := readValidatedProtectionFillsForStore(filepath.Dir(store.fillDir), fills)
	store.mu.Unlock()
	if err != nil {
		return AccountingReport{}, "", err
	}
	if protectionErr != nil {
		return AccountingReport{}, "", protectionErr
	}
	knownAlgos, err := loadProtectionAccountingAlgos(filepath.Dir(store.fillDir), protection)
	if err != nil {
		return AccountingReport{}, "", err
	}
	report, err := buildAttestedAccounting(records, fills, protection, knownAlgos, b, clock().UTC())
	if err != nil {
		return AccountingReport{}, "", err
	}
	final, err := LocalAccountingFingerprint(dataDir)
	if err != nil || final != before {
		return AccountingReport{}, "", errors.New("보고서 생성 도중 로컬 장부가 변했습니다")
	}
	return report, before, nil
}
