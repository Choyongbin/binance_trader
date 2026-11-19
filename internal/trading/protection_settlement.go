package trading

// Restart-safe, read-only reconciliation for already-submitted protective
// conditional orders. This file has no POST/DELETE capability. It imports
// signed actual-order fills into a separate immutable protection ledger and
// reconciles those exits against the pooled Hedge-side account position before
// sealing a protection group as settled.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

type ProtectionSettlementStage string

const (
	ProtectionSettlementPending        ProtectionSettlementStage = "PENDING_SIGNED_RECONCILIATION"
	ProtectionSettlementFillsImported  ProtectionSettlementStage = "PROTECTION_FILLS_IMPORTED"
	ProtectionSettlementSideReconciled ProtectionSettlementStage = "POOLED_SIDE_RECONCILED"
	ProtectionSettlementSettled        ProtectionSettlementStage = "SETTLED"
)

type ProtectionSettlementReader interface {
	DemoTradeReader
	SignedProtectionReader
	Fetch(context.Context, string, string) (exchange.Snapshot, error)
}

type ProtectionSettlementResult struct {
	GroupID            string                    `json:"group_id"`
	InitialStage       ProtectionSettlementStage `json:"initial_stage"`
	FinalStage         ProtectionSettlementStage `json:"final_stage"`
	Progressed         bool                      `json:"progressed"`
	ImportedFillCount  int                       `json:"imported_fill_count"`
	OwnerRemainingQty  string                    `json:"owner_remaining_qty,omitempty"`
	ExchangeSideQty    string                    `json:"exchange_side_qty,omitempty"`
	PendingReason      string                    `json:"pending_reason,omitempty"`
	TradingAuthorized  bool                      `json:"trading_authorized"` // ALWAYS false
	ManualReviewNeeded bool                      `json:"manual_review_required"`
}

type ProtectionStoredFill struct {
	Schema        int                   `json:"schema"`
	GroupID       string                `json:"group_id"`
	PlanDigest    string                `json:"plan_digest"`
	Leg           string                `json:"leg"`
	Source        Source                `json:"source"`
	ModelID       string                `json:"model_id,omitempty"`
	PositionSide  PositionSide          `json:"position_side"`
	AlgoID        int64                 `json:"algo_id"`
	ActualOrderID int64                 `json:"actual_order_id"`
	Trade         exchange.AccountTrade `json:"trade"`
	Checksum      string                `json:"checksum"`
}

func (f ProtectionStoredFill) digest() string {
	f.Checksum = ""
	raw, _ := json.Marshal(f)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type protectionSettlementMarker struct {
	Schema            int                       `json:"schema"`
	GroupID           string                    `json:"group_id"`
	PlanDigest        string                    `json:"plan_digest"`
	Stage             ProtectionSettlementStage `json:"stage"`
	FillCount         int                       `json:"fill_count"`
	FillDigest        string                    `json:"fill_digest"`
	OwnerRemainingQty string                    `json:"owner_remaining_qty,omitempty"`
	ExchangeSideQty   string                    `json:"exchange_side_qty,omitempty"`
	SnapshotAsOf      string                    `json:"snapshot_as_of,omitempty"`
	PreviousChecksum  string                    `json:"previous_checksum,omitempty"`
	CreatedAt         string                    `json:"created_at"`
	Checksum          string                    `json:"checksum"`
}

func (m protectionSettlementMarker) digest() string {
	m.Checksum = ""
	raw, _ := json.Marshal(m)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type ProtectionSettlementStore struct {
	mu       sync.Mutex
	root     string
	fillDir  string
	stageDir string
}

func NewProtectionSettlementStore(dataDir string) (*ProtectionSettlementStore, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("데이터 디렉터리가 필요합니다")
	}
	root := filepath.Join(dataDir, "trading")
	fills := filepath.Join(root, "protection_fills")
	stages := filepath.Join(root, "protection_settlements")
	if err := os.MkdirAll(fills, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(stages, 0700); err != nil {
		return nil, err
	}
	return &ProtectionSettlementStore{root: root, fillDir: fills, stageDir: stages}, nil
}

func protectionFillName(tradeID int64) string { return fmt.Sprintf("BTCUSDT_%d.json", tradeID) }

func readProtectionFillsDir(dir string) ([]ProtectionStoredFill, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []ProtectionStoredFill{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]ProtectionStoredFill, 0, len(entries))
	seenTrade := make(map[int64]bool)
	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 || !strings.HasPrefix(e.Name(), "BTCUSDT_") || !strings.HasSuffix(e.Name(), ".json") {
			return nil, fmt.Errorf("비정상 protection fill 파일: %s", e.Name())
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var f ProtectionStoredFill
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&f); err != nil {
			return nil, fmt.Errorf("protection fill 손상: %w", err)
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, errors.New("protection fill에 예상하지 못한 추가 JSON이 있습니다")
		}
		if f.Schema != 1 || f.GroupID == "" || f.PlanDigest == "" || (f.Leg != "tp" && f.Leg != "sl") ||
			(f.Source != Manual && f.Source != Model) || (f.Source == Manual && f.ModelID != "") ||
			(f.Source == Model && !modelIDPattern.MatchString(f.ModelID)) || (f.PositionSide != Long && f.PositionSide != Short) ||
			f.AlgoID <= 0 || f.Trade.Symbol != "BTCUSDT" || f.Trade.TradeID <= 0 || f.Trade.TimeMS <= 0 ||
			f.ActualOrderID <= 0 || f.Trade.OrderID != f.ActualOrderID || f.Trade.PositionSide != string(f.PositionSide) ||
			e.Name() != protectionFillName(f.Trade.TradeID) || f.Checksum == "" || f.Checksum != f.digest() || seenTrade[f.Trade.TradeID] {
			return nil, errors.New("protection fill 무결성 오류")
		}
		wantSide := "SELL"
		if f.PositionSide == Short {
			wantSide = "BUY"
		}
		if f.Trade.Side != wantSide || f.Trade.CommissionAsset == "" {
			return nil, errors.New("protection fill 방향 또는 수수료 자산 오류")
		}
		if _, err := positiveRat(f.Trade.Quantity); err != nil {
			return nil, errors.New("protection fill 수량 오류")
		}
		if _, err := positiveRat(f.Trade.Price); err != nil {
			return nil, errors.New("protection fill 가격 오류")
		}
		if _, err := signedRat(f.Trade.Commission); err != nil {
			return nil, errors.New("protection fill 수수료 오류")
		}
		if _, err := signedRat(f.Trade.RealizedPnL); err != nil {
			return nil, errors.New("protection fill 실현손익 오류")
		}
		seenTrade[f.Trade.TradeID] = true
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Trade.TimeMS != out[j].Trade.TimeMS {
			return out[i].Trade.TimeMS < out[j].Trade.TimeMS
		}
		return out[i].Trade.TradeID < out[j].Trade.TradeID
	})
	return out, nil
}

func (s *ProtectionSettlementStore) readProtectionFillsLocked() ([]ProtectionStoredFill, error) {
	return readProtectionFillsDir(s.fillDir)
}

func protectionFillSetDigest(fills []ProtectionStoredFill) string {
	h := sha256.New()
	sorted := append([]ProtectionStoredFill(nil), fills...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Trade.TradeID < sorted[j].Trade.TradeID })
	for _, f := range sorted {
		h.Write([]byte(f.Checksum))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validateProtectionTrade(plan ProtectionDispatchPlan, leg string, o exchange.DemoAlgoResult, t exchange.AccountTrade) error {
	instruction := plan.TP
	if leg == "sl" {
		instruction = plan.SL
	}
	id, err := strconv.ParseInt(o.ActualOrderID, 10, 64)
	if err != nil || id <= 0 || t.OrderID != id || t.Symbol != instruction.Symbol || t.Side != instruction.Side || t.PositionSide != string(instruction.PositionSide) || t.TradeID <= 0 || t.TimeMS <= 0 {
		return errors.New("조건부 actual order 체결 ID·방향·종목 불일치")
	}
	if _, err := positiveRat(t.Quantity); err != nil {
		return errors.New("조건부 체결 수량 오류")
	}
	if _, err := positiveRat(t.Price); err != nil {
		return errors.New("조건부 체결 가격 오류")
	}
	if _, err := signedRat(t.Commission); err != nil {
		return errors.New("조건부 체결 수수료 오류")
	}
	if _, err := signedRat(t.RealizedPnL); err != nil {
		return errors.New("조건부 체결 실현손익 오류")
	}
	if t.CommissionAsset == "" {
		return errors.New("조건부 체결 수수료 자산 누락")
	}
	return nil
}

func validateProtectionAlgo(plan ProtectionDispatchPlan, leg string, algoID int64, o exchange.DemoAlgoResult) error {
	instruction := plan.TP
	if leg == "sl" {
		instruction = plan.SL
	}
	if o.AlgoID != algoID || o.ClientAlgoID != instruction.ClientAlgoID || o.Symbol != instruction.Symbol ||
		o.Side != instruction.Side || o.PositionSide != string(instruction.PositionSide) || o.Type != instruction.OrderType ||
		o.AlgoType != instruction.AlgoType || o.WorkingType != instruction.WorkingType || o.ClosePosition || o.ReduceOnly {
		return errors.New("조건부 주문 signed GET이 보호 계획과 일치하지 않습니다")
	}
	q, err := decimalRat(string(o.Quantity), false)
	want, err2 := decimalRat(instruction.Quantity, false)
	if err != nil || err2 != nil || q.Cmp(want) != 0 {
		return errors.New("조건부 주문 수량 불일치")
	}
	trigger, err := decimalRat(string(o.TriggerPrice), false)
	expectedTrigger, err2 := decimalRat(instruction.TriggerPrice, false)
	if err != nil || err2 != nil || trigger.Cmp(expectedTrigger) != 0 {
		return errors.New("조건부 주문 trigger price 불일치")
	}
	if !validKnownProtectionStatus(o.Status) {
		return errors.New("알 수 없는 조건부 주문 상태")
	}
	actual, err := decimalRat(string(o.ActualQty), true)
	if err != nil || actual.Cmp(want) > 0 {
		return errors.New("조건부 actualQty 오류")
	}
	if actual.Sign() > 0 && strings.TrimSpace(o.ActualOrderID) == "" {
		return errors.New("조건부 실제 체결 수량은 있으나 actualOrderId가 없습니다")
	}
	return nil
}

type protectionLegRead struct {
	leg    string
	algoID int64
	first  exchange.DemoAlgoResult
	second exchange.DemoAlgoResult
	trades []exchange.AccountTrade
}

func terminalProtectionEvidence(status string) bool {
	switch status {
	case "FINISHED", "CANCELED", "CANCELLED", "EXPIRED":
		return true
	default:
		return false
	}
}

func (s *ProtectionSettlementStore) collectStableLegEvidence(ctx context.Context, plan ProtectionDispatchPlan, submissions *ProtectionSubmissionStore, reader ProtectionSettlementReader, creds DemoCredentials) ([]protectionLegRead, string, error) {
	reads := make([]protectionLegRead, 0, 2)
	for _, leg := range []string{"tp", "sl"} {
		state, err := submissions.Inspect(plan.GroupID, leg)
		if err != nil {
			return nil, "", err
		}
		if !state.MayHaveSent || !state.SignedObserved || state.ObservedAlgoID <= 0 {
			return nil, "보호 레그의 MAY_HAVE_SENT + signed active observation이 모두 필요합니다", nil
		}
		first, err := reader.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, state.ObservedAlgoID)
		if err != nil {
			return nil, "", fmt.Errorf("%s signed GET 실패: %w", leg, err)
		}
		if err := validateProtectionAlgo(plan, leg, state.ObservedAlgoID, first); err != nil {
			return nil, "", fmt.Errorf("%s: %w", leg, err)
		}
		if first.Status == "REJECTED" {
			return nil, "", fmt.Errorf("%s 보호 주문이 REJECTED 상태입니다", leg)
		}
		actual, _ := decimalRat(string(first.ActualQty), true)
		trades := []exchange.AccountTrade{}
		if actual.Sign() > 0 {
			orderID, err := strconv.ParseInt(first.ActualOrderID, 10, 64)
			if err != nil || orderID <= 0 {
				return nil, "", fmt.Errorf("%s actualOrderId 오류", leg)
			}
			trades, err = reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, "BTCUSDT", orderID)
			if err != nil {
				return nil, "", fmt.Errorf("%s actual order 체결 조회 실패: %w", leg, err)
			}
			sum := new(big.Rat)
			seen := make(map[int64]bool)
			for _, t := range trades {
				if seen[t.TradeID] {
					return nil, "", errors.New("조건부 체결 ID 중복")
				}
				seen[t.TradeID] = true
				if err := validateProtectionTrade(plan, leg, first, t); err != nil {
					return nil, "", err
				}
				q, _ := positiveRat(t.Quantity)
				sum.Add(sum, q)
			}
			if sum.Cmp(actual) != 0 {
				return nil, "", fmt.Errorf("%s actualQty와 userTrades 합계 불일치", leg)
			}
		}
		second, err := reader.QueryDemoAlgo(ctx, creds.APIKey, creds.Secret, state.ObservedAlgoID)
		if err != nil {
			return nil, "", fmt.Errorf("%s second signed GET 실패: %w", leg, err)
		}
		if err := validateProtectionAlgo(plan, leg, state.ObservedAlgoID, second); err != nil {
			return nil, "", fmt.Errorf("%s second: %w", leg, err)
		}
		if first.Status != second.Status || first.ActualOrderID != second.ActualOrderID || string(first.ActualQty) != string(second.ActualQty) {
			return nil, "조건부 주문 상태가 체결 조회 중 변경되었습니다. 재조회가 필요합니다", nil
		}
		reads = append(reads, protectionLegRead{leg: leg, algoID: state.ObservedAlgoID, first: first, second: second, trades: trades})
	}
	for _, r := range reads {
		if r.second.Status == "TRIGGERED" || r.second.Status == "TRIGGERING" || r.second.Status == "NEW" || r.second.Status == "WORKING" {
			return reads, "보호 주문 중 하나가 아직 종료 상태가 아닙니다", nil
		}
		if !terminalProtectionEvidence(r.second.Status) {
			return nil, "", errors.New("보호 주문 terminal 상태를 확정할 수 없습니다")
		}
	}
	return reads, "", nil
}

func (s *ProtectionSettlementStore) persistProtectionFillsLocked(plan ProtectionDispatchPlan, reads []protectionLegRead, regular []StoredFill) (int, []ProtectionStoredFill, error) {
	regularIDs := make(map[int64]bool, len(regular))
	for _, f := range regular {
		regularIDs[f.Trade.TradeID] = true
	}
	current, err := s.readProtectionFillsLocked()
	if err != nil {
		return 0, nil, err
	}
	existing := make(map[int64]ProtectionStoredFill, len(current))
	for _, f := range current {
		if regularIDs[f.Trade.TradeID] {
			return 0, nil, errors.New("일반 체결 장부와 protection 체결 장부의 tradeId가 충돌합니다")
		}
		existing[f.Trade.TradeID] = f
	}
	staged := make([]ProtectionStoredFill, 0)
	groupQty := new(big.Rat)
	for _, r := range reads {
		for _, t := range r.trades {
			if regularIDs[t.TradeID] {
				return 0, nil, errors.New("조건부 체결 tradeId가 일반 체결 장부와 중복됩니다")
			}
			f := ProtectionStoredFill{Schema: 1, GroupID: plan.GroupID, PlanDigest: plan.Digest, Leg: r.leg,
				Source: plan.Source, ModelID: plan.ModelID, PositionSide: plan.PositionSide, AlgoID: r.algoID, Trade: t, ActualOrderID: t.OrderID}
			f.Checksum = f.digest()
			if prior, ok := existing[t.TradeID]; ok {
				if prior != f {
					return 0, nil, errors.New("기존 protection fill과 새 signed evidence가 충돌합니다")
				}
			} else {
				staged = append(staged, f)
			}
			q, _ := positiveRat(t.Quantity)
			groupQty.Add(groupQty, q)
		}
	}
	ownerQty, _ := positiveRat(plan.TP.Quantity)
	if groupQty.Sign() == 0 {
		return 0, nil, errors.New("종료된 보호 그룹에 실제 체결이 없습니다: 자동 settle 불가")
	}
	if groupQty.Cmp(ownerQty) != 0 {
		if groupQty.Cmp(ownerQty) > 0 {
			return 0, nil, errors.New("두 보호 레그의 합산 체결이 원래 소유 수량을 초과했습니다: pooled-side 과청산 가능성")
		}
		return 0, nil, errors.New("보호 주문 합산 체결이 원래 소유 수량보다 작습니다: 잔여 포지션 수동 보호가 필요합니다")
	}
	added := 0
	for _, f := range staged {
		raw, _ := json.MarshalIndent(f, "", "  ")
		if err := exclusiveDurable(filepath.Join(s.fillDir, protectionFillName(f.Trade.TradeID)), raw); err != nil {
			return added, nil, fmt.Errorf("protection fill 저장 실패: %w", err)
		}
		added++
	}
	all, err := s.readProtectionFillsLocked()
	return added, all, err
}

func replayWithProtection(records []Record, regular []StoredFill, protection []ProtectionStoredFill) (map[ownerKey]*lot, error) {
	byID := make(map[string]Record, len(records))
	for _, r := range records {
		if _, exists := byID[r.Plan.ClientOrderID]; exists {
			return nil, errors.New("중복 주문 ID")
		}
		byID[r.Plan.ClientOrderID] = r
	}
	type event struct {
		when       int64
		tradeID    int64
		regular    *StoredFill
		protection *ProtectionStoredFill
	}
	events := make([]event, 0, len(regular)+len(protection))
	for i := range regular {
		f := regular[i]
		events = append(events, event{when: f.Trade.TimeMS, tradeID: f.Trade.TradeID, regular: &regular[i]})
	}
	for i := range protection {
		f := protection[i]
		events = append(events, event{when: f.Trade.TimeMS, tradeID: f.Trade.TradeID, protection: &protection[i]})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].when != events[j].when {
			return events[i].when < events[j].when
		}
		return events[i].tradeID < events[j].tradeID
	})
	owners := make(map[ownerKey]*lot)
	perOrder := make(map[string]*big.Rat)
	seenTrades := make(map[int64]bool)
	for _, ev := range events {
		if ev.tradeID <= 0 || ev.when <= 0 || seenTrades[ev.tradeID] {
			return nil, errors.New("체결 ID/시각 중복 또는 오류")
		}
		seenTrades[ev.tradeID] = true
		if ev.regular != nil {
			f := *ev.regular
			r, ok := byID[f.ClientOrderID]
			if !ok {
				return nil, fmt.Errorf("소유권을 알 수 없는 체결: %d", f.Trade.TradeID)
			}
			if err := validateFill(r, f.Trade); err != nil {
				return nil, err
			}
			k := buildOwnerKey(r)
			l := owners[k]
			if l == nil {
				l = newLot()
				owners[k] = l
			}
			qty, _ := positiveRat(f.Trade.Quantity)
			price, _ := positiveRat(f.Trade.Price)
			fee, _ := signedRat(f.Trade.Commission)
			exPnL, _ := signedRat(f.Trade.RealizedPnL)
			if perOrder[f.ClientOrderID] == nil {
				perOrder[f.ClientOrderID] = new(big.Rat)
			}
			perOrder[f.ClientOrderID].Add(perOrder[f.ClientOrderID], qty)
			max, _ := positiveRat(r.Plan.Quantity)
			if perOrder[f.ClientOrderID].Cmp(max) > 0 {
				return nil, errors.New("하나의 주문에 주문 수량보다 많은 체결이 있습니다")
			}
			if r.Intent.Action == Enter {
				total := new(big.Rat).Add(l.qty, qty)
				cost := new(big.Rat).Add(new(big.Rat).Mul(l.avg, l.qty), new(big.Rat).Mul(price, qty))
				l.avg.Quo(cost, total)
				l.qty.Set(total)
			} else {
				if qty.Cmp(l.qty) > 0 {
					return nil, errors.New("소유자의 미보유 수량을 청산하는 체결이 발견되었습니다")
				}
				diff := new(big.Rat).Sub(price, l.avg)
				if r.Intent.PositionSide == Short {
					diff.Neg(diff)
				}
				l.gross.Add(l.gross, new(big.Rat).Mul(diff, qty))
				l.qty.Sub(l.qty, qty)
				if l.qty.Sign() == 0 {
					l.avg.SetInt64(0)
				}
			}
			l.exchangeGross.Add(l.exchangeGross, exPnL)
			if l.fees[f.Trade.CommissionAsset] == nil {
				l.fees[f.Trade.CommissionAsset] = new(big.Rat)
			}
			l.fees[f.Trade.CommissionAsset].Add(l.fees[f.Trade.CommissionAsset], fee)
			continue
		}

		f := *ev.protection
		if f.Source != Manual && f.Source != Model || f.PositionSide != Long && f.PositionSide != Short || f.Trade.Symbol != "BTCUSDT" ||
			f.Trade.PositionSide != string(f.PositionSide) || f.ActualOrderID <= 0 || f.Trade.OrderID != f.ActualOrderID {
			return nil, errors.New("protection fill 소유권 메타데이터 오류")
		}
		k := ownerKey{source: f.Source, model: f.ModelID, side: f.PositionSide, symbol: "BTCUSDT"}
		l := owners[k]
		if l == nil {
			return nil, errors.New("protection fill 시점에 해당 소유자의 포지션이 없습니다")
		}
		q, err := positiveRat(f.Trade.Quantity)
		if err != nil || q.Cmp(l.qty) > 0 {
			return nil, errors.New("protection fill이 해당 시점 소유자의 잔여 수량을 초과합니다")
		}
		price, err := positiveRat(f.Trade.Price)
		if err != nil {
			return nil, err
		}
		fee, err := signedRat(f.Trade.Commission)
		if err != nil {
			return nil, err
		}
		exPnL, err := signedRat(f.Trade.RealizedPnL)
		if err != nil {
			return nil, err
		}
		diff := new(big.Rat).Sub(price, l.avg)
		if f.PositionSide == Short {
			diff.Neg(diff)
		}
		l.gross.Add(l.gross, new(big.Rat).Mul(diff, q))
		l.exchangeGross.Add(l.exchangeGross, exPnL)
		if l.fees[f.Trade.CommissionAsset] == nil {
			l.fees[f.Trade.CommissionAsset] = new(big.Rat)
		}
		l.fees[f.Trade.CommissionAsset].Add(l.fees[f.Trade.CommissionAsset], fee)
		l.qty.Sub(l.qty, q)
		if l.qty.Sign() == 0 {
			l.avg.SetInt64(0)
		}
	}
	return owners, nil
}

func applyProtectionFills(records []Record, regular []StoredFill, protection []ProtectionStoredFill) (map[ownerKey]*lot, error) {
	return replayWithProtection(records, regular, protection)
}

func validateKnownOpenProtectionAlgos(snapshot exchange.Snapshot, submissions *ProtectionSubmissionStore, current ProtectionDispatchPlan) error {
	submissions.mu.Lock()
	plans, err := submissions.scanPlansLocked()
	if err == nil {
		plans, _, err = activeProtectionPlansLocked(submissions, plans)
	}
	if err == nil {
		if review := ReviewProtectionArmingPlans(plans); review.State == ProtectionArmingPooledConflict {
			err = ErrPooledProtectionConflict
		}
	}
	type expected struct {
		instruction ProtectionInstruction
		group       string
		algoID      int64
	}
	known := make(map[string]expected)
	if err == nil {
		for _, p := range plans {
			for _, leg := range []string{"tp", "sl"} {
				instruction := p.TP
				if leg == "sl" {
					instruction = p.SL
				}
				if _, dup := known[instruction.ClientAlgoID]; dup {
					err = errors.New("protection clientAlgoId 중복")
					break
				}
				state, stateErr := submissions.inspectLocked(p.GroupID, leg)
				if stateErr != nil {
					err = stateErr
					break
				}
				algoID := int64(0)
				if state.SignedObserved {
					algoID = state.ObservedAlgoID
				}
				known[instruction.ClientAlgoID] = expected{instruction: instruction, group: p.GroupID, algoID: algoID}
			}
			if err != nil {
				break
			}
		}
	}
	submissions.mu.Unlock()
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, a := range snapshot.AlgoOrders {
		e, ok := known[a.ClientAlgoID]
		if !ok || seen[a.ClientAlgoID] {
			return errors.New("소유권을 확인할 수 없는 open algo order가 존재합니다")
		}
		seen[a.ClientAlgoID] = true
		q, e1 := decimalRat(a.Quantity, false)
		wq, e2 := decimalRat(e.instruction.Quantity, false)
		tp, e3 := decimalRat(a.TriggerPrice, false)
		wtp, e4 := decimalRat(e.instruction.TriggerPrice, false)
		if e.algoID <= 0 || a.AlgoID != e.algoID || a.Symbol != e.instruction.Symbol || a.Side != e.instruction.Side || a.PositionSide != string(e.instruction.PositionSide) ||
			a.OrderType != e.instruction.OrderType || a.WorkingType != e.instruction.WorkingType || a.ClosePosition ||
			e1 != nil || e2 != nil || e3 != nil || e4 != nil || q.Cmp(wq) != 0 || tp.Cmp(wtp) != 0 {
			return errors.New("open algo order가 저장된 protection plan과 일치하지 않습니다")
		}
		if e.group == current.GroupID {
			return errors.New("settle 대상 보호 그룹의 레그가 아직 openAlgoOrders에 남아 있습니다")
		}
	}
	return nil
}

func actualSideQty(snapshot exchange.Snapshot, side PositionSide) (string, error) {
	total := new(big.Rat)
	for _, p := range snapshot.Positions {
		if p.Symbol != "BTCUSDT" {
			continue
		}
		if p.Side != string(side) {
			continue
		}
		q, err := signedRat(p.Qty)
		if err != nil {
			return "", err
		}
		if side == Short {
			q.Abs(q)
		} else if q.Sign() < 0 {
			return "", errors.New("LONG 포지션 부호 오류")
		}
		total.Add(total, q)
	}
	return formatOwnershipDecimal(total), nil
}

func (s *ProtectionSettlementStore) auditAdjustedOwnership(ctx context.Context, plan ProtectionDispatchPlan, submissions *ProtectionSubmissionStore, owners *OwnershipStore, reader ProtectionSettlementReader, creds DemoCredentials, allProtection []ProtectionStoredFill) (string, string, string, error) {
	observations, err := owners.CollectDemoOrderObservations(ctx, reader, creds)
	if err != nil {
		return "", "", "", err
	}
	snapshot, err := reader.Fetch(ctx, creds.APIKey, creds.Secret)
	if err != nil {
		return "", "", "", err
	}
	if err := validateKnownOpenProtectionAlgos(snapshot, submissions, plan); err != nil {
		return "", "", "", err
	}
	auditSnapshot := snapshot
	// Protection algos were validated above. OwnershipStore is intentionally
	// unaware of live conditional orders, so remove only that already-validated
	// inventory while preserving the real post-protection account positions.
	auditSnapshot.AlgoOrders = []exchange.AlgoOrder{}
	now := time.Now().UTC()
	report, err := owners.audit(auditSnapshot, observations, now, plan.GroupID)
	if err != nil {
		return "", "", "", err
	}
	if !report.ReadyForAssessment {
		return "", "", "", fmt.Errorf("일반 소유권 감사 실패: %s", strings.Join(report.Blockers, "; "))
	}

	records, err := owners.ScanJournal()
	if err != nil {
		return "", "", "", err
	}
	regular, err := owners.readFills()
	if err != nil {
		return "", "", "", err
	}
	lots, err := applyProtectionFills(records, regular, allProtection)
	if err != nil {
		return "", "", "", err
	}
	k := ownerKey{source: plan.Source, model: plan.ModelID, side: plan.PositionSide, symbol: "BTCUSDT"}
	l := lots[k]
	if l == nil {
		return "", "", "", errors.New("settle 대상 owner가 소유권 장부에 없습니다")
	}
	remaining := formatOwnershipDecimal(l.qty)
	if remaining != "0" {
		return "", "", "", errors.New("보호 체결 이후 대상 owner 잔량이 0이 아닙니다")
	}
	sideQty, err := actualSideQty(snapshot, plan.PositionSide)
	if err != nil {
		return "", "", "", err
	}
	return remaining, sideQty, snapshot.AsOf, nil
}

func (s *ProtectionSettlementStore) markerPath(group string, stage ProtectionSettlementStage) string {
	suffix := "fills"
	if stage == ProtectionSettlementSideReconciled {
		suffix = "reconciled"
	}
	if stage == ProtectionSettlementSettled {
		suffix = "settled"
	}
	return filepath.Join(s.stageDir, group+"."+suffix+".json")
}

func (s *ProtectionSettlementStore) readMarkerLocked(group string, stage ProtectionSettlementStage, plan ProtectionDispatchPlan) (protectionSettlementMarker, bool, error) {
	path := s.markerPath(group, stage)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return protectionSettlementMarker{}, false, nil
	}
	if err != nil {
		return protectionSettlementMarker{}, false, err
	}
	var m protectionSettlementMarker
	if json.Unmarshal(raw, &m) != nil || m.Schema != 1 || m.GroupID != group || m.PlanDigest != plan.Digest || m.Stage != stage ||
		m.CreatedAt == "" || m.FillCount <= 0 || m.FillDigest == "" || m.Checksum == "" || m.Checksum != m.digest() {
		return protectionSettlementMarker{}, false, errors.New("protection settlement marker 손상")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err != nil {
		return protectionSettlementMarker{}, false, errors.New("protection settlement marker 시각 손상")
	}
	switch stage {
	case ProtectionSettlementFillsImported:
		if m.PreviousChecksum != "" || m.OwnerRemainingQty != "" || m.ExchangeSideQty != "" || m.SnapshotAsOf != "" {
			return protectionSettlementMarker{}, false, errors.New("fill-import marker에 허용되지 않는 필드가 있습니다")
		}
	case ProtectionSettlementSideReconciled, ProtectionSettlementSettled:
		if m.PreviousChecksum == "" || m.OwnerRemainingQty == "" || m.ExchangeSideQty == "" || m.SnapshotAsOf == "" {
			return protectionSettlementMarker{}, false, errors.New("reconciliation marker 필수 필드가 누락되었습니다")
		}
		if _, err := time.Parse(time.RFC3339Nano, m.SnapshotAsOf); err != nil {
			return protectionSettlementMarker{}, false, errors.New("reconciliation snapshot 시각 손상")
		}
	default:
		return protectionSettlementMarker{}, false, errors.New("알 수 없는 protection settlement marker stage")
	}
	return m, true, nil
}

func sameMarkerEvidence(a, b protectionSettlementMarker) bool {
	return a.GroupID == b.GroupID && a.PlanDigest == b.PlanDigest && a.Stage == b.Stage && a.FillCount == b.FillCount && a.FillDigest == b.FillDigest &&
		a.OwnerRemainingQty == b.OwnerRemainingQty && a.ExchangeSideQty == b.ExchangeSideQty && a.PreviousChecksum == b.PreviousChecksum
}

func (s *ProtectionSettlementStore) writeMarkerLocked(m protectionSettlementMarker) (protectionSettlementMarker, error) {
	m.Schema = 1
	m.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	m.Checksum = m.digest()
	raw, _ := json.MarshalIndent(m, "", "  ")
	path := s.markerPath(m.GroupID, m.Stage)
	if err := exclusiveDurable(path, raw); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return protectionSettlementMarker{}, err
		}
		existingRaw, readErr := os.ReadFile(path)
		if readErr != nil {
			return protectionSettlementMarker{}, readErr
		}
		var existing protectionSettlementMarker
		if json.Unmarshal(existingRaw, &existing) != nil || existing.Checksum == "" || existing.Checksum != existing.digest() || !sameMarkerEvidence(existing, m) {
			return protectionSettlementMarker{}, errors.New("동시 protection settlement marker가 현재 evidence와 충돌합니다")
		}
		return existing, nil
	}
	return m, nil
}

func (s *ProtectionSettlementStore) currentStageLocked(group string, plan ProtectionDispatchPlan) (ProtectionSettlementStage, protectionSettlementMarker, error) {
	fills, hasFills, err := s.readMarkerLocked(group, ProtectionSettlementFillsImported, plan)
	if err != nil {
		return ProtectionSettlementPending, protectionSettlementMarker{}, err
	}
	rec, hasRec, err := s.readMarkerLocked(group, ProtectionSettlementSideReconciled, plan)
	if err != nil {
		return ProtectionSettlementPending, protectionSettlementMarker{}, err
	}
	settled, hasSettled, err := s.readMarkerLocked(group, ProtectionSettlementSettled, plan)
	if err != nil {
		return ProtectionSettlementPending, protectionSettlementMarker{}, err
	}
	if hasSettled {
		if !hasFills || !hasRec || settled.PreviousChecksum != rec.Checksum || rec.PreviousChecksum != fills.Checksum {
			return ProtectionSettlementPending, protectionSettlementMarker{}, errors.New("protection settlement chain 손상")
		}
		return ProtectionSettlementSettled, settled, nil
	}
	if hasRec {
		if !hasFills || rec.PreviousChecksum != fills.Checksum {
			return ProtectionSettlementPending, protectionSettlementMarker{}, errors.New("protection settlement reconcile chain 손상")
		}
		return ProtectionSettlementSideReconciled, rec, nil
	}
	if hasFills {
		return ProtectionSettlementFillsImported, fills, nil
	}
	return ProtectionSettlementPending, protectionSettlementMarker{}, nil
}

// RecoverProtectionSettlement is deliberately read-only with respect to the
// exchange. It can only GET signed orders/trades/account state and write local
// immutable evidence. It never submits or cancels a conditional order.
func (s *ProtectionSettlementStore) RecoverProtectionSettlement(ctx context.Context, group string, submissions *ProtectionSubmissionStore, owners *OwnershipStore, reader ProtectionSettlementReader, creds DemoCredentials) (ProtectionSettlementResult, error) {
	result := ProtectionSettlementResult{GroupID: group, TradingAuthorized: false, ManualReviewNeeded: true, InitialStage: ProtectionSettlementPending, FinalStage: ProtectionSettlementPending}
	if s == nil || submissions == nil || owners == nil || reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return result, errors.New("protection settlement 구성요소 또는 Demo 인증 정보가 없습니다")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	expectedTradingRoot := filepath.Clean(s.root)
	if filepath.Clean(filepath.Dir(submissions.dir)) != expectedTradingRoot || filepath.Clean(filepath.Dir(owners.journalDir)) != expectedTradingRoot || filepath.Clean(filepath.Dir(owners.fillDir)) != expectedTradingRoot {
		return result, errors.New("protection settlement 구성요소의 data/trading 경로가 서로 다릅니다")
	}
	submissions.mu.Lock()
	plan, err := submissions.read(group)
	submissions.mu.Unlock()
	if err != nil {
		return result, err
	}

	s.mu.Lock()
	stage, prior, err := s.currentStageLocked(group, plan)
	s.mu.Unlock()
	if err != nil {
		return result, err
	}
	result.InitialStage, result.FinalStage = stage, stage
	if stage == ProtectionSettlementSettled {
		result.OwnerRemainingQty, result.ExchangeSideQty = prior.OwnerRemainingQty, prior.ExchangeSideQty
		return result, nil
	}

	reads, pending, err := s.collectStableLegEvidence(ctx, plan, submissions, reader, creds)
	if err != nil {
		return result, err
	}
	if pending != "" {
		result.PendingReason = pending
		return result, nil
	}

	regular, err := owners.readFills()
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	added, allProtection, err := s.persistProtectionFillsLocked(plan, reads, regular)
	if err != nil {
		s.mu.Unlock()
		return result, err
	}
	groupFills := make([]ProtectionStoredFill, 0)
	for _, f := range allProtection {
		if f.GroupID == group {
			groupFills = append(groupFills, f)
		}
	}
	fillDigest := protectionFillSetDigest(groupFills)
	if stage == ProtectionSettlementPending {
		m, err := s.writeMarkerLocked(protectionSettlementMarker{GroupID: group, PlanDigest: plan.Digest, Stage: ProtectionSettlementFillsImported, FillCount: len(groupFills), FillDigest: fillDigest})
		if err != nil {
			s.mu.Unlock()
			return result, err
		}
		prior = m
		stage = ProtectionSettlementFillsImported
		result.Progressed = true
	} else if prior.FillDigest != fillDigest || prior.FillCount != len(groupFills) {
		s.mu.Unlock()
		return result, errors.New("기존 fill-import marker와 protection fill ledger가 다릅니다")
	}
	s.mu.Unlock()
	result.ImportedFillCount += added
	result.FinalStage = stage

	remaining, sideQty, asOf, err := s.auditAdjustedOwnership(ctx, plan, submissions, owners, reader, creds, allProtection)
	if err != nil {
		return result, fmt.Errorf("pooled-side protection reconciliation 실패: %w", err)
	}
	result.OwnerRemainingQty, result.ExchangeSideQty = remaining, sideQty
	if stage == ProtectionSettlementFillsImported {
		s.mu.Lock()
		m, err := s.writeMarkerLocked(protectionSettlementMarker{GroupID: group, PlanDigest: plan.Digest, Stage: ProtectionSettlementSideReconciled,
			FillCount: len(groupFills), FillDigest: fillDigest, OwnerRemainingQty: remaining, ExchangeSideQty: sideQty, SnapshotAsOf: asOf, PreviousChecksum: prior.Checksum})
		s.mu.Unlock()
		if err != nil {
			return result, err
		}
		prior = m
		stage = ProtectionSettlementSideReconciled
		result.Progressed = true
		result.FinalStage = stage
	}

	// Final settlement requires another independent signed round. This narrows
	// the gap between the first account reconciliation and durable SETTLED.
	reads2, pending2, err := s.collectStableLegEvidence(ctx, plan, submissions, reader, creds)
	if err != nil {
		return result, err
	}
	if pending2 != "" {
		result.PendingReason = pending2
		return result, nil
	}
	if err := sameProtectionLegEvidence(reads, reads2); err != nil {
		return result, err
	}
	remaining2, sideQty2, asOf2, err := s.auditAdjustedOwnership(ctx, plan, submissions, owners, reader, creds, allProtection)
	if err != nil {
		return result, fmt.Errorf("최종 pooled-side 재검증 실패: %w", err)
	}
	if remaining2 != remaining || sideQty2 != sideQty {
		return result, errors.New("settlement 직전 계정 상태가 이전 reconciliation과 달라졌습니다")
	}

	s.mu.Lock()
	_, exists, err := s.readMarkerLocked(group, ProtectionSettlementSettled, plan)
	if err == nil && !exists {
		_, err = s.writeMarkerLocked(protectionSettlementMarker{GroupID: group, PlanDigest: plan.Digest, Stage: ProtectionSettlementSettled,
			FillCount: len(groupFills), FillDigest: fillDigest, OwnerRemainingQty: remaining2, ExchangeSideQty: sideQty2, SnapshotAsOf: asOf2, PreviousChecksum: prior.Checksum})
	}
	s.mu.Unlock()
	if err != nil {
		return result, err
	}
	result.Progressed = true
	result.FinalStage = ProtectionSettlementSettled
	result.OwnerRemainingQty, result.ExchangeSideQty = remaining2, sideQty2
	return result, nil
}

// validateProtectionFillsForOwnership binds every immutable protection fill
// back to its immutable plan, signed-observed AlgoID, and complete fill-import
// marker before the general ownership audit is allowed to consume it. A crash
// between individual fill fsyncs and the group marker therefore remains
// fail-closed until RecoverProtectionSettlement completes that stage.
func validateProtectionFillsForOwnership(tradingRoot string, fills []ProtectionStoredFill) error {
	if len(fills) == 0 {
		return nil
	}
	ps := &ProtectionSubmissionStore{dir: filepath.Join(tradingRoot, "protection_submissions")}
	settlements := &ProtectionSettlementStore{root: tradingRoot, fillDir: filepath.Join(tradingRoot, "protection_fills"), stageDir: filepath.Join(tradingRoot, "protection_settlements")}
	byGroup := make(map[string][]ProtectionStoredFill)
	plans := make(map[string]ProtectionDispatchPlan)
	for _, f := range fills {
		plan, ok := plans[f.GroupID]
		if !ok {
			var err error
			plan, err = ps.read(f.GroupID)
			if err != nil {
				return fmt.Errorf("protection fill 원본 plan 검증 실패: %w", err)
			}
			plans[f.GroupID] = plan
		}
		if f.PlanDigest != plan.Digest || f.Source != plan.Source || f.ModelID != plan.ModelID || f.PositionSide != plan.PositionSide {
			return errors.New("protection fill 소유자/plan digest 불일치")
		}
		instruction := plan.TP
		if f.Leg == "sl" {
			instruction = plan.SL
		}
		if f.Trade.Symbol != instruction.Symbol || f.Trade.Side != instruction.Side || f.Trade.PositionSide != string(instruction.PositionSide) {
			return errors.New("protection fill 방향이 원본 plan과 다릅니다")
		}
		state, err := ps.inspectLocked(f.GroupID, f.Leg)
		if err != nil || !state.SignedObserved || state.ObservedAlgoID != f.AlgoID {
			return errors.New("protection fill의 signed-observed AlgoID를 검증할 수 없습니다")
		}
		byGroup[f.GroupID] = append(byGroup[f.GroupID], f)
	}
	for group, groupFills := range byGroup {
		plan := plans[group]
		total := new(big.Rat)
		seenTrade := make(map[int64]bool)
		for _, f := range groupFills {
			if seenTrade[f.Trade.TradeID] {
				return errors.New("protection fill group tradeId 중복")
			}
			seenTrade[f.Trade.TradeID] = true
			q, err := positiveRat(f.Trade.Quantity)
			if err != nil {
				return err
			}
			total.Add(total, q)
		}
		want, err := positiveRat(plan.TP.Quantity)
		if err != nil || total.Cmp(want) != 0 {
			return errors.New("protection fill group 합계가 원본 owner 수량과 다릅니다")
		}
		marker, ok, err := settlements.readMarkerLocked(group, ProtectionSettlementFillsImported, plan)
		if err != nil {
			return err
		}
		if !ok || marker.FillCount != len(groupFills) || marker.FillDigest != protectionFillSetDigest(groupFills) {
			return errors.New("protection fill group이 완전한 fill-import marker로 봉인되지 않았습니다")
		}
	}
	return nil
}

// readValidatedProtectionFillsForStore is shared by read-only recovery,
// wallet-history and accounting gates. It never creates or mutates protection
// evidence. Every returned fill is checksum/plan/AlgoID/fill-import-marker
// validated. When regular fills are supplied, trade IDs must also be globally
// unique across both ledgers.
func readValidatedProtectionFillsForStore(tradingRoot string, regular []StoredFill) ([]ProtectionStoredFill, error) {
	fills, err := readProtectionFillsDir(filepath.Join(tradingRoot, "protection_fills"))
	if err != nil {
		return nil, err
	}
	if err := validateProtectionFillsForOwnership(tradingRoot, fills); err != nil {
		return nil, err
	}
	if len(fills) == 0 || len(regular) == 0 {
		return fills, nil
	}
	regularTradeIDs := make(map[int64]bool, len(regular))
	for _, f := range regular {
		if f.Trade.TradeID <= 0 || regularTradeIDs[f.Trade.TradeID] {
			return nil, errors.New("일반 체결 tradeId 중복 또는 오류")
		}
		regularTradeIDs[f.Trade.TradeID] = true
	}
	for _, f := range fills {
		if regularTradeIDs[f.Trade.TradeID] {
			return nil, errors.New("일반 체결과 protection 체결의 tradeId가 중복됩니다")
		}
	}
	return fills, nil
}

func protectionSettlementBlockers(tradingRoot string, fills []ProtectionStoredFill, allowedInProgress string) ([]string, error) {
	if len(fills) == 0 {
		return nil, nil
	}
	ps := &ProtectionSubmissionStore{dir: filepath.Join(tradingRoot, "protection_submissions")}
	ss := &ProtectionSettlementStore{root: tradingRoot, fillDir: filepath.Join(tradingRoot, "protection_fills"), stageDir: filepath.Join(tradingRoot, "protection_settlements")}
	groups := make(map[string]bool)
	for _, f := range fills {
		groups[f.GroupID] = true
	}
	var blockers []string
	for group := range groups {
		plan, err := ps.read(group)
		if err != nil {
			return nil, err
		}
		stage, _, err := ss.currentStageLocked(group, plan)
		if err != nil {
			return nil, err
		}
		if stage != ProtectionSettlementSettled && group != allowedInProgress {
			blockers = append(blockers, "미완료 protection settlement: "+group+" ("+string(stage)+")")
		}
	}
	sort.Strings(blockers)
	return blockers, nil
}

// activeProtectionPlansLocked classifies historical SETTLED groups as archived
// rather than active arming conflicts. The caller must hold submissions.mu.
// A settled group is excluded only after its settlement chain and protection
// fill ledger both validate; corrupted or half-settled history remains
// fail-closed and prevents new protection staging.
func activeProtectionPlansLocked(submissions *ProtectionSubmissionStore, plans []ProtectionDispatchPlan) ([]ProtectionDispatchPlan, map[string]bool, error) {
	tradingRoot := filepath.Dir(submissions.dir)
	fills, err := readProtectionFillsDir(filepath.Join(tradingRoot, "protection_fills"))
	if err != nil {
		return nil, nil, err
	}
	if err := validateProtectionFillsForOwnership(tradingRoot, fills); err != nil {
		return nil, nil, err
	}
	ss := &ProtectionSettlementStore{root: tradingRoot, fillDir: filepath.Join(tradingRoot, "protection_fills"), stageDir: filepath.Join(tradingRoot, "protection_settlements")}
	active := make([]ProtectionDispatchPlan, 0, len(plans))
	settled := make(map[string]bool)
	for _, plan := range plans {
		stage, marker, err := ss.currentStageLocked(plan.GroupID, plan)
		if err != nil {
			return nil, nil, err
		}
		if stage == ProtectionSettlementSettled {
			groupFills := make([]ProtectionStoredFill, 0)
			for _, f := range fills {
				if f.GroupID == plan.GroupID {
					groupFills = append(groupFills, f)
				}
			}
			if len(groupFills) == 0 || marker.FillCount != len(groupFills) || marker.FillDigest != protectionFillSetDigest(groupFills) {
				return nil, nil, errors.New("SETTLED protection group의 fill evidence가 일치하지 않습니다")
			}
			settled[plan.GroupID] = true
			continue
		}
		active = append(active, plan)
	}
	return active, settled, nil
}

func sameProtectionLegEvidence(a, b []protectionLegRead) error {
	if len(a) != len(b) {
		return errors.New("최종 protection leg evidence 개수가 달라졌습니다")
	}
	byLeg := make(map[string]protectionLegRead, len(a))
	for _, r := range a {
		byLeg[r.leg] = r
	}
	for _, r := range b {
		prior, ok := byLeg[r.leg]
		if !ok || prior.algoID != r.algoID || prior.second.Status != r.second.Status || prior.second.ActualOrderID != r.second.ActualOrderID ||
			string(prior.second.ActualQty) != string(r.second.ActualQty) || len(prior.trades) != len(r.trades) {
			return errors.New("최종 protection signed evidence가 최초 fill 고정 시점과 달라졌습니다")
		}
		for i := range prior.trades {
			if prior.trades[i] != r.trades[i] {
				return errors.New("최종 protection userTrades가 최초 fill ledger와 달라졌습니다")
			}
		}
	}
	return nil
}
