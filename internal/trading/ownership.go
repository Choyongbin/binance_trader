package trading

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
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

// The ownership ledger is deliberately READ-ONLY relative to Binance. The
// account aggregates same-side BTCUSDT positions; these virtual positions are
// analytical allocations, NOT individual exchange-isolated positions. Import
// only from authenticated signed USER_DATA order/trade reads, never browser
// supplied numbers, websocket public trades, or an order ACK.
//
// No trading HTTP route instantiates this ledger. Reconciliation is NOT an
// authorization token, especially before external trade detection, protected
// TP/SL ownership and lifecycle gating are complete.
type OwnershipStore struct {
	mu         sync.Mutex
	journalDir string
	fillDir    string
}

func NewOwnershipStore(dataDir string) (*OwnershipStore, error) {
	if dataDir == "" {
		return nil, errors.New("기록 디렉터리가 필요합니다")
	}
	journalDir := filepath.Join(dataDir, "trading", "requests")
	fillDir := filepath.Join(dataDir, "trading", "fills")
	if err := os.MkdirAll(journalDir, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(fillDir, 0700); err != nil {
		return nil, err
	}
	return &OwnershipStore{journalDir: journalDir, fillDir: fillDir}, nil
}

var journalName = regexp.MustCompile(`^bt_[a-f0-9]{28}\.json$`)
var fillName = regexp.MustCompile(`^BTCUSDT_[1-9][0-9]*\.json$`)

// ScanJournal loads the original durable reservation and every immutable
// sidecar. Orphaned sidecars, unknown JSON or bad order-plan hashes all block
// the assessment rather than silently dropping potentially submitted orders.
func (s *OwnershipStore) ScanJournal() ([]Record, error) {
	files, err := os.ReadDir(s.journalDir)
	if err != nil {
		return nil, err
	}
	baseFiles := make(map[string]bool)
	for _, f := range files {
		n := f.Name()
		if f.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("주문 기록 symlink는 허용되지 않습니다: %s", n)
		}
		switch {
		case journalName.MatchString(n) && !f.IsDir():
			baseFiles[n] = true
		case strings.HasPrefix(n, "bt_") && strings.Contains(n, ".state.") && strings.HasSuffix(n, ".json") && !f.IsDir():
			base := strings.SplitN(n, ".state.", 2)[0] + ".json"
			if !journalName.MatchString(base) {
				return nil, fmt.Errorf("비정상 주문 상태 파일: %s", n)
			}
		default:
			return nil, fmt.Errorf("알 수 없는 주문 기록 파일: %s", n)
		}
	}
	out := make([]Record, 0, len(baseFiles))
	for _, f := range files {
		n := f.Name()
		if strings.Contains(n, ".state.") {
			base := strings.SplitN(n, ".state.", 2)[0] + ".json"
			if !baseFiles[base] {
				return nil, errors.New("원본이 없는 주문 상태 파일이 존재합니다")
			}
		}
		if !baseFiles[n] {
			continue
		}
		r, err := readRecord(filepath.Join(s.journalDir, n))
		if err != nil {
			return nil, fmt.Errorf("주문 저널 손상 (%s): %w", n, err)
		}
		p, err := r.Intent.Plan()
		if err != nil {
			return nil, err
		}
		digest, err := digestIntent(r.Intent)
		if err != nil {
			return nil, err
		}
		if p != r.Plan || digest != r.IntentDigest || n != p.ClientOrderID+".json" || (r.State != Prepared && r.State != Unknown && r.State != Acknowledged && r.State != Rejected && r.State != Reconciled) {
			return nil, fmt.Errorf("주문 기록 불일치: %s", n)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Plan.ClientOrderID < out[j].Plan.ClientOrderID })
	return out, nil
}

// Every record corresponds to a signed Binance trade. The journal, not the
// event itself, establishes its manual/model ownership and its ENTER/EXIT side.
type StoredFill struct {
	Schema        int                   `json:"schema"`
	ClientOrderID string                `json:"client_order_id"`
	Trade         exchange.AccountTrade `json:"trade"`
	Checksum      string                `json:"checksum"`
}

func (f StoredFill) digest() string {
	f.Checksum = ""
	raw, _ := json.Marshal(f)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func (f *StoredFill) seal() { f.Checksum = f.digest() }

func (s *OwnershipStore) readFills() ([]StoredFill, error) {
	files, err := os.ReadDir(s.fillDir)
	if err != nil {
		return nil, err
	}
	out := make([]StoredFill, 0, len(files))
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !fillName.MatchString(f.Name()) {
			return nil, fmt.Errorf("비정상 체결 기록 파일: %s", f.Name())
		}
		raw, err := os.ReadFile(filepath.Join(s.fillDir, f.Name()))
		if err != nil {
			return nil, err
		}
		var ev StoredFill
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&ev); err != nil {
			return nil, fmt.Errorf("체결 기록 손상: %w", err)
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) || ev.Schema != 1 || f.Name() != fmt.Sprintf("%s_%d.json", ev.Trade.Symbol, ev.Trade.TradeID) {
			return nil, errors.New("체결 기록이 불완전합니다")
		}
		if ev.Checksum == "" || ev.Checksum != ev.digest() {
			return nil, errors.New("체결 기록 체크섬이 일치하지 않습니다")
		}
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Trade.TradeID < out[j].Trade.TradeID })
	return out, nil
}

// DemoTradeReader is intentionally a read-only signed exchange interface.
// In production this can only be constructed from the authenticated Demo
// client after unlocking the Demo vault and checking the Demo endpoint.
type DemoTradeReader interface {
	QueryDemoOrder(context.Context, string, string, string, string) (exchange.OrderResult, error)
	FetchDemoOrderTrades(context.Context, string, string, string, int64) ([]exchange.AccountTrade, error)
}

func positiveRat(s string) (*big.Rat, error) { return decimalRat(s, false) }
func decimalRat(s string, allowZero bool) (*big.Rat, error) {
	if len(s) < 1 || len(s) > 64 {
		return nil, errors.New("허용되지 않는 십진수 길이")
	}
	if strings.HasPrefix(s, "-") {
		s = s[1:] // sign is handled by signedRat instead
		return nil, errors.New("음수는 허용되지 않습니다")
	}
	if s[0] == '.' || s[len(s)-1] == '.' {
		return nil, errors.New("유효하지 않은 십진수")
	}
	dots := 0
	for _, c := range s {
		if c == '.' {
			dots++
			continue
		}
		if c < '0' || c > '9' {
			return nil, errors.New("유효하지 않은 십진수")
		}
	}
	if dots > 1 {
		return nil, errors.New("유효하지 않은 십진수")
	}
	v, ok := new(big.Rat).SetString(s)
	if !ok || (v.Sign() == 0 && !allowZero) {
		return nil, errors.New("양의 십진수여야 합니다")
	}
	return v, nil
}
func signedRat(s string) (*big.Rat, error) {
	if strings.HasPrefix(s, "-") {
		v, err := decimalRat(s[1:], true)
		if err != nil {
			return nil, err
		}
		return v.Neg(v), nil
	}
	return decimalRat(s, true)
}
func formatOwnershipDecimal(v *big.Rat) string {
	s := v.FloatString(12)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	if s == "-0" || s == "" {
		return "0"
	}
	return s
}

func verifyRemoteOrder(record Record, order exchange.OrderResult) (*big.Rat, error) {
	p := record.Plan
	if order.Symbol != p.Symbol || order.ClientOrderID != p.ClientOrderID || order.OrderID <= 0 ||
		order.Side != p.ExchangeSide || order.PositionSide != string(p.PositionSide) || order.Type != string(p.Type) {
		return nil, errors.New("거래소 주문의 ID·종목·방향·종류가 저널과 일치하지 않습니다")
	}
	switch order.Status {
	case "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
	default:
		return nil, errors.New("알 수 없는 거래소 주문 상태")
	}
	qty, err := decimalRat(order.ExecutedQty, true)
	if err != nil {
		return nil, errors.New("체결 수량이 유효하지 않습니다")
	}
	planned, err := positiveRat(p.Quantity)
	if err != nil || qty.Cmp(planned) > 0 {
		return nil, errors.New("체결 수량이 주문 수량보다 큽니다")
	}
	// If API gives origQty, it must match what this program originally sent.
	if order.OriginalQty == "" {
		return nil, errors.New("거래소 원주문 수량 누락")
	}
	remote, err := positiveRat(order.OriginalQty)
	if err != nil || remote.Cmp(planned) != 0 {
		return nil, errors.New("거래소 원주문 수량 불일치")
	}
	return qty, nil
}
func validateFill(rec Record, t exchange.AccountTrade) error {
	p := rec.Plan
	if t.Symbol != p.Symbol || t.OrderID <= 0 || t.OrderID != rec.ExchangeOrderID && rec.ExchangeOrderID > 0 ||
		t.TradeID <= 0 || t.TimeMS <= 0 || t.Side != p.ExchangeSide || t.PositionSide != string(p.PositionSide) {
		return errors.New("체결 ID·방향·종목이 원래 주문과 일치하지 않습니다")
	}
	if _, err := positiveRat(t.Quantity); err != nil {
		return errors.New("체결 수량 오류")
	}
	if _, err := positiveRat(t.Price); err != nil {
		return errors.New("체결 가격 오류")
	}
	if _, err := signedRat(t.Commission); err != nil {
		return errors.New("체결 수수료 오류")
	}
	if _, err := signedRat(t.RealizedPnL); err != nil {
		return errors.New("실제 거래소 실현손익 응답 오류")
	}
	if !regexp.MustCompile(`^[A-Z0-9]{2,20}$`).MatchString(t.CommissionAsset) {
		return errors.New("수수료 자산이 유효하지 않습니다")
	}
	return nil
}

// ImportDemoOrder is a staged, mock-testable, read-only exchange import.
// It checks signed order status BOTH before and after the trade-list fetch.
// Incomplete histories, partial downloads and changed orders are rejected.
// Each validated execution has an immutable, O_EXCL+fsync disk file. It never
// sets a journal UNKNOWN to ACKNOWLEDGED or sends an order.
func (s *OwnershipStore) ImportDemoOrder(ctx context.Context, reader DemoTradeReader, creds DemoCredentials, intent Intent) (int, error) {
	if reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return 0, errors.New("서명된 Demo 거래소 조회가 필요합니다")
	}
	plan, err := intent.Plan()
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := readRecord(filepath.Join(s.journalDir, plan.ClientOrderID+".json"))
	if err != nil {
		return 0, err
	}
	hash, _ := digestIntent(intent)
	if rec.Plan != plan || rec.IntentDigest != hash {
		return 0, errors.New("주문 요청 ID 불일치")
	}
	first, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, plan.Symbol, plan.ClientOrderID)
	if err != nil {
		return 0, err
	}
	if _, err = verifyRemoteOrder(rec, first); err != nil {
		return 0, err
	}
	trades, err := reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, plan.Symbol, first.OrderID)
	if err != nil {
		return 0, err
	}
	second, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, plan.Symbol, plan.ClientOrderID)
	if err != nil {
		return 0, err
	}
	want, err := verifyRemoteOrder(rec, second)
	if err != nil {
		return 0, err
	}
	if first.OrderID != second.OrderID || first.Status != second.Status || first.ExecutedQty != second.ExecutedQty {
		return 0, errors.New("거래소 주문 상태가 조회 도중 변경되었습니다. 재조회 필요")
	}
	staged := make([]StoredFill, 0, len(trades))
	seen := make(map[int64]bool)
	sum := new(big.Rat)
	for _, t := range trades {
		if t.OrderID != second.OrderID || seen[t.TradeID] {
			return 0, errors.New("주문 ID 또는 체결 ID 중복")
		}
		seen[t.TradeID] = true
		// Older journal records from ACK may have an orderId; compare it to
		// signed GET even though the latter is the canonical readback.
		if rec.ExchangeOrderID > 0 && rec.ExchangeOrderID != second.OrderID {
			return 0, errors.New("저널과 거래소의 주문 번호가 다릅니다")
		}
		if err := validateFill(rec, t); err != nil {
			return 0, err
		}
		q, _ := positiveRat(t.Quantity)
		sum.Add(sum, q)
		event := StoredFill{Schema: 1, ClientOrderID: plan.ClientOrderID, Trade: t}
		event.seal()
		staged = append(staged, event)
	}
	if sum.Cmp(want) != 0 {
		return 0, errors.New("거래소 누적 체결 수량과 체결 목록 합계가 다릅니다")
	}
	existing, err := s.readFills()
	if err != nil {
		return 0, err
	}
	persisted := make(map[int64]StoredFill, len(existing))
	for _, f := range existing {
		persisted[f.Trade.TradeID] = f
	}
	for _, f := range staged {
		if prior, ok := persisted[f.Trade.TradeID]; ok && prior != f {
			return 0, errors.New("기존 체결 기록과 거래소 내역이 충돌합니다")
		}
	}
	// Prevent an import that would generate impossible negative owner lots:
	// combine the old known fills and all new ones, then replay exactly.
	records, err := s.ScanJournal()
	if err != nil {
		return 0, err
	}
	combined := append([]StoredFill(nil), existing...)
	for _, f := range staged {
		if _, ok := persisted[f.Trade.TradeID]; !ok {
			combined = append(combined, f)
		}
	}
	if _, err := replay(records, combined); err != nil {
		return 0, fmt.Errorf("체결 장부 사전 검증 실패: %w", err)
	}
	added := 0
	for _, f := range staged {
		if _, ok := persisted[f.Trade.TradeID]; ok {
			continue
		}
		path := filepath.Join(s.fillDir, fmt.Sprintf("BTCUSDT_%d.json", f.Trade.TradeID))
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return added, fmt.Errorf("체결 기록을 생성하지 못했습니다. 재조회 필요: %w", err)
		}
		raw, _ := json.MarshalIndent(f, "", "  ")
		_, err = file.Write(append(raw, '\n'))
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return added, fmt.Errorf("체결 기록 저장 중 실패. 수동 검사가 필요합니다: %w", err)
		}
		added++
	}
	return added, nil
}

type OwnerPosition struct {
	Source                    Source            `json:"source"`
	ModelID                   string            `json:"model_id,omitempty"`
	Symbol                    string            `json:"symbol"`
	Side                      PositionSide      `json:"side"`
	Quantity                  string            `json:"quantity"`
	AverageEntry              string            `json:"average_entry"`
	VirtualGrossRealizedUSDT  string            `json:"virtual_gross_realized_usdt"`
	ExchangeGrossRealizedUSDT string            `json:"exchange_gross_realized_usdt"`
	FeesByAsset               map[string]string `json:"fees_by_asset"`
	// This omits non-USDT fees and unallocated funding. Never label it as
	// actual net account P&L or a model's exchange-reported result.
	VirtualAfterUSDTFeesExFunding string `json:"virtual_after_usdt_fees_ex_funding"`
	OtherFeeAssetPresent          bool   `json:"other_fee_asset_present"`
}
type ownerKey struct {
	source Source
	model  string
	side   PositionSide
	symbol string
}
type lot struct {
	qty, avg, gross, exchangeGross *big.Rat
	fees                           map[string]*big.Rat
}

func newLot() *lot {
	return &lot{qty: new(big.Rat), avg: new(big.Rat), gross: new(big.Rat), exchangeGross: new(big.Rat), fees: make(map[string]*big.Rat)}
}
func buildOwnerKey(r Record) ownerKey {
	return ownerKey{r.Intent.Source, r.Intent.ModelID, r.Intent.PositionSide, r.Intent.Symbol}
}
func replay(records []Record, fills []StoredFill) (map[ownerKey]*lot, error) {
	byID := make(map[string]Record, len(records))
	seenTrades := make(map[int64]bool)
	for _, r := range records {
		if _, ok := byID[r.Plan.ClientOrderID]; ok {
			return nil, errors.New("중복 주문 ID")
		}
		byID[r.Plan.ClientOrderID] = r
	}
	sorted := append([]StoredFill(nil), fills...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i].Trade, sorted[j].Trade
		if a.TimeMS != b.TimeMS {
			return a.TimeMS < b.TimeMS
		}
		return a.TradeID < b.TradeID
	})
	owners := make(map[ownerKey]*lot)
	perOrder := make(map[string]*big.Rat)
	for _, f := range sorted {
		r, ok := byID[f.ClientOrderID]
		if !ok {
			return nil, fmt.Errorf("소유권을 알 수 없는 체결: %d", f.Trade.TradeID)
		}
		if seenTrades[f.Trade.TradeID] {
			return nil, errors.New("체결 ID 중복")
		}
		seenTrades[f.Trade.TradeID] = true
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
	}
	return owners, nil
}

type OrderObservation struct {
	Order     exchange.OrderResult `json:"order"`
	CheckedAt time.Time            `json:"checked_at"`
}

// CollectDemoOrderObservations only performs signed read-only queries.
// Fetch the complete account snapshot AFTER these queries for a current comparison.
func (s *OwnershipStore) CollectDemoOrderObservations(ctx context.Context, reader DemoTradeReader, creds DemoCredentials) (map[string]OrderObservation, error) {
	if reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return nil, errors.New("Demo 인증 조회가 필요합니다")
	}
	records, err := s.ScanJournal()
	if err != nil {
		return nil, err
	}
	result := make(map[string]OrderObservation, len(records))
	for _, r := range records {
		o, err := reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, r.Plan.Symbol, r.Plan.ClientOrderID)
		if err != nil {
			return nil, err
		}
		if _, err = verifyRemoteOrder(r, o); err != nil {
			return nil, err
		}
		result[r.Plan.ClientOrderID] = OrderObservation{Order: o, CheckedAt: time.Now().UTC()}
	}
	return result, nil
}

type OwnershipReport struct {
	ReadyForAssessment bool `json:"ready_for_assessment"`
	// Not a trade permission; this v1 evaluator always remains disconnected
	// from HTTP order execution, even when assessment has no blockers.
	Blockers                []string        `json:"blockers"`
	Owned                   []OwnerPosition `json:"owned"`
	ObservedOrders          int             `json:"observed_orders"`
	ObservedFills           int             `json:"observed_fills"`
	ObservedProtectionFills int             `json:"observed_protection_fills"`
}

// Audit verifies full known journal coverage, verified order GET quantities,
// account positions and open orders. All signed inputs must refer to one fresh
// complete Demo snapshot. This alone cannot detect a flat external roundtrip
// or history outside Binance retention: a future continuous user stream and
// explicit baseline are required before order authorization.
func (s *OwnershipStore) audit(snapshot exchange.Snapshot, orders map[string]OrderObservation, now time.Time, allowedProtectionInProgress string) (OwnershipReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := OwnershipReport{Blockers: []string{}, Owned: []OwnerPosition{}}
	records, err := s.ScanJournal()
	if err != nil {
		return report, err
	}
	fills, err := s.readFills()
	if err != nil {
		return report, err
	}
	protectionFills, err := readValidatedProtectionFillsForStore(filepath.Dir(s.fillDir), fills)
	if err != nil {
		return report, err
	}
	lots, err := applyProtectionFills(records, fills, protectionFills)
	if err != nil {
		return report, err
	}
	report.ObservedOrders = len(records)
	report.ObservedFills = len(fills)
	report.ObservedProtectionFills = len(protectionFills)
	block := func(msg string) { report.Blockers = append(report.Blockers, msg) }
	settlementBlockers, err := protectionSettlementBlockers(filepath.Dir(s.fillDir), protectionFills, allowedProtectionInProgress)
	if err != nil {
		return report, err
	}
	for _, msg := range settlementBlockers {
		block(msg)
	}
	if snapshot.Status != "connected" || snapshot.Environment != "demo" || snapshot.Balances == nil || snapshot.Positions == nil || snapshot.Orders == nil || snapshot.AlgoOrders == nil {
		block("완전한 Demo 계정 스냅샷이 없습니다")
	}
	when, err := time.Parse(time.RFC3339Nano, snapshot.AsOf)
	if err != nil || when.After(now.Add(time.Second)) || now.Sub(when) > 5*time.Second {
		block("계정 스냅샷이 오래되었습니다")
	}
	if len(snapshot.AlgoOrders) > 0 {
		block("별도 소유권이 없는 조건부 주문이 존재합니다")
	}
	byID := make(map[string]Record, len(records))
	perOrder := make(map[string]*big.Rat)
	for _, f := range fills {
		if perOrder[f.ClientOrderID] == nil {
			perOrder[f.ClientOrderID] = new(big.Rat)
		}
		q, _ := positiveRat(f.Trade.Quantity)
		perOrder[f.ClientOrderID].Add(perOrder[f.ClientOrderID], q)
	}
	for _, r := range records {
		cid := r.Plan.ClientOrderID
		byID[cid] = r
		if r.State == Prepared || r.State == Unknown || r.State == Rejected {
			block("미확정 주문 저널: " + cid)
		}
		observation, ok := orders[cid]
		if !ok {
			block("조회되지 않은 주문: " + cid)
			continue
		}
		obs := observation.Order
		if observation.CheckedAt.IsZero() || observation.CheckedAt.After(now.Add(time.Second)) || now.Sub(observation.CheckedAt) > 5*time.Second {
			block("오래된 개별 주문 조회: " + cid)
		}
		filled, err := verifyRemoteOrder(r, obs)
		if err != nil {
			block("거래소 주문 내용 불일치: " + cid)
			continue
		}
		if r.ExchangeOrderID > 0 && r.ExchangeOrderID != obs.OrderID {
			block("저널 거래소 주문 ID 불일치: " + cid)
		}
		known := perOrder[cid]
		if known == nil {
			known = new(big.Rat)
		}
		if known.Cmp(filled) != 0 {
			block("주문 누적 수량과 저장된 체결 내역 불일치: " + cid)
		}
		if obs.Status == "NEW" || obs.Status == "PARTIALLY_FILLED" {
			block("미체결 주문이 남아 있습니다: " + cid)
		}
	}
	for _, f := range fills {
		check, ok := orders[f.ClientOrderID]
		if !ok || f.Trade.OrderID != check.Order.OrderID {
			block("체결과 현재 거래소 주문 ID 불일치")
		}
	}
	if len(orders) != len(byID) {
		block("소유권 저널에 없는 주문 조회가 포함되었습니다")
	}
	for _, o := range snapshot.Orders {
		r, ok := byID[o.ClientOrderID]
		if !ok || o.Symbol != "BTCUSDT" || o.OrderID <= 0 {
			block("소유권 확인이 불가능한 미체결 주문")
			continue
		}
		checkObservation, ok := orders[o.ClientOrderID]
		check := checkObservation.Order
		if !ok || check.OrderID != o.OrderID || r.Plan.ExchangeSide != o.Side || string(r.Plan.PositionSide) != o.PositionSide {
			block("미체결 주문의 소유권 확인 실패")
		}
	}
	expected := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	keys := make([]ownerKey, 0, len(lots))
	for k, l := range lots {
		expected[k.side].Add(expected[k.side], l.qty)
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return string(a.source)+":"+a.model+":"+string(a.side) < string(b.source)+":"+b.model+":"+string(b.side)
	})
	for _, k := range keys {
		l := lots[k]
		fees := map[string]string{}
		other := false
		for asset, v := range l.fees {
			fees[asset] = formatOwnershipDecimal(v)
			if asset != "USDT" && v.Sign() != 0 {
				other = true
			}
		}
		usdt := l.fees["USDT"]
		if usdt == nil {
			usdt = new(big.Rat)
		}
		report.Owned = append(report.Owned, OwnerPosition{Source: k.source, ModelID: k.model, Symbol: k.symbol, Side: k.side,
			Quantity: formatOwnershipDecimal(l.qty), AverageEntry: formatOwnershipDecimal(l.avg), VirtualGrossRealizedUSDT: formatOwnershipDecimal(l.gross),
			ExchangeGrossRealizedUSDT: formatOwnershipDecimal(l.exchangeGross), FeesByAsset: fees,
			VirtualAfterUSDTFeesExFunding: formatOwnershipDecimal(new(big.Rat).Sub(l.gross, usdt)), OtherFeeAssetPresent: other})
	}
	actual := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	for _, p := range snapshot.Positions {
		if p.Symbol != "BTCUSDT" {
			block("BTCUSDT 이외의 포지션 스냅샷")
			continue
		}
		var side PositionSide
		if p.Side == string(Long) {
			side = Long
		} else if p.Side == string(Short) {
			side = Short
		} else {
			block("지원하지 않는 포지션 방향")
			continue
		}
		q, err := signedRat(p.Qty)
		if err != nil {
			block("거래소 포지션 수량 오류")
			continue
		}
		if side == Short {
			q.Abs(q)
		} else if q.Sign() < 0 {
			block("LONG 수량 부호 오류")
			continue
		}
		actual[side].Add(actual[side], q)
	}
	for _, side := range []PositionSide{Long, Short} {
		if expected[side].Cmp(actual[side]) != 0 {
			block(fmt.Sprintf("%s 가상/거래소 포지션 불일치 (가상 %s, 거래소 %s)", side, formatOwnershipDecimal(expected[side]), formatOwnershipDecimal(actual[side])))
		}
	}
	report.ReadyForAssessment = len(report.Blockers) == 0
	return report, nil
}

func (s *OwnershipStore) Audit(snapshot exchange.Snapshot, orders map[string]OrderObservation, now time.Time) (OwnershipReport, error) {
	return s.audit(snapshot, orders, now, "")
}
