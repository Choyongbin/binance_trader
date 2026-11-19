package trading

// Historical, read-only, exact-decimal accounting for trusted *locally stored*
// order/fill evidence and a caller-acquired signed Demo income window. Nothing
// in this module submits orders or determines whether trading may start.
// Binance shares each symbol/side position across owners. These are VIRTUAL
// accounting allocations, not exchange-isolated positions or final tax P&L.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

// BackendQuote must be sourced from the backend's recorded, pre-submit WS
// observation, not from a browser field. Even then its resulting slippage is
// an *estimate*; a candle close is NOT a fill-time executable bid/ask quote.
type BackendQuote struct {
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Price         string `json:"reference_price"`
	ObservedMS    int64  `json:"observed_ms"`
	Source        string `json:"source"` // BACKEND_WS_TRADE only in this preview
}

type AccountingInput struct {
	// Bounded, signed ALL-account income read, obtained separately from the
	// persisted fills. Neither the start/end bounds nor apparent completeness
	// prove full account history or continuous account-stream delivery.
	WindowStartMS int64                   `json:"window_start_ms"`
	WindowEndMS   int64                   `json:"window_end_ms"`
	SignedIncome  []exchange.IncomeRecord `json:"signed_income"`
	Quotes        []BackendQuote          `json:"quotes"`
}

type ProtectionAccountingAttribution struct {
	FillCount                 int               `json:"fill_count"`
	GroupCount                int               `json:"group_count"`
	DualTriggeredGroupCount   int               `json:"dual_triggered_group_count"`
	TakeProfitFillCount       int               `json:"take_profit_fill_count"`
	StopLossFillCount         int               `json:"stop_loss_fill_count"`
	TakeProfitQuantity        string            `json:"take_profit_quantity"`
	StopLossQuantity          string            `json:"stop_loss_quantity"`
	VirtualGrossRealizedUSDT  string            `json:"virtual_gross_realized_usdt"`
	ExchangeReportedGrossUSDT string            `json:"exchange_reported_gross_usdt"`
	CommissionByAsset         map[string]string `json:"commission_by_asset"`
}

type OwnerAccounting struct {
	Source                        Source            `json:"source"`
	ModelID                       string            `json:"model_id,omitempty"`
	Symbol                        string            `json:"symbol"`
	PositionSide                  PositionSide      `json:"position_side"`
	OpenQuantity                  string            `json:"open_quantity"`
	AverageEntry                  string            `json:"average_entry"`
	VirtualGrossRealizedUSDT      string            `json:"virtual_gross_realized_usdt"`
	ExchangeReportedGrossUSDT     string            `json:"exchange_reported_gross_usdt"`
	CommissionByAsset             map[string]string `json:"commission_by_asset"`
	VirtualAfterUSDTFeesExFunding string            `json:"virtual_after_usdt_fees_ex_funding"`
	ProvisionalFundingUSDT        string            `json:"provisional_funding_usdt"`
	ProvisionalAfterFundingUSDT   string            `json:"provisional_after_funding_usdt"`
	SignedUnfavorableSlippageUSDT string            `json:"signed_unfavorable_slippage_usdt,omitempty"`
	SlippageCoverage              string            `json:"slippage_coverage"`
	NonUSDTCommission             bool              `json:"non_usdt_commission"`
	HasProvisionalFunding         bool              `json:"has_provisional_funding"`
	// No owner net is final because contemporaneous account positions, full
	// funding history, non-USDT fee FX and external activity aren't proven.
	ActualNetVerified bool                             `json:"actual_net_verified"`
	Protection        *ProtectionAccountingAttribution `json:"protection,omitempty"`
}

type AccountingReport struct {
	ID                        string            `json:"id,omitempty"`
	Schema                    int               `json:"schema"`
	CreatedAt                 time.Time         `json:"created_at"`
	WindowStartMS             int64             `json:"window_start_ms"`
	WindowEndMS               int64             `json:"window_end_ms"`
	KnownOrders               int               `json:"known_orders"`
	KnownFills                int               `json:"known_fills"`
	IncomeRows                int               `json:"income_rows"`
	Owners                    []OwnerAccounting `json:"owners"`
	AccountFundingByAsset     map[string]string `json:"account_funding_by_asset"`
	UnallocatedFundingByAsset map[string]string `json:"unallocated_funding_by_asset"`
	OtherIncomeByTypeAsset    map[string]string `json:"other_income_by_type_asset"`
	FundingAttribution        string            `json:"funding_attribution"`
	Coverage                  string            `json:"coverage"`
	Warnings                  []string          `json:"warnings"`
	TradingAuthorized         bool              `json:"trading_authorized"`  // ALWAYS FALSE
	ActualNetVerified         bool              `json:"actual_net_verified"` // ALWAYS FALSE
	EvidenceCoverage          string            `json:"evidence_coverage,omitempty"`
	EvidenceSHA256            string            `json:"evidence_sha256,omitempty"`
	ProtectionFillCount       int               `json:"protection_fill_count,omitempty"`
	ProtectionGroupCount      int               `json:"protection_group_count,omitempty"`
	DualTriggeredGroupCount   int               `json:"dual_triggered_group_count,omitempty"`
	SHA256                    string            `json:"sha256"`
}

func (r AccountingReport) digest() string {
	r.SHA256 = ""
	b, _ := json.Marshal(r)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (r *AccountingReport) seal() { r.SHA256 = r.digest() }

type accountingEvent struct {
	at            int64
	trade         exchange.AccountTrade
	key           ownerKey
	action        Action
	orderType     OrderType
	clientOrderID string
	protection    bool
}
type protectionOwnerAcct struct {
	fills         int
	groups        map[string]map[string]bool
	tpFills       int
	slFills       int
	tpQty         *big.Rat
	slQty         *big.Rat
	gross         *big.Rat
	exchangeGross *big.Rat
	fees          map[string]*big.Rat
}

func newProtectionOwnerAcct() *protectionOwnerAcct {
	return &protectionOwnerAcct{groups: make(map[string]map[string]bool), tpQty: new(big.Rat), slQty: new(big.Rat), gross: new(big.Rat), exchangeGross: new(big.Rat), fees: make(map[string]*big.Rat)}
}

type ownerAcct struct {
	key          ownerKey
	lot          *lot
	funding      *big.Rat
	slippage     *big.Rat
	slipEligible int
	slipCovered  int
	limitFills   int
}

func newOwnerAcct(k ownerKey) *ownerAcct {
	return &ownerAcct{key: k, lot: newLot(), funding: new(big.Rat), slippage: new(big.Rat)}
}
func incomeKey(v exchange.IncomeRecord) string {
	// tranId must be unique within the signed window for a given income type
	// and asset/symbol. Duplicates can imply truncated/replayed pages.
	return fmt.Sprintf("%d|%s|%s|%s", v.TransactionID, v.Type, v.Asset, v.Symbol)
}
func sumAsset(m map[string]*big.Rat, a string, v *big.Rat) {
	if m[a] == nil {
		m[a] = new(big.Rat)
	}
	m[a].Add(m[a], v)
}
func mapDecimal(m map[string]*big.Rat) map[string]string {
	out := make(map[string]string, len(m))
	for a, v := range m {
		out[a] = formatOwnershipDecimal(v)
	}
	return out
}
func keysSorted(m map[ownerKey]*ownerAcct) []ownerKey {
	keys := make([]ownerKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return string(a.source)+":"+a.model+":"+a.symbol+":"+string(a.side) < string(b.source)+":"+b.model+":"+b.symbol+":"+string(b.side)
	})
	return keys
}

// EvaluateAccountingOffline is intentionally PURE: it can be exercised against
// independently mock-verified signed evidence. It validates the durable fill
// checksum, full local journal and owner replay. Missing historical activity
// can never be inferred to be absent from these inputs.
func EvaluateAccountingOffline(records []Record, fills []StoredFill, in AccountingInput, created time.Time) (AccountingReport, error) {
	return evaluateAccountingOffline(records, fills, nil, in, created, nil)
}

// Opening lots are reconstructed from PRE-WINDOW immutable local fills by
// BuildAttestedAccounting. This private entry point is only used after the
// COMPLETE local history has replayed successfully and ALL in-window fills
// have matched their signed independent exchange counterparts. Old positions
// are LOCAL opening estimates: no prior account-wide signed checkpoint or
// continuous stream is implied. Period figures EXCLUDE pre-window gross/fees.
func evaluateAccountingOffline(records []Record, fills []StoredFill, protection []ProtectionStoredFill, in AccountingInput, created time.Time, opening map[ownerKey]*lot) (AccountingReport, error) {
	r := AccountingReport{Schema: 1, CreatedAt: created.UTC(), WindowStartMS: in.WindowStartMS, WindowEndMS: in.WindowEndMS,
		Owners: []OwnerAccounting{}, AccountFundingByAsset: map[string]string{}, UnallocatedFundingByAsset: map[string]string{}, OtherIncomeByTypeAsset: map[string]string{},
		FundingAttribution: "PROVISIONAL_OR_UNALLOCATED", Coverage: "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS",
		Warnings: []string{"정확한 계정 최종 순손익이 아닙니다: 기록 범위 밖의 거래, 외부 주문, 비USDT 수수료 환산 및 실시간 계정 연속성은 검증되지 않았습니다.",
			"펀딩비 배분은 조건을 충족할 때만 소유 수량에 비례한 잠정 추정이며 실제 계정의 모델별 펀딩비가 아닙니다."}}
	if created.IsZero() || in.WindowStartMS <= 0 || in.WindowEndMS <= in.WindowStartMS ||
		in.WindowEndMS > created.Add(time.Second).UnixMilli() ||
		in.WindowEndMS-in.WindowStartMS > int64((6*24*time.Hour)/time.Millisecond) || in.SignedIncome == nil {
		return AccountingReport{}, errors.New("명시적 6일 이내 서명된 소득 조회 범위 및 조회 시각이 필요합니다")
	}
	if len(in.SignedIncome) >= 1000 { // conservative; caller must prove pagination coverage outside this preview
		return AccountingReport{}, errors.New("소득 1000건 상한: 페이지 완전성 재확인이 필요합니다")
	}
	byOrder := make(map[string]Record, len(records))
	for _, rec := range records {
		plan, err := rec.Intent.Plan()
		if err != nil || !supportedJournalSchema(rec.Schema) || plan != rec.Plan || rec.Plan.Symbol != "BTCUSDT" || byOrder[plan.ClientOrderID].Plan.ClientOrderID != "" {
			return AccountingReport{}, errors.New("주문 저널 ID·원본이 유효하지 않습니다")
		}
		hash, err := digestIntent(rec.Intent)
		if err != nil || hash != rec.IntentDigest {
			return AccountingReport{}, errors.New("주문 저널 체크섬 오류")
		}
		byOrder[plan.ClientOrderID] = rec
	}
	// Existing replay independently rejects unknown owners, duplicate trade
	// IDs, overfills and exits beyond the owner's actual signed fills.
	if opening == nil {
		if _, err := replay(records, fills); err != nil {
			return AccountingReport{}, fmt.Errorf("소유 장부: %w", err)
		}
	}
	seenFills := map[int64]bool{}
	sorted := make([]accountingEvent, 0, len(fills)+len(protection))
	protectionByOwner := make(map[ownerKey]*protectionOwnerAcct)
	protectionGroups := make(map[string]map[string]bool)
	for _, f := range fills {
		if f.Schema != 1 || f.Checksum == "" || f.Checksum != f.digest() || f.Trade.Symbol != "BTCUSDT" || seenFills[f.Trade.TradeID] {
			return AccountingReport{}, errors.New("체결 체크섬·종목·중복 오류")
		}
		rec, ok := byOrder[f.ClientOrderID]
		if !ok || validateFill(rec, f.Trade) != nil {
			return AccountingReport{}, errors.New("체결의 원래 주문 소유자 확인 실패")
		}
		seenFills[f.Trade.TradeID] = true
		sorted = append(sorted, accountingEvent{at: f.Trade.TimeMS, trade: f.Trade, key: buildOwnerKey(rec), action: rec.Intent.Action,
			orderType: rec.Plan.Type, clientOrderID: rec.Plan.ClientOrderID})
	}
	for _, f := range protection {
		if f.Schema != 1 || f.Checksum == "" || f.Checksum != f.digest() || f.Trade.Symbol != "BTCUSDT" || f.Trade.TradeID <= 0 || seenFills[f.Trade.TradeID] ||
			(f.Source != Manual && f.Source != Model) || (f.Source == Manual && f.ModelID != "") ||
			(f.Source == Model && !modelIDPattern.MatchString(f.ModelID)) || (f.PositionSide != Long && f.PositionSide != Short) ||
			f.Trade.PositionSide != string(f.PositionSide) {
			return AccountingReport{}, errors.New("protection 체결 체크섬·소유자·중복 오류")
		}
		wantSide := "SELL"
		if f.PositionSide == Short {
			wantSide = "BUY"
		}
		if f.Trade.Side != wantSide {
			return AccountingReport{}, errors.New("protection 체결 방향이 소유 포지션과 다릅니다")
		}
		if _, err := positiveRat(f.Trade.Quantity); err != nil {
			return AccountingReport{}, errors.New("protection 체결 수량 오류")
		}
		if _, err := positiveRat(f.Trade.Price); err != nil {
			return AccountingReport{}, errors.New("protection 체결 가격 오류")
		}
		if _, err := signedRat(f.Trade.Commission); err != nil || f.Trade.CommissionAsset == "" {
			return AccountingReport{}, errors.New("protection 체결 수수료 오류")
		}
		if _, err := signedRat(f.Trade.RealizedPnL); err != nil {
			return AccountingReport{}, errors.New("protection 체결 실현손익 오류")
		}
		seenFills[f.Trade.TradeID] = true
		key := ownerKey{source: f.Source, model: f.ModelID, symbol: "BTCUSDT", side: f.PositionSide}
		pa := protectionByOwner[key]
		if pa == nil {
			pa = newProtectionOwnerAcct()
			protectionByOwner[key] = pa
		}
		if pa.groups[f.GroupID] == nil {
			pa.groups[f.GroupID] = make(map[string]bool)
		}
		pa.groups[f.GroupID][f.Leg] = true
		if protectionGroups[f.GroupID] == nil {
			protectionGroups[f.GroupID] = make(map[string]bool)
		}
		protectionGroups[f.GroupID][f.Leg] = true
		qty, _ := positiveRat(f.Trade.Quantity)
		price, _ := positiveRat(f.Trade.Price)
		fee, _ := signedRat(f.Trade.Commission)
		exGross, _ := signedRat(f.Trade.RealizedPnL)
		pa.fills++
		switch f.Leg {
		case "tp":
			pa.tpFills++
			pa.tpQty.Add(pa.tpQty, qty)
		case "sl":
			pa.slFills++
			pa.slQty.Add(pa.slQty, qty)
		default:
			return AccountingReport{}, errors.New("알 수 없는 protection leg")
		}
		// Virtual protection gross uses the same owner cost basis during replay;
		// it is accumulated in processFill when the exact historical order is applied.
		_ = price
		pa.exchangeGross.Add(pa.exchangeGross, exGross)
		sumAsset(pa.fees, f.Trade.CommissionAsset, fee)
		sorted = append(sorted, accountingEvent{at: f.Trade.TimeMS, trade: f.Trade, key: key, action: Exit, protection: true})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].at != sorted[j].at {
			return sorted[i].at < sorted[j].at
		}
		return sorted[i].trade.TradeID < sorted[j].trade.TradeID
	})
	r.KnownOrders, r.KnownFills, r.IncomeRows = len(records), len(fills)+len(protection), len(in.SignedIncome)
	quotes := make(map[string]BackendQuote, len(in.Quotes))
	for _, q := range in.Quotes {
		if q.ClientOrderID == "" || q.Symbol != "BTCUSDT" || q.Source != "BACKEND_WS_TRADE" || q.ObservedMS <= 0 ||
			q.ObservedMS > created.Add(time.Second).UnixMilli() || byOrder[q.ClientOrderID].Plan.ClientOrderID == "" || quotes[q.ClientOrderID].ClientOrderID != "" {
			return AccountingReport{}, errors.New("슬리피지 기준 시세의 출처 또는 주문 ID 검증 실패")
		}
		if _, err := positiveRat(q.Price); err != nil {
			return AccountingReport{}, errors.New("잘못된 기준 시세")
		}
		quotes[q.ClientOrderID] = q
	}
	funding := make([]exchange.IncomeRecord, 0)
	fundingAccount := make(map[string]*big.Rat)
	unallocated := make(map[string]*big.Rat)
	otherIncome := make(map[string]*big.Rat)
	seenIncome := make(map[string]bool)
	for _, v := range in.SignedIncome {
		if v.TransactionID <= 0 || v.Type == "" || v.Asset == "" || v.TimeMS < in.WindowStartMS || v.TimeMS > in.WindowEndMS ||
			strings.ContainsAny(v.Symbol+v.Type+v.Asset, "\x00\r\n|/") || seenIncome[incomeKey(v)] {
			return AccountingReport{}, errors.New("소득 내역 ID·조회 범위·중복 오류")
		}
		x, err := signedRat(string(v.Income))
		if err != nil {
			return AccountingReport{}, errors.New("소득 금액을 정확한 십진수로 읽을 수 없습니다")
		}
		seenIncome[incomeKey(v)] = true
		if v.Type == "FUNDING_FEE" {
			sumAsset(fundingAccount, v.Asset, x)
			funding = append(funding, v)
		} else {
			sumAsset(otherIncome, v.Type+":"+v.Asset, x)
		}
	}
	sort.Slice(funding, func(i, j int) bool {
		if funding[i].TimeMS != funding[j].TimeMS {
			return funding[i].TimeMS < funding[j].TimeMS
		}
		return funding[i].TransactionID < funding[j].TransactionID
	})
	owners := make(map[ownerKey]*ownerAcct)
	if opening != nil {
		for key, prior := range opening {
			if prior == nil || prior.qty == nil || prior.avg == nil || prior.qty.Sign() < 0 {
				return AccountingReport{}, errors.New("개시 수량 기록 손상")
			}
			if prior.qty.Sign() == 0 {
				continue
			}
			if prior.avg.Sign() <= 0 {
				return AccountingReport{}, errors.New("개시 원가 기록 손상")
			}
			owner := newOwnerAcct(key)
			owner.lot.qty.Set(prior.qty)
			owner.lot.avg.Set(prior.avg)
			owners[key] = owner
		}
		if len(owners) > 0 {
			r.Warnings = append(r.Warnings, "구간 시작 전 미청산 포지션의 개시 원가와 수량은 이전 로컬 체결 재생값입니다. 구간 이전의 독립 서명 이력과 지속 스트림이 검증된 것은 아니며 이 구간의 확정 순손익을 의미하지 않습니다.")
		}
	}
	get := func(k ownerKey) *ownerAcct {
		if owners[k] == nil {
			owners[k] = newOwnerAcct(k)
		}
		return owners[k]
	}
	processFill := func(e accountingEvent) error {
		t, k := e.trade, e.key
		o := get(k)
		qty, _ := positiveRat(t.Quantity)
		price, _ := positiveRat(t.Price)
		fee, _ := signedRat(t.Commission)
		gross, _ := signedRat(t.RealizedPnL)
		if e.action == Enter {
			total := new(big.Rat).Add(o.lot.qty, qty)
			sum := new(big.Rat).Add(new(big.Rat).Mul(o.lot.qty, o.lot.avg), new(big.Rat).Mul(price, qty))
			o.lot.avg.Quo(sum, total)
			o.lot.qty.Set(total)
		} else {
			if o.lot.qty.Cmp(qty) < 0 {
				return errors.New("소유자 보유량보다 청산이 큽니다")
			}
			diff := new(big.Rat).Sub(price, o.lot.avg)
			if k.side == Short {
				diff.Neg(diff)
			}
			fillGross := new(big.Rat).Mul(diff, qty)
			o.lot.gross.Add(o.lot.gross, fillGross)
			if e.protection {
				if pa := protectionByOwner[k]; pa != nil {
					pa.gross.Add(pa.gross, fillGross)
				}
			}
			o.lot.qty.Sub(o.lot.qty, qty)
			if o.lot.qty.Sign() == 0 {
				o.lot.avg.SetInt64(0)
			}
		}
		o.lot.exchangeGross.Add(o.lot.exchangeGross, gross)
		sumAsset(o.lot.fees, t.CommissionAsset, fee)
		// Only market-order executions can use the captured trade quote as a
		// *reference*. No quote or stale quote => unknown, not zero slippage.
		if !e.protection && e.orderType == Market {
			o.slipEligible++
			q, exists := quotes[e.clientOrderID]
			if exists && q.ObservedMS <= t.TimeMS && t.TimeMS-q.ObservedMS <= 15000 {
				ref, _ := positiveRat(q.Price)
				unfavorable := new(big.Rat).Sub(price, ref)
				if t.Side == "SELL" {
					unfavorable.Neg(unfavorable)
				}
				o.slippage.Add(o.slippage, new(big.Rat).Mul(unfavorable, qty))
				o.slipCovered++
			}
		} else if !e.protection {
			o.limitFills++
		}
		return nil
	}
	fi := 0
	for _, f := range funding {
		// The historical income aggregate contains no model/side allocation.
		// Provisional assignment is only meaningful if exactly one direction
		// was held and all known owners in that direction can share by qty.
		for fi < len(sorted) && sorted[fi].at < f.TimeMS {
			if err := processFill(sorted[fi]); err != nil {
				return AccountingReport{}, err
			}
			fi++
		}
		amount, _ := signedRat(string(f.Income))
		ambiguous := (fi < len(sorted) && sorted[fi].at == f.TimeMS)
		eligible := make([]*ownerAcct, 0)
		var side PositionSide
		total := new(big.Rat)
		if f.Symbol != "BTCUSDT" || f.Asset != "USDT" {
			ambiguous = true
		}
		for _, k := range keysSorted(owners) {
			o := owners[k]
			if o.lot.qty.Sign() == 0 {
				continue
			}
			if side != "" && side != k.side {
				ambiguous = true
			}
			side = k.side
			eligible = append(eligible, o)
			total.Add(total, o.lot.qty)
		}
		if ambiguous || total.Sign() == 0 || len(eligible) == 0 {
			sumAsset(unallocated, f.Asset, amount)
			continue
		}
		// Exact internal rational shares, rounded only when serializing.
		// This remains PROVISIONAL even for one owner: a flat external trade
		// between snapshots is not observable from the local fill archive.
		allocated := new(big.Rat)
		for i, o := range eligible {
			share := new(big.Rat)
			if i == len(eligible)-1 {
				share.Sub(amount, allocated)
			} else {
				share.Mul(amount, new(big.Rat).Quo(o.lot.qty, total))
				allocated.Add(allocated, share)
			}
			o.funding.Add(o.funding, share)
		}
	}
	for ; fi < len(sorted); fi++ {
		if err := processFill(sorted[fi]); err != nil {
			return AccountingReport{}, err
		}
	}
	r.AccountFundingByAsset = mapDecimal(fundingAccount)
	r.UnallocatedFundingByAsset = mapDecimal(unallocated)
	r.OtherIncomeByTypeAsset = mapDecimal(otherIncome)
	r.ProtectionFillCount = len(protection)
	r.ProtectionGroupCount = len(protectionGroups)
	for _, legs := range protectionGroups {
		if legs["tp"] && legs["sl"] {
			r.DualTriggeredGroupCount++
		}
	}
	if r.DualTriggeredGroupCount > 0 {
		r.Warnings = append(r.Warnings, "TP와 SL이 모두 실제 체결된 protection group이 존재합니다. 합산 청산량은 검증되었지만 조건부 주문의 동시 발동 위험이 실제로 발생한 이력으로 표시합니다.")
	}
	for _, k := range keysSorted(owners) {
		o := owners[k]
		usdt := o.lot.fees["USDT"]
		if usdt == nil {
			usdt = new(big.Rat)
		}
		afterFees := new(big.Rat).Sub(o.lot.gross, usdt)
		afterFunding := new(big.Rat).Add(afterFees, o.funding)
		otherFee := false
		for asset, amount := range o.lot.fees {
			if asset != "USDT" && amount.Sign() != 0 {
				otherFee = true
			}
		}
		slipCoverage := "NO_MARKET_EXECUTIONS"
		if o.limitFills > 0 {
			slipCoverage = "LIMIT_FILLS_NOT_COMPARABLE"
		}
		slipAmount := ""
		if o.slipEligible > 0 {
			slipCoverage = "INCOMPLETE_OR_STALE_BACKEND_QUOTE"
			if o.slipCovered == o.slipEligible {
				slipCoverage = "MARKET_FILL_REFERENCE_ESTIMATE_ONLY"
				slipAmount = formatOwnershipDecimal(o.slippage)
			}
		}
		ownerOut := OwnerAccounting{Source: k.source, ModelID: k.model, Symbol: k.symbol, PositionSide: k.side,
			OpenQuantity: formatOwnershipDecimal(o.lot.qty), AverageEntry: formatOwnershipDecimal(o.lot.avg),
			VirtualGrossRealizedUSDT: formatOwnershipDecimal(o.lot.gross), ExchangeReportedGrossUSDT: formatOwnershipDecimal(o.lot.exchangeGross),
			CommissionByAsset: mapDecimal(o.lot.fees), VirtualAfterUSDTFeesExFunding: formatOwnershipDecimal(afterFees),
			ProvisionalFundingUSDT: formatOwnershipDecimal(o.funding), ProvisionalAfterFundingUSDT: formatOwnershipDecimal(afterFunding),
			SignedUnfavorableSlippageUSDT: slipAmount, SlippageCoverage: slipCoverage, NonUSDTCommission: otherFee,
			HasProvisionalFunding: o.funding.Sign() != 0}
		if pa := protectionByOwner[k]; pa != nil && pa.fills > 0 {
			dual := 0
			for _, legs := range pa.groups {
				if legs["tp"] && legs["sl"] {
					dual++
				}
			}
			ownerOut.Protection = &ProtectionAccountingAttribution{FillCount: pa.fills, GroupCount: len(pa.groups), DualTriggeredGroupCount: dual,
				TakeProfitFillCount: pa.tpFills, StopLossFillCount: pa.slFills, TakeProfitQuantity: formatOwnershipDecimal(pa.tpQty), StopLossQuantity: formatOwnershipDecimal(pa.slQty),
				VirtualGrossRealizedUSDT: formatOwnershipDecimal(pa.gross), ExchangeReportedGrossUSDT: formatOwnershipDecimal(pa.exchangeGross), CommissionByAsset: mapDecimal(pa.fees)}
		}
		r.Owners = append(r.Owners, ownerOut)
	}
	if len(unallocated) > 0 {
		r.Warnings = append(r.Warnings, "미배분 펀딩비가 존재하므로 어떤 모델의 전체 최종 순손익도 확정하지 않습니다.")
	}
	if len(sorted) > 0 && in.WindowStartMS > sorted[0].at {
		r.Warnings = append(r.Warnings, "계정 소득 조회가 일부 체결 이전부터 시작하지 않았습니다.")
	}
	r.seal()
	return r, nil
}

// AnalyzeOfflineFromLocalStore reads the complete immutable journal/fill set
// under the store's lock. It never fetches account data or grants permission;
// the caller must separately obtain income via signed Demo read-only REST.
func (s *OwnershipStore) AnalyzeOfflineFromLocalStore(in AccountingInput, created time.Time) (AccountingReport, error) {
	if s == nil {
		return AccountingReport{}, errors.New("소유 장부가 없습니다")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.ScanJournal()
	if err != nil {
		return AccountingReport{}, err
	}
	fills, err := s.readFills()
	if err != nil {
		return AccountingReport{}, err
	}
	protection, err := readValidatedProtectionFillsForStore(filepath.Dir(s.fillDir), fills)
	if err != nil {
		return AccountingReport{}, err
	}
	return evaluateAccountingOffline(records, fills, protection, in, created, nil)
}

// AnalyzeFromSignedDemoBaseline is the ONLY intended production entry into
// this offline estimator. Callers must obtain b through the internal Demo-only
// authenticated signed REST recovery reader, never through an HTTP body.
// A bounded REST bracket is not atomic and does NOT certify outside activity.
func (s *OwnershipStore) AnalyzeFromSignedDemoBaseline(b exchange.RecoveryBaseline, quotes []BackendQuote, now time.Time) (AccountingReport, error) {
	if b.Environment != "demo" || b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		b.StartedAt.IsZero() || b.FinishedAt.IsZero() || b.FinishedAt.Before(b.StartedAt) ||
		b.FinishedAt.After(now.Add(time.Second)) || now.Sub(b.FinishedAt) > 5*time.Second ||
		b.RecentIncome == nil {
		return AccountingReport{}, errors.New("신선한 실제 Demo 서명 조회 결과가 필요합니다")
	}
	in := AccountingInput{WindowStartMS: b.WindowStartMS, WindowEndMS: b.WindowEndMS,
		SignedIncome: b.RecentIncome, Quotes: quotes}
	out, err := s.AnalyzeOfflineFromLocalStore(in, now)
	if err != nil {
		return out, err
	}
	if len(b.OpenOrders) > 0 || len(b.OpenAlgos) > 0 {
		out.Warnings = append(out.Warnings, "현재 계정에 미체결 일반/조건부 주문이 있습니다. 전체 최종 PnL과 주문 소유권 확인이 필요합니다.")
	}
	// A transient full-account view cannot determine ownership at previous
	// funding times. No automatic PnL authorization is implied.
	out.seal()
	return out, nil
}
