package exchange

// READ-ONLY bounded, authenticated Demo recovery observations. A historical
// REST window is not proof of complete account lifetime, nor is a pair of
// snapshots proof that no same-size flat round trip occurred between them.
// Nothing here grants trading permission or sends an order.
import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"time"
)

type PortfolioAsset struct {
	Asset            string    `json:"asset"`
	WalletBalance    wsDecimal `json:"walletBalance"`
	AvailableBalance wsDecimal `json:"availableBalance"`
	MarginBalance    wsDecimal `json:"marginBalance"`
}
type PortfolioPosition struct {
	Symbol        string    `json:"symbol"`
	PositionSide  string    `json:"positionSide"`
	PositionAmt   wsDecimal `json:"positionAmt"`
	InitialMargin wsDecimal `json:"initialMargin"`
	Isolated      bool      `json:"isolated"`
}

// V3 values are preserved exactly as decimal strings: never float64.
type AccountPortfolio struct {
	CanTrade                    bool                `json:"canTrade"`
	MultiAssetsMargin           bool                `json:"multiAssetsMargin"`
	TotalWalletBalance          wsDecimal           `json:"totalWalletBalance"`
	TotalUnrealizedProfit       wsDecimal           `json:"totalUnrealizedProfit"`
	TotalMarginBalance          wsDecimal           `json:"totalMarginBalance"`
	TotalInitialMargin          wsDecimal           `json:"totalInitialMargin"`
	TotalPositionInitialMargin  wsDecimal           `json:"totalPositionInitialMargin"`
	TotalOpenOrderInitialMargin wsDecimal           `json:"totalOpenOrderInitialMargin"`
	AvailableBalance            wsDecimal           `json:"availableBalance"`
	Assets                      []PortfolioAsset    `json:"assets"`
	Positions                   []PortfolioPosition `json:"positions"`
}
type IncomeRecord struct {
	Symbol        string    `json:"symbol"`
	Type          string    `json:"incomeType"`
	Income        wsDecimal `json:"income"`
	Asset         string    `json:"asset"`
	TimeMS        int64     `json:"time"`
	TransactionID int64     `json:"tranId"`
}
type RecoveryBaseline struct {
	Environment string `json:"environment"`
	// ObservedDuring is a bracket of authenticated REST reads, not an exchange snapshot transaction.
	StartedAt       time.Time        `json:"started_at"`
	FinishedAt      time.Time        `json:"finished_at"`
	WindowStartMS   int64            `json:"window_start_ms"`
	WindowEndMS     int64            `json:"window_end_ms"`
	Portfolio       AccountPortfolio `json:"portfolio"`
	OpenOrders      []Order          `json:"open_orders"` // ALL symbols
	OpenAlgos       []AlgoOrder      `json:"open_algos"`  // ALL symbols
	RecentBTCOrders []Order          `json:"recent_btc_orders"`
	RecentBTCAlgos  []AlgoOrder      `json:"recent_btc_algos"`
	RecentBTCTrades []AccountTrade   `json:"recent_btc_trades"`
	RecentIncome    []IncomeRecord   `json:"recent_income"` // ALL symbols + all income types
	// Important: these calls cannot show records already expired by Binance retention.
	Coverage string `json:"coverage"`
}

func validPortfolio(p AccountPortfolio) error {
	if p.Assets == nil || p.Positions == nil ||
		!decimalNonNegative(string(p.TotalWalletBalance), true) ||
		!decimalSigned(string(p.TotalUnrealizedProfit)) ||
		!decimalNonNegative(string(p.TotalMarginBalance), true) ||
		!decimalNonNegative(string(p.TotalInitialMargin), true) ||
		!decimalNonNegative(string(p.TotalPositionInitialMargin), true) ||
		!decimalNonNegative(string(p.TotalOpenOrderInitialMargin), true) ||
		!decimalNonNegative(string(p.AvailableBalance), true) {
		return errors.New("불완전한 계정 자산/포지션 V3 응답")
	}
	uniqueAsset := map[string]bool{}
	for _, a := range p.Assets {
		if a.Asset == "" || uniqueAsset[a.Asset] ||
			!decimalNonNegative(string(a.WalletBalance), true) ||
			!decimalNonNegative(string(a.AvailableBalance), true) ||
			!decimalNonNegative(string(a.MarginBalance), true) {
			return errors.New("잘못되거나 중복된 계정 자산")
		}
		uniqueAsset[a.Asset] = true
	}
	if !uniqueAsset["USDT"] {
		return errors.New("USDT 계정 자산이 누락되었습니다")
	}
	uniquePosition := map[string]bool{}
	for _, p := range p.Positions {
		k := p.Symbol + ":" + p.PositionSide
		if p.Symbol == "" || (p.PositionSide != "LONG" && p.PositionSide != "SHORT" && p.PositionSide != "BOTH") ||
			uniquePosition[k] || !decimalSigned(string(p.PositionAmt)) ||
			!decimalNonNegative(string(p.InitialMargin), true) {
			return errors.New("잘못되거나 중복된 전체 계정 포지션")
		}
		uniquePosition[k] = true
	}
	return nil
}
func sameAccountBoundary(a, b AccountPortfolio) bool {
	if a.CanTrade != b.CanTrade || a.MultiAssetsMargin != b.MultiAssetsMargin ||
		a.TotalWalletBalance != b.TotalWalletBalance || len(a.Assets) != len(b.Assets) || len(a.Positions) != len(b.Positions) {
		return false
	}
	aa, ba := map[string]PortfolioAsset{}, map[string]PortfolioAsset{}
	for _, v := range a.Assets {
		aa[v.Asset] = v
	}
	for _, v := range b.Assets {
		ba[v.Asset] = v
	}
	for k, v := range aa {
		other, ok := ba[k]
		// Available/margin balances fluctuate with price or pending margin;
		// compare wallet only. Separate exposure checks inspect fresh ending values.
		if !ok || v.WalletBalance != other.WalletBalance {
			return false
		}
	}
	ap, bp := map[string]string{}, map[string]string{}
	for _, v := range a.Positions {
		ap[v.Symbol+":"+v.PositionSide] = string(v.PositionAmt)
	}
	for _, v := range b.Positions {
		bp[v.Symbol+":"+v.PositionSide] = string(v.PositionAmt)
	}
	for k, v := range ap {
		if bp[k] != v {
			return false
		}
	}
	return true
}
func stableOrders(a, b []Order) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := map[int64]string{}, map[int64]string{}
	for _, o := range a {
		if o.OrderID <= 0 {
			return false
		}
		x[o.OrderID] = fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%d", o.Symbol, o.ClientOrderID, o.Side, o.PositionSide, o.Type, o.Status, o.OrigQty, o.ExecutedQty, o.Price, o.StopPrice, o.UpdateTime)
	}
	for _, o := range b {
		if o.OrderID <= 0 {
			return false
		}
		y[o.OrderID] = fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%d", o.Symbol, o.ClientOrderID, o.Side, o.PositionSide, o.Type, o.Status, o.OrigQty, o.ExecutedQty, o.Price, o.StopPrice, o.UpdateTime)
	}
	if len(x) != len(a) || len(y) != len(b) {
		return false
	}
	for k, v := range x {
		if y[k] != v {
			return false
		}
	}
	return true
}
func stableAlgos(a, b []AlgoOrder) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := map[int64]string{}, map[int64]string{}
	for _, o := range a {
		if o.AlgoID <= 0 {
			return false
		}
		x[o.AlgoID] = fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s:%s:%t", o.Symbol, o.ClientAlgoID, o.Side, o.PositionSide, o.AlgoStatus, o.Quantity, o.TriggerPrice, o.OrderType, o.WorkingType, o.ClosePosition)
	}
	for _, o := range b {
		if o.AlgoID <= 0 {
			return false
		}
		y[o.AlgoID] = fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s:%s:%t", o.Symbol, o.ClientAlgoID, o.Side, o.PositionSide, o.AlgoStatus, o.Quantity, o.TriggerPrice, o.OrderType, o.WorkingType, o.ClosePosition)
	}
	if len(x) != len(a) || len(y) != len(b) {
		return false
	}
	for k, v := range x {
		if y[k] != v {
			return false
		}
	}
	return true
}
func uniqueHistory[T any](items []T, id func(T) string) error {
	ids := map[string]bool{}
	for _, v := range items {
		k := id(v)
		if k == "" || ids[k] {
			return errors.New("중복되거나 불완전한 거래소 이력")
		}
		ids[k] = true
	}
	return nil
}

// FetchDemoRecoveryBaseline collects a SHORT explicit historical window with
// strict list bounds; full pages are rejected rather than mistaking truncation
// for proof of no hidden orders/trades. Income pages are fetched exhaustively
// within a bounded maximum (rate-limit cost can be significant). Must run
// under an operator-controlled recovery procedure, NOT on every UI refresh.
func (c *Client) FetchDemoRecoveryBaseline(ctx context.Context, key, secret string, fromMS int64) (RecoveryBaseline, error) {
	if err := c.demoOnly(); err != nil {
		return RecoveryBaseline{}, err
	}
	started := c.now().UTC()
	nowMS := started.UnixMilli()
	if fromMS <= 0 || fromMS >= nowMS || nowMS-fromMS > int64((6*24*time.Hour)/time.Millisecond) {
		return RecoveryBaseline{}, errors.New("복구 조회는 명시적인 시작 시각과 최대 6일 범위가 필요합니다")
	}
	offset, err := c.serverOffset(ctx)
	if err != nil {
		return RecoveryBaseline{}, err
	}
	get := func(path string, values url.Values, out any) error {
		return c.signedGet(ctx, path, values, key, secret, offset, out)
	}
	first, _, err := c.fetchAccountPortfolio(ctx, key, secret, offset)
	if err != nil {
		return RecoveryBaseline{}, err
	}
	var open []Order
	if err = get("/fapi/v1/openOrders", url.Values{}, &open); err != nil {
		return RecoveryBaseline{}, err
	}
	if open == nil {
		return RecoveryBaseline{}, errors.New("전체 미체결 주문 목록 누락")
	}
	var algos []AlgoOrder
	if err = get("/fapi/v1/openAlgoOrders", url.Values{}, &algos); err != nil {
		return RecoveryBaseline{}, err
	}
	if algos == nil {
		return RecoveryBaseline{}, errors.New("전체 미체결 조건부 주문 목록 누락")
	}
	from := strconv.FormatInt(fromMS, 10)
	until := strconv.FormatInt(nowMS, 10)
	q := func() url.Values {
		return url.Values{"symbol": {"BTCUSDT"}, "startTime": {from}, "endTime": {until}, "limit": {"1000"}}
	}
	var orders []Order
	if err = get("/fapi/v1/allOrders", q(), &orders); err != nil {
		return RecoveryBaseline{}, err
	}
	var recentAlgos []AlgoOrder
	if err = get("/fapi/v1/allAlgoOrders", q(), &recentAlgos); err != nil {
		return RecoveryBaseline{}, err
	}
	var trades []AccountTrade
	if err = get("/fapi/v1/userTrades", q(), &trades); err != nil {
		return RecoveryBaseline{}, err
	}
	if orders == nil || recentAlgos == nil || trades == nil || len(orders) >= 1000 || len(recentAlgos) >= 1000 || len(trades) >= 1000 {
		return RecoveryBaseline{}, errors.New("최근 주문·조건부 주문·체결 이력 누락 또는 1000건 조회 상한: 수동 조사 필요")
	}
	if err = uniqueHistory(orders, func(v Order) string {
		if v.Symbol != "BTCUSDT" || v.OrderID <= 0 || v.ClientOrderID == "" {
			return ""
		}
		return strconv.FormatInt(v.OrderID, 10)
	}); err != nil {
		return RecoveryBaseline{}, err
	}
	if err = uniqueHistory(recentAlgos, func(v AlgoOrder) string {
		if v.Symbol != "BTCUSDT" || v.AlgoID <= 0 {
			return ""
		}
		return strconv.FormatInt(v.AlgoID, 10)
	}); err != nil {
		return RecoveryBaseline{}, err
	}
	if err = uniqueHistory(trades, func(v AccountTrade) string {
		if v.OrderID <= 0 || (v.Side != "BUY" && v.Side != "SELL") || (v.PositionSide != "LONG" && v.PositionSide != "SHORT") ||
			!decimalNonNegative(v.Price, false) || !decimalNonNegative(v.Quantity, false) ||
			!decimalSigned(v.Commission) || !decimalSigned(v.RealizedPnL) || v.CommissionAsset == "" {
			return ""
		}
		if v.Symbol != "BTCUSDT" || v.TradeID <= 0 || v.TimeMS < fromMS || v.TimeMS > nowMS {
			return ""
		}
		return strconv.FormatInt(v.TradeID, 10)
	}); err != nil {
		return RecoveryBaseline{}, err
	}
	// Do NOT filter income by symbol: transfers and fees elsewhere change
	// portfolio equity and a BTC-only snapshot would hide external activity.
	income := make([]IncomeRecord, 0)
	for page := 1; page <= 20; page++ {
		p := url.Values{"startTime": {from}, "endTime": {until}, "page": {strconv.Itoa(page)}, "limit": {"1000"}}
		var rows []IncomeRecord
		if err = get("/fapi/v1/income", p, &rows); err != nil {
			return RecoveryBaseline{}, err
		}
		if rows == nil {
			return RecoveryBaseline{}, errors.New("수익·펀딩 이력 응답 누락")
		}
		for _, r := range rows {
			if r.Type == "" || r.Asset == "" || r.TransactionID <= 0 || r.TimeMS < fromMS || r.TimeMS > nowMS || !decimalSigned(string(r.Income)) {
				return RecoveryBaseline{}, errors.New("수익·펀딩 이력 데이터 불완전")
			}
		}
		income = append(income, rows...)
		if len(rows) < 1000 {
			break
		}
		if page == 20 {
			return RecoveryBaseline{}, errors.New("수익·펀딩 이력 20000건 페이지 상한: 수동 조사 필요")
		}
	}
	if err = uniqueHistory(income, func(v IncomeRecord) string { return v.Type + ":" + strconv.FormatInt(v.TransactionID, 10) }); err != nil {
		return RecoveryBaseline{}, err
	}
	last, _, err := c.fetchAccountPortfolio(ctx, key, secret, offset)
	if err != nil {
		return RecoveryBaseline{}, err
	}
	var finalOpen []Order
	if err = get("/fapi/v1/openOrders", url.Values{}, &finalOpen); err != nil {
		return RecoveryBaseline{}, err
	}
	var finalAlgos []AlgoOrder
	if err = get("/fapi/v1/openAlgoOrders", url.Values{}, &finalAlgos); err != nil {
		return RecoveryBaseline{}, err
	}
	if finalOpen == nil || finalAlgos == nil || !sameAccountBoundary(first, last) || !stableOrders(open, finalOpen) || !stableAlgos(algos, finalAlgos) {
		return RecoveryBaseline{}, errors.New("복구 조회 중 계정/미체결 주문 상태가 변경됐습니다. 재시도 전에 활동을 확인하세요")
	}
	finished := c.now().UTC()
	if finished.Before(started) || finished.Sub(started) > 30*time.Second {
		return RecoveryBaseline{}, errors.New("복구 조회에 너무 오래 걸렸습니다. 관측값이 오래되었습니다")
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].OrderID < orders[j].OrderID })
	sort.Slice(trades, func(i, j int) bool { return trades[i].TradeID < trades[j].TradeID })
	return RecoveryBaseline{Environment: "demo", StartedAt: started, FinishedAt: finished,
		WindowStartMS: fromMS, WindowEndMS: nowMS, Portfolio: last, OpenOrders: finalOpen, OpenAlgos: finalAlgos,
		RecentBTCOrders: orders, RecentBTCAlgos: recentAlgos, RecentBTCTrades: trades, RecentIncome: income,
		Coverage: "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS"}, nil
}

// ValidatePortfolioSnapshot is shared with risk diagnostics, not order authorization.
func ValidatePortfolioSnapshot(p AccountPortfolio) error { return validPortfolio(p) }
