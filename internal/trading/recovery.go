package trading

// This is a read-only startup/reconnect RECOVERY REVIEW, not an order gate.
// It compares a bounded signed REST history with a durable ownership ledger.
// No finite REST window proves that no external same-size round trip, hidden
// retention-expired trade, or unobserved user-data event ever occurred.
import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

type RecoveryReview struct {
	Version              int                 `json:"schema"`
	TradingAuthorized    bool                `json:"trading_authorized"` // always FALSE in this release
	ManualReviewRequired bool                `json:"manual_review_required"`
	WindowStartMS        int64               `json:"window_start_ms"`
	WindowEndMS          int64               `json:"window_end_ms"`
	ObservedAt           time.Time           `json:"observed_at"`
	Issues               []string            `json:"issues"`
	KnownOrders          int                 `json:"known_orders"`
	RecentOrders         int                 `json:"recent_orders"`
	RecentAlgos          int                 `json:"recent_algos"`
	RecentTrades         int                 `json:"recent_trades"`
	RecentIncome         int                 `json:"recent_income"`
	FundingByAsset       map[string]string   `json:"funding_by_asset"`
	Ownership            *OwnershipReport    `json:"ownership,omitempty"`
	Risk                 PortfolioRiskReview `json:"portfolio_risk"`
	Coverage             string              `json:"coverage"`
}

type PortfolioRiskReview struct {
	EquityUSDT                            string   `json:"equity_usdt"`
	AutoWalletAssignedUSDT                string   `json:"auto_wallet_assigned_usdt,omitempty"`
	AutoWalletFreeUSDT                    string   `json:"auto_wallet_free_usdt,omitempty"`
	AutoWalletMaxEntryUSDT                string   `json:"auto_wallet_max_entry_usdt,omitempty"`
	Cap30PercentUSDT                      string   `json:"cap_30_percent_usdt,omitempty"` // deprecated read-only field
	ExistingConservativeInitialMarginUSDT string   `json:"existing_conservative_initial_margin_usdt"`
	PendingReservedMarginUSDT             string   `json:"pending_reserved_margin_usdt"`
	ProposedMarginUSDT                    string   `json:"proposed_margin_usdt"`
	USDTAvailable                         string   `json:"usdt_available"`
	FitsCap                               bool     `json:"fits_cap"` // informative ONLY, no executable permission
	Issues                                []string `json:"issues"`
}

func absBigRat(x *big.Rat) *big.Rat          { return new(big.Rat).Abs(x) }
func mustDecimal(x string) (*big.Rat, error) { return decimalRat(x, true) }

// AssessPortfolioCap remains only for reading old recovery diagnostics.
// The 30% equity rule is RETIRED. Without a validated account-bound internal
// allocation and verified owner reservations, it always fails closed.
func AssessPortfolioCap(port exchange.AccountPortfolio, pending, proposed string) (PortfolioRiskReview, error) {
	r := PortfolioRiskReview{PendingReservedMarginUSDT: pending, ProposedMarginUSDT: proposed, Issues: []string{"자동매매 전용 배정 지갑과 실제 Demo 계정 일치 검증이 필요합니다"}}
	if err := exchange.ValidatePortfolioSnapshot(port); err != nil {
		return r, err
	}
	if _, err := mustDecimal(pending); err != nil {
		return r, err
	}
	if _, err := mustDecimal(proposed); err != nil {
		return r, err
	}
	r.EquityUSDT = string(port.TotalMarginBalance)
	r.USDTAvailable = string(port.AvailableBalance)
	r.ExistingConservativeInitialMarginUSDT = string(port.TotalInitialMargin)
	r.FitsCap = false
	return r, nil
}

func matchingTrades(a, b exchange.AccountTrade) bool {
	if a.Symbol != b.Symbol || a.TradeID != b.TradeID || a.OrderID != b.OrderID || a.Side != b.Side || a.PositionSide != b.PositionSide || a.CommissionAsset != b.CommissionAsset || a.TimeMS != b.TimeMS {
		return false
	}
	for _, v := range [][2]string{{a.Price, b.Price}, {a.Quantity, b.Quantity}, {a.Commission, b.Commission}, {a.RealizedPnL, b.RealizedPnL}} {
		x, e := signedRat(v[0])
		if e != nil {
			return false
		}
		y, e := signedRat(v[1])
		if e != nil || x.Cmp(y) != 0 {
			return false
		}
	}
	return true
}
func verifyRecoveryMarkers(dataDir string) (int, error) {
	path := filepath.Join(dataDir, "trading", "stream_discontinuities")
	files, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	for _, f := range files {
		name := f.Name()
		if f.IsDir() || len(name) != 37 || !strings.HasSuffix(name, ".json") {
			return 0, errors.New("비정상 계정 스트림 불연속 마커")
		}
		raw, err := os.ReadFile(filepath.Join(path, name))
		if err != nil {
			return 0, err
		}
		var v struct {
			Schema int    `json:"schema"`
			Reason string `json:"reason"`
			At     string `json:"at"`
		}
		if err := json.Unmarshal(raw, &v); err != nil || v.Schema != 1 || v.Reason == "" {
			return 0, errors.New("계정 스트림 불연속 기록 손상")
		}
		if _, err := time.Parse(time.RFC3339Nano, v.At); err != nil {
			return 0, errors.New("불연속 마커 시각 오류")
		}
	}
	return len(files), nil
}

// This scans the v0.3.7 on-disk coordinator reservation format READ-ONLY.
// ALL active entry reservations (including manual) reduce remaining capital.
// An unknown or corrupt reservation stops recovery assessment. No caller-
// supplied "pending amount" is trusted by ReviewRecovery.
func pendingDurableMargin(dataDir string) (string, error) {
	dir := filepath.Join(dataDir, "trading", "reservations")
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return "0", nil
	}
	if err != nil {
		return "", err
	}
	c := &Coordinator{reserveDir: dir}
	items, err := c.scan()
	if err != nil {
		return "", err
	}
	sum := new(big.Rat)
	for _, r := range items {
		if c.isDone(r) {
			continue
		}
		if r.Intent.Action != Enter {
			continue
		}
		v, err := mustDecimal(r.MarginUSDT)
		if err != nil {
			return "", err
		}
		sum.Add(sum, v)
	}
	return formatOwnershipDecimal(sum), nil
}

// ReviewRecovery checks local records against an *independently authenticated*
// signed-window observation. The caller must supply the correct dataDir and
// the snapshot must be collected explicitly, never accepted from HTTP/UI.
// Even an empty Issues list is NOT approval to place orders in v0.3.9.
func ReviewRecovery(dataDir string, store *OwnershipStore, b exchange.RecoveryBaseline, now time.Time) (RecoveryReview, error) {
	review := RecoveryReview{Version: 1, ManualReviewRequired: true, TradingAuthorized: false,
		WindowStartMS: b.WindowStartMS, WindowEndMS: b.WindowEndMS, ObservedAt: b.FinishedAt,
		Coverage: "BOUNDED_REST_WINDOW_NOT_PROOF_OF_STREAM_CONTINUITY", Issues: []string{}, FundingByAsset: map[string]string{}}
	issue := func(s string) { review.Issues = append(review.Issues, s) }
	if dataDir == "" || store == nil || filepath.Clean(filepath.Join(dataDir, "trading", "fills")) != filepath.Clean(store.fillDir) {
		return review, errors.New("소유권 장부와 동일한 저장 경로가 필요합니다")
	}
	if b.Environment != "demo" || b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" || b.WindowStartMS <= 0 || b.WindowEndMS <= b.WindowStartMS ||
		b.StartedAt.IsZero() || b.FinishedAt.Before(b.StartedAt) || b.FinishedAt.After(now.Add(time.Second)) || now.Sub(b.FinishedAt) > 5*time.Second ||
		b.WindowEndMS != b.StartedAt.UnixMilli() || b.FinishedAt.Sub(b.StartedAt) > 30*time.Second ||
		b.OpenOrders == nil || b.OpenAlgos == nil || b.RecentBTCOrders == nil || b.RecentBTCAlgos == nil || b.RecentBTCTrades == nil || b.RecentIncome == nil {
		return review, errors.New("완전하고 최근의 인증된 복구 스냅샷이 필요합니다")
	}
	// Binance REST list ordering is not an evidence semantic. Normalize an
	// independent copy before building issues, ownership snapshots and the
	// immutable recovery archive so the same signed evidence yields the same
	// report/checksum. Never mutate the caller-owned baseline.
	b = canonicalRecoveryReviewBaseline(b)
	pending, err := pendingDurableMargin(dataDir)
	if err != nil {
		return review, fmt.Errorf("보존된 주문 예약 정합성 확인 실패: %w", err)
	}
	risk, err := AssessPortfolioCap(b.Portfolio, pending, "0")
	if err != nil {
		return review, err
	}
	review.Risk = risk
	review.RecentOrders = len(b.RecentBTCOrders)
	review.RecentAlgos = len(b.RecentBTCAlgos)
	review.RecentTrades = len(b.RecentBTCTrades)
	review.RecentIncome = len(b.RecentIncome)
	for _, v := range risk.Issues {
		issue("계정 자산 위험: " + v)
	}
	for _, p := range b.Portfolio.Positions {
		q, err := signedRat(string(p.PositionAmt))
		if err != nil {
			return review, err
		}
		if p.Symbol != "BTCUSDT" && q.Sign() != 0 {
			issue("다른 종목의 미청산 포지션: " + p.Symbol)
		}
	}
	for _, o := range b.OpenOrders {
		if o.Symbol != "BTCUSDT" {
			issue("다른 종목의 미체결 주문: " + o.Symbol)
		}
	}
	if len(b.OpenAlgos) > 0 {
		issue("계정에 미체결 조건부 주문이 남아 있습니다")
	}
	if len(b.RecentBTCAlgos) > 0 {
		issue("최근 조건부 주문 이력의 소유권 검증이 아직 지원되지 않습니다")
	}
	eventsPath := filepath.Join(dataDir, "trading", "user_events")
	if stat, eventErr := os.Stat(eventsPath); eventErr == nil && stat.IsDir() {
		observed, err := (&EventStore{dir: eventsPath}).ReadAll()
		if err != nil {
			return review, err
		}
		if len(observed) > 0 {
			issue(fmt.Sprintf("과거 계정 이벤트 %d건: 연속성/체결 반영 승인 미완료", len(observed)))
		}
	} else if eventErr != nil && !errors.Is(eventErr, os.ErrNotExist) {
		return review, eventErr
	}
	markerN, err := verifyRecoveryMarkers(dataDir)
	if err != nil {
		return review, err
	}
	if markerN > 0 {
		issue(fmt.Sprintf("미해결 계정 스트림 불연속 마커 %d건", markerN))
	}
	records, err := store.ScanJournal()
	if err != nil {
		return review, err
	}
	fills, err := store.readFills()
	if err != nil {
		return review, err
	}
	protectionFills, err := readValidatedProtectionFillsForStore(filepath.Join(dataDir, "trading"), fills)
	if err != nil {
		return review, err
	}
	if _, err := replayWithProtection(records, fills, protectionFills); err != nil {
		return review, err
	}
	review.KnownOrders = len(records)
	journalByID := map[string]Record{}
	for _, r := range records {
		journalByID[r.Plan.ClientOrderID] = r
		update, err := time.Parse(time.RFC3339Nano, r.UpdatedAt)
		if err != nil {
			return review, errors.New("주문 저널 시간 형식 오류")
		}
		if update.UnixMilli() < b.WindowStartMS {
			issue("주문 저널 이전 기간의 이력 검증 불가능: " + r.Plan.ClientOrderID)
		}
	}
	localTrades := map[int64]StoredFill{}
	for _, f := range fills {
		localTrades[f.Trade.TradeID] = f
		if f.Trade.TimeMS < b.WindowStartMS {
			issue("이전 기간 체결 이력 보증 없음")
		}
	}
	recentByOrderID := map[int64]exchange.Order{}
	observations := map[string]OrderObservation{}
	for _, o := range b.RecentBTCOrders {
		if o.Symbol != "BTCUSDT" || o.OrderID <= 0 || o.ClientOrderID == "" {
			return review, errors.New("최근 주문 데이터 손상")
		}
		if _, dup := recentByOrderID[o.OrderID]; dup {
			return review, errors.New("최근 주문 ID 중복")
		}
		recentByOrderID[o.OrderID] = o
		rec, has := journalByID[o.ClientOrderID]
		if !has {
			issue("장부에 없는 최근 BTC 주문: " + o.ClientOrderID)
			continue
		}
		v := exchange.OrderResult{Symbol: o.Symbol, ClientOrderID: o.ClientOrderID, OrderID: o.OrderID,
			Side: o.Side, PositionSide: o.PositionSide, Type: o.Type, Status: o.Status, OriginalQty: o.OrigQty, ExecutedQty: o.ExecutedQty}
		if _, err := verifyRemoteOrder(rec, v); err != nil {
			issue("최근 거래소 주문 내용 불일치: " + o.ClientOrderID)
		}
		observations[o.ClientOrderID] = OrderObservation{Order: v, CheckedAt: b.FinishedAt}
	}
	for _, r := range records {
		if _, ok := observations[r.Plan.ClientOrderID]; !ok {
			issue("저널 주문의 최근 이력 누락: " + r.Plan.ClientOrderID)
		}
	}
	remoteTrades := map[int64]exchange.AccountTrade{}
	for _, t := range b.RecentBTCTrades {
		if t.Symbol != "BTCUSDT" || t.TradeID <= 0 || t.TimeMS < b.WindowStartMS || t.TimeMS > b.WindowEndMS {
			return review, errors.New("최근 체결 데이터 오류")
		}
		if _, dup := remoteTrades[t.TradeID]; dup {
			return review, errors.New("최근 체결 ID 중복")
		}
		remoteTrades[t.TradeID] = t
		stored, found := localTrades[t.TradeID]
		if !found {
			issue(fmt.Sprintf("외부 또는 미수집 체결 발견: %d", t.TradeID))
			continue
		}
		if !matchingTrades(t, stored.Trade) {
			issue(fmt.Sprintf("체결 장부와 거래소의 값 불일치: %d", t.TradeID))
		}
		if remoteOrder, ok := recentByOrderID[t.OrderID]; !ok || remoteOrder.ClientOrderID != stored.ClientOrderID {
			issue(fmt.Sprintf("거래소 주문·체결 연결 불일치: %d", t.TradeID))
		}
	}
	for _, f := range fills {
		if f.Trade.TimeMS >= b.WindowStartMS && f.Trade.TimeMS <= b.WindowEndMS {
			if _, ok := remoteTrades[f.Trade.TradeID]; !ok {
				issue(fmt.Sprintf("저장된 체결의 거래소 이력 누락: %d", f.Trade.TradeID))
			}
		}
	}
	funding := map[string]*big.Rat{}
	unidentifiedIncome := map[string]bool{}
	for _, inc := range b.RecentIncome {
		if inc.Type == "FUNDING_FEE" {
			v, err := signedRat(string(inc.Income))
			if err != nil {
				return review, err
			}
			if funding[inc.Asset] == nil {
				funding[inc.Asset] = new(big.Rat)
			}
			funding[inc.Asset].Add(funding[inc.Asset], v)
			if inc.Symbol != "BTCUSDT" && v.Sign() != 0 {
				issue("다른 종목에서 발생한 펀딩비: " + inc.Symbol)
			}
		} else {
			// Other nonzero income (commission, transfers, insurance, etc.)
			// must be allocated to signed fills or account history before
			// model-level NET PnL and cross-symbol risk can be trusted.
			amount, err := signedRat(string(inc.Income))
			if err != nil {
				return review, err
			}
			if amount.Sign() != 0 {
				signature := inc.Type + ":" + inc.Symbol
				if !unidentifiedIncome[signature] {
					issue("미배분 계정 수익/수수료 이력: " + signature)
					unidentifiedIncome[signature] = true
				}
			}
		}
	}
	keys := make([]string, 0, len(funding))
	for k := range funding {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		review.FundingByAsset[k] = formatOwnershipDecimal(funding[k])
		if funding[k].Sign() != 0 {
			issue("모델별 미배분 펀딩비가 존재합니다: " + k)
		}
	}
	// Convert the authenticated, account-WIDE snapshot to the BTC-only shape
	// used by the preexisting ledger. Account-wide external risk was checked
	// ABOVE; do not silently treat absent BTC positions as the entire account.
	snap := exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: b.FinishedAt.Format(time.RFC3339Nano),
		Balances: []exchange.Balance{}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}
	for _, a := range b.Portfolio.Assets {
		if a.Asset == "USDT" {
			snap.Balances = append(snap.Balances, exchange.Balance{Asset: a.Asset, Wallet: string(a.WalletBalance), Available: string(a.AvailableBalance)})
		}
	}
	for _, p := range b.Portfolio.Positions {
		if p.Symbol == "BTCUSDT" {
			q, err := signedRat(string(p.PositionAmt))
			if err != nil {
				return review, err
			}
			if q.Sign() != 0 {
				snap.Positions = append(snap.Positions, exchange.Position{Symbol: p.Symbol, Side: p.PositionSide, Qty: string(p.PositionAmt)})
			}
		}
	}
	for _, o := range b.OpenOrders {
		if o.Symbol == "BTCUSDT" {
			snap.Orders = append(snap.Orders, o)
		}
	}
	for _, a := range b.OpenAlgos {
		if a.Symbol == "BTCUSDT" {
			snap.AlgoOrders = append(snap.AlgoOrders, a)
		}
	}
	owned, err := store.Audit(snap, observations, now)
	if err != nil {
		return review, err
	}
	review.Ownership = &owned
	for _, s := range owned.Blockers {
		issue("소유권 감사: " + s)
	}
	// Missing historical/stream continuity cannot be inferred from the clean
	// bounded result. A future durable baseline and streaming replay must
	// explicitly resolve it; operators cannot override this flag with UI.
	issue("과거 bounded review만으로는 live continuity 승인 체크포인트를 대체할 수 없음: 현재 프로세스의 연속 관측과 별도 거래 직전 preflight 필요")
	return review, nil
}

// AssessPortfolioAllocation uses the NEW, separate Demo auto-allocation
// policy; the calling service must read 'assigned' from the durable wallet
// store, and 'owned/pending' only from authenticated/reconciled internal state.
// Its result is informative and is never an executable authorization token.
func AssessPortfolioAllocation(p exchange.AccountPortfolio, assigned, owned, pending, proposed string) (PortfolioRiskReview, error) {
	out := PortfolioRiskReview{Issues: []string{}, PendingReservedMarginUSDT: pending, ProposedMarginUSDT: proposed, AutoWalletAssignedUSDT: assigned}
	if err := exchange.ValidatePortfolioSnapshot(p); err != nil {
		return out, err
	}
	if p.MultiAssetsMargin || !p.CanTrade {
		out.Issues = append(out.Issues, "다중 자산 또는 거래 권한이 없는 계정")
	}
	walletBal, err := mustDecimal(string(p.TotalWalletBalance))
	if err != nil {
		return out, err
	}
	unreal, err := signedRat(string(p.TotalUnrealizedProfit))
	if err != nil {
		return out, err
	}
	equity, err := mustDecimal(string(p.TotalMarginBalance))
	if err != nil {
		return out, err
	}
	if absBigRat(new(big.Rat).Sub(new(big.Rat).Add(walletBal, unreal), equity)).Cmp(big.NewRat(1, 1000000)) > 0 {
		out.Issues = append(out.Issues, "계정 순자산 수치 불일치")
	}
	out.EquityUSDT = formatOwnershipDecimal(equity)
	// Binance totalInitialMargin already includes position + open-order margins;
	// never add individual position totals on top of this number.
	initial, err := mustDecimal(string(p.TotalInitialMargin))
	if err != nil {
		return out, err
	}
	positionMargin, err := mustDecimal(string(p.TotalPositionInitialMargin))
	if err != nil {
		return out, err
	}
	openMargin, err := mustDecimal(string(p.TotalOpenOrderInitialMargin))
	if err != nil {
		return out, err
	}
	if initial.Cmp(new(big.Rat).Add(positionMargin, openMargin)) < 0 {
		out.Issues = append(out.Issues, "거래소 증거금 합계 불일치")
	}
	out.ExistingConservativeInitialMarginUSDT = formatOwnershipDecimal(initial)
	seen := false
	for _, a := range p.Assets {
		if a.Asset == "USDT" {
			seen = true
			out.USDTAvailable = string(a.AvailableBalance)
		} else if a.WalletBalance != "0" && a.WalletBalance != "0.0" {
			v, e := mustDecimal(string(a.WalletBalance))
			if e != nil {
				return out, e
			}
			if v.Sign() != 0 {
				out.Issues = append(out.Issues, "다른 자산 잔액의 환산 미지원")
			}
		}
	}
	if !seen {
		return out, errors.New("USDT 가용 잔액 누락")
	}
	calc, err := wallet.Assess(assigned, owned, pending, proposed, out.USDTAvailable)
	if err != nil {
		return out, err
	}
	out.AutoWalletAssignedUSDT = calc.AssignedUSDT
	out.AutoWalletFreeUSDT = calc.FreeBudgetUSDT
	out.AutoWalletMaxEntryUSDT = calc.MaxNewModelEntryUSDT
	if !calc.Fits {
		out.Issues = append(out.Issues, calc.Reason)
	}
	out.FitsCap = len(out.Issues) == 0
	return out, nil
}
