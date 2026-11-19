package trading

// This module joins NORMALIZED private user-stream signals to independently
// signed REST order and userTrades evidence. It is an OFFLINE read-only
// diagnostic, NOT a user-data stream consumer or an execution gate. Binance
// events have no gap-free account-wide sequence; a matching set of events
// cannot prove that unrelated/external orders did not happen between reads.
import (
	"context"
	"errors"
	"math/big"
	"sort"
	"time"

	"binance_trader/internal/exchange"
)

// SignedOrderEvidence should only be built by FetchSignedOrderEvidence using
// an authenticated DEMO reader, or by an isolated mock in tests. Two separate
// order reads bracket the full per-order trade-list fetch. Even a stable
// bracket is an observation, not a transactional exchange snapshot.
type SignedOrderEvidence struct {
	ClientOrderID        string
	Before               exchange.OrderResult
	After                exchange.OrderResult
	Trades               []exchange.AccountTrade
	BeforeReadAt         time.Time
	TradesReadAt         time.Time
	AfterReadAt          time.Time
	SignedOrderReads     bool
	CompleteSignedTrades bool
}

func FetchSignedOrderEvidence(ctx context.Context, reader DemoTradeReader, creds DemoCredentials, rec Record) (SignedOrderEvidence, error) {
	if reader == nil || creds.APIKey == "" || creds.Secret == "" {
		return SignedOrderEvidence{}, errors.New("독립된 Demo 서명 조회가 필요합니다")
	}
	plan, err := rec.Intent.Plan()
	if err != nil || rec.Plan != plan || rec.IntentDigest == "" {
		return SignedOrderEvidence{}, errors.New("주문 저널 검증 실패")
	}
	digest, _ := digestIntent(rec.Intent)
	if !supportedJournalSchema(rec.Schema) || rec.IntentDigest != digest {
		return SignedOrderEvidence{}, errors.New("원본 주문 요청 기록 불일치")
	}
	evidence := SignedOrderEvidence{ClientOrderID: plan.ClientOrderID}
	evidence.Before, err = reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, plan.Symbol, plan.ClientOrderID)
	evidence.BeforeReadAt = time.Now().UTC()
	if err != nil {
		return SignedOrderEvidence{}, errors.New("첫 번째 독립 서명 주문 조회 실패")
	}
	if _, err = verifyRemoteOrder(rec, evidence.Before); err != nil {
		return SignedOrderEvidence{}, errors.New("첫 번째 서명 주문 일치 검사 실패")
	}
	if rec.ExchangeOrderID > 0 && rec.ExchangeOrderID != evidence.Before.OrderID {
		return SignedOrderEvidence{}, errors.New("저널과 서명 주문 ID 불일치")
	}
	evidence.Trades, err = reader.FetchDemoOrderTrades(ctx, creds.APIKey, creds.Secret, plan.Symbol, evidence.Before.OrderID)
	evidence.TradesReadAt = time.Now().UTC()
	if err != nil || evidence.Trades == nil || len(evidence.Trades) >= 1000 {
		return SignedOrderEvidence{}, errors.New("전체 체결 서명 조회 불완전")
	}
	evidence.After, err = reader.QueryDemoOrder(ctx, creds.APIKey, creds.Secret, plan.Symbol, plan.ClientOrderID)
	evidence.AfterReadAt = time.Now().UTC()
	if err != nil {
		return SignedOrderEvidence{}, errors.New("두 번째 독립 서명 주문 조회 실패")
	}
	if _, err = verifyRemoteOrder(rec, evidence.After); err != nil || evidence.Before != evidence.After {
		return SignedOrderEvidence{}, errors.New("조회 중 주문 상태 변경: 다시 독립 진단해야 합니다")
	}
	if evidence.AfterReadAt.Sub(evidence.BeforeReadAt) > 5*time.Second {
		return SignedOrderEvidence{}, errors.New("서명 주문 조회 창이 너무 오래되었습니다")
	}
	evidence.SignedOrderReads = true
	evidence.CompleteSignedTrades = true // caller must additionally verify sums/IDs below
	// A successful signed trade HTTP response is not yet a complete fill proof.
	if _, err := verifySignedEvidence(rec, evidence, evidence.AfterReadAt); err != nil {
		return SignedOrderEvidence{}, err
	}
	return evidence, nil
}

func equalFinancialDecimal(a, b string) bool {
	x, ex := signedRat(a)
	y, ey := signedRat(b)
	return ex == nil && ey == nil && x.Cmp(y) == 0
}

// verifySignedEvidence rejects stale, contradictory or partial REST brackets.
// It is separate from account-wide audit, which remains mandatory.
func verifySignedEvidence(rec Record, e SignedOrderEvidence, now time.Time) (map[int64]exchange.AccountTrade, error) {
	if !e.SignedOrderReads || !e.CompleteSignedTrades || e.ClientOrderID != rec.Plan.ClientOrderID ||
		e.BeforeReadAt.IsZero() || e.TradesReadAt.IsZero() || e.AfterReadAt.IsZero() ||
		e.BeforeReadAt.After(e.TradesReadAt) || e.TradesReadAt.After(e.AfterReadAt) ||
		e.AfterReadAt.After(now.Add(time.Second)) || now.Sub(e.AfterReadAt) > 5*time.Second ||
		e.AfterReadAt.Sub(e.BeforeReadAt) > 5*time.Second || e.Before != e.After || e.Trades == nil || len(e.Trades) >= 1000 {
		return nil, errors.New("신선하고 안정적인 독립 서명 주문/전체 체결 관측 필요")
	}
	plan, err := rec.Intent.Plan()
	if err != nil || !supportedJournalSchema(rec.Schema) || rec.Plan != plan || rec.IntentDigest == "" {
		return nil, errors.New("주문 저널 형식 오류")
	}
	d, _ := digestIntent(rec.Intent)
	if rec.IntentDigest != d {
		return nil, errors.New("주문 저널 요청 해시 불일치")
	}
	qty, err := verifyRemoteOrder(rec, e.After)
	if err != nil || rec.ExchangeOrderID > 0 && e.After.OrderID != rec.ExchangeOrderID {
		return nil, errors.New("서명 주문과 저널 불일치")
	}
	fills := make(map[int64]exchange.AccountTrade, len(e.Trades))
	sum := new(big.Rat)
	for _, trade := range e.Trades {
		if trade.OrderID != e.After.OrderID || fills[trade.TradeID].TradeID != 0 || trade.TimeMS > now.Add(time.Second).UnixMilli() || validateFill(rec, trade) != nil {
			return nil, errors.New("서명 체결 중복·미래 시점·주문 불일치")
		}
		q, er := positiveRat(trade.Quantity)
		if er != nil {
			return nil, errors.New("서명 체결 수량 오류")
		}
		fills[trade.TradeID] = trade
		sum.Add(sum, q)
	}
	if sum.Cmp(qty) != 0 {
		return nil, errors.New("서명 체결 총합과 누적 수량 불일치")
	}
	switch e.After.Status {
	case "NEW":
		if qty.Sign() != 0 {
			return nil, errors.New("NEW인데 체결 수량 존재")
		}
	case "PARTIALLY_FILLED":
		full, er := positiveRat(rec.Plan.Quantity)
		if er != nil || qty.Sign() <= 0 || qty.Cmp(full) >= 0 {
			return nil, errors.New("부분 체결 상태 모순")
		}
	case "FILLED":
		full, er := positiveRat(rec.Plan.Quantity)
		if er != nil || qty.Cmp(full) != 0 {
			return nil, errors.New("FILLED 상태 수량 모순")
		}
	}
	return fills, nil
}

type StreamCorrelationReview struct {
	State                     string    `json:"state"` // BLOCKED / OBSERVED_SIGNALS_MATCH / NO_ORDER_SIGNALS
	CheckedAt                 time.Time `json:"checked_at"`
	OrderEventCount           int       `json:"order_event_count"`
	AccountEventCount         int       `json:"account_event_count"`
	MatchedOrderCount         int       `json:"matched_order_count"`
	MatchedTradeEvents        int       `json:"matched_trade_events"`
	ObservedClientIDs         []string  `json:"observed_client_ids"` // locally generated non-secret IDs
	Blockers                  []string  `json:"blockers"`
	RequiresFullAccountReview bool      `json:"requires_full_account_review"`
	StreamContinuityProven    bool      `json:"stream_continuity_proven"` // always false
	TradingAuthorized         bool      `json:"trading_authorized"`       // always false
	ManualReviewRequired      bool      `json:"manual_review_required"`   // always true
}

// ReviewStreamOrderCorrelation analyzes every supplied stored event (never
// silently drops out-of-window observations) against per-order signed GET and
// userTrades. Callers must additionally review the complete account REST
// baseline, external orders, Algo orders, wallet, funding and stream gaps.
// Input observations MUST NOT come from browser requests.
func ReviewStreamOrderCorrelation(events []AccountEvent, records []Record, signed []SignedOrderEvidence, now time.Time) StreamCorrelationReview {
	review := StreamCorrelationReview{State: "BLOCKED", CheckedAt: now.UTC(), RequiresFullAccountReview: true,
		ManualReviewRequired: true, Blockers: []string{}, ObservedClientIDs: []string{}}
	block := func(reason string) StreamCorrelationReview {
		review.Blockers = append(review.Blockers, reason)
		return review
	}
	if now.IsZero() || len(events) > 50000 || len(records) > 10000 || len(signed) > 10000 {
		return block("시각 또는 관측 수량 한도 오류")
	}
	known := make(map[string]Record, len(records))
	for _, rec := range records {
		plan, err := rec.Intent.Plan()
		d, _ := digestIntent(rec.Intent)
		if err != nil || !supportedJournalSchema(rec.Schema) || plan != rec.Plan || rec.IntentDigest != d || known[plan.ClientOrderID].Plan.ClientOrderID != "" {
			return block("중복되거나 변조된 원본 주문 저널")
		}
		known[plan.ClientOrderID] = rec
	}
	authenticated := make(map[string]SignedOrderEvidence, len(signed))
	trades := make(map[string]map[int64]exchange.AccountTrade, len(signed))
	seenSignedOrderIDs := make(map[int64]bool, len(signed))
	seenSignedTradeIDs := map[int64]bool{}
	for _, e := range signed {
		rec, ok := known[e.ClientOrderID]
		if !ok || authenticated[e.ClientOrderID].ClientOrderID != "" {
			return block("서명 증적에 없는 원본 주문 또는 중복 증적")
		}
		fills, err := verifySignedEvidence(rec, e, now)
		if err != nil {
			return block("독립 서명 주문/체결 검증 실패: " + err.Error())
		}
		if seenSignedOrderIDs[e.After.OrderID] {
			return block("두 소유자가 동일한 원격 주문 ID를 중복 지정했습니다")
		}
		seenSignedOrderIDs[e.After.OrderID] = true
		for tradeID := range fills {
			if seenSignedTradeIDs[tradeID] {
				return block("서로 다른 주문 사이에 중복된 실제 체결 ID")
			}
			seenSignedTradeIDs[tradeID] = true
		}
		authenticated[e.ClientOrderID] = e
		trades[e.ClientOrderID] = fills
	}
	seenEvents := map[string]bool{}
	seenTradeEvents := map[string]map[int64]string{}
	touched := map[string]bool{}
	for _, event := range events {
		if !eventDigest.MatchString(event.Digest) || event.EventTime <= 0 || event.EventTime > now.Add(time.Second).UnixMilli() || seenEvents[event.Digest] {
			return block("계정 이벤트 중복·미래 시점·해시 형식 오류")
		}
		seenEvents[event.Digest] = true
		switch event.Kind {
		case "ACCOUNT_UPDATE":
			review.AccountEventCount++
			if event.Account == nil || event.Account.Reason == "" {
				return block("불완전한 계정 업데이트")
			}
			// Sparse account updates never replace full signed account data.
		case "ORDER_TRADE_UPDATE":
			review.OrderEventCount++
			if event.Order == nil {
				return block("불완전한 주문 이벤트")
			}
			o := event.Order
			if !validStreamExecution(o.Execution) || !validStreamStatus(o.Status) ||
				(o.Execution == "TRADE" && o.Status != "PARTIALLY_FILLED" && o.Status != "FILLED") {
				return block("허용하지 않은 주문 실행/상태 조합: 전체 계정 감사 필요")
			}
			rec, ok := known[o.ClientID]
			if !ok {
				return block("외부 또는 저널에 없는 주문 이벤트")
			}
			e, ok := authenticated[o.ClientID]
			if !ok {
				return block("주문 이벤트에 대한 독립 서명 조회 누락")
			}
			if o.Symbol != rec.Plan.Symbol || o.PositionSide != string(rec.Plan.PositionSide) || o.Side != rec.Plan.ExchangeSide || o.OrderID != e.After.OrderID ||
				(rec.ExchangeOrderID > 0 && o.OrderID != rec.ExchangeOrderID) {
				return block("주문 스트림/저널/서명 주문의 식별자 불일치")
			}
			cum, err := decimalRat(o.CumulativeQty, true)
			signedCum, er := decimalRat(e.After.ExecutedQty, true)
			if err != nil || er != nil || cum.Cmp(signedCum) > 0 {
				return block("스트림 누적 체결량이 서명 거래 내역을 초과")
			}
			if o.Status == "NEW" && cum.Sign() != 0 {
				return block("미체결 주문 상태와 실시간 누적 체결 수량 모순")
			}
			if event.EventTime > e.AfterReadAt.Add(time.Second).UnixMilli() || event.TxTime > e.AfterReadAt.Add(time.Second).UnixMilli() {
				return block("최종 서명 주문 조회 이후 이벤트 발생: 조회 무효")
			}
			if isTerminalOrderStatus(o.Status) && e.After.Status != o.Status {
				return block("스트림 종료 상태와 서명 상태 불일치")
			}
			if o.Execution == "TRADE" {
				fill, present := trades[o.ClientID][o.TradeID]
				if !present || !equalFinancialDecimal(o.LastQty, fill.Quantity) || !equalFinancialDecimal(o.LastPrice, fill.Price) ||
					!equalFinancialDecimal(o.Fee, fill.Commission) || !equalFinancialDecimal(o.RealizedPnL, fill.RealizedPnL) ||
					o.FeeAsset != fill.CommissionAsset {
					return block("실시간 체결 이벤트와 서명 체결 자료 불일치")
				}
				if seenTradeEvents[o.ClientID] == nil {
					seenTradeEvents[o.ClientID] = map[int64]string{}
				}
				if seenTradeEvents[o.ClientID][o.TradeID] != "" {
					return block("같은 체결 ID에 여러 상이한 이벤트 존재")
				}
				seenTradeEvents[o.ClientID][o.TradeID] = event.Digest
				review.MatchedTradeEvents++
			} else {
				qty, err := decimalRat(o.LastQty, true)
				if err != nil || qty.Sign() != 0 {
					return block("TRADE 외 이벤트의 체결 수량 모순")
				}
			}
			touched[o.ClientID] = true
		default:
			// ACCOUNT_CONFIG_UPDATE / ALGO-related / liquidation / unknown
			// events require independent human-led global account review.
			return block("중요한 비주문 계정 이벤트: 전체 계정 상태 수동 조사 필요")
		}
	}
	for clientID := range authenticated {
		if !touched[clientID] {
			return block("이벤트가 없는 주문에 대한 불필요한 증적: 관측 범위 검증 필요")
		}
	}
	for clientID := range touched {
		review.ObservedClientIDs = append(review.ObservedClientIDs, clientID)
	}
	// Deliberately deterministic, even with randomized map iteration.
	sort.Strings(review.ObservedClientIDs)
	review.MatchedOrderCount = len(touched)
	if review.OrderEventCount == 0 {
		review.State = "NO_ORDER_SIGNALS"
		review.Blockers = append(review.Blockers, "주문 이벤트가 없다는 사실로 무거래/무누락을 증명할 수 없습니다")
	} else {
		review.State = "OBSERVED_SIGNALS_MATCH"
		review.Blockers = append(review.Blockers, "관측된 이벤트만 일치. 계정 전체/누락/조건부 주문·펀딩비 재검증 전 거래 금지")
	}
	return review
}

func isTerminalOrderStatus(s string) bool {
	switch s {
	case "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH", "FILLED":
		return true
	}
	return false
}

func validStreamExecution(v string) bool {
	switch v {
	case "NEW", "CANCELED", "CALCULATED", "EXPIRED", "TRADE", "REPLACED", "REJECTED", "AMENDMENT", "TRADE_PREVENTION":
		return true
	}
	return false
}
func validStreamStatus(v string) bool {
	switch v {
	case "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
		return true
	}
	return false
}
