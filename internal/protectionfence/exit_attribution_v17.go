package protectionfence

// V17 is a strictly read-only, non-authorizing exit-fill attribution review.
// Inputs must be assembled from signed Binance Demo algo / actual-order GETs,
// signed trade history and verified immutable local entry/protection journals.
// A caller-supplied struct does NOT prove provenance. Binance Hedge positions
// pool all entries per side and separate TP/SL legs are NOT an atomic OCO.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"sort"
	"strings"
	"time"
)

type ExitTradeV17 struct {
	TradeID         int64  `json:"trade_id"`
	OrderID         int64  `json:"order_id"`
	QuantityBTC     string `json:"quantity_btc"`
	PriceUSDT       string `json:"price_usdt"`
	CommissionUSDT  string `json:"commission_usdt"`
	RealizedPnLUSDT string `json:"realized_pnl_usdt"`
	CommissionAsset string `json:"commission_asset"`
	Side            string `json:"side"`
	PositionSide    string `json:"position_side"`
}

// ExitLegV17 represents ONE independently verified signed Algo and its actual
// signed order trades. ActualOrderID=0 is required when no fill is known.
// The adapter must compare two GETs bracketing all trade reads.
type ExitLegV17 struct {
	GroupID       string         `json:"group_id"`
	Leg           string         `json:"leg"` // tp or sl
	AlgoID        int64          `json:"algo_id"`
	ClientAlgoID  string         `json:"client_algo_id"`
	ActualOrderID int64          `json:"actual_order_id"`
	Status        string         `json:"status"`
	ActualQtyBTC  string         `json:"actual_qty_btc"`
	Trades        []ExitTradeV17 `json:"trades"`
}

type ExitAttributionReviewV17 struct {
	State                 string   `json:"state"`
	TargetGroupID         string   `json:"target_group_id"`
	SameSideEntryGroups   int      `json:"same_side_entry_groups"`
	TotalEntryGroups      int      `json:"total_entry_groups"`
	TotalExitTrades       int      `json:"total_exit_trades"`
	TargetExitQtyBTC      string   `json:"target_exit_qty_btc"`
	TargetRealizedPnLUSDT string   `json:"target_observed_realized_pnl_usdt"`
	TargetCommissionUSDT  string   `json:"target_observed_commission_usdt"`
	InputDigestSHA256     string   `json:"input_digest_sha256,omitempty"`
	Reasons               []string `json:"reasons"`
	EvidenceLimit         string   `json:"evidence_limit"`
	ManualReviewRequired  bool     `json:"manual_review_required"`
	TradingAuthorized     bool     `json:"trading_authorized"`
	SubmitAuthorized      bool     `json:"submit_authorized"`
	CancelAuthorized      bool     `json:"cancel_authorized"`
	RetryAuthorized       bool     `json:"retry_authorized"`
	SettlementAuthorized  bool     `json:"settlement_authorized"`
	FinanciallySettled    bool     `json:"financially_settled"`
}

func blockedExitV17(f Fence, why string) ExitAttributionReviewV17 {
	return ExitAttributionReviewV17{State: "BLOCKED_EXIT_ATTRIBUTION", TargetGroupID: f.Intent.GroupID,
		TargetExitQtyBTC: "0", TargetRealizedPnLUSDT: "0", TargetCommissionUSDT: "0",
		Reasons: []string{why}, ManualReviewRequired: true,
		EvidenceLimit: "BOUNDED_SIGNED_READ_AND_IMMUTABLE_LEDGER_NOT_CONTINUOUS_OR_OCO_OR_FINANCIAL_COMMIT"}
}

// Decimal money is exact; never round commission, funding or realized PnL.
func moneyRatV17(s string, signed bool) (*big.Rat, bool) {
	if len(s) == 0 || len(s) > 48 {
		return nil, false
	}
	if s[0] == '-' {
		if !signed {
			return nil, false
		}
		s = s[1:]
	}
	if len(s) == 0 {
		return nil, false
	}
	dot := 0
	before := 0
	after := 0
	for _, c := range s {
		if c == '.' {
			dot++
			continue
		}
		if c < '0' || c > '9' {
			return nil, false
		}
		if dot == 0 {
			before++
		} else {
			after++
		}
	}
	if before == 0 || before > 18 || dot > 1 || after > 12 || (dot == 1 && after == 0) {
		return nil, false
	}
	// Use the original signed text for exact parsing.
	if signed && len(s) > 0 { /* original sign recovered below by caller */
	}
	return new(big.Rat).SetString(s)
}

// StrictSignedMoneyV17 is a non-floating-point parser for signed financial
// fields. This does not establish the source of a claimed fee or PnL.
func StrictSignedMoneyV17(s string) (*big.Rat, bool) {
	neg := len(s) > 0 && s[0] == '-'
	v, ok := moneyRatV17(s, true)
	if !ok {
		return nil, false
	}
	if neg {
		v.Neg(v)
	}
	return v, true
}

// Every accepted input has <=12 fractional decimal digits, so FloatString(12)
// is an EXACT decimal conversion here, not a rounding operation.
func formatMoneyV17(x *big.Rat) string {
	s := x.FloatString(12)
	return strings.TrimRight(strings.TrimRight(s, "0"), ".")
}
func positiveMoneyV17(s string) (*big.Rat, bool) {
	r, ok := moneyRatV17(s, false)
	return r, ok && r.Sign() > 0
}

// ReviewExitAttributionV17 never posts to the exchange or writes accounting.
// It detects exact signed order/fill joins, impossible quantities, double
// triggers, and pooled Hedge ambiguity. Even the single-entry outcome means
// only "bounded consistency" and CANNOT authorize a financial posting.
func ReviewExitAttributionV17(f Fence, first, second SignedAccountFrameV15,
	entries []EntryGroupV16, legs []ExitLegV17, now time.Time) ExitAttributionReviewV17 {
	out := blockedExitV17(f, "signed exit attribution evidence missing or invalid")
	frame := InspectSignedAccountFramePairV15(f, first, second, now)
	if frame.State != "SIGNED_BOUNDED_ACCOUNT_STABLE_OWNER_UNPROVEN" {
		out.Reasons = append([]string{"two independent signed account reads did not reconcile"}, frame.Reasons...)
		return out
	}
	if len(entries) == 0 || len(entries) > 128 || len(legs) != 2*len(entries) || len(legs) > 256 {
		out.Reasons = []string{"complete bounded entry and both TP/SL leg inventories required"}
		return out
	}
	groups := append([]EntryGroupV16(nil), entries...)
	for i := range groups {
		groups[i].EntryTrades = append([]EntryTradeV16(nil), groups[i].EntryTrades...)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	ls := append([]ExitLegV17(nil), legs...)
	for i := range ls {
		ls[i].Trades = append([]ExitTradeV17(nil), ls[i].Trades...)
	}
	sort.Slice(ls, func(i, j int) bool {
		if ls[i].GroupID != ls[j].GroupID {
			return ls[i].GroupID < ls[j].GroupID
		}
		return ls[i].Leg < ls[j].Leg
	})
	groupIDs := map[string]bool{}
	clientIDs := map[string]bool{}
	entryOrders := map[int64]bool{}
	tradeIDs := map[int64]bool{}
	algoClients := map[string]bool{}
	remaining := map[string]*big.Rat{"LONG": new(big.Rat), "SHORT": new(big.Rat)}
	targetFound := false
	for _, g := range groups {
		if !validID(g.GroupID) || !validID(g.EntryClientOrderID) || g.GroupID != "entry_"+g.EntryClientOrderID ||
			g.EntryExchangeOrderID <= 0 || g.Symbol != "BTCUSDT" || (g.PositionSide != "LONG" && g.PositionSide != "SHORT") ||
			(g.Source != "manual" && g.Source != "model") || (g.Source == "manual" && g.ModelID != "") ||
			(g.Source == "model" && !ownerModelID.MatchString(g.ModelID)) ||
			!isSHA256(g.ProtectionPlanSHA256) || !validID(g.TPClientAlgoID) || !validID(g.SLClientAlgoID) ||
			g.TPClientAlgoID == g.SLClientAlgoID || groupIDs[g.GroupID] || clientIDs[g.EntryClientOrderID] || entryOrders[g.EntryExchangeOrderID] ||
			algoClients[g.TPClientAlgoID] || algoClients[g.SLClientAlgoID] {
			out.Reasons = []string{"invalid or duplicate immutable group, entry or protection identity"}
			return out
		}
		groupIDs[g.GroupID] = true
		clientIDs[g.EntryClientOrderID] = true
		entryOrders[g.EntryExchangeOrderID] = true
		algoClients[g.TPClientAlgoID] = true
		algoClients[g.SLClientAlgoID] = true
		requested, e1 := ownerQty(g.OriginalQtyBTC, false)
		executed, e2 := ownerQty(g.ExecutedQtyBTC, false)
		tp, e3 := ownerQty(g.TPPlannedQtyBTC, false)
		sl, e4 := ownerQty(g.SLPlannedQtyBTC, false)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || requested.Cmp(executed) != 0 || executed.Cmp(tp) != 0 || executed.Cmp(sl) != 0 ||
			len(g.EntryTrades) == 0 || len(g.EntryTrades) > 1000 {
			out.Reasons = []string{"entry fills or TP/SL plan quantities incomplete"}
			return out
		}
		entrySum := new(big.Rat)
		for _, t := range g.EntryTrades {
			q, err := ownerQty(t.QuantityBTC, false)
			if err != nil || t.TradeID <= 0 || t.OrderID != g.EntryExchangeOrderID || tradeIDs[t.TradeID] {
				out.Reasons = []string{"unmatched or duplicated signed entry fill"}
				return out
			}
			tradeIDs[t.TradeID] = true
			entrySum.Add(entrySum, q)
		}
		if entrySum.Cmp(executed) != 0 {
			out.Reasons = []string{"signed entry fill total disagrees with immutable entry"}
			return out
		}
		if g.PositionSide == f.Intent.PositionSide {
			out.SameSideEntryGroups++
		}
		if g.GroupID == f.Intent.GroupID {
			fq, e := ownerQty(f.Intent.QuantityBTC, false)
			if e != nil || g.PositionSide != f.Intent.PositionSide || g.ProtectionPlanSHA256 != f.Intent.PlanSHA256 || fq.Cmp(executed) != 0 ||
				(f.Intent.TargetClientAlgoID != g.TPClientAlgoID && f.Intent.TargetClientAlgoID != g.SLClientAlgoID) {
				out.Reasons = []string{"fence target differs from original group plan"}
				return out
			}
			targetFound = true
		}
	}
	if !targetFound || f.validate() != nil {
		out.Reasons = []string{"missing, corrupt or mismatched durable target mutation fence"}
		return out
	}
	usedLeg := map[string]bool{}
	algoIDs := map[int64]bool{}
	exitOrders := map[int64]bool{}
	byGroup := map[string][]ExitLegV17{}
	for _, l := range ls {
		if !groupIDs[l.GroupID] || (l.Leg != "tp" && l.Leg != "sl") || usedLeg[l.GroupID+"|"+l.Leg] ||
			l.AlgoID <= 0 || algoIDs[l.AlgoID] || l.ClientAlgoID == "" || !validID(l.ClientAlgoID) ||
			len(l.Trades) > 1000 {
			out.Reasons = []string{"duplicate, missing or unexpected signed Algo leg"}
			return out
		}
		usedLeg[l.GroupID+"|"+l.Leg] = true
		algoIDs[l.AlgoID] = true
		byGroup[l.GroupID] = append(byGroup[l.GroupID], l)
	}
	targetQty := new(big.Rat)
	targetPnL := new(big.Rat)
	targetFee := new(big.Rat)
	targetPartial := false
	targetDouble := false
	anyOtherExit := false
	anyOpen := false
	for _, g := range groups {
		pair := byGroup[g.GroupID]
		if len(pair) != 2 {
			out.Reasons = []string{"missing complete signed TP and SL pair"}
			return out
		}
		total := new(big.Rat)
		triggered := 0
		for _, l := range pair {
			wantCID := g.TPClientAlgoID
			if l.Leg == "sl" {
				wantCID = g.SLClientAlgoID
			}
			if l.ClientAlgoID != wantCID {
				out.Reasons = []string{"signed Algo ID does not match immutable plan leg"}
				return out
			}
			qty, err := ownerQty(l.ActualQtyBTC, true)
			if err != nil {
				out.Reasons = []string{"invalid signed Algo actual quantity"}
				return out
			}
			if l.Status != "FINISHED" && l.Status != "CANCELED" && l.Status != "CANCELLED" && l.Status != "EXPIRED" {
				anyOpen = true
			}
			sum := new(big.Rat)
			if len(l.Trades) > 0 {
				if l.ActualOrderID <= 0 || entryOrders[l.ActualOrderID] || exitOrders[l.ActualOrderID] || l.Status != "FINISHED" {
					out.Reasons = []string{"executed leg lacks unique terminal signed actual order"}
					return out
				}
				exitOrders[l.ActualOrderID] = true
			} else if l.ActualOrderID != 0 {
				out.Reasons = []string{"actual order exists but signed fill list empty"}
				return out
			}
			for _, t := range l.Trades {
				amount, e := ownerQty(t.QuantityBTC, false)
				price, okPrice := positiveMoneyV17(t.PriceUSDT)
				fee, okFee := StrictSignedMoneyV17(t.CommissionUSDT)
				pnl, okPnL := StrictSignedMoneyV17(t.RealizedPnLUSDT)
				_ = price
				side := "SELL"
				if g.PositionSide == "SHORT" {
					side = "BUY"
				}
				if e != nil || !okPrice || !okFee || !okPnL || t.TradeID <= 0 || tradeIDs[t.TradeID] ||
					t.OrderID != l.ActualOrderID || t.CommissionAsset != "USDT" || t.Side != side || t.PositionSide != g.PositionSide {
					out.Reasons = []string{"signed exit trade missing, duplicated or incompatible with plan"}
					return out
				}
				tradeIDs[t.TradeID] = true
				sum.Add(sum, amount)
				out.TotalExitTrades++
				if g.GroupID == f.Intent.GroupID {
					targetPnL.Add(targetPnL, pnl)
					targetFee.Add(targetFee, fee)
				}
			}
			if qty.Cmp(sum) != 0 {
				out.Reasons = []string{"signed Algo actual quantity does not equal individual trade fills"}
				return out
			}
			if qty.Sign() > 0 {
				triggered++
			} else if l.Status == "FINISHED" {
				out.Reasons = []string{"FINISHED Algo has no reconciled actual-order fills"}
				return out
			}
			total.Add(total, qty)
		}
		executed, _ := ownerQty(g.ExecutedQtyBTC, false)
		if total.Cmp(executed) > 0 {
			out.Reasons = []string{"signed TP/SL executions overclose entry size"}
			return out
		}
		remaining[g.PositionSide].Add(remaining[g.PositionSide], new(big.Rat).Sub(executed, total))
		if total.Sign() > 0 && !g.ExitOrProtectionFillSeen {
			out.Reasons = []string{"signed exit exists but entry lineage marked no exit"}
			return out
		}
		if g.GroupID == f.Intent.GroupID {
			targetQty.Set(total)
			targetPartial = total.Cmp(executed) < 0
			targetDouble = triggered > 1
		} else if total.Sign() > 0 {
			anyOtherExit = true
		}
		if triggered > 1 {
			targetDouble = true // any double triggered group blocks account-wide attribution
		}
	}
	if f.Intent.Operation == CancelSibling {
		found := false
		for _, l := range byGroup[f.Intent.GroupID] {
			if l.ClientAlgoID == f.Intent.TargetClientAlgoID && l.AlgoID == f.Intent.TargetAlgoID {
				found = true
			}
		}
		if !found {
			out.Reasons = []string{"cancel fence exchange Algo ID does not match signed target leg"}
			return out
		}
	}
	if anyOpen {
		out.Reasons = []string{"one or more Algo legs not terminal; sibling may still trigger"}
		return out
	}
	actualLong, eL := ownerQty(second.LongQtyBTC, true)
	actualShort, eS := ownerQty(second.ShortQtyBTC, true)
	if eL != nil || eS != nil || actualLong.Cmp(remaining["LONG"]) != 0 || actualShort.Cmp(remaining["SHORT"]) != 0 {
		out.Reasons = []string{"post-exit signed Hedge exposure differs from all imported entry and exit trades"}
		return out
	}
	out.TotalEntryGroups = len(groups)
	out.TargetExitQtyBTC = targetQty.FloatString(8)
	out.TargetRealizedPnLUSDT = formatMoneyV17(targetPnL)
	out.TargetCommissionUSDT = formatMoneyV17(targetFee)
	for i := range groups {
		sort.Slice(groups[i].EntryTrades, func(a, b int) bool { return groups[i].EntryTrades[a].TradeID < groups[i].EntryTrades[b].TradeID })
	}
	for i := range ls {
		sort.Slice(ls[i].Trades, func(a, b int) bool { return ls[i].Trades[a].TradeID < ls[i].Trades[b].TradeID })
	}
	raw, _ := json.Marshal(struct {
		Fence   string
		Account string
		Entries []EntryGroupV16
		Legs    []ExitLegV17
	}{f.Checksum, second.AccountEvidenceSHA256, groups, ls})
	hash := sha256.Sum256(raw)
	out.InputDigestSHA256 = hex.EncodeToString(hash[:])
	switch {
	case targetDouble:
		out.State = "DOUBLE_TP_SL_TRIGGER_HALT_NOT_SETTLED"
		out.Reasons = []string{"TP and SL both executed on the same entry; attribution unsafe"}
	case targetPartial:
		out.State = "PARTIAL_EXIT_HALT_NOT_SETTLED"
		out.Reasons = []string{"target exit quantity is incomplete; remain exposed to future fills"}
	case out.SameSideEntryGroups > 1:
		out.State = "POOLED_HEDGE_EXIT_UNATTRIBUTABLE"
		out.Reasons = []string{"multiple entry groups share one Hedge side, signed order IDs do not prove independent model PnL"}
	case len(groups) > 1 || anyOtherExit:
		out.State = "MULTI_GROUP_EXIT_MANUAL_RECONCILIATION"
		out.Reasons = []string{"another entry/side exists; bounded history cannot certify account-wide lifetime isolation"}
	default:
		out.State = "SINGLE_ENTRY_EXIT_BOUNDED_CONSISTENT_NOT_SETTLED"
		out.Reasons = []string{"one entry and one executed TP/SL leg match bounded signed facts; funding, omitted history, OCO and account settlement are unproven"}
	}
	return out
}
