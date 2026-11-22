package protectionfence

// Read-only per-entry protection lineage. Binance Hedge positions are pooled
// by positionSide: even exact local entry fills and two matching signed REST
// snapshots CANNOT attribute a later exit fill to an individual model/entry.
// No result authorizes Binance POST, DELETE, cancellation retry or settlement.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"sort"
	"time"
)

type EntryTradeV16 struct {
	TradeID     int64  `json:"trade_id"`
	OrderID     int64  `json:"order_id"`
	QuantityBTC string `json:"quantity_btc"`
}

// EntryGroupV16 must be assembled from the durable request/fill journals,
// immutable TP/SL plan AND independently signed order/trade observations by a
// trusted adapter. A caller-supplied struct has no authentication authority.
// Separate entries from one model have separate ClientOrderID and GroupID.
type EntryGroupV16 struct {
	GroupID              string          `json:"group_id"`
	EntryClientOrderID   string          `json:"entry_client_order_id"`
	EntryExchangeOrderID int64           `json:"entry_exchange_order_id"`
	Source               string          `json:"source"`
	ModelID              string          `json:"model_id,omitempty"`
	Symbol               string          `json:"symbol"`
	PositionSide         string          `json:"position_side"`
	OriginalQtyBTC       string          `json:"original_qty_btc"`
	ExecutedQtyBTC       string          `json:"executed_qty_btc"`
	EntryTrades          []EntryTradeV16 `json:"entry_trades"`
	ProtectionPlanSHA256 string          `json:"protection_plan_sha256"`
	TPClientAlgoID       string          `json:"tp_client_algo_id"`
	SLClientAlgoID       string          `json:"sl_client_algo_id"`
	TPPlannedQtyBTC      string          `json:"tp_planned_qty_btc"`
	SLPlannedQtyBTC      string          `json:"sl_planned_qty_btc"`
	// Any observed exit or protection execution means the original entry lot
	// cannot be assigned an individual remaining quantity without a separate
	// signed exit-fill ownership allocator.
	ExitOrProtectionFillSeen bool `json:"exit_or_protection_fill_seen"`
}

type EntryGroupLineageV16 struct {
	GroupID            string `json:"group_id"`
	EntryClientOrderID string `json:"entry_client_order_id"`
	Source             string `json:"source"`
	ModelID            string `json:"model_id,omitempty"`
	PositionSide       string `json:"position_side"`
	EntryFilledBTC     string `json:"entry_filled_btc"`
	TPClientAlgoID     string `json:"tp_client_algo_id"`
	SLClientAlgoID     string `json:"sl_client_algo_id"`
	State              string `json:"state"`
}

type EntryLineageReviewV16 struct {
	State                string                 `json:"state"`
	TargetGroupID        string                 `json:"target_group_id"`
	SameSideEntryGroups  int                    `json:"same_side_entry_groups"`
	TotalEntryGroups     int                    `json:"total_entry_groups"`
	LongEntryQtyBTC      string                 `json:"long_entry_qty_btc"`
	ShortEntryQtyBTC     string                 `json:"short_entry_qty_btc"`
	LocalLineageSHA256   string                 `json:"local_lineage_sha256,omitempty"`
	Groups               []EntryGroupLineageV16 `json:"groups"`
	Reasons              []string               `json:"reasons"`
	EvidenceLimit        string                 `json:"evidence_limit"`
	TradingAuthorized    bool                   `json:"trading_authorized"`     // ALWAYS false
	SubmitAuthorized     bool                   `json:"submit_authorized"`      // ALWAYS false
	CancelAuthorized     bool                   `json:"cancel_authorized"`      // ALWAYS false
	RetryAuthorized      bool                   `json:"retry_authorized"`       // ALWAYS false
	SettlementAuthorized bool                   `json:"settlement_authorized"`  // ALWAYS false
	ManualReviewRequired bool                   `json:"manual_review_required"` // ALWAYS true
}

func blockedEntryLineageV16(f Fence, why string) EntryLineageReviewV16 {
	return EntryLineageReviewV16{
		State: "BLOCKED_ENTRY_GROUP_LINEAGE", TargetGroupID: f.Intent.GroupID,
		ManualReviewRequired: true, Reasons: []string{why},
		LongEntryQtyBTC: "0", ShortEntryQtyBTC: "0", Groups: []EntryGroupLineageV16{},
		EvidenceLimit: "BOUNDED_SIGNED_ACCOUNT_AND_LOCAL_ENTRY_JOURNAL_NOT_PROOF_OF_EXIT_ATTRIBUTION_OR_OCO",
	}
}

// ReviewEntryGroupLineageV16 verifies immutable per-order identity, exact
// signed entry-fill totals (as assembled by its adapter), globally unique
// trade / exchange order / TP / SL IDs and full Hedge-side quantities.
// The returned lineage is *descriptive*. It never claims TP/SL is currently
// armed or that a protection exit belongs to any individual group.
func ReviewEntryGroupLineageV16(f Fence, first, second SignedAccountFrameV15, in []EntryGroupV16, now time.Time) EntryLineageReviewV16 {
	out := blockedEntryLineageV16(f, "entry group or signed account evidence unavailable")
	pair := InspectSignedAccountFramePairV15(f, first, second, now)
	if pair.State != "SIGNED_BOUNDED_ACCOUNT_STABLE_OWNER_UNPROVEN" {
		out.Reasons = append([]string{"account-wide signed evidence did not pass two-window comparison"}, pair.Reasons...)
		return out
	}
	if len(in) == 0 || len(in) > 128 {
		out.Reasons = []string{"entry group inventory incomplete or exceeds maximum size"}
		return out
	}
	ordered := append([]EntryGroupV16(nil), in...)
	for i := range ordered {
		ordered[i].EntryTrades = append([]EntryTradeV16(nil), ordered[i].EntryTrades...)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].GroupID < ordered[j].GroupID })
	seenGroup := map[string]bool{}
	seenOrder := map[int64]bool{}
	seenCID := map[string]bool{}
	seenTrade := map[int64]bool{}
	seenAlgoCID := map[string]bool{}
	totals := map[string]*big.Rat{"LONG": new(big.Rat), "SHORT": new(big.Rat)}
	targetSeen := false
	anyExit := false
	for _, g := range ordered {
		if !validID(g.GroupID) || !validID(g.EntryClientOrderID) ||
			g.GroupID != "entry_"+g.EntryClientOrderID || g.EntryExchangeOrderID <= 0 ||
			g.Symbol != "BTCUSDT" || (g.PositionSide != "LONG" && g.PositionSide != "SHORT") ||
			(g.Source != "model" && g.Source != "manual") ||
			(g.Source == "model" && !ownerModelID.MatchString(g.ModelID)) ||
			(g.Source == "manual" && g.ModelID != "") ||
			!isSHA256(g.ProtectionPlanSHA256) ||
			!validID(g.TPClientAlgoID) || !validID(g.SLClientAlgoID) ||
			g.TPClientAlgoID == g.SLClientAlgoID ||
			seenGroup[g.GroupID] || seenOrder[g.EntryExchangeOrderID] || seenCID[g.EntryClientOrderID] ||
			seenAlgoCID[g.TPClientAlgoID] || seenAlgoCID[g.SLClientAlgoID] {
			out.Reasons = []string{"invalid, orphaned or duplicate entry/protection group identity"}
			return out
		}
		seenGroup[g.GroupID], seenOrder[g.EntryExchangeOrderID], seenCID[g.EntryClientOrderID] = true, true, true
		seenAlgoCID[g.TPClientAlgoID], seenAlgoCID[g.SLClientAlgoID] = true, true
		original, e1 := ownerQty(g.OriginalQtyBTC, false)
		executed, e2 := ownerQty(g.ExecutedQtyBTC, false)
		tp, e3 := ownerQty(g.TPPlannedQtyBTC, false)
		sl, e4 := ownerQty(g.SLPlannedQtyBTC, false)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil ||
			original.Cmp(executed) != 0 || executed.Cmp(tp) != 0 || executed.Cmp(sl) != 0 {
			out.Reasons = []string{"entry requested, executed and planned TP/SL quantities differ or are invalid"}
			return out
		}
		if len(g.EntryTrades) == 0 || len(g.EntryTrades) > 1000 {
			out.Reasons = []string{"no complete independent entry trade inventory"}
			return out
		}
		sum := new(big.Rat)
		for _, t := range g.EntryTrades {
			q, err := ownerQty(t.QuantityBTC, false)
			if err != nil || t.TradeID <= 0 || t.OrderID != g.EntryExchangeOrderID || seenTrade[t.TradeID] {
				out.Reasons = []string{"duplicate, foreign or incomplete signed entry fill"}
				return out
			}
			seenTrade[t.TradeID] = true
			sum.Add(sum, q)
		}
		if sum.Cmp(executed) != 0 {
			out.Reasons = []string{"sum of individual signed entry fills does not equal executed order quantity"}
			return out
		}
		if g.ExitOrProtectionFillSeen {
			anyExit = true
		}
		totals[g.PositionSide].Add(totals[g.PositionSide], executed)
		out.Groups = append(out.Groups, EntryGroupLineageV16{
			GroupID: g.GroupID, EntryClientOrderID: g.EntryClientOrderID, Source: g.Source, ModelID: g.ModelID,
			PositionSide: g.PositionSide, EntryFilledBTC: g.ExecutedQtyBTC,
			TPClientAlgoID: g.TPClientAlgoID, SLClientAlgoID: g.SLClientAlgoID,
			State: "ENTRY_FILL_LINKED_TO_IMMUTABLE_TP_SL_PLAN_NOT_OBSERVED_ACTIVE",
		})
		if g.PositionSide == f.Intent.PositionSide {
			out.SameSideEntryGroups++
		}
		if g.GroupID == f.Intent.GroupID {
			targetSeen = true
			want, _ := ownerQty(f.Intent.QuantityBTC, false)
			if g.PositionSide != f.Intent.PositionSide || g.ProtectionPlanSHA256 != f.Intent.PlanSHA256 ||
				want.Cmp(executed) != 0 ||
				(f.Intent.TargetClientAlgoID != g.TPClientAlgoID && f.Intent.TargetClientAlgoID != g.SLClientAlgoID) {
				out.Reasons = []string{"target fence differs from immutable entry group or TP/SL plan"}
				return out
			}
		}
	}
	if !targetSeen {
		out.Reasons = []string{"target immutable fence has no corresponding individual entry group"}
		return out
	}
	observedLong, errL := ownerQty(second.LongQtyBTC, true)
	observedShort, errS := ownerQty(second.ShortQtyBTC, true)
	if errL != nil || errS != nil || totals["LONG"].Cmp(observedLong) != 0 || totals["SHORT"].Cmp(observedShort) != 0 {
		out.Reasons = []string{"complete per-entry sum differs from signed account-wide Hedge position"}
		return out
	}
	// This is a checksum of this bounded *review input*, not a signed proof or
	// an authorization. It is stable under equivalent input ordering.
	for i := range ordered {
		sort.Slice(ordered[i].EntryTrades, func(a, b int) bool {
			return ordered[i].EntryTrades[a].TradeID < ordered[i].EntryTrades[b].TradeID
		})
	}
	raw, _ := json.Marshal(struct {
		FenceChecksum string
		AccountDigest string
		Groups        []EntryGroupV16
	}{f.Checksum, second.AccountEvidenceSHA256, ordered})
	sum := sha256.Sum256(raw)
	out.LocalLineageSHA256 = hex.EncodeToString(sum[:])
	out.TotalEntryGroups = len(ordered)
	out.LongEntryQtyBTC = totals["LONG"].FloatString(8)
	out.ShortEntryQtyBTC = totals["SHORT"].FloatString(8)
	if anyExit {
		out.State = "EXIT_FILL_ATTRIBUTION_UNPROVEN_MANUAL_REVIEW"
		out.Reasons = []string{"an exit/protection execution was observed; per-entry remaining quantity cannot be inferred from pooled Hedge totals"}
	} else if out.SameSideEntryGroups > 1 {
		out.State = "MULTI_ENTRY_POOLED_HEDGE_UNATTRIBUTABLE"
		out.Reasons = []string{"per-entry local fills and plans match bounded account quantity; distinct pooled Hedge entries cannot be assigned independent future TP/SL exits"}
	} else if len(ordered) > 1 {
		out.State = "OTHER_HEDGE_SIDE_PRESENT_MANUAL_REVIEW"
		out.Reasons = []string{"other Hedge side positions exist; current bounded observations do not prove independent fills or OCO"}
	} else {
		out.State = "SINGLE_ENTRY_GROUP_BOUNDED_LINKED_NOT_AUTHORIZED"
		out.Reasons = []string{"one local entry order links to one TP/SL plan; existence, OCO, fills, fees and long-term ownership of protection orders are not proven"}
	}
	return out
}
