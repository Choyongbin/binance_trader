package trading

// Read-only V17 evidence adapter. Inputs (signed algos and signed actual-order
// fills) MUST be obtained independently from Binance Demo by a trusted caller,
// bracketed by two GETs and checked against immutable protection fill files.
// This assembler alone does not authenticate its arguments and is NEVER an
// order, settlement or authorisation endpoint. No POST/DELETE or journal writes.
import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"binance_trader/internal/exchange"
	"binance_trader/internal/protectionfence"
)

// buildSignedExitLegsV17 pairs each immutable plan with its two independently
// observed exchange Algo IDs and EXACT imported/signed actual-order trades.
// It rejects omitted/extra local or signed trade IDs, duplicate Algo IDs,
// mismatched foreign fee assets, or divergent before/after signed GETs.
func buildSignedExitLegsV17(plans []ProtectionDispatchPlan, local []ProtectionStoredFill,
	signed []exchange.AccountTrade, before, after []exchange.DemoAlgoResult,
) ([]protectionfence.ExitLegV17, error) {
	if len(plans) == 0 || len(plans) > 128 || len(before) != 2*len(plans) || len(after) != len(before) ||
		len(local) > 10000 || len(signed) != len(local) {
		return nil, errors.New("incomplete independent signed TP/SL leg and trade inventory")
	}
	planMap := map[string]ProtectionDispatchPlan{}
	for _, p := range plans {
		if err := p.validate(); err != nil {
			return nil, err
		}
		if _, has := planMap[p.GroupID]; has {
			return nil, errors.New("duplicate immutable group")
		}
		planMap[p.GroupID] = p
	}
	remoteTrades := map[int64]exchange.AccountTrade{}
	for _, t := range signed {
		if t.TradeID <= 0 || t.OrderID <= 0 || t.Symbol != "BTCUSDT" || remoteTrades[t.TradeID].TradeID != 0 {
			return nil, errors.New("corrupt or duplicate independently signed exit trade")
		}
		remoteTrades[t.TradeID] = t
	}
	grouped := map[string][]ProtectionStoredFill{}
	for _, lf := range local {
		if lf.Schema != 1 || lf.Checksum == "" || lf.Checksum != lf.digest() || lf.GroupID == "" ||
			lf.Trade.TradeID <= 0 || lf.AlgoID <= 0 || lf.ActualOrderID <= 0 ||
			(lf.Leg != "tp" && lf.Leg != "sl") {
			return nil, errors.New("invalid immutable protection fill checkpoint")
		}
		p, ok := planMap[lf.GroupID]
		if !ok || lf.PlanDigest != p.Digest || lf.Source != p.Source || lf.ModelID != p.ModelID || lf.PositionSide != p.PositionSide {
			return nil, errors.New("protection fill is not owned by immutable entry plan")
		}
		signedFill, ok := remoteTrades[lf.Trade.TradeID]
		if !ok || !matchingTrades(lf.Trade, signedFill) || signedFill.OrderID != lf.ActualOrderID {
			return nil, errors.New("signed actual-order fill does not exactly match protected local fill")
		}
		// A duplicate local trade ID would otherwise falsely duplicate amount.
		delete(remoteTrades, lf.Trade.TradeID)
		grouped[lf.GroupID+"|"+lf.Leg] = append(grouped[lf.GroupID+"|"+lf.Leg], lf)
	}
	if len(remoteTrades) != 0 {
		return nil, errors.New("unowned signed actual-order exit fill found")
	}

	first := map[string]exchange.DemoAlgoResult{}
	second := map[string]exchange.DemoAlgoResult{}
	seenAlgoID := map[int64]bool{}
	for _, a := range before {
		if a.AlgoID <= 0 || a.ClientAlgoID == "" || seenAlgoID[a.AlgoID] || first[a.ClientAlgoID].AlgoID != 0 {
			return nil, errors.New("duplicate signed Algo identity")
		}
		seenAlgoID[a.AlgoID] = true
		first[a.ClientAlgoID] = a
	}
	for _, a := range after {
		if a.AlgoID <= 0 || a.ClientAlgoID == "" || second[a.ClientAlgoID].AlgoID != 0 {
			return nil, errors.New("duplicate or missing signed Algo identity")
		}
		prev, ok := first[a.ClientAlgoID]
		if !ok || !reflect.DeepEqual(prev, a) {
			return nil, errors.New("Algo changed while fetching its signed actual-order trades")
		}
		second[a.ClientAlgoID] = a
	}
	if len(first) != 2*len(plans) || len(second) != len(first) {
		return nil, errors.New("incomplete signed Algo inventory")
	}
	out := make([]protectionfence.ExitLegV17, 0, len(first))
	seenActualOrder := map[int64]bool{}
	for _, p := range plans {
		for _, l := range []struct {
			name        string
			instruction ProtectionInstruction
		}{
			{"tp", p.TP}, {"sl", p.SL},
		} {
			o, ok := second[l.instruction.ClientAlgoID]
			if !ok || o.AlgoID <= 0 || o.Symbol != l.instruction.Symbol || o.Side != l.instruction.Side ||
				o.PositionSide != string(l.instruction.PositionSide) || o.Type != l.instruction.OrderType ||
				o.AlgoType != l.instruction.AlgoType || o.WorkingType != l.instruction.WorkingType ||
				o.ClosePosition || o.ReduceOnly || !sameBTCQtyV15(string(o.Quantity), l.instruction.Quantity, false) ||
				!sameBTCQtyV15(string(o.TriggerPrice), l.instruction.TriggerPrice, false) {
				return nil, fmt.Errorf("signed Algo differs from immutable %s protection leg", l.name)
			}
			actual := int64(0)
			if o.ActualOrderID != "" && o.ActualOrderID != "0" {
				n, e := strconv.ParseInt(o.ActualOrderID, 10, 64)
				if e != nil || n <= 0 || seenActualOrder[n] {
					return nil, errors.New("duplicate or invalid actual order ID")
				}
				actual = n
				seenActualOrder[n] = true
			}
			fills := grouped[p.GroupID+"|"+l.name]
			records := make([]protectionfence.ExitTradeV17, 0, len(fills))
			for _, f := range fills {
				if actual == 0 || f.ActualOrderID != actual || f.AlgoID != o.AlgoID ||
					f.Trade.OrderID != actual || f.Trade.PositionSide != string(p.PositionSide) {
					return nil, errors.New("signed exit fill references wrong actual order or Algo ID")
				}
				records = append(records, protectionfence.ExitTradeV17{
					TradeID: f.Trade.TradeID, OrderID: f.Trade.OrderID, QuantityBTC: f.Trade.Quantity,
					PriceUSDT: f.Trade.Price, CommissionUSDT: f.Trade.Commission,
					RealizedPnLUSDT: f.Trade.RealizedPnL, CommissionAsset: f.Trade.CommissionAsset,
					Side: f.Trade.Side, PositionSide: f.Trade.PositionSide,
				})
			}
			// A Binance actual order may exist with no matched fill; this is
			// ambiguous and cannot be interpreted as a safe empty leg.
			if actual != 0 && len(fills) == 0 {
				return nil, errors.New("unmatched or unfilled signed actual order: manual review")
			}
			out = append(out, protectionfence.ExitLegV17{GroupID: p.GroupID, Leg: l.name,
				AlgoID: o.AlgoID, ClientAlgoID: o.ClientAlgoID, ActualOrderID: actual,
				Status: o.Status, ActualQtyBTC: string(o.ActualQty), Trades: records})
		}
	}
	return out, nil
}
