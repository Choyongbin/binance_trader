package trading

// Deterministic ordering for the read-only recovery review. Binance does not
// promise a semantic order for independent REST arrays, so archive identity
// must not depend on how equivalent rows happened to be returned.

import (
	"sort"

	"binance_trader/internal/exchange"
)

func cloneRecoverySlice[T any](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	copy(out, in)
	return out
}

func canonicalRecoveryReviewBaseline(b exchange.RecoveryBaseline) exchange.RecoveryBaseline {
	b.Portfolio.Assets = cloneRecoverySlice(b.Portfolio.Assets)
	b.Portfolio.Positions = cloneRecoverySlice(b.Portfolio.Positions)
	b.OpenOrders = cloneRecoverySlice(b.OpenOrders)
	b.OpenAlgos = cloneRecoverySlice(b.OpenAlgos)
	b.RecentBTCOrders = cloneRecoverySlice(b.RecentBTCOrders)
	b.RecentBTCAlgos = cloneRecoverySlice(b.RecentBTCAlgos)
	b.RecentBTCTrades = cloneRecoverySlice(b.RecentBTCTrades)
	b.RecentIncome = cloneRecoverySlice(b.RecentIncome)

	sort.Slice(b.Portfolio.Assets, func(i, j int) bool {
		return b.Portfolio.Assets[i].Asset < b.Portfolio.Assets[j].Asset
	})
	sort.Slice(b.Portfolio.Positions, func(i, j int) bool {
		a, c := b.Portfolio.Positions[i], b.Portfolio.Positions[j]
		if a.Symbol != c.Symbol {
			return a.Symbol < c.Symbol
		}
		return a.PositionSide < c.PositionSide
	})
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
	sortOrders(b.OpenOrders)
	sortAlgos(b.OpenAlgos)
	sortOrders(b.RecentBTCOrders)
	sortAlgos(b.RecentBTCAlgos)
	sort.Slice(b.RecentBTCTrades, func(i, j int) bool {
		if b.RecentBTCTrades[i].TradeID != b.RecentBTCTrades[j].TradeID {
			return b.RecentBTCTrades[i].TradeID < b.RecentBTCTrades[j].TradeID
		}
		return b.RecentBTCTrades[i].OrderID < b.RecentBTCTrades[j].OrderID
	})
	sort.Slice(b.RecentIncome, func(i, j int) bool {
		a, c := b.RecentIncome[i], b.RecentIncome[j]
		if a.TransactionID != c.TransactionID {
			return a.TransactionID < c.TransactionID
		}
		if a.TimeMS != c.TimeMS {
			return a.TimeMS < c.TimeMS
		}
		if a.Type != c.Type {
			return a.Type < c.Type
		}
		if a.Asset != c.Asset {
			return a.Asset < c.Asset
		}
		return a.Symbol < c.Symbol
	})
	return b
}
