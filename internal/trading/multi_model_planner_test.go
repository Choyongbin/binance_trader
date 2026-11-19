package trading

import (
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

type previewModel struct {
	id, action string
	seen       *[]model.OwnerState
	invalid    bool
}

func (p previewModel) ID() string { return p.id }
func (p previewModel) EvaluateLive(_ exchange.LiveMarketSnapshot, state model.OwnerState, now time.Time) (model.Decision, error) {
	if p.seen != nil {
		*p.seen = append(*p.seen, state)
	}
	id := p.id
	if p.invalid {
		id = "other-id"
	}
	return model.Decision{ModelID: id, Status: model.StatusReady, Action: p.action,
		CandleOpenMS: now.UnixMilli() / 60000 * 60000}, nil
}
func previewFixture(t *testing.T, actions ...string) (*model.Registry, []string, exchange.LiveMarketSnapshot, exchange.Snapshot, OwnershipReport, exchange.SymbolRules, time.Time) {
	t.Helper()
	reg := model.NewRegistry()
	ids := make([]string, len(actions))
	for i, action := range actions {
		ids[i] = "preview_model_" + string(rune('A'+i))
		if err := reg.Register(previewModel{id: ids[i], action: action}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.Freeze(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 20, 10, 5, 30, 0, time.UTC)
	signed := accountSnapshot(now, "0", "0")
	signed.Balances = []exchange.Balance{{Asset: "USDT", Available: "1000", Wallet: "1000"}}
	market := exchange.LiveMarketSnapshot{MarketSnapshot: exchange.MarketSnapshot{Symbol: "BTCUSDT", Interval: "1m", Source: "BINANCE_FUTURES_DEMO_WEBSOCKET", Candles: []exchange.Candle{
		{OpenTime: now.UnixMilli() / 60000 * 60000, CloseTime: now.UnixMilli()/60000*60000 + 59999, Open: 100000, High: 100100, Low: 99900, Close: 100000, Volume: 1},
	}}, LiveMarketStatus: exchange.LiveMarketStatus{State: "live", Ready: true, LastReceivedAt: now.Add(-time.Second).Format(time.RFC3339Nano)}}
	report := OwnershipReport{ReadyForAssessment: true, Blockers: []string{}, Owned: []OwnerPosition{}}
	return reg, ids, market, signed, report, sizingRules(now), now
}
func TestMultiModelPreviewOppositeSidesReserveOneSharedBudget(t *testing.T) {
	r, ids, m, s, a, rules, now := previewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	p, e := PreviewMultiModelSignals(r, ids, m, s, a, nil, rules, "1000", now)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Candidates) != 2 || p.Candidates[0].State != "PREVIEW_ONLY" || p.Candidates[1].State != "PREVIEW_ONLY" {
		t.Fatalf("two opposite signals should be previewed: %+v", p)
	}
	if p.Candidates[0].ProvisionalMarginUSDT != "100" || p.Candidates[1].ProvisionalMarginUSDT != "90" || p.ProvisionalTotalUSDT != "190" {
		t.Fatalf("shared budget was duplicated: %+v", p)
	}
	if p.TradingAuthorized || p.ExecutionEnabled || p.Candidates[0].TradingAuthorized || p.Candidates[1].TradingAuthorized {
		t.Fatal("read-only preview claimed trading permission")
	}
	if len(p.Exposures) != 2 || p.Exposures[0].OwnedMarginUSDT != "0" {
		t.Fatal(p.Exposures)
	}
}
func TestMultiModelPreviewSameHedgeSideConflictsWithoutSideEffects(t *testing.T) {
	r, ids, m, s, a, rules, now := previewFixture(t, model.ActionEnterLong, model.ActionEnterLong)
	p, e := PreviewMultiModelSignals(r, ids, m, s, a, nil, rules, "1000", now)
	if e != nil || len(p.Candidates) != 2 || p.Candidates[0].State != "PREVIEW_ONLY" || p.Candidates[1].State != "BLOCKED" || p.ProvisionalTotalUSDT != "100" {
		t.Fatalf("same side conflict not blocked: %+v %v", p, e)
	}
	// Reversing selection priority changes WHICH model receives the preview,
	// never the total reserved account amount.
	p, e = PreviewMultiModelSignals(r, []string{ids[1], ids[0]}, m, s, a, nil, rules, "1000", now)
	if e != nil || p.Candidates[0].ModelID != ids[1] || p.Candidates[1].State != "BLOCKED" || p.ProvisionalTotalUSDT != "100" {
		t.Fatalf("priority broken: %+v %v", p, e)
	}
}
func TestMultiModelPreviewHistoricalDeregisteredOwnerAndPendingStillConsumeBudget(t *testing.T) {
	r, ids, m, s, a, rules, now := previewFixture(t, model.ActionEnterLong, model.ActionEnterShort)
	a.Owned = []OwnerPosition{{Source: Model, ModelID: "historical_owner", Symbol: "BTCUSDT", Side: Long, Quantity: "0.003", AverageEntry: "100000"}}
	s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.003"}}
	active := []Reservation{{ClientOrderID: "bt_aaaaaaaaaaaaaaaaaaaaaaaaaaaa", Intent: Intent{Source: Model, ModelID: "old_pending", Symbol: "BTCUSDT", PositionSide: Short, Action: Enter}, MarginUSDT: "20"}}
	p, e := PreviewMultiModelSignals(r, ids, m, s, a, active, rules, "1000", now)
	if e != nil {
		t.Fatal(e)
	}
	if p.Candidates[0].State != "BLOCKED" || p.Candidates[1].State != "BLOCKED" || p.ProvisionalTotalUSDT != "0" {
		t.Fatal(p)
	}
	if p.GlobalBudget.OwnedUSDT != "20" || p.GlobalBudget.PendingUSDT != "20" {
		t.Fatalf("non-selected model exposure omitted: %+v", p.GlobalBudget)
	}
	found := false
	for _, e := range p.Exposures {
		if e.ModelID == "historical_owner" && e.OwnedMarginUSDT == "20" {
			found = true
		}
	}
	if !found {
		t.Fatal("historical model missing", p.Exposures)
	}
}
func TestMultiModelPreviewSeparatedOwnersAndExitOnlyPreview(t *testing.T) {
	r, ids, m, s, a, rules, now := previewFixture(t, model.ActionExitLong, model.ActionHold)
	a.Owned = []OwnerPosition{{Source: Model, ModelID: ids[0], Symbol: "BTCUSDT", Side: Long, Quantity: "0.002", AverageEntry: "100000"}}
	s.Positions = []exchange.Position{{Symbol: "BTCUSDT", Side: "LONG", Qty: "0.002"}}
	p, e := PreviewMultiModelSignals(r, ids, m, s, a, nil, rules, "1000", now)
	if e != nil || p.Candidates[0].State != "EXIT_REVIEW_ONLY" || p.Candidates[1].State != "HOLD" || p.ProvisionalTotalUSDT != "0" {
		t.Fatalf("unexpected exit preview %+v %v", p, e)
	}
}
func TestMultiModelPreviewRejectsUntrustedEvidence(t *testing.T) {
	r, ids, m, s, a, rules, now := previewFixture(t, model.ActionEnterLong, model.ActionHold)
	cases := []struct {
		name   string
		mutate func(*exchange.LiveMarketSnapshot, *exchange.Snapshot, *OwnershipReport)
		ids    []string
	}{
		{"duplicate model", func(*exchange.LiveMarketSnapshot, *exchange.Snapshot, *OwnershipReport) {}, []string{ids[0], ids[0]}},
		{"unknown model", func(*exchange.LiveMarketSnapshot, *exchange.Snapshot, *OwnershipReport) {}, []string{"unknown"}},
		{"stale account", func(_ *exchange.LiveMarketSnapshot, s *exchange.Snapshot, _ *OwnershipReport) {
			s.AsOf = now.Add(-time.Minute).Format(time.RFC3339Nano)
		}, ids},
		{"account empty collections", func(_ *exchange.LiveMarketSnapshot, s *exchange.Snapshot, _ *OwnershipReport) { s.Orders = nil }, ids},
		{"open algo order", func(_ *exchange.LiveMarketSnapshot, s *exchange.Snapshot, _ *OwnershipReport) {
			s.AlgoOrders = []exchange.AlgoOrder{{}}
		}, ids},
		{"stale WS", func(m *exchange.LiveMarketSnapshot, _ *exchange.Snapshot, _ *OwnershipReport) {
			m.LastReceivedAt = now.Add(-time.Minute).Format(time.RFC3339Nano)
		}, ids},
		{"unreconciled owner", func(_ *exchange.LiveMarketSnapshot, _ *exchange.Snapshot, a *OwnershipReport) {
			a.ReadyForAssessment = false
		}, ids},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			market, signed, report := m, s, a
			tc.mutate(&market, &signed, &report)
			p, e := PreviewMultiModelSignals(r, tc.ids, market, signed, report, nil, rules, "1000", now)
			if e == nil || p.TradingAuthorized || p.ExecutionEnabled {
				t.Fatalf("unsafe evidence accepted: %+v %v", p, e)
			}
		})
	}
	p, e := PreviewMultiModelSignals(r, ids, m, s, a, []Reservation{{Intent: Intent{Symbol: "BTCUSDT", PositionSide: Long, Source: Model, Action: Enter, ModelID: "x"}, MarginUSDT: "NaN"}}, rules, "1000", now)
	if e == nil || p.TradingAuthorized || !strings.Contains(e.Error(), "예산") {
		t.Fatalf("corrupted margin was accepted: %+v %v", p, e)
	}
}
func TestMultiModelPreviewRejectsModelImpersonation(t *testing.T) {
	reg, ids, m, s, a, rules, now := previewFixture(t, model.ActionEnterLong)
	reg2 := model.NewRegistry()
	if err := reg2.Register(previewModel{id: ids[0], action: model.ActionEnterLong, invalid: true}); err != nil {
		t.Fatal(err)
	}
	if err := reg2.Freeze(); err != nil {
		t.Fatal(err)
	}
	p, e := PreviewMultiModelSignals(reg2, ids, m, s, a, nil, rules, "1000", now)
	if e != nil || p.Candidates[0].State != "BLOCKED" || p.ProvisionalTotalUSDT != "0" {
		t.Fatalf("model identity spoofed: %+v %v", p, e)
	}
	_ = reg
}
