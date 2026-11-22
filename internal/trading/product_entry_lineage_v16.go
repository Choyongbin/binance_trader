package trading

// Read-only product bridge: reconstruct distinct entry protection groups from
// sealed local order/fill/plan journals and TWO independently signed full
// Binance Demo account windows. No HTTP handler, POST, DELETE or permission.
// The evidence is bounded: same-side position is pooled and future TP/SL exits
// CANNOT be attributed to one entry group by account-side totals alone.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/protectionfence"
)

// Construct all entry groups by joining LOCAL immutable fills and requests with
// independently obtained signed account history, never a caller-supplied owner
// list or per-model aggregate. An unmatched/old trade cannot be silently skipped.
func buildSignedEntryLineageV16(records []Record, fills []StoredFill, plans []ProtectionDispatchPlan,
	baseline exchange.RecoveryBaseline, anyProtectedExit bool,
) ([]protectionfence.EntryGroupV16, error) {
	if len(records) == 0 || len(records) > 1024 || len(plans) == 0 || len(plans) > 128 ||
		baseline.RecentBTCOrders == nil || baseline.RecentBTCTrades == nil || baseline.RecentBTCAlgos == nil {
		return nil, errors.New("entry lineage inventory incomplete or bounded history unavailable")
	}
	plansByGroup := map[string]ProtectionDispatchPlan{}
	for _, p := range plans {
		if err := p.validate(); err != nil {
			return nil, fmt.Errorf("immutable TP/SL plan invalid: %w", err)
		}
		if _, exists := plansByGroup[p.GroupID]; exists {
			return nil, errors.New("duplicate immutable protection group")
		}
		plansByGroup[p.GroupID] = p
	}
	remoteOrders := map[string]exchange.Order{}
	for _, o := range baseline.RecentBTCOrders {
		if o.Symbol != "BTCUSDT" || o.ClientOrderID == "" || o.OrderID <= 0 {
			return nil, errors.New("invalid signed entry order history")
		}
		if _, exists := remoteOrders[o.ClientOrderID]; exists {
			return nil, errors.New("duplicate signed order client ID")
		}
		remoteOrders[o.ClientOrderID] = o
	}
	remoteTrades := map[int64]exchange.AccountTrade{}
	for _, t := range baseline.RecentBTCTrades {
		if t.Symbol != "BTCUSDT" || t.TradeID <= 0 {
			return nil, errors.New("invalid signed trade history")
		}
		if _, exists := remoteTrades[t.TradeID]; exists {
			return nil, errors.New("duplicate signed trade ID")
		}
		remoteTrades[t.TradeID] = t
	}
	receipts := map[string][]protectionfence.EntryTradeV16{}
	seenLocal := map[int64]bool{}
	for _, f := range fills {
		id := f.ClientOrderID
		if id == "" || f.Trade.TradeID <= 0 || seenLocal[f.Trade.TradeID] {
			return nil, errors.New("corrupt or duplicate local entry trade ID")
		}
		seenLocal[f.Trade.TradeID] = true
		signed, ok := remoteTrades[f.Trade.TradeID]
		if !ok || !matchingTrades(f.Trade, signed) {
			return nil, errors.New("local fill missing or different in signed bounded history")
		}
		receipts[id] = append(receipts[id], protectionfence.EntryTradeV16{
			TradeID: f.Trade.TradeID, OrderID: f.Trade.OrderID, QuantityBTC: f.Trade.Quantity})
	}
	// Signed trades not attributable to a local fill are external/unreconciled.
	// Protection exits require a separate signed per-entry attribution model.
	if len(seenLocal) != len(remoteTrades) {
		return nil, errors.New("foreign or unimported signed trade exists")
	}
	groups := make([]protectionfence.EntryGroupV16, 0, len(records))
	seenEntry := map[string]bool{}
	entryCount := 0
	anyExit := anyProtectedExit || len(baseline.RecentBTCAlgos) > 0
	for _, r := range records {
		if r.Intent.Symbol != "BTCUSDT" {
			return nil, errors.New("unsupported foreign symbol in durable entry journal")
		}
		if r.Intent.Action == Exit {
			if len(receipts[r.Plan.ClientOrderID]) > 0 {
				anyExit = true
			}
			continue
		}
		if r.Intent.Action != Enter {
			return nil, errors.New("unknown durable journal action")
		}
		entryCount++
		cid := r.Plan.ClientOrderID
		if seenEntry[cid] || r.ExchangeOrderID <= 0 {
			return nil, errors.New("entry duplicated or lacks signed exchange order identity")
		}
		seenEntry[cid] = true
		remote, ok := remoteOrders[cid]
		if !ok || remote.OrderID != r.ExchangeOrderID || remote.Status != "FILLED" ||
			remote.ClientOrderID != cid || remote.PositionSide != string(r.Intent.PositionSide) ||
			remote.Side != r.Plan.ExchangeSide || remote.Type != string(r.Plan.Type) ||
			!sameBTCQtyV15(remote.OrigQty, r.Intent.Quantity, false) ||
			!sameBTCQtyV15(remote.ExecutedQty, r.Intent.Quantity, false) {
			return nil, errors.New("entry order cannot be exactly joined to signed full-fill history")
		}
		group := "entry_" + cid
		plan, ok := plansByGroup[group]
		if !ok || plan.GroupID != group || plan.Source != r.Intent.Source || plan.ModelID != r.Intent.ModelID ||
			plan.PositionSide != r.Intent.PositionSide || !sameBTCQtyV15(plan.TP.Quantity, r.Intent.Quantity, false) ||
			!sameBTCQtyV15(plan.SL.Quantity, r.Intent.Quantity, false) {
			return nil, errors.New("immutable entry protection plan missing or owner mismatch")
		}
		if len(receipts[cid]) == 0 {
			return nil, errors.New("full entry order has no independently matched local signed fills")
		}
		groups = append(groups, protectionfence.EntryGroupV16{
			GroupID: group, EntryClientOrderID: cid, EntryExchangeOrderID: r.ExchangeOrderID,
			Source: string(r.Intent.Source), ModelID: r.Intent.ModelID, Symbol: r.Intent.Symbol,
			PositionSide: string(r.Intent.PositionSide), OriginalQtyBTC: r.Intent.Quantity,
			ExecutedQtyBTC: remote.ExecutedQty, EntryTrades: receipts[cid],
			ProtectionPlanSHA256: plan.Digest, TPClientAlgoID: plan.TP.ClientAlgoID, SLClientAlgoID: plan.SL.ClientAlgoID,
			TPPlannedQtyBTC: plan.TP.Quantity, SLPlannedQtyBTC: plan.SL.Quantity,
			ExitOrProtectionFillSeen: anyExit,
		})
	}
	if entryCount != len(plansByGroup) || len(groups) != entryCount {
		return nil, errors.New("unmatched or orphaned protection plan in product account inventory")
	}
	return groups, nil
}

// InspectProductEntryGroupLineageV16 is operator-invoked signed GET-only
// diagnostic. It does not authorize execution even if a bounded inspection is
// consistent. Other trading paths must NOT consume its result as permission.
func (r *DemoProtectionRuntimeCoordinator) InspectProductEntryGroupLineageV16(ctx context.Context, fromMS int64) (protectionfence.EntryLineageReviewV16, error) {
	blocked := protectionfence.EntryLineageReviewV16{State: "BLOCKED_ENTRY_GROUP_LINEAGE", ManualReviewRequired: true,
		Reasons:       []string{"independent signed account and entry protection lineage incomplete"},
		EvidenceLimit: "SIGNED_BOUNDED_GET_NOT_STREAM_CONTINUITY_OR_OCO"}
	if ctx == nil || r == nil || r.coordinator == nil || r.submissions == nil || r.owners == nil || r.client == nil || r.credentials == nil || r.dataDir == "" {
		return blocked, errors.New("product entry lineage dependencies missing")
	}
	if err := ctx.Err(); err != nil {
		return blocked, err
	}
	if err := r.coordinator.ownsLock(); err != nil {
		return blocked, err
	}
	fenceDir := filepath.Join(r.dataDir, "trading", "product_mutation_fences")
	stat, err := os.Lstat(fenceDir)
	if err != nil || !stat.IsDir() || stat.Mode()&os.ModeSymlink != 0 {
		return blocked, errors.New("immutable mutation fence directory missing or invalid")
	}
	store, err := protectionfence.Open(r.dataDir)
	if err != nil {
		return blocked, err
	}
	fences, err := store.InspectAll()
	if err != nil || len(fences) != 1 {
		return blocked, errors.New("exactly one intact no-replay fence required")
	}
	f := fences[0]
	blocked.TargetGroupID = f.Intent.GroupID
	creds, err := r.credentials(ctx)
	if err != nil || creds.APIKey == "" || creds.Secret == "" {
		return blocked, errors.New("Demo credentials unavailable")
	}
	finger := sha256.Sum256([]byte(creds.APIKey))
	fingerprint := hex.EncodeToString(finger[:])
	if fingerprint != f.Intent.AccountFingerprintSHA256 {
		return blocked, errors.New("credential account fingerprint differs from immutable fence")
	}
	now := time.Now().UTC()
	if fromMS <= 0 || fromMS >= now.UnixMilli() || now.Sub(time.UnixMilli(fromMS)) > 6*24*time.Hour {
		return blocked, errors.New("invalid bounded signed audit window")
	}
	capture := func() ([]Record, []StoredFill, []ProtectionDispatchPlan, bool, []byte, error) {
		records, err := r.owners.ScanJournal()
		if err != nil {
			return nil, nil, nil, false, nil, err
		}
		fills, err := r.owners.readFills()
		if err != nil {
			return nil, nil, nil, false, nil, err
		}
		pFills, err := readValidatedProtectionFillsForStore(filepath.Join(r.dataDir, "trading"), fills)
		if err != nil {
			return nil, nil, nil, false, nil, err
		}
		r.submissions.mu.Lock()
		plans, err := r.submissions.scanPlansLocked()
		r.submissions.mu.Unlock()
		if err != nil {
			return nil, nil, nil, false, nil, err
		}
		data, err := json.Marshal(struct {
			Records             []Record
			Fills               []StoredFill
			Plans               []ProtectionDispatchPlan
			ProtectionFillCount int
		}{records, fills, plans, len(pFills)})
		return records, fills, plans, len(pFills) > 0, data, err
	}
	records, fills, plans, hasProtectionExit, before, err := capture()
	if err != nil {
		return blocked, err
	}
	// Only observed full entry groups are supported. Never allocate a TP/SL
	// group from a model-level sum or silently ignore a settled old entry.
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	first, err := r.client.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return blocked, fmt.Errorf("first full signed recovery baseline: %w", err)
	}
	firstReview, err := ReviewRecovery(r.dataDir, r.owners, first, time.Now().UTC())
	if err != nil {
		return blocked, err
	}
	second, err := r.client.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return blocked, fmt.Errorf("second full signed recovery baseline: %w", err)
	}
	secondReview, err := ReviewRecovery(r.dataDir, r.owners, second, time.Now().UTC())
	if err != nil {
		return blocked, err
	}
	frame1, err := signedAccountFrameV15(first, fingerprint)
	if err != nil {
		return blocked, err
	}
	frame2, err := signedAccountFrameV15(second, fingerprint)
	if err != nil {
		return blocked, err
	}
	groups, err := buildSignedEntryLineageV16(records, fills, plans, second, hasProtectionExit)
	if err != nil {
		return blocked, err
	}
	// Also demand the first signed window joins to every local entry fill.
	firstGroups, err := buildSignedEntryLineageV16(records, fills, plans, first, hasProtectionExit)
	if err != nil || !reflect.DeepEqual(firstGroups, groups) {
		return blocked, errors.New("entry ownership changed or is incomplete between independent signed windows")
	}
	out := protectionfence.ReviewEntryGroupLineageV16(f, frame1, frame2, groups, time.Now().UTC())
	_, _, _, _, after, err := capture()
	if err != nil || !reflect.DeepEqual(before, after) || r.coordinator.ownsLock() != nil {
		return blocked, errors.New("local ledger or coordinator lease changed during signed inspection")
	}
	fences, err = store.InspectAll()
	if err != nil || len(fences) != 1 || fences[0] != f {
		return blocked, errors.New("immutable mutation fence changed during signed inspection")
	}
	// ReviewRecovery explicitly records bounded continuity and other safety
	// blockers. The groups may be described to an operator, but NO caller may
	// interpret them as an individually owned live TP/SL position or permission.
	if len(firstReview.Issues) > 0 || len(secondReview.Issues) > 0 || firstReview.Ownership == nil || secondReview.Ownership == nil ||
		!firstReview.Ownership.ReadyForAssessment || !secondReview.Ownership.ReadyForAssessment {
		out.State = "LOCAL_ORDER_FILL_OR_STREAM_RECOVERY_NOT_CERTIFIED"
		out.Reasons = append(out.Reasons, "underlying signed ledger recovery, funding or stream continuity has unresolved blockers")
	}
	return out, nil
}
