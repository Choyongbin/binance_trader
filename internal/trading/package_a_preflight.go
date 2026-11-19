package trading

// Package A integrated preflight. Every result is NON-POSTING: an authenticated
// bounded trade/funding read is neither a lifetime-account audit nor proof that
// pooled Hedge funding belongs to one strategy. A checksum is not a signature.
// This module deliberately exposes no Apply, Settle or TradingAuthorization API.
import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

const (
	PackageAPrepared  = "PACKAGE_A_PREPARED_NOT_POSTED"
	PackageARechecked = "PACKAGE_A_RECHECKED_NOT_POSTED"
)

type PackageAPreflight struct {
	Schema                             int          `json:"schema"`
	Stage                              string       `json:"stage"`
	ClientOrderID                      string       `json:"client_order_id"`
	ModelID                            string       `json:"model_id"`
	PositionSide                       PositionSide `json:"position_side"`
	DualLedgerSHA256                   string       `json:"dual_ledger_sha256"`
	SignedDetailSHA256                 string       `json:"signed_detail_sha256"`
	ReplayKey                          string       `json:"replay_key"`
	SourceSHA256                       string       `json:"source_sha256"`
	MissingEntryTradeIDs               []int64      `json:"missing_entry_trade_ids"`
	UnownedExitTradeIDs                []int64      `json:"unowned_exit_trade_ids"`
	BoundedFundingNetUSDT              string       `json:"bounded_funding_net_usdt"`
	ProvisionalNetExcludingFundingUSDT string       `json:"provisional_net_excluding_funding_usdt"`
	HistoryScope                       string       `json:"history_scope"`
	Blockers                           []string     `json:"blockers"`
	PreviousChecksum                   string       `json:"previous_checksum,omitempty"`
	Checksum                           string       `json:"checksum"`
	PostingApplied                     bool         `json:"posting_applied"`
	SettlementAuthorized               bool         `json:"settlement_authorized"`
	TradingAuthorized                  bool         `json:"trading_authorized"`
	LifecycleChanged                   bool         `json:"lifecycle_changed"`
	OwnershipChanged                   bool         `json:"ownership_changed"`
}

func (p PackageAPreflight) digest() string { p.Checksum = ""; return externalCloseHash(p) }

func checkLocalSignedFill(dataDir, clientID string, t exchange.AccountTrade) (bool, error) {
	path := filepath.Join(dataDir, "trading", "fills", fmt.Sprintf("BTCUSDT_%d.json", t.TradeID))
	var f StoredFill
	err := strictReviewJSON(path, &f)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if f.Schema != 1 || f.Checksum != f.digest() || f.ClientOrderID != clientID || !equalCloseTrade(f.Trade, t) {
		return false, errors.New("local signed fill mismatch/tampering")
	}
	return true, nil
}
func checkSignedFinancials(p ExternalCloseSettlementProposal, ev SignedCloseEvidence) error {
	if len(ev.EntryFills) != len(p.EntryTradeIDs) || len(ev.ExitFills) != len(p.ExitTradeIDs) || ev.SignedDetailSHA256 == "" {
		return errors.New("signed full fill details missing")
	}
	expectedIn, expectedOut := map[int64]bool{}, map[int64]bool{}
	for _, id := range p.EntryTradeIDs {
		expectedIn[id] = true
	}
	for _, id := range p.ExitTradeIDs {
		expectedOut[id] = true
	}
	totalIn, totalOut := new(big.Rat), new(big.Rat)
	entryFee, exitFee, realized := new(big.Rat), new(big.Rat), new(big.Rat)
	lastEntry, firstExit := int64(0), int64(1<<63-1)
	for _, phase := range []struct {
		fills []exchange.AccountTrade
		ids   map[int64]bool
		in    bool
	}{{ev.EntryFills, expectedIn, true}, {ev.ExitFills, expectedOut, false}} {
		seen := map[int64]bool{}
		for _, t := range phase.fills {
			if !phase.ids[t.TradeID] || seen[t.TradeID] || t.Symbol != "BTCUSDT" || t.CommissionAsset != "USDT" || t.PositionSide != string(p.PositionSide) || t.TimeMS <= 0 {
				return errors.New("signed fill identity/asset invalid")
			}
			if phase.in && (t.OrderID != p.EntryOrderID || (p.PositionSide == Long && t.Side != "BUY") || (p.PositionSide == Short && t.Side != "SELL")) {
				return errors.New("entry signed direction invalid")
			}
			if !phase.in && (t.OrderID != p.ExitOrderID || (p.PositionSide == Long && t.Side != "SELL") || (p.PositionSide == Short && t.Side != "BUY")) {
				return errors.New("exit signed direction invalid")
			}
			qty, e1 := positiveRat(t.Quantity)
			price, e2 := positiveRat(t.Price)
			fee, e3 := signedRat(t.Commission)
			pnl, e4 := signedRat(t.RealizedPnL)
			if e1 != nil || e2 != nil || price.Sign() <= 0 || e3 != nil || e4 != nil || fee.Sign() < 0 {
				return errors.New("invalid signed fill financial value")
			}
			seen[t.TradeID] = true
			if phase.in {
				if pnl.Sign() != 0 {
					return errors.New("entry with nonzero realized PnL")
				}
				totalIn.Add(totalIn, qty)
				entryFee.Add(entryFee, fee)
				if t.TimeMS > lastEntry {
					lastEntry = t.TimeMS
				}
			} else {
				totalOut.Add(totalOut, qty)
				exitFee.Add(exitFee, fee)
				realized.Add(realized, pnl)
				if t.TimeMS < firstExit {
					firstExit = t.TimeMS
				}
			}
		}
	}
	qty, e := signedRat(p.ProposedPositionDelta)
	if e != nil || qty.Sign() >= 0 || totalIn.Cmp(totalOut) != 0 || totalIn.Cmp(new(big.Rat).Neg(qty)) != 0 || lastEntry >= firstExit {
		return errors.New("signed quantity/timeline mismatch")
	}
	if formatOwnershipDecimal(entryFee) != p.EntryCommission || formatOwnershipDecimal(exitFee) != p.ExitCommission || formatOwnershipDecimal(realized) != p.SignedRealizedPnL {
		return errors.New("signed fee or PnL differs from sealed proposal")
	}
	if formatOwnershipDecimal(new(big.Rat).Sub(new(big.Rat).Sub(realized, entryFee), exitFee)) != p.ProvisionalNetExcludingFunding {
		return errors.New("signed net differs from sealed proposal")
	}
	return nil
}

// PreparePackageAPreflight performs fresh signed reads and verifies exact
// economic values against the original sealed proposal. It does not persist.
func PreparePackageAPreflight(ctx context.Context, dataDir, clientID string, reader SignedCloseEvidenceReader, now time.Time) (PackageAPreflight, error) {
	if reader == nil {
		return PackageAPreflight{}, errors.New("signed reader required")
	}
	store, err := NewExternalCloseDualLedgerPlanStore(dataDir)
	if err != nil {
		return PackageAPreflight{}, err
	}
	root, verified, err := store.Load(clientID)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if verified == nil {
		return PackageAPreflight{}, errors.New("verified dual-ledger envelope required")
	}
	p, err := store.shadow.intents.replay.proposal.Load(clientID)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if p.ModelID != root.ModelID || p.PositionSide != root.PositionSide || p.ProvisionalNetExcludingFunding != root.NetExcludingFundingUSDT {
		return PackageAPreflight{}, errors.New("dual-ledger and proposal mismatch")
	}
	if err := store.shadow.intents.replay.proposal.review.ensureOffline(); err != nil {
		return PackageAPreflight{}, err
	}
	ev, err := reader.RefreshExternalCloseEvidence(ctx, p.EntryOrderID, p.ExitOrderID)
	if err != nil {
		return PackageAPreflight{}, err
	}
	reval, err := RevalidateExternalClose(ctx, p, staticPackageAReader{ev}, now)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if err := checkSignedFinancials(p, ev); err != nil {
		return PackageAPreflight{}, err
	}
	local, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if local != root.LocalEvidenceSHA256 {
		return PackageAPreflight{}, errors.New("local source evidence changed")
	}
	missing, exit := []int64{}, []int64{}
	for _, t := range ev.EntryFills {
		present, e := checkLocalSignedFill(dataDir, clientID, t)
		if e != nil {
			return PackageAPreflight{}, e
		}
		if !present {
			missing = append(missing, t.TradeID)
		}
	}
	for _, t := range ev.ExitFills { // An external/manual exit has no matching local ownership journal.
		path := filepath.Join(dataDir, "trading", "fills", fmt.Sprintf("BTCUSDT_%d.json", t.TradeID))
		if _, e := os.Lstat(path); e == nil {
			return PackageAPreflight{}, errors.New("external exit fill unexpectedly already imported; audit attribution manually")
		} else if !errors.Is(e, os.ErrNotExist) {
			return PackageAPreflight{}, e
		}
		exit = append(exit, t.TradeID)
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	sort.Slice(exit, func(i, j int) bool { return exit[i] < exit[j] })
	blockers := []string{"full pre-entry/account-wide history not independently proven", "pooled Hedge funding attribution not established", "original lifecycle and pooled side reconciliation not atomically committed", "operator settlement authorization and crash-safe financial posting unavailable"}
	if len(missing) > 0 {
		blockers = append(blockers, "signed original entry fills missing in immutable local fill journal")
	}
	if root.ObservedLifecycleStage == ExecutionStage("MISSING_LIFECYCLE_BLOCKED") {
		blockers = append(blockers, "original execution lifecycle missing")
	}
	if ev.HistoricalCoverageComplete || ev.FundingVerified || ev.HistoryScope != "ENTRY_FIRST_FILL_TO_EXIT_LAST_FILL_ONLY" || ev.TradeWindowsRead <= 0 || ev.FundingWindowsRead != ev.TradeWindowsRead || ev.HistoryFromMS <= 0 || ev.HistoryToMS < ev.HistoryFromMS || ev.BoundedTradeRows < len(p.EntryTradeIDs)+len(p.ExitTradeIDs) {
		return PackageAPreflight{}, errors.New("invalid bounded evidence provenance or unsupported completeness claim")
	}
	funding, e := signedRat(ev.BoundedFundingNetUSDT)
	if e != nil {
		return PackageAPreflight{}, errors.New("invalid bounded funding aggregate")
	}
	if len(ev.FundingRows) != ev.BoundedFundingRows {
		return PackageAPreflight{}, errors.New("missing funding rows for bounded audit")
	}
	signedNet := new(big.Rat)
	seenFunding := map[int64]bool{}
	for _, f := range ev.FundingRows {
		v, err := signedRat(f.Income)
		if err != nil || f.Symbol != "BTCUSDT" || f.Asset != "USDT" || f.IncomeType != "FUNDING_FEE" || f.TranID <= 0 || seenFunding[f.TranID] || f.TimeMS < ev.HistoryFromMS || f.TimeMS > ev.HistoryToMS {
			return PackageAPreflight{}, errors.New("invalid/duplicated funding provenance")
		}
		seenFunding[f.TranID] = true
		signedNet.Add(signedNet, v)
	}
	if signedNet.Cmp(funding) != 0 {
		return PackageAPreflight{}, errors.New("bounded funding total does not reconcile")
	}
	out := PackageAPreflight{Schema: 1, Stage: PackageAPrepared, ClientOrderID: clientID, ModelID: p.ModelID, PositionSide: p.PositionSide, DualLedgerSHA256: verified.Checksum, SignedDetailSHA256: ev.SignedDetailSHA256, ReplayKey: reval.ReplayKey, SourceSHA256: local, MissingEntryTradeIDs: missing, UnownedExitTradeIDs: exit, BoundedFundingNetUSDT: formatOwnershipDecimal(funding), ProvisionalNetExcludingFundingUSDT: p.ProvisionalNetExcludingFunding, HistoryScope: ev.HistoryScope, Blockers: blockers}
	out.Checksum = out.digest()
	return out, nil
}

type staticPackageAReader struct{ e SignedCloseEvidence }

func (r staticPackageAReader) RefreshExternalCloseEvidence(context.Context, int64, int64) (SignedCloseEvidence, error) {
	return r.e, nil
}

type PackageAPreflightStore struct {
	mu           sync.Mutex
	dir, dataDir string
}

func NewPackageAPreflightStore(dataDir string) (*PackageAPreflightStore, error) {
	_, err := NewExternalCloseDualLedgerPlanStore(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(dataDir, "trading", "external_close_package_a")
	if fi, e := os.Lstat(dir); e == nil {
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe Package A directory")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	return &PackageAPreflightStore{dataDir: dataDir, dir: dir}, nil
}
func (s *PackageAPreflightStore) path(stage string) string {
	if stage == PackageAPrepared {
		return filepath.Join(s.dir, "prepared.json")
	}
	return filepath.Join(s.dir, "rechecked.json")
}
func (s *PackageAPreflightStore) directory(create bool) error {
	if create {
		if e := os.Mkdir(s.dir, 0700); e != nil && !errors.Is(e, os.ErrExist) {
			return e
		}
	}
	fi, e := os.Lstat(s.dir)
	if e != nil {
		return e
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe Package A directory")
	}
	entries, e := os.ReadDir(s.dir)
	if e != nil {
		return e
	}
	for _, v := range entries {
		if v.IsDir() || v.Type()&os.ModeSymlink != 0 || (v.Name() != "prepared.json" && v.Name() != "rechecked.json") {
			return errors.New("unexpected Package A artifact")
		}
	}
	return nil
}
func packageAVerified(root PackageAPreflight) PackageAPreflight {
	r := root
	r.Stage = PackageARechecked
	r.PreviousChecksum = root.Checksum
	r.Checksum = r.digest()
	return r
}
func validatePackageARecord(r, expected PackageAPreflight) error {
	if r.Schema != 1 || r.Stage != expected.Stage || r.Checksum != r.digest() || r.Checksum != expected.Checksum || r.PostingApplied || r.SettlementAuthorized || r.TradingAuthorized || r.LifecycleChanged || r.OwnershipChanged || len(r.Blockers) < 4 {
		return errors.New("invalid Package A non-posting receipt")
	}
	return nil
}
func (s *PackageAPreflightStore) load(expected PackageAPreflight) (PackageAPreflight, *PackageAPreflight, error) {
	if err := s.directory(false); err != nil {
		return PackageAPreflight{}, nil, err
	}
	var root PackageAPreflight
	if err := strictReviewJSON(s.path(PackageAPrepared), &root); err != nil {
		return root, nil, err
	}
	if err := validatePackageARecord(root, expected); err != nil {
		return root, nil, err
	}
	var event PackageAPreflight
	err := strictReviewJSON(s.path(PackageARechecked), &event)
	if errors.Is(err, os.ErrNotExist) {
		return root, nil, nil
	}
	if err != nil {
		return root, nil, err
	}
	if err := validatePackageARecord(event, packageAVerified(root)); err != nil {
		return root, nil, err
	}
	return root, &event, nil
}

// Stage and Recheck each fetch fresh signed evidence. O_EXCL + fsync leaves
// a torn/orphan file as an explicit error, never as an automatic repair.
func (s *PackageAPreflightStore) Stage(ctx context.Context, id string, reader SignedCloseEvidenceReader, now time.Time) (PackageAPreflight, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, err := PreparePackageAPreflight(ctx, s.dataDir, id, reader, now)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if err = s.directory(true); err != nil {
		return PackageAPreflight{}, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if len(entries) != 0 {
		got, _, e := s.load(expected)
		return got, e
	}
	if err = exclusiveReviewWrite(s.path(PackageAPrepared), expected); err != nil {
		return PackageAPreflight{}, err
	}
	got, _, err := s.load(expected)
	return got, err
}
func (s *PackageAPreflightStore) Recheck(ctx context.Context, id string, reader SignedCloseEvidenceReader, now time.Time) (PackageAPreflight, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, err := PreparePackageAPreflight(ctx, s.dataDir, id, reader, now)
	if err != nil {
		return PackageAPreflight{}, err
	}
	root, prev, err := s.load(expected)
	if err != nil {
		return PackageAPreflight{}, err
	}
	if prev != nil {
		return *prev, nil
	}
	verified := packageAVerified(root)
	if err = exclusiveReviewWrite(s.path(PackageARechecked), verified); err != nil {
		return PackageAPreflight{}, err
	}
	_, got, err := s.load(expected)
	if err != nil || got == nil {
		return PackageAPreflight{}, errors.New("Package A recheck missing or invalid")
	}
	return *got, nil
}
