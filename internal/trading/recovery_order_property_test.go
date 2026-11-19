package trading

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func shuffleRecovery[T any](in []T, rng *rand.Rand) []T {
	out := cloneRecoverySlice(in)
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func recoveryOrderingFixture(now time.Time) exchange.RecoveryBaseline {
	b := recoveryBaseline(now)
	b.Portfolio.Assets = []exchange.PortfolioAsset{
		{Asset: "USDT", WalletBalance: "100", AvailableBalance: "100", MarginBalance: "100"},
		{Asset: "BNB", WalletBalance: "0", AvailableBalance: "0", MarginBalance: "0"},
	}
	b.Portfolio.Positions = []exchange.PortfolioPosition{
		{Symbol: "ETHUSDT", PositionSide: "LONG", PositionAmt: "1", InitialMargin: "1", Isolated: true},
		{Symbol: "BNBUSDT", PositionSide: "SHORT", PositionAmt: "-2", InitialMargin: "1", Isolated: true},
	}
	b.OpenOrders = []exchange.Order{
		{Symbol: "ETHUSDT", OrderID: 41, ClientOrderID: "external-eth", Status: "NEW"},
		{Symbol: "BNBUSDT", OrderID: 42, ClientOrderID: "external-bnb", Status: "NEW"},
	}
	b.OpenAlgos = []exchange.AlgoOrder{{AlgoID: 51, ClientAlgoID: "algo-a"}, {AlgoID: 52, ClientAlgoID: "algo-b"}}
	b.RecentBTCOrders = []exchange.Order{
		{Symbol: "BTCUSDT", OrderID: 61, ClientOrderID: "outside-a"},
		{Symbol: "BTCUSDT", OrderID: 62, ClientOrderID: "outside-b"},
	}
	b.RecentBTCAlgos = []exchange.AlgoOrder{{AlgoID: 71, ClientAlgoID: "hist-a"}, {AlgoID: 72, ClientAlgoID: "hist-b"}}
	b.RecentBTCTrades = []exchange.AccountTrade{
		{Symbol: "BTCUSDT", OrderID: 61, TradeID: 81, TimeMS: b.WindowEndMS - 200},
		{Symbol: "BTCUSDT", OrderID: 62, TradeID: 82, TimeMS: b.WindowEndMS - 100},
	}
	b.RecentIncome = []exchange.IncomeRecord{
		{Type: "COMMISSION", Symbol: "BTCUSDT", Asset: "USDT", Income: "1", TransactionID: 91, TimeMS: b.WindowEndMS - 300},
		{Type: "TRANSFER", Symbol: "", Asset: "USDT", Income: "2", TransactionID: 92, TimeMS: b.WindowEndMS - 200},
		{Type: "FUNDING_FEE", Symbol: "BTCUSDT", Asset: "USDT", Income: "-0.1", TransactionID: 93, TimeMS: b.WindowEndMS - 100},
	}
	return b
}

func TestRecoveryReviewOrderingInvariantAndDoesNotMutateCaller(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	base := recoveryOrderingFixture(now)
	before, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}

	build := func(b exchange.RecoveryBaseline) RecoveryReview {
		root := t.TempDir()
		store, err := NewOwnershipStore(root)
		if err != nil {
			t.Fatal(err)
		}
		r, err := ReviewRecovery(root, store, b, now)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	want := build(base)
	wantJSON, _ := json.Marshal(want)
	wantEnvelope := reviewEnvelope{Schema: 1, Report: want}
	wantDigest := wantEnvelope.sum()

	for seed := int64(1); seed <= 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		b := base
		b.Portfolio.Assets = shuffleRecovery(base.Portfolio.Assets, rng)
		b.Portfolio.Positions = shuffleRecovery(base.Portfolio.Positions, rng)
		b.OpenOrders = shuffleRecovery(base.OpenOrders, rng)
		b.OpenAlgos = shuffleRecovery(base.OpenAlgos, rng)
		b.RecentBTCOrders = shuffleRecovery(base.RecentBTCOrders, rng)
		b.RecentBTCAlgos = shuffleRecovery(base.RecentBTCAlgos, rng)
		b.RecentBTCTrades = shuffleRecovery(base.RecentBTCTrades, rng)
		b.RecentIncome = shuffleRecovery(base.RecentIncome, rng)

		got := build(b)
		gotJSON, _ := json.Marshal(got)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("seed %d: equivalent signed recovery evidence changed report\n got=%s\nwant=%s", seed, gotJSON, wantJSON)
		}
		gotEnvelope := reviewEnvelope{Schema: 1, Report: got}
		if gotEnvelope.sum() != wantDigest {
			t.Fatalf("seed %d: equivalent recovery report changed archive digest", seed)
		}
	}

	after, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("ReviewRecovery mutated caller-owned baseline slices")
	}
}

func TestCanonicalRecoveryReviewPreservesRequiredEmptySlices(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	b := recoveryBaseline(now)
	canonical := canonicalRecoveryReviewBaseline(b)
	if canonical.Portfolio.Assets == nil || canonical.Portfolio.Positions == nil || canonical.OpenOrders == nil ||
		canonical.OpenAlgos == nil || canonical.RecentBTCOrders == nil || canonical.RecentBTCAlgos == nil ||
		canonical.RecentBTCTrades == nil || canonical.RecentIncome == nil {
		t.Fatal("canonical recovery copy changed required empty arrays into nil evidence")
	}
	root := t.TempDir()
	store, err := NewOwnershipStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReviewRecovery(root, store, b, now); err != nil {
		t.Fatalf("valid empty recovery baseline rejected after canonicalization: %v", err)
	}
}

func TestRecoveryArchiveKeepsHistoricalIssueOrderReadable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	legacy := RecoveryReview{
		Version:              1,
		ManualReviewRequired: true,
		ObservedAt:           now,
		Issues:               []string{"historical-z", "historical-a"},
		FundingByAsset:       map[string]string{},
		Coverage:             "BOUNDED_REST_WINDOW_NOT_PROOF_OF_STREAM_CONTINUITY",
	}
	root := t.TempDir()
	if _, err := ArchiveRecoveryReview(root, legacy); err != nil {
		t.Fatal(err)
	}
	got, err := LoadRecoveryReviews(root)
	if err != nil || len(got) != 1 {
		t.Fatalf("historical recovery archive unreadable: %v %+v", err, got)
	}
	if len(got[0].Issues) != 2 || got[0].Issues[0] != "historical-z" || got[0].Issues[1] != "historical-a" {
		t.Fatalf("historical report was canonicalized/reinterpreted instead of verified as stored: %+v", got[0].Issues)
	}
}
