package trading

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

func shuffledCopy[T any](in []T, seed int64) []T {
	out := append([]T(nil), in...)
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func TestAttestedAccountingEvidenceDigestOrderInvariant(t *testing.T) {
	records, fills, baseline := attestedFixture(t)
	now := baseline.FinishedAt.Add(100 * time.Millisecond)
	baseline.RecentIncome = []exchange.IncomeRecord{
		incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "-1", baseline.WindowStartMS+10, 81001),
		incomeFixture(t, "", "TRANSFER", "USDT", "2", baseline.WindowStartMS+20, 81002),
	}
	first, err := BuildAttestedAccounting(records, fills, baseline, now)
	if err != nil {
		t.Fatal(err)
	}
	for seed := int64(1); seed <= 50; seed++ {
		r2 := shuffledCopy(records, seed)
		f2 := shuffledCopy(fills, seed+100)
		b2 := baseline
		b2.RecentBTCOrders = shuffledCopy(baseline.RecentBTCOrders, seed+200)
		b2.RecentBTCTrades = shuffledCopy(baseline.RecentBTCTrades, seed+300)
		b2.RecentIncome = shuffledCopy(baseline.RecentIncome, seed+400)
		got, err := BuildAttestedAccounting(r2, f2, b2, now)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if got.EvidenceSHA256 != first.EvidenceSHA256 || got.SHA256 != first.SHA256 {
			t.Fatalf("seed %d: same evidence changed digest/report: evidence %s != %s report %s != %s", seed, got.EvidenceSHA256, first.EvidenceSHA256, got.SHA256, first.SHA256)
		}
	}
}

func TestAttestedAccountingEvidenceDigestCoversSignedOrderAndPortfolio(t *testing.T) {
	records, fills, baseline := attestedFixture(t)
	now := baseline.FinishedAt.Add(100 * time.Millisecond)
	first, err := BuildAttestedAccounting(records, fills, baseline, now)
	if err != nil {
		t.Fatal(err)
	}

	changedOrder := baseline
	changedOrder.RecentBTCOrders = append([]exchange.Order(nil), baseline.RecentBTCOrders...)
	changedOrder.RecentBTCOrders[0].Status = "CANCELED"
	orderReport, err := BuildAttestedAccounting(records, fills, changedOrder, now)
	if err != nil {
		t.Fatal(err)
	}
	if orderReport.EvidenceSHA256 == first.EvidenceSHA256 {
		t.Fatal("signed order state was not covered by accounting evidence digest")
	}

	changedPortfolio := baseline
	changedPortfolio.Portfolio.Assets = append([]exchange.PortfolioAsset(nil), baseline.Portfolio.Assets...)
	// Re-unmarshal to modify the unexported exact-decimal field without lossy
	// float conversion, matching the production JSON path.
	raw, _ := json.Marshal(changedPortfolio.Portfolio)
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	assets := obj["assets"].([]any)
	for _, item := range assets {
		a := item.(map[string]any)
		if a["asset"] == "USDT" {
			a["walletBalance"] = "4001"
			a["marginBalance"] = "4001"
			a["availableBalance"] = "4001"
		}
	}
	raw, _ = json.Marshal(obj)
	if err := json.Unmarshal(raw, &changedPortfolio.Portfolio); err != nil {
		t.Fatal(err)
	}
	portfolioReport, err := BuildAttestedAccounting(records, fills, changedPortfolio, now)
	if err != nil {
		t.Fatal(err)
	}
	if portfolioReport.EvidenceSHA256 == first.EvidenceSHA256 {
		t.Fatal("signed portfolio snapshot was not covered by accounting evidence digest")
	}
}
