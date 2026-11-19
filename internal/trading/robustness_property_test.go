package trading

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

// Accounting is historical evidence analysis. Its result must not depend on the
// retrieval order of independently verified records, fills, income rows, or
// reference quotes.
func TestAccountingPermutationInvariant(t *testing.T) {
	base := time.Now().UTC().Add(-time.Minute).UnixMilli()
	created := time.UnixMilli(base + 10_000).UTC()
	records, fills, cids := accountFixture(base)
	in := AccountingInput{
		WindowStartMS: base,
		WindowEndMS:   base + 9_000,
		SignedIncome: []exchange.IncomeRecord{
			incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "USDT", "-3", base+3_000, 30),
			incomeFixture(t, "ETHUSDT", "FUNDING_FEE", "USDT", "-7", base+3_500, 31),
			incomeFixture(t, "", "TRANSFER", "USDT", "50", base+4_000, 32),
			incomeFixture(t, "BTCUSDT", "FUNDING_FEE", "BNB", "-0.01", base+6_000, 33),
		},
		Quotes: []BackendQuote{
			{ClientOrderID: cids["account-model-A-1"], Symbol: "BTCUSDT", Price: "99", ObservedMS: base + 990, Source: "BACKEND_WS_TRADE"},
			{ClientOrderID: cids["account-model-B-1"], Symbol: "BTCUSDT", Price: "111", ObservedMS: base + 1_900, Source: "BACKEND_WS_TRADE"},
			{ClientOrderID: cids["account-model-A-2"], Symbol: "BTCUSDT", Price: "121", ObservedMS: base + 4_400, Source: "BACKEND_WS_TRADE"},
		},
	}
	want, err := EvaluateAccountingOffline(records, fills, in, created)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(341))
	for n := 0; n < 200; n++ {
		rr := append([]Record(nil), records...)
		ff := append([]StoredFill(nil), fills...)
		ii := append([]exchange.IncomeRecord(nil), in.SignedIncome...)
		qq := append([]BackendQuote(nil), in.Quotes...)
		rng.Shuffle(len(rr), func(i, j int) { rr[i], rr[j] = rr[j], rr[i] })
		rng.Shuffle(len(ff), func(i, j int) { ff[i], ff[j] = ff[j], ff[i] })
		rng.Shuffle(len(ii), func(i, j int) { ii[i], ii[j] = ii[j], ii[i] })
		rng.Shuffle(len(qq), func(i, j int) { qq[i], qq[j] = qq[j], qq[i] })
		got, err := EvaluateAccountingOffline(rr, ff, AccountingInput{WindowStartMS: in.WindowStartMS, WindowEndMS: in.WindowEndMS, SignedIncome: ii, Quotes: qq}, created)
		if err != nil {
			t.Fatalf("permutation %d failed: %v", n, err)
		}
		if !reflect.DeepEqual(got, want) {
			gb, _ := json.Marshal(got)
			wb, _ := json.Marshal(want)
			t.Fatalf("permutation changed accounting result\n got=%s\nwant=%s", gb, wb)
		}
	}
}

func TestJournalEveryTruncationFailsClosed(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewEngine(dir, &fakeExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	intent := sampleIntent()
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err != nil || rec.State != Acknowledged {
		t.Fatalf("fixture failed: %+v %v", rec, err)
	}
	plan, _ := intent.Plan()
	paths := []string{
		engine.path(plan),
		filepath.Join(engine.dir, plan.ClientOrderID+".state.000001.json"),
	}
	for _, src := range paths {
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		canonical := bytes.TrimSpace(raw)
		for cut := 0; cut < len(canonical); cut++ {
			p := filepath.Join(t.TempDir(), filepath.Base(src))
			if err := os.WriteFile(p, canonical[:cut], 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := decodeRecord(p); err == nil {
				t.Fatalf("accepted truncated journal %s at %d/%d", filepath.Base(src), cut, len(canonical))
			}
		}
		p := filepath.Join(t.TempDir(), filepath.Base(src))
		if err := os.WriteFile(p, append(append([]byte(nil), raw...), []byte("\n{}")...), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := decodeRecord(p); err == nil {
			t.Fatalf("accepted trailing JSON in %s", filepath.Base(src))
		}
	}
}

func TestExecutionLifecycleEveryTruncationFailsClosed(t *testing.T) {
	root := t.TempDir()
	intent := makeIntent("prop-life-trunc-1", Manual, "", Long, Enter, "0.001")
	c, _, lifecycle, res := lifecycleReservation(t, root, intent, time.Now().UTC().Add(-time.Second))
	defer c.Close()
	if _, err := lifecycle.MarkMayHaveSent(res, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	paths := []struct {
		path string
		kind string
	}{
		{executionRootPath(lifecycle.dir, res.ClientOrderID), "root"},
		{executionEventPath(lifecycle.dir, res.ClientOrderID, 1), "event"},
	}
	for _, tc := range paths {
		raw, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		canonical := bytes.TrimSpace(raw)
		for cut := 0; cut < len(canonical); cut++ {
			p := filepath.Join(t.TempDir(), filepath.Base(tc.path))
			if err := os.WriteFile(p, canonical[:cut], 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			if tc.kind == "root" {
				_, err = decodeExecutionRoot(p)
			} else {
				_, err = decodeExecutionEvent(p)
			}
			if err == nil {
				t.Fatalf("accepted truncated lifecycle %s at %d/%d", tc.kind, cut, len(canonical))
			}
		}
	}
}

// Fuzz targets are intentionally decoder-only. They assert the stronger safety
// property that arbitrary bytes can be rejected but can never panic or cause a
// side effect. Seed cases include valid-shaped, truncated, and trailing JSON.
func FuzzJournalDecoderNeverPanics(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"schema":2}`))
	f.Add([]byte("{broken"))
	f.Add([]byte("{}\n{}"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		p := filepath.Join(t.TempDir(), "fuzz.json")
		if err := os.WriteFile(p, raw, 0600); err != nil {
			t.Fatal(err)
		}
		_, _ = decodeRecord(p)
	})
}

func FuzzExecutionLifecycleDecodersNeverPanic(f *testing.F) {
	f.Add([]byte(`{}`), true)
	f.Add([]byte(`{"schema":1}`), false)
	f.Add([]byte("{broken"), true)
	f.Add([]byte("{}\n{}"), false)
	f.Fuzz(func(t *testing.T, raw []byte, root bool) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		p := filepath.Join(t.TempDir(), "fuzz.json")
		if err := os.WriteFile(p, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if root {
			_, _ = decodeExecutionRoot(p)
		} else {
			_, _ = decodeExecutionEvent(p)
		}
	})
}

func validProtectionFillForProperty() ProtectionStoredFill {
	f := ProtectionStoredFill{
		Schema: 1, GroupID: "pg_property_0001", PlanDigest: "digest-property-1", Leg: "tp",
		Source: Manual, PositionSide: Long, AlgoID: 7001, ActualOrderID: 8001,
		Trade: exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 9001, OrderID: 8001, Side: "SELL", PositionSide: "LONG",
			Price: "100000", Quantity: "0.001", Commission: "0.01", CommissionAsset: "USDT", RealizedPnL: "1.25", TimeMS: time.Now().UTC().Add(-time.Second).UnixMilli()},
	}
	f.Checksum = f.digest()
	return f
}

func writeProtectionFillProperty(t *testing.T, f ProtectionStoredFill) string {
	t.Helper()
	dir := t.TempDir()
	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, protectionFillName(f.Trade.TradeID)), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestProtectionFillLoaderRejectsResealedSemanticCorruption(t *testing.T) {
	mutations := []struct {
		name string
		fn   func(*ProtectionStoredFill)
	}{
		{"zero algo", func(f *ProtectionStoredFill) { f.AlgoID = 0 }},
		{"zero time", func(f *ProtectionStoredFill) { f.Trade.TimeMS = 0 }},
		{"wrong side", func(f *ProtectionStoredFill) { f.Trade.Side = "BUY" }},
		{"wrong position side", func(f *ProtectionStoredFill) { f.Trade.PositionSide = "SHORT" }},
		{"zero quantity", func(f *ProtectionStoredFill) { f.Trade.Quantity = "0" }},
		{"negative price", func(f *ProtectionStoredFill) { f.Trade.Price = "-1" }},
		{"bad commission", func(f *ProtectionStoredFill) { f.Trade.Commission = "NaN" }},
		{"bad pnl", func(f *ProtectionStoredFill) { f.Trade.RealizedPnL = "Inf" }},
		{"empty commission asset", func(f *ProtectionStoredFill) { f.Trade.CommissionAsset = "" }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			f := validProtectionFillForProperty()
			tc.fn(&f)
			f.Checksum = f.digest() // simulate semantic corruption with recomputed unkeyed checksum
			dir := writeProtectionFillProperty(t, f)
			if _, err := readProtectionFillsDir(dir); err == nil {
				t.Fatalf("accepted resealed semantic corruption: %+v", f)
			}
		})
	}
}
