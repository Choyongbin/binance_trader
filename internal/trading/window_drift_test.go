package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type twoSnapshotReader struct {
	first, second exchange.RecoveryBaseline
	calls         int
	afterFirst    func()
	failSecond    bool
}

func (m *twoSnapshotReader) FetchDemoRecoveryBaseline(ctx context.Context, k, s string, start int64) (exchange.RecoveryBaseline, error) {
	if k != "demo_mock_key" || s != "mock_secret" {
		return exchange.RecoveryBaseline{}, errors.New("wrong fake credentials")
	}
	m.calls++
	if m.calls == 1 {
		if m.afterFirst != nil {
			m.afterFirst()
		}
		return m.first, nil
	}
	if m.failSecond {
		return exchange.RecoveryBaseline{}, errors.New("private fake network failure")
	}
	return m.second, nil
}
func driftSetup(t *testing.T) (string, string, int64, time.Time, *twoSnapshotReader) {
	t.Helper()
	data, _, fp, b, now := emptyIntegration(t)
	first, second := b, b
	first.StartedAt = now.Add(-4 * time.Second)
	first.FinishedAt = now.Add(-3 * time.Second)
	first.WindowEndMS = first.StartedAt.UnixMilli()
	second.StartedAt = now.Add(-2 * time.Second)
	second.FinishedAt = now.Add(-time.Second)
	second.WindowEndMS = second.StartedAt.UnixMilli()
	return data, fp, b.WindowStartMS, now, &twoSnapshotReader{first: first, second: second}
}
func runDrift(data, fp string, from int64, now time.Time, mock *twoSnapshotReader) (BoundedDriftReview, error) {
	return DiagnoseBoundedDrift(context.Background(), data, mock, DemoCredentials{APIKey: "demo_mock_key", Secret: "mock_secret"}, fp, from, func() time.Time { return now })
}
func TestBoundedDriftStableStillNeverAuthorizes(t *testing.T) {
	data, fp, from, now, m := driftSetup(t)
	r, err := runDrift(data, fp, from, now, m)
	if err != nil || m.calls != 2 || r.State != "OBSERVED_STABLE_UNPROVEN" || r.TradingAuthorized || r.ContinuousCoverageProven || !r.ManualReviewRequired ||
		r.InitialExposureSHA256 != r.FinalExposureSHA256 || r.InitialExposureSHA256 == "" || r.BeforeEvidenceSHA256 != r.AfterEvidenceSHA256 || len(r.Blockers) == 0 {
		t.Fatalf("false stable authorization %+v err=%v", r, err)
	}
}
func TestBoundedDriftDetectsExposureChangeEvenWithoutWSMessages(t *testing.T) {
	data, fp, from, now, m := driftSetup(t)
	m.second.Portfolio.Positions = append(m.second.Portfolio.Positions, exchange.PortfolioPosition{Symbol: "BTCUSDT", PositionSide: "LONG", PositionAmt: "0.01", InitialMargin: "100", Isolated: true})
	r, err := runDrift(data, fp, from, now, m)
	if err != nil || r.State != "BLOCKED" || r.InitialExposureSHA256 == r.FinalExposureSHA256 || r.TradingAuthorized || len(r.Blockers) == 0 {
		t.Fatalf("drift hidden %+v %v", r, err)
	}
}
func TestBoundedDriftRejectsConcurrentGapAndUnknownJournal(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		m.afterFirst = func() {
			s := &EventStore{dir: filepath.Join(data, "trading", "user_events")}
			if err := s.RecordDiscontinuity("DISCONNECT"); err != nil {
				t.Error(err)
			}
		}
		r, err := runDrift(data, fp, from, now, m)
		if err != nil || r.State != "BLOCKED" || r.BeforeEvidenceSHA256 == r.AfterEvidenceSHA256 || r.TradingAuthorized {
			t.Fatalf("gap not seen: %+v %v", r, err)
		}
	})
	t.Run("new unknown request", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		m.afterFirst = func() {
			path := filepath.Join(data, "trading", "requests")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(filepath.Join(path, "unexpected.partial"), []byte("{}"), 0600); err != nil {
				t.Error(err)
			}
		}
		r, err := runDrift(data, fp, from, now, m)
		if err == nil || r.TradingAuthorized {
			t.Fatalf("journal mutation ignored: %+v %v", r, err)
		}
	})
}
func TestBoundedDriftGuardsReadCoverageAndCredentials(t *testing.T) {
	t.Run("wrong account", func(t *testing.T) {
		data, _, from, now, m := driftSetup(t)
		_, err := runDrift(data, strings.Repeat("a", 64), from, now, m)
		if err == nil || m.calls != 0 {
			t.Fatalf("wrong key queried %d %v", m.calls, err)
		}
	})
	t.Run("second read error", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		m.failSecond = true
		r, err := runDrift(data, fp, from, now, m)
		if err == nil || r.TradingAuthorized {
			t.Fatalf("unknown second read accepted %+v %v", r, err)
		}
		if strings.Contains(err.Error(), "private fake") {
			t.Fatal("network error raw message copied into output")
		}
	})
	t.Run("overlapping signed read", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		m.second.StartedAt = m.first.FinishedAt.Add(-time.Millisecond)
		m.second.WindowEndMS = m.second.StartedAt.UnixMilli()
		r, err := runDrift(data, fp, from, now, m)
		if err != nil || r.State != "BLOCKED" {
			t.Fatalf("overlap accepted %+v %v", r, err)
		}
	})
	t.Run("duplicate recent trade IDs", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		trade := exchange.AccountTrade{Symbol: "BTCUSDT", TradeID: 7}
		m.first.RecentBTCTrades = []exchange.AccountTrade{trade, trade}
		r, err := runDrift(data, fp, from, now, m)
		if err == nil || r.TradingAuthorized {
			t.Fatalf("duplicate trusted %+v %v", r, err)
		}
	})
	t.Run("old gap marker", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		s := &EventStore{dir: filepath.Join(data, "trading", "user_events")}
		if err := s.RecordDiscontinuity("STARTUP"); err != nil {
			t.Fatal(err)
		}
		r, err := runDrift(data, fp, from, now, m)
		if err != nil || r.State != "BLOCKED" || r.GapCount != 1 {
			t.Fatalf("forgot gap %+v %v", r, err)
		}
	})
	t.Run("old uncorrelated account event", func(t *testing.T) {
		data, fp, from, now, m := driftSetup(t)
		s, e := OpenEventStore(data)
		if e != nil {
			t.Fatal(e)
		}
		a, e := ParseAccountEvent([]byte(`{"e":"ACCOUNT_UPDATE","E":1790525396365,"T":1790525396364,"a":{"m":"ORDER","B":[],"P":[]}}`))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Append(a); e != nil {
			t.Fatal(e)
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		r, e := runDrift(data, fp, from, now, m)
		if e != nil || r.State != "BLOCKED" || r.EventCount != 1 {
			t.Fatalf("old event assumed reconciled %+v %v", r, e)
		}
	})
}

func TestBoundedDriftArchiveRoundTripAndTamper(t *testing.T) {
	data, fp, from, now, m := driftSetup(t)
	got, err := runDrift(data, fp, from, now, m)
	if err != nil || got.ID == "" || got.SHA256 == "" {
		t.Fatalf("durable archive missing %+v %v", got, err)
	}
	out, err := LoadBoundedDriftReviews(data)
	if err != nil || len(out) != 1 || out[0].ID != got.ID || out[0].TradingAuthorized {
		t.Fatalf("archive invalid %+v %v", out, err)
	}
	if err := os.WriteFile(filepath.Join(data, "trading", "bounded_windows", got.ID+".json"), []byte(`{"trading_authorized":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadBoundedDriftReviews(data); err == nil {
		t.Fatal("tampered archive accepted")
	}
}

func TestBoundedDriftIgnoresOnlyMarkToMarketNoise(t *testing.T) {
	data, fp, from, now, m := driftSetup(t)
	m.second.Portfolio.TotalUnrealizedProfit = "1.25"
	m.second.Portfolio.TotalMarginBalance = "5001.25"
	m.second.Portfolio.AvailableBalance = "5001.25"
	m.second.Portfolio.Assets[0].AvailableBalance = "5001.25"
	m.second.Portfolio.Assets[0].MarginBalance = "5001.25"
	r, err := runDrift(data, fp, from, now, m)
	if err != nil || r.State != "OBSERVED_STABLE_UNPROVEN" {
		t.Fatalf("mark-only change classified as fill %+v %v", r, err)
	}
}
