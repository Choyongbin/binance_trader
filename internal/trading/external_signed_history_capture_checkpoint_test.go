package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"binance_trader/internal/exchange"
)

func captureFixtureReader(incident PackageAIncidentArchive) ExternalHistoryCaptureReader {
	return func(ctx context.Context, from, to int64) ([]exchange.AccountTrade, []exchange.FundingIncome, error) {
		trades := make([]exchange.AccountTrade, 0)
		funds := make([]exchange.FundingIncome, 0)
		for _, r := range incident.Close.HistoricalTrades {
			if r.TimeMS >= from && r.TimeMS <= to {
				trades = append(trades, r)
			}
		}
		for _, r := range incident.Close.FundingRows {
			if r.TimeMS >= from && r.TimeMS <= to {
				funds = append(funds, r)
			}
		}
		return trades, funds, nil
	}
}
func TestExternalHistoryCaptureResumeAndRevalidation(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			data, id, incident, _ := boundedFinancialFixtureWithFunding(t, side, "-0.125")
			baseline, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewExternalHistoryCaptureStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			read := captureFixtureReader(incident)
			var calls atomic.Int32
			interrupted := func(ctx context.Context, from, to int64) ([]exchange.AccountTrade, []exchange.FundingIncome, error) {
				n := calls.Add(1)
				if n == 1 { // force a saturated response so it must split the interval
					return make([]exchange.AccountTrade, externalHistoryPageLimit), make([]exchange.FundingIncome, 0), nil
				}
				if n == 3 {
					return nil, nil, errors.New("simulated offline response loss")
				}
				return read(ctx, from, to)
			}
			hash := externalCloseHash(struct{ Origin string }{"fixed offline mock"})
			if _, err := store.Capture(context.Background(), data, id, incident, hash, interrupted); err == nil {
				t.Fatal("interruption was not blocked")
			}
			partial, err := store.Inspect(context.Background(), data, id, incident, hash)
			if err != nil || partial.Complete || partial.CompletedWindows != 1 || partial.Stage != ExternalHistoryCapturePartial || partial.BoundedContinuity != nil {
				t.Fatalf("bad partial state: %+v %v", partial, err)
			}
			final, err := store.Capture(context.Background(), data, id, incident, hash, read)
			if err != nil {
				t.Fatal(err)
			}
			if !final.Complete || final.CompletedWindows < 2 || final.Stage != ExternalHistoryCaptureComplete || final.BoundedContinuity == nil ||
				final.BoundedContinuity.WholeAccountHistoryProven || final.BoundedContinuity.SettlementAuthorized || final.TradingAuthorized {
				t.Fatalf("unsafe final state: %+v", final)
			}
			replay, err := NewExternalHistoryCaptureStore(filepath.Dir(store.dir))
			if err != nil {
				t.Fatal(err)
			}
			again, err := replay.Inspect(context.Background(), data, id, incident, hash)
			if err != nil || !reflect.DeepEqual(final, again) {
				t.Fatalf("restart mismatch: %v", err)
			}
			beforeCalls := calls.Load()
			if _, err := replay.Capture(context.Background(), data, id, incident, hash, interrupted); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != beforeCalls {
				t.Fatal("replayed already-completed capture")
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || after != baseline {
				t.Fatal("capture modified original trading data")
			}
		})
	}
}
func TestExternalHistoryCaptureRejectsCorruptInterruptedAndForeignState(t *testing.T) {
	for _, kind := range []string{"slot_hash", "slot_data", "slot_missing", "slot_partial", "slot_symlink", "extra_file", "header_hash", "header_missing", "foreign_source", "foreign_incident", "foreign_id", "unsafe_root", "nested_root", "original_drift"} {
		t.Run(kind, func(t *testing.T) {
			data, id, incident, _ := boundedFinancialFixture(t, Long)
			root := t.TempDir()
			store, err := NewExternalHistoryCaptureStore(root)
			if err != nil {
				t.Fatal(err)
			}
			hash := externalCloseHash("source")
			if _, err := store.Capture(context.Background(), data, id, incident, hash, captureFixtureReader(incident)); err != nil {
				t.Fatal(err)
			}
			slot := filepath.Join(store.dir, captureSlot(1))
			switch kind {
			case "slot_hash":
				var c ExternalHistoryCaptureCheckpoint
				if err := strictReviewJSON(slot, &c); err != nil {
					t.Fatal(err)
				}
				c.Checksum = strings.Repeat("0", 64)
				b, _ := jsonTestMarshal(c)
				if err := os.WriteFile(slot, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "slot_data":
				b, _ := os.ReadFile(slot)
				if err := os.WriteFile(slot, append(b, []byte("\n{}")...), 0600); err != nil {
					t.Fatal(err)
				}
			case "slot_missing":
				if err := os.Remove(slot); err != nil {
					t.Fatal(err)
				}
			case "slot_partial":
				if err := os.WriteFile(slot, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "slot_symlink":
				if err := os.Remove(slot); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(store.dir, "header.json"), slot); err != nil {
					t.Skip(err)
				}
			case "extra_file":
				if err := os.WriteFile(filepath.Join(store.dir, "unexpected.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "header_hash":
				p := filepath.Join(store.dir, "header.json")
				b, _ := os.ReadFile(p)
				if err := os.WriteFile(p, append(b, []byte("\n")...), 0600); err != nil {
					t.Fatal(err)
				} // whitespace is semantically identical and allowed; mutate instead
				var h ExternalHistoryCaptureHeader
				if err := strictReviewJSON(p, &h); err != nil {
					t.Fatal(err)
				}
				h.Checksum = strings.Repeat("0", 64)
				x, _ := jsonTestMarshal(h)
				if err := os.WriteFile(p, x, 0600); err != nil {
					t.Fatal(err)
				}
			case "header_missing":
				if err := os.Remove(filepath.Join(store.dir, "header.json")); err != nil {
					t.Fatal(err)
				}
			case "foreign_source":
				hash = externalCloseHash("different source")
			case "foreign_incident":
				incident.Checksum = strings.Repeat("0", 64)
			case "foreign_id":
				id = "bt_foreign_order"
			case "unsafe_root":
				os.RemoveAll(store.dir)
				if err := os.Symlink(t.TempDir(), store.dir); err != nil {
					t.Skip(err)
				}
			case "nested_root":
				nested := filepath.Join(data, "offline")
				if err := os.Mkdir(nested, 0700); err != nil {
					t.Fatal(err)
				}
				store, err = NewExternalHistoryCaptureStore(nested)
				if err != nil {
					t.Fatal(err)
				}
			case "original_drift":
				if err := os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.Inspect(context.Background(), data, id, incident, hash); err == nil {
				t.Fatal("corrupt/foreign capture accepted")
			}
		})
	}
}
func jsonTestMarshal(v any) ([]byte, error) { return json.Marshal(v) }
func TestExternalHistoryCaptureMissingAndSaturatedSingleMS(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixture(t, Short)
	for _, tc := range []struct {
		name string
		read ExternalHistoryCaptureReader
	}{
		{"null", func(ctx context.Context, f, t int64) ([]exchange.AccountTrade, []exchange.FundingIncome, error) {
			return nil, []exchange.FundingIncome{}, nil
		}},
		{"too_many", func(ctx context.Context, f, t int64) ([]exchange.AccountTrade, []exchange.FundingIncome, error) {
			return make([]exchange.AccountTrade, 1000), []exchange.FundingIncome{}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := NewExternalHistoryCaptureStore(t.TempDir())
			_, err := store.Capture(context.Background(), data, id, incident, externalCloseHash("source"), tc.read)
			if err == nil {
				t.Fatal("incomplete capture accepted")
			}
			state, err := store.Inspect(context.Background(), data, id, incident, externalCloseHash("source"))
			if err != nil || state.Complete {
				t.Fatalf("unsafe state: %v %+v", err, state)
			}
		})
	}
}
func TestExternalHistoryCaptureConcurrentWritersFailClosed(t *testing.T) {
	data, id, incident, _ := boundedFinancialFixture(t, Long)
	store, _ := NewExternalHistoryCaptureStore(t.TempDir())
	hash := externalCloseHash("source")
	var wg sync.WaitGroup
	success := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Capture(context.Background(), data, id, incident, hash, captureFixtureReader(incident))
			success <- err == nil
		}()
	}
	wg.Wait()
	close(success)
	final, err := store.Inspect(context.Background(), data, id, incident, hash)
	if err != nil { // concurrent first-header creation can race with in-flight writer; no auto repair
		t.Fatalf("checkpoint damaged by concurrent writers: %v", err)
	}
	if !final.Complete {
		t.Fatalf("unfinished after concurrent writers: %+v", final)
	}
}
