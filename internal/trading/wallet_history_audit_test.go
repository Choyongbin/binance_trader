package trading

import (
	"context"
	"time"

	"binance_trader/internal/exchange"
	"os"
	"path/filepath"
	"testing"
)

func TestWalletHistoryCompletedOrdersRemainEditable(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	root := filepath.Dir(filepath.Dir(engine.dir))
	sendAndImport(t, engine, store, reader, makeIntent("wallet-old-entry", Manual, "", Long, Enter, "1"), 901, "1", "100", "0.1", "0")
	sendAndImport(t, engine, store, reader, makeIntent("wallet-old-exit", Manual, "", Long, Exit, "1"), 902, "1", "110", "0.1", "10")
	first, historical, err := CheckWalletEditHistory(root)
	if err != nil || !historical || first == "" {
		t.Fatalf("closed historical trades must be inspectable: %q %v %v", first, historical, err)
	}
	second, historical, err := CheckWalletEditHistory(root)
	if err != nil || !historical || second != first {
		t.Fatalf("unchanged history fingerprint drift: %q %q %v", first, second, err)
	}
}

func TestWalletHistoryBlocksOpenTamperedAndInterruptedEvidence(t *testing.T) {
	engine, store, reader := setupOwnership(t)
	root := filepath.Dir(filepath.Dir(engine.dir))
	sendAndImport(t, engine, store, reader, makeIntent("wallet-open-entry", Manual, "", Long, Enter, "1"), 911, "1", "100", "0.1", "0")
	if _, _, err := CheckWalletEditHistory(root); err == nil {
		t.Fatal("open local position accepted")
	}
	sendAndImport(t, engine, store, reader, makeIntent("wallet-close-exit", Manual, "", Long, Exit, "1"), 912, "1", "110", "0.1", "10")
	if _, _, err := CheckWalletEditHistory(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "trading", "coordinator.active"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CheckWalletEditHistory(root); err == nil {
		t.Fatal("empty coordinator lock accepted")
	}
	if err := os.Remove(filepath.Join(root, "trading", "coordinator.active")); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(filepath.Join(root, "trading", "fills"))
	if err != nil || len(files) == 0 {
		t.Fatalf("missing fill evidence: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "trading", "fills", files[0].Name()), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CheckWalletEditHistory(root); err == nil {
		t.Fatal("tampered historical fill accepted")
	}
}

func TestPreparedProtectivePreviewNotPermanentHistoryBlockButAttemptIs(t *testing.T) {
	store, plan, _, root := testProtectionGroup(t, Long)
	if _, historical, err := CheckWalletEditHistory(root); err != nil || !historical {
		t.Fatalf("valid unsent preview not eligible for independent account audit: %v", err)
	}
	if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CheckWalletEditHistory(root); err == nil {
		t.Fatal("ambiguous historical protective dispatch accepted as terminal")
	}
}

// A current flat account snapshot or local cancellation ACK is not proof that
// an earlier attempted conditional order or DELETE is terminal. Those cases
// remain blocked until the operator completes per-order signed reconciliation.
func TestPrePCWalletHistoryRetainsAmbiguousAlgoAndCancelLocks(t *testing.T) {
	t.Run("attempted protection", func(t *testing.T) {
		store, plan, _, root := testProtectionGroup(t, Long)
		if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
			t.Fatal(err)
		}
		if _, _, err := CheckWalletEditHistory(root); err == nil {
			t.Fatal("attempted protection incorrectly unlocked")
		}
	})
	t.Run("prepared algo cancel", func(t *testing.T) {
		root := t.TempDir()
		s, err := NewCancelIntentStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Plan("guard-test", 501, "btp_testguard"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := CheckWalletEditHistory(root); err == nil {
			t.Fatal("algo cancellation evidence incorrectly unlocked")
		}
	})
	t.Run("orphan manual marker", func(t *testing.T) {
		root := t.TempDir()
		s, err := OpenManualCancelStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.dir, "bt_aaaaaaaaaaaaaaaaaaaaaaaaaaaa.attempt.json"), []byte(`{}`), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := CheckWalletEditHistory(root); err == nil {
			t.Fatal("orphan manual cancel incorrectly unlocked")
		}
	})
}

func TestWalletHistoryBlocksRecoveryAndReconnectExclusiveLocks(t *testing.T) {
	for _, rel := range []string{
		filepath.Join("trading", "recovery_sessions", "exclusive.active"),
		filepath.Join("trading", "reconnect_watch_sessions", "exclusive.active"),
	} {
		t.Run(filepath.Base(filepath.Dir(rel)), func(t *testing.T) {
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, rel), 0700); err != nil {
				t.Fatal(err)
			}
			if _, _, err := CheckWalletEditHistory(root); err == nil {
				t.Fatalf("unfinished diagnostic lock accepted: %s", rel)
			}
		})
	}
}

func TestWalletHistoryValidatesExecutionLifecycleArchiveDirectly(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "trading", "execution_lifecycle")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// The wallet history audit used to ignore this directory completely and
	// could therefore miss orphan/tampered durable lifecycle evidence.
	if err := os.WriteFile(filepath.Join(dir, "unexpected.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CheckWalletEditHistory(root); err == nil {
		t.Fatal("unknown execution lifecycle evidence accepted")
	}
}

func TestSettledExecutionLifecycleRemainsHistoricalAndEditable(t *testing.T) {
	root := t.TempDir()
	start := time.Now().UTC().Add(-2 * time.Second)
	intent := makeIntent("wallet-life-settled", Manual, "", Long, Enter, "0.001")
	c, store, lifecycle, res := lifecycleReservation(t, root, intent, start)
	exec := &lifecycleExecutor{id: 7701}
	if _, err := lifecycle.DispatchOnce(context.Background(), res, exec, allow); err != nil {
		t.Fatal(err)
	}
	rec, order := finalOrderForReservation(t, root, res, 7701, "FILLED", "0.001")
	reader := &readerOrders{orders: map[string]exchange.OrderResult{res.ClientOrderID: order}, trades: map[int64][]exchange.AccountTrade{7701: {receipt(rec, 770101, "0.001", "100000", "0.1", "0", time.Now().UTC().Add(-time.Second).UnixMilli())}}}
	now := time.Now().UTC()
	obs := OrderObservation{Order: order, CheckedAt: now}
	if _, err := lifecycle.RecordSignedObservation(res, obs, now); err != nil {
		t.Fatal(err)
	}
	if n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "k", Secret: "s"}, intent); err != nil || n != 1 {
		t.Fatalf("import %d %v", n, err)
	}
	now = time.Now().UTC()
	obs.CheckedAt = now
	if _, err := lifecycle.RecordFillsImported(res, store, obs, now); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	snapshot := accountSnapshot(now, "0.001", "0")
	orders := map[string]OrderObservation{res.ClientOrderID: {Order: order, CheckedAt: now}}
	if _, err := lifecycle.RecordWholeSideReconciled(res, store, snapshot, orders, now); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	snapshot.AsOf = now.Format(time.RFC3339Nano)
	orders[res.ClientOrderID] = OrderObservation{Order: order, CheckedAt: now}
	if _, err := lifecycle.Finalize(res, c, store, snapshot, orders, now); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Close the local position with a second historical order so wallet history
	// is flat while the settled lifecycle remains retained for audit.
	engine, err := NewEngine(root, &uniqueOrders{next: 7701})
	if err != nil {
		t.Fatal(err)
	}
	exitIntent := makeIntent("wallet-life-exit", Manual, "", Long, Exit, "0.001")
	exitRec, err := engine.Submit(context.Background(), exitIntent, allow)
	if err != nil {
		t.Fatal(err)
	}
	exitOrder := orderFromRecord(exitRec, "FILLED", "0.001")
	reader.orders[exitRec.Plan.ClientOrderID] = exitOrder
	reader.trades[exitOrder.OrderID] = []exchange.AccountTrade{receipt(exitRec, 770201, "0.001", "101000", "0.1", "1", time.Now().UTC().Add(time.Second).UnixMilli())}
	if n, err := store.ImportDemoOrder(context.Background(), reader, DemoCredentials{APIKey: "k", Secret: "s"}, exitIntent); err != nil || n != 1 {
		t.Fatalf("exit import %d %v", n, err)
	}
	fingerprint, historical, err := CheckWalletEditHistory(root)
	if err != nil || !historical || fingerprint == "" {
		t.Fatalf("settled execution lifecycle blocked historical wallet edit: %q %v %v", fingerprint, historical, err)
	}
}
