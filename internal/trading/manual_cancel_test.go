package trading

import (
	"binance_trader/internal/exchange"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

type mockManualCancel struct {
	reader *readerOrders
	calls  atomic.Int32
	fail   bool
	status string
	filled string
}

func (m *mockManualCancel) CancelDemoOrder(_ context.Context, _, _, _, cid string) (exchange.OrderResult, error) {
	m.calls.Add(1)
	if m.fail {
		return exchange.OrderResult{}, errors.New("connection lost after DELETE")
	}
	o := m.reader.orders[cid]
	o.Status = m.status
	o.ExecutedQty = m.filled
	return o, nil
}
func setupManualCancel(t *testing.T) (*ManualCancelStore, *readerOrders, Record, string) {
	t.Helper()
	dir := t.TempDir()
	eng, err := NewEngine(dir, &uniqueOrders{next: 100})
	if err != nil {
		t.Fatal(err)
	}
	i := makeIntent("manual-limit-001", Manual, "", Long, Enter, "0.010")
	i.OrderType = Limit
	i.Price = "100000"
	rec, err := eng.Submit(context.Background(), i, allow)
	if err != nil {
		t.Fatal(err)
	}
	reader := &readerOrders{orders: map[string]exchange.OrderResult{}, trades: map[int64][]exchange.AccountTrade{}}
	reader.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "PARTIALLY_FILLED", "0.004")
	store, err := OpenManualCancelStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store, reader, rec, dir
}
func TestManualCancelPartialFillOneShotAndRestart(t *testing.T) {
	store, reader, rec, dir := setupManualCancel(t)
	ctx := context.Background()
	in, err := store.PlanVerified(ctx, reader, "key", "secret", rec.Plan.ClientOrderID)
	if err != nil {
		t.Fatal(err)
	}
	mock := &mockManualCancel{reader: reader, status: "CANCELED", filled: "0.004"}
	result, err := store.AttemptOnce(ctx, mock, "key", "secret", in)
	if err != nil || result.Status != "CANCELED" || result.ExecutedQty != "0.004" || mock.calls.Load() != 1 {
		t.Fatalf("unexpected cancel %+v %v", result, err)
	}
	again, err := OpenManualCancelStore(filepath.Dir(filepath.Dir(store.dir)))
	if err != nil {
		t.Fatal(err)
	}
	_ = dir
	saved, attempted, err := again.Inspect(in.ClientOrderID)
	if err != nil || !attempted || saved.Digest != in.Digest {
		t.Fatalf("cancel record lost: %+v %v %v", saved, attempted, err)
	}
	if _, err = again.AttemptOnce(ctx, mock, "key", "secret", in); !errors.Is(err, ErrManualCancelAttempted) || mock.calls.Load() != 1 {
		t.Fatalf("cancellation replayed: %v calls=%d", err, mock.calls.Load())
	}
}
func TestManualCancelTimeoutAndCorruptionFailClosed(t *testing.T) {
	store, reader, rec, _ := setupManualCancel(t)
	ctx := context.Background()
	in, err := store.PlanVerified(ctx, reader, "key", "secret", rec.Plan.ClientOrderID)
	if err != nil {
		t.Fatal(err)
	}
	mock := &mockManualCancel{reader: reader, fail: true}
	if _, err := store.AttemptOnce(ctx, mock, "key", "secret", in); err == nil {
		t.Fatal("ambiguous cancel returned success")
	}
	if _, err := store.AttemptOnce(ctx, mock, "key", "secret", in); !errors.Is(err, ErrManualCancelAttempted) || mock.calls.Load() != 1 {
		t.Fatalf("ambiguous cancel replayed %v calls=%d", err, mock.calls.Load())
	}
	if err := store.MarkMayHaveSent(in); !errors.Is(err, ErrManualCancelAttempted) {
		t.Fatalf("marker replay accepted %v", err)
	}
	corrupt := []byte(`{"digest":"modified","state":"MAY_HAVE_SENT"}`)
	if err := os.WriteFile(filepath.Join(store.dir, in.ClientOrderID+".attempt.json"), corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Inspect(in.ClientOrderID); err == nil {
		t.Fatal("corrupt marker accepted")
	}
}
func TestManualCancelRejectsForeignOrTerminal(t *testing.T) {
	store, reader, rec, _ := setupManualCancel(t)
	reader.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "FILLED", "0.010")
	if _, err := store.PlanVerified(context.Background(), reader, "key", "secret", rec.Plan.ClientOrderID); err == nil {
		t.Fatal("FILLED order cancel allowed")
	}
	reader.orders[rec.Plan.ClientOrderID] = orderFromRecord(rec, "NEW", "0")
	wrong := reader.orders[rec.Plan.ClientOrderID]
	wrong.OrderID = 999
	reader.orders[rec.Plan.ClientOrderID] = wrong
	if _, err := store.PlanVerified(context.Background(), reader, "key", "secret", rec.Plan.ClientOrderID); err == nil {
		t.Fatal("foreign ID accepted")
	}
}

func TestManualCancelAuditDetectsOrphanAndJournalTampering(t *testing.T) {
	store, reader, rec, dir := setupManualCancel(t)
	in, err := store.PlanVerified(context.Background(), reader, "key", "secret", rec.Plan.ClientOrderID)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := store.AuditAll(); err != nil || n != 1 {
		t.Fatalf("unexpected audit count %d %v", n, err)
	}
	marker := filepath.Join(dir, "trading", "manual_cancels", "bt_aaaaaaaaaaaaaaaaaaaaaaaaaaaa.attempt.json")
	if err := os.WriteFile(marker, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuditAll(); err == nil {
		t.Fatal("orphan cancel marker overlooked")
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "trading", "requests", in.ClientOrderID+".json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuditAll(); err == nil {
		t.Fatal("damaged original order overlooked")
	}
}
