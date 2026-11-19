package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeExecutor struct {
	n         atomic.Int32
	err       error
	malformed bool
}

func (f *fakeExecutor) Submit(_ context.Context, o PlannedOrder) (Submission, error) {
	f.n.Add(1)
	if f.err != nil {
		return Submission{}, f.err
	}
	if f.malformed {
		return Submission{ClientOrderID: "wrong", OrderID: 111, Status: "NEW"}, nil
	}
	return Submission{ClientOrderID: o.ClientOrderID, OrderID: 111, Status: "NEW"}, nil
}

type fakeReader struct {
	calls  int
	err    error
	lastID string
}

func (f *fakeReader) QueryByClientOrderID(_ context.Context, _, clientOrderID string) (Submission, error) {
	f.calls++
	f.lastID = clientOrderID
	if f.err != nil {
		return Submission{}, f.err
	}
	return Submission{ClientOrderID: clientOrderID, OrderID: 111, Status: "FILLED"}, nil
}
func sampleIntent() Intent {
	return Intent{RequestID: "manual-unique-0001", Source: Manual, Symbol: "BTCUSDT", PositionSide: Long, Action: Enter, OrderType: Market, Quantity: "0.001"}
}
func allow(context.Context, Intent) error { return nil }
func TestIntentDirectionValidationAndPlan(t *testing.T) {
	table := []struct {
		side   PositionSide
		action Action
		want   string
	}{{Long, Enter, "BUY"}, {Long, Exit, "SELL"}, {Short, Enter, "SELL"}, {Short, Exit, "BUY"}}
	for _, tt := range table {
		i := sampleIntent()
		i.PositionSide = tt.side
		i.Action = tt.action
		p, err := i.Plan()
		if err != nil || p.ExchangeSide != tt.want || len(p.ClientOrderID) > 36 {
			t.Fatalf("bad plan %+v: %v", p, err)
		}
	}
	bad := []Intent{}
	i := sampleIntent()
	i.Quantity = "NaN"
	bad = append(bad, i)
	i = sampleIntent()
	i.Quantity = "0"
	bad = append(bad, i)
	i = sampleIntent()
	i.Quantity = "0.123456789"
	bad = append(bad, i)
	i = sampleIntent()
	i.Price = "100"
	bad = append(bad, i)
	i = sampleIntent()
	i.OrderType = Limit
	bad = append(bad, i)
	i = sampleIntent()
	i.Source = Model
	bad = append(bad, i)
	i = sampleIntent()
	i.Symbol = "ETHUSDT"
	bad = append(bad, i)
	for _, v := range bad {
		if err := v.Validate(); err == nil {
			t.Fatalf("accepted invalid request: %+v", v)
		}
	}
}
func TestDurableDedupAcrossConcurrentRunnersAndRestarts(t *testing.T) {
	dir := t.TempDir()
	executor := &fakeExecutor{}
	a, err := NewEngine(dir, executor)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewEngine(dir, executor)
	if err != nil {
		t.Fatal(err)
	}
	intent := sampleIntent()
	var wg sync.WaitGroup
	for n := 0; n < 30; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			engine := a
			if n%2 == 0 {
				engine = b
			}
			rec, err := engine.Submit(context.Background(), intent, allow)
			// Concurrent second engine may see the PREPARED file mid-write,
			// in which case it must fail closed rather than submit again.
			_ = rec
			_ = err
		}(n)
	}
	wg.Wait()
	if got := executor.n.Load(); got != 1 {
		t.Fatalf("duplicate exchange submissions: %d", got)
	}
	another, err := NewEngine(dir, executor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := another.Submit(context.Background(), intent, allow); err != nil {
		t.Fatal(err)
	}
	if executor.n.Load() != 1 {
		t.Fatal("restart resent request")
	}
	changed := intent
	changed.Quantity = "0.002"
	if _, err := another.Submit(context.Background(), changed, allow); err == nil {
		t.Fatal("request id collision accepted")
	}
	rec, err := another.Lookup(intent)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != Acknowledged || rec.ExchangeOrderID != 111 || rec.ExchangeStatus != "NEW" {
		t.Fatalf("bad record %+v", rec)
	}
	if err := os.WriteFile(dir+"/do-not-read-secrets", []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestAmbiguousExecutionNeverRetriedAndReconciliation(t *testing.T) {
	dir := t.TempDir()
	executor := &fakeExecutor{err: errors.New("network interrupted")}
	engine, _ := NewEngine(dir, executor)
	intent := sampleIntent()
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err == nil || rec.State != Unknown || executor.n.Load() != 1 {
		t.Fatalf("ambiguous request not retained %+v %v", rec, err)
	}
	rec, err = engine.Submit(context.Background(), intent, allow)
	if !errors.Is(err, ErrNeedsReconciliation) || rec.State != Unknown || executor.n.Load() != 1 {
		t.Fatalf("ambiguous request retried %+v %v", rec, err)
	}
	reader := &fakeReader{err: errors.New("not found (ambiguous)")}
	if _, err = engine.Reconcile(context.Background(), intent, reader); err == nil {
		t.Fatal("missing order assumed safe to resend")
	}
	reader.err = nil
	rec, err = engine.Reconcile(context.Background(), intent, reader)
	if err != nil || rec.State != Reconciled || rec.ExchangeStatus != "FILLED" || reader.lastID != rec.Plan.ClientOrderID {
		t.Fatalf("reconciliation failure %+v %v", rec, err)
	}
	if executor.n.Load() != 1 {
		t.Fatal("reconciler submitted again")
	}
}
func TestGuardBlocksAndMalformedAckFailsClosed(t *testing.T) {
	dir := t.TempDir()
	executor := &fakeExecutor{malformed: true}
	engine, _ := NewEngine(dir, executor)
	intent := sampleIntent()
	if _, err := engine.Submit(context.Background(), intent, nil); err == nil {
		t.Fatal("nil guard accepted")
	}
	if _, err := engine.Submit(context.Background(), intent, func(context.Context, Intent) error { return errors.New("mode mismatch") }); err == nil {
		t.Fatal("failed guard accepted")
	}
	if executor.n.Load() != 0 {
		t.Fatal("submitted despite guard")
	}
	rec, err := engine.Submit(context.Background(), intent, allow)
	if err == nil || rec.State != Unknown {
		t.Fatalf("malformed ACK accepted: %+v %v", rec, err)
	}
	if _, err := engine.Submit(context.Background(), intent, allow); !errors.Is(err, ErrNeedsReconciliation) || executor.n.Load() != 1 {
		t.Fatal("malformed ACK retried")
	}
}
func TestCorruptedJournalHaltsSubmission(t *testing.T) {
	dir := t.TempDir()
	ex := &fakeExecutor{}
	engine, _ := NewEngine(dir, ex)
	intent := sampleIntent()
	plan, _ := intent.Plan()
	if err := os.WriteFile(engine.path(plan), []byte(`{"broken":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(context.Background(), intent, allow); err == nil || ex.n.Load() != 0 {
		t.Fatalf("corrupt journal did not block: %v", err)
	}
}

// State files must work on Windows, where replacing an existing JSON by
// os.Rename can fail. Test that the reservation is preserved and state files
// are append-only so a crash/partial write blocks unsafe resends.
func TestImmutableJournalStateAndCorruptSidecarFailClosed(t *testing.T) {
	dir := t.TempDir()
	ex := &fakeExecutor{}
	engine, err := NewEngine(dir, ex)
	if err != nil {
		t.Fatal(err)
	}
	i := sampleIntent()
	rec, err := engine.Submit(context.Background(), i, allow)
	if err != nil || rec.State != Acknowledged {
		t.Fatalf("submit %+v %v", rec, err)
	}
	plan, _ := i.Plan()
	base := engine.path(plan)
	original, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(original), `"state": "PREPARED"`) {
		t.Fatal("reservation mutated after submit")
	}
	if _, err := os.Stat(strings.TrimSuffix(base, ".json") + ".state.000001.json"); err != nil {
		t.Fatal("first immutable state missing", err)
	}
	rec, err = engine.Reconcile(context.Background(), i, &fakeReader{})
	if err != nil || rec.State != Reconciled {
		t.Fatalf("reconcile %+v %v", rec, err)
	}
	if _, err := os.Stat(strings.TrimSuffix(base, ".json") + ".state.000002.json"); err != nil {
		t.Fatal("second immutable state missing", err)
	}
	if err := os.WriteFile(strings.TrimSuffix(base, ".json")+".state.000003.json", []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(context.Background(), i, allow); err == nil {
		t.Fatal("corrupt state did not fail closed")
	}
	if ex.n.Load() != 1 {
		t.Fatal("corruption triggered duplicate submission")
	}
}

func TestJournalSchema2ChecksumChainDetectsSemanticTamper(t *testing.T) {
	dir := t.TempDir()
	ex := &fakeExecutor{}
	engine, err := NewEngine(dir, ex)
	if err != nil {
		t.Fatal(err)
	}
	intent := sampleIntent()
	if _, err := engine.Submit(context.Background(), intent, allow); err != nil {
		t.Fatal(err)
	}
	plan, _ := intent.Plan()
	base := engine.path(plan)
	root, err := decodeRecord(base)
	if err != nil || root.Schema != 2 || root.Sequence != 0 || root.Checksum == "" {
		t.Fatalf("schema2 root missing: %+v %v", root, err)
	}
	side := strings.TrimSuffix(base, ".json") + ".state.000001.json"
	raw, err := os.ReadFile(side)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	obj["exchange_status"] = "FILLED" // valid-looking semantic mutation; checksum left unchanged
	tampered, _ := json.MarshalIndent(obj, "", "  ")
	if err := os.WriteFile(side, append(tampered, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Lookup(intent); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("semantic tamper was not rejected by checksum chain: %v", err)
	}
	if ex.n.Load() != 1 {
		t.Fatalf("tamper caused duplicate submit: %d", ex.n.Load())
	}
}

func TestLegacySchema1JournalRemainsReadable(t *testing.T) {
	dir := t.TempDir()
	ex := &fakeExecutor{}
	engine, err := NewEngine(dir, ex)
	if err != nil {
		t.Fatal(err)
	}
	intent := sampleIntent()
	plan, _ := intent.Plan()
	digest, _ := digestIntent(intent)
	legacy := Record{Schema: 1, Intent: intent, IntentDigest: digest, Plan: plan, State: Prepared, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	raw, _ := json.MarshalIndent(legacy, "", "  ")
	if err := os.WriteFile(engine.path(plan), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := engine.Lookup(intent)
	if err != nil || got.Schema != 1 || got.State != Prepared {
		t.Fatalf("legacy schema1 journal compatibility failed: %+v %v", got, err)
	}
}
