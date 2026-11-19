package trading

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type mockSignedAlgo struct {
	orders map[int64]exchange.DemoAlgoResult
	fail   bool
}

func (m *mockSignedAlgo) QueryDemoAlgo(_ context.Context, _, _ string, id int64) (exchange.DemoAlgoResult, error) {
	if m.fail {
		return exchange.DemoAlgoResult{}, errors.New("timeout on signed GET")
	}
	o, ok := m.orders[id]
	if !ok {
		return exchange.DemoAlgoResult{}, errors.New("missing signed order")
	}
	return o, nil
}
func testProtectionGroup(t *testing.T, side PositionSide) (*ProtectionSubmissionStore, ProtectionDispatchPlan, *mockSignedAlgo, string) {
	t.Helper()
	d := t.TempDir()
	s, err := OpenProtectionSubmissionStore(d)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	q := Quote{Symbol: "BTCUSDT", Price: "100000", ReceivedAt: now}
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: side, Quantity: "0.01", AverageEntry: "100000"}
	draft, err := PreviewProtection(owner, sizingRules(now), q, q, "manual-protection-001", now)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Prepare("manual-protection-001", owner, draft)
	if err != nil {
		t.Fatal(err)
	}
	orders := make(map[int64]exchange.DemoAlgoResult)
	for i, inst := range []ProtectionInstruction{p.TP, p.SL} {
		id := int64(200 + i)
		body, _ := json.Marshal(map[string]any{"algoId": id, "clientAlgoId": inst.ClientAlgoID, "symbol": inst.Symbol, "side": inst.Side,
			"positionSide": string(inst.PositionSide), "orderType": inst.OrderType, "algoType": "CONDITIONAL", "algoStatus": "NEW",
			"quantity": inst.Quantity, "triggerPrice": inst.TriggerPrice, "workingType": inst.WorkingType})
		var v exchange.DemoAlgoResult
		if err := json.Unmarshal(body, &v); err != nil {
			t.Fatal(err)
		}
		orders[id] = v
	}
	return s, p, &mockSignedAlgo{orders: orders}, d
}

func TestProtectionDispatchStagedMockAndReplayAcrossRestart(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			store, plan, reader, dir := testProtectionGroup(t, side)
			if err := store.MarkMayHaveSent(plan.GroupID, "tp", false); !errors.Is(err, ErrProtectionApproval) {
				t.Fatalf("live may-have-sent incorrectly allowed: %v", err)
			}
			if err := store.MarkMayHaveSent(plan.GroupID, "sl", true); err == nil {
				t.Fatal("SL without TP readback allowed")
			}
			if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); !errors.Is(err, ErrProtectionAttempted) {
				t.Fatalf("TP was resent: %v", err)
			}
			if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 200, reader, "key", "secret"); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkMayHaveSent(plan.GroupID, "sl", true); err != nil {
				t.Fatal(err)
			}
			if err := store.RecordObserved(context.Background(), plan.GroupID, "sl", 201, reader, "key", "secret"); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenProtectionSubmissionStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reopened.Prepare(plan.GroupID, OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: side, Quantity: "0.01"}, ProtectionPreview{TP: plan.TP, SL: plan.SL, Leverage: 15, TargetGrossROI: "0.05"}); err != nil {
				t.Fatal(err)
			}
			for _, leg := range []string{"tp", "sl"} {
				state, err := reopened.Inspect(plan.GroupID, leg)
				if err != nil || !state.SignedObserved || state.LiveAuthorized || !state.ManualReviewRequired {
					t.Fatalf("lost safe state: %+v %v", state, err)
				}
				if err := reopened.MarkMayHaveSent(plan.GroupID, leg, true); !errors.Is(err, ErrProtectionAttempted) {
					t.Fatalf("replayed leg %s: %v", leg, err)
				}
			}
		})
	}
}
func TestProtectionDispatchAmbiguousAndCorruptSignedDataBlocks(t *testing.T) {
	store, plan, reader, dir := testProtectionGroup(t, Long)
	if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	reader.fail = true
	if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 200, reader, "key", "secret"); err == nil {
		t.Fatal("signed GET failure allowed")
	}
	if err := store.MarkMayHaveSent(plan.GroupID, "sl", true); err == nil {
		t.Fatal("SL allowed after unknown TP")
	}
	if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); !errors.Is(err, ErrProtectionAttempted) {
		t.Fatalf("unknown TP replayed: %v", err)
	}
	reader.fail = false
	bad := reader.orders[200]
	bad.ClosePosition = true
	reader.orders[200] = bad
	if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 200, reader, "key", "secret"); err == nil {
		t.Fatal("close-all algo accepted")
	}
	bad.ClosePosition = false
	bad.ClientAlgoID = "external"
	reader.orders[200] = bad
	if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 200, reader, "key", "secret"); err == nil {
		t.Fatal("foreign signed order accepted")
	}
	bad.ClientAlgoID = plan.TP.ClientAlgoID
	reader.orders[200] = bad
	if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 200, reader, "key", "secret"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "trading", "protection_submissions", plan.GroupID+".tp.observed.json")
	if err := os.WriteFile(path, []byte(`{"state":"SIGNED_ACTIVE_OBSERVED"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Inspect(plan.GroupID, "tp"); err == nil {
		t.Fatal("tampered observed file accepted")
	}
}
func TestProtectionDispatchRefusesWrongOwnerAndPlanReuse(t *testing.T) {
	store, plan, _, _ := testProtectionGroup(t, Long)
	owner := OwnerPosition{Source: Manual, Symbol: "BTCUSDT", Side: Long, Quantity: "0.02"}
	if _, err := store.Prepare(plan.GroupID, owner, ProtectionPreview{TP: plan.TP, SL: plan.SL, Leverage: 15, TargetGrossROI: "0.05"}); err == nil {
		t.Fatal("owner quantity mismatch accepted")
	}
	owner.Quantity = "0.01"
	other := plan
	other.SL.TriggerPrice = "1234"
	if _, err := store.Prepare(plan.GroupID, owner, ProtectionPreview{TP: other.TP, SL: other.SL, Leverage: 15, TargetGrossROI: "0.05"}); err == nil {
		t.Fatal("plan reuse accepted")
	}
}

func TestProtectionSubmissionAuditAllDetectsOrphansAndDuplicateIDs(t *testing.T) {
	s, plan, _, dir := testProtectionGroup(t, Long)
	states, err := s.AuditAll()
	if err != nil || len(states) != 2 || states[0].LiveAuthorized || states[1].LiveAuthorized {
		t.Fatalf("bad all-scan: %+v %v", states, err)
	}
	path := filepath.Join(dir, "trading", "protection_submissions", "orphan-group-001.tp.attempt.json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuditAll(); err == nil {
		t.Fatal("orphan TP marker not detected")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Simulated extra group that reuses a clientAlgoId: still blocks full scan.
	other := plan
	other.GroupID = "orphan-group-001"
	other.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	other.Digest = other.digest()
	b, _ := json.Marshal(other)
	if err := os.WriteFile(filepath.Join(dir, "trading", "protection_submissions", other.GroupID+".json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuditAll(); err == nil {
		t.Fatal("reused TP client ID not detected")
	}
}

// An immutable signed GET observation only records past status: the second
// protective leg must not be staged from an old TP observation even in mock.
func TestProtectionDispatchRejectsOldOrTamperedObservation(t *testing.T) {
	store, plan, reader, dir := testProtectionGroup(t, Long)
	if err := store.MarkMayHaveSent(plan.GroupID, "tp", true); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordObserved(context.Background(), plan.GroupID, "tp", 200, reader, "key", "secret"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "trading", "protection_submissions", plan.GroupID+".tp.observed.json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker observedProtectionMarker
	if err := json.Unmarshal(original, &marker); err != nil {
		t.Fatal(err)
	}
	marker.At = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	marker.Checksum = marker.digest()
	old, _ := json.Marshal(marker)
	if err := os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkMayHaveSent(plan.GroupID, "sl", true); err == nil {
		t.Fatal("mock SL staged using stale TP observation")
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	marker.Checksum = "1234" // field-level edit even when other data are syntactically valid
	bad, _ := json.Marshal(marker)
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Inspect(plan.GroupID, "tp"); err == nil {
		t.Fatal("observation checksum corruption undetected")
	}
}
