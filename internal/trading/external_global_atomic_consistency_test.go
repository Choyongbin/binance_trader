package trading

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func prepareGlobalAtomicFixture(t *testing.T, e ExternalFinancialEventSource, fullySeal bool) *ExternalAtomicRehearsalStore {
	t.Helper()
	ctx := context.Background()
	data, id, archive := e.DataDir, e.ClientOrderID, e.Incident.Entry
	proposal, err := NewExternalCloseSettlementProposalStore(data)
	if err != nil {
		t.Fatal(err)
	}
	p, err := proposal.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := NewExternalCloseReplayCheckpointStore(data)
	if err != nil {
		t.Fatal(err)
	}
	reader := mockCloseReader{v: SignedCloseEvidence{
		EntryOrderID: p.EntryOrderID, ExitOrderID: p.ExitOrderID,
		EntryTradeIDs: p.EntryTradeIDs, ExitTradeIDs: p.ExitTradeIDs,
		AccountFlat: true, ObservedAt: time.Now().UTC(),
	}}
	if _, err := replay.Stage(ctx, id, reader, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := replay.Recheck(ctx, id, reader, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	intent, err := NewExternalClosePostingIntentStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := intent.Reserve(id); err != nil {
		t.Fatal(err)
	}
	shadow, err := NewExternalCloseShadowLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.Prepare(id); err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.Replay(id); err != nil {
		t.Fatal(err)
	}
	dual, err := NewExternalCloseDualLedgerPlanStore(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dual.Prepare(id); err != nil {
		t.Fatal(err)
	}
	if _, err := dual.Verify(id); err != nil {
		t.Fatal(err)
	}
	journal, err := NewExternalSettlementEventJournalStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Prepare(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Fence(ctx, id, archive); err != nil {
		t.Fatal(err)
	}
	atomic, err := NewExternalAtomicLinkedEventStore(data, id)
	if err != nil {
		t.Fatal(err)
	}
	if fullySeal {
		sealLinkedScoped(t, atomic, id, archive)
	}
	return atomic
}

func TestGlobalAtomicConsistencyRequiresExactReservationAndAtomicSeal(t *testing.T) {
	ctx := context.Background()
	a := crossFinancialFixture(t, Long, "joined-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "joined-b", 1000, 80*time.Minute, 0)
	events := []ExternalFinancialEventSource{a, b}
	root := t.TempDir()
	registry, err := NewExternalGlobalEvidenceReservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events); err == nil {
		t.Fatal("unreserved batch accepted")
	}
	if _, err := registry.ReserveBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	left := prepareGlobalAtomicFixture(t, a, false)
	prepareGlobalAtomicFixture(t, b, true)
	beforeA, err := snapshotExecutionEvidence(a.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	beforeB, err := snapshotExecutionEvidence(b.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	if stage.Stage != ExternalGlobalAtomicIncomplete || stage.AllSimulatedPairsVisible || len(stage.Candidates) != 2 || stage.SettlementAuthorized || stage.PostingApplied {
		t.Fatalf("partial seal visible as complete: %+v", stage)
	}
	for _, c := range stage.Candidates {
		if c.SimulatedPairVisible || c.AtomicCommitSHA256 != "" || c.FinancialPreflightSHA256 != "" {
			t.Fatalf("partial batch leaked sealed candidate: %+v", c)
		}
	}
	sealLinkedScoped(t, left, a.ClientOrderID, a.Incident.Entry)
	completed, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Stage != ExternalGlobalAtomicConsistent || !completed.AllSimulatedPairsVisible || completed.DigestSHA256 != completed.digest() || len(completed.Candidates) != 2 {
		t.Fatalf("sealed check: %+v %v", completed, err)
	}
	for _, c := range completed.Candidates {
		if !c.SimulatedPairVisible || c.AtomicCommitSHA256 == "" || c.FinancialPreflightSHA256 == "" || c.PostingApplied || c.SettlementAuthorized || c.TradingAuthorized {
			t.Fatalf("bad candidate %+v", c)
		}
	}
	restarted, err := NewExternalGlobalEvidenceReservationStore(root)
	if err != nil {
		t.Fatal(err)
	}
	again, err := InspectExternalGlobalAtomicConsistency(ctx, restarted, []ExternalFinancialEventSource{b, a})
	if err != nil || !reflect.DeepEqual(completed, again) {
		t.Fatalf("restart/reorder drift: %v", err)
	}
	afterA, err := snapshotExecutionEvidence(a.DataDir)
	if err != nil || beforeA != afterA {
		t.Fatalf("original A modified %v", err)
	}
	afterB, err := snapshotExecutionEvidence(b.DataDir)
	if err != nil || beforeB != afterB {
		t.Fatalf("original B modified %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "external_global_evidence_reservations", reservationSlot(2))); !os.IsNotExist(err) {
		t.Fatal("read-only inspection appended a reservation")
	}
}

func TestGlobalAtomicConsistencyRejectsTamperedSealAndRegistry(t *testing.T) {
	ctx := context.Background()
	a := crossFinancialFixture(t, Long, "join-damaged-a", 0, 80*time.Minute, 0)
	b := crossFinancialFixture(t, Short, "join-damaged-b", 1000, 80*time.Minute, 0)
	events := []ExternalFinancialEventSource{a, b}
	registry, _ := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	if _, err := registry.ReserveBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	one := prepareGlobalAtomicFixture(t, a, true)
	prepareGlobalAtomicFixture(t, b, true)
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(one.dir, "seal.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), ExternalAtomicRehearsalSealed, "INVALID_STAGE", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events); err == nil {
		t.Fatal("damaged seal accepted")
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	slot := filepath.Join(registry.dir, reservationSlot(1))
	original, err := os.ReadFile(slot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(slot, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events); err == nil {
		t.Fatal("torn slot accepted")
	}
	if err := os.WriteFile(slot, original, 0600); err != nil {
		t.Fatal(err)
	}
	other, err := NewExternalGlobalEvidenceReservationStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, other, events); err == nil {
		t.Fatal("foreign empty registry accepted")
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, nil); err == nil {
		t.Fatal("empty manifest accepted")
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events[:1]); err == nil {
		t.Fatal("subset accepted")
	}
	if _, err := InspectExternalGlobalAtomicConsistency(ctx, registry, events[:2]); err != nil {
		t.Fatal(err)
	}
	cancel, cancelFn := context.WithCancel(ctx)
	cancelFn()
	if _, err := InspectExternalGlobalAtomicConsistency(cancel, registry, events); err == nil {
		t.Fatal("cancelled context accepted")
	}
	if !strings.Contains(ExternalGlobalAtomicConsistent, "NOT_POSTED") {
		t.Fatal("authority confusion")
	}
}
