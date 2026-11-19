package trading

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAtomicLedgerPairRequiresSealedSameTokenLongShort(t *testing.T) {
	for _, side := range []PositionSide{Long, Short} {
		t.Run(string(side), func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := atomicRehearsalFixture(t, side)
			before, err := snapshotExecutionEvidence(data)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReadLedgerPair(ctx, id, archive, strings.Repeat("a", 64)); err == nil {
				t.Fatal("missing seal exposed a ledger candidate")
			}
			steps := []func() (ExternalAtomicRehearsalInspection, error){
				func() (ExternalAtomicRehearsalInspection, error) { return store.Prepare(ctx, id, archive) },
				func() (ExternalAtomicRehearsalInspection, error) { return store.StageLifecycle(ctx, id, archive) },
				func() (ExternalAtomicRehearsalInspection, error) { return store.StageOwnership(ctx, id, archive) },
				func() (ExternalAtomicRehearsalInspection, error) { return store.Seal(ctx, id, archive) },
			}
			for i, step := range steps {
				state, err := step()
				if err != nil {
					t.Fatal(err)
				}
				if state.AtomicProjectionVisible != (i == 3) || (state.CommitSHA256 != "") != (i == 3) {
					t.Fatalf("seal token visible at stage %d: %+v", i, state)
				}
				if i < 3 {
					if _, err := store.ReadLedgerPair(ctx, id, archive, strings.Repeat("a", 64)); err == nil {
						t.Fatal("partial candidate exposed")
					}
					continue
				}
				if _, err := store.ReadLedgerPair(ctx, id, archive, ""); err == nil {
					t.Fatal("empty token accepted")
				}
				if _, err := store.ReadLedgerPair(ctx, id, archive, strings.Repeat("f", 64)); err == nil {
					t.Fatal("stale token accepted")
				}
				pair, err := store.ReadLedgerPair(ctx, id, archive, state.CommitSHA256)
				if err != nil {
					t.Fatal(err)
				}
				if pair.Stage != ExternalAtomicPairViewNotPosted || pair.CommitSHA256 != state.CommitSHA256 ||
					pair.TransactionKey != state.TransactionKey || pair.PositionSide != side || pair.ClientOrderID != id ||
					pair.PostingApplied || pair.SettlementAuthorized || pair.TradingAuthorized ||
					pair.Lifecycle.Applied || pair.Ownership.Applied || pair.Lifecycle.CommitSHA256 != pair.Ownership.CommitSHA256 ||
					pair.Lifecycle.TransactionKey != pair.Ownership.TransactionKey || pair.Lifecycle.Proposal == "" || pair.Ownership.Proposal == "" {
					t.Fatalf("unsafe or inconsistent pair: %+v", pair)
				}
				life, err := store.ReadLifecycleCandidate(ctx, id, archive, state.CommitSHA256)
				if err != nil || !reflect.DeepEqual(life, pair.Lifecycle) {
					t.Fatalf("lifecycle: %+v %v", life, err)
				}
				owner, err := store.ReadOwnershipCandidate(ctx, id, archive, state.CommitSHA256)
				if err != nil || !reflect.DeepEqual(owner, pair.Ownership) {
					t.Fatalf("ownership: %+v %v", owner, err)
				}
				restart, err := NewExternalAtomicRehearsalStore(data)
				if err != nil {
					t.Fatal(err)
				}
				again, err := restart.ReadLedgerPair(ctx, id, archive, state.CommitSHA256)
				if err != nil || !reflect.DeepEqual(pair, again) {
					t.Fatalf("restart changed pair: %+v %v", again, err)
				}
			}
			after, err := snapshotExecutionEvidence(data)
			if err != nil || before != after {
				t.Fatalf("original real journals changed: %v", err)
			}
		})
	}
}

func TestAtomicLedgerPairRejectsCorruptionAndSourceDrift(t *testing.T) {
	for _, mutation := range []string{"missing_seal", "orphan_stage", "forged_seal", "source_drift", "foreign_archive"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			data, store, id, archive := atomicRehearsalFixture(t, Short)
			if _, err := store.Prepare(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			if _, err := store.StageLifecycle(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			if _, err := store.StageOwnership(ctx, id, archive); err != nil {
				t.Fatal(err)
			}
			state, err := store.Seal(ctx, id, archive)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "missing_seal":
				err = os.Remove(filepath.Join(store.dir, atomicRehearsalFiles[3]))
			case "orphan_stage":
				err = os.Remove(filepath.Join(store.dir, atomicRehearsalFiles[1]))
			case "forged_seal":
				path := filepath.Join(store.dir, atomicRehearsalFiles[3])
				var record ExternalAtomicRehearsalRecord
				if err = strictReviewJSON(path, &record); err != nil {
					t.Fatal(err)
				}
				record.PostingApplied = true
				record.Checksum = record.digest()
				var raw []byte
				raw, err = json.Marshal(record)
				if err == nil {
					err = os.WriteFile(path, raw, 0600)
				}
			case "source_drift":
				err = os.Remove(filepath.Join(data, "trading", "fills", "BTCUSDT_901.json"))
			case "foreign_archive":
				archive.Gap.ClientOrderID = "bt_ffffffffffffffffffffffffffff"
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReadLedgerPair(ctx, id, archive, state.CommitSHA256); err == nil {
				t.Fatal("modified evidence still exposed cross-ledger candidate")
			}
			if _, err := store.ReadLifecycleCandidate(ctx, id, archive, state.CommitSHA256); err == nil {
				t.Fatal("modified evidence still exposed lifecycle candidate")
			}
			if _, err := store.ReadOwnershipCandidate(ctx, id, archive, state.CommitSHA256); err == nil {
				t.Fatal("modified evidence still exposed ownership candidate")
			}
		})
	}
}
