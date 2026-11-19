package trading

// Offline, read-only join between the 3.124 global evidence reservation chain
// and the per-event, NON-POSTED atomic rehearsal. This is not a posting gate:
// "candidate visible" means only an indivisible simulation read model.
// It cannot verify shared Binance account identity, funding ownership, or
// complete signed history. Never call this from the live trading runtime.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const (
	ExternalGlobalAtomicConsistent = "GLOBAL_ATOMIC_CANDIDATES_SEALED_NOT_POSTED"
	ExternalGlobalAtomicIncomplete = "GLOBAL_ATOMIC_CANDIDATES_INCOMPLETE_NOT_POSTED"
)

type ExternalGlobalAtomicCandidate struct {
	ClientOrderID            string       `json:"client_order_id"`
	ModelID                  string       `json:"model_id"`
	PositionSide             PositionSide `json:"position_side"`
	ReservationSHA256        string       `json:"reservation_sha256"`
	AtomicStage              string       `json:"atomic_stage"`
	AtomicCommitSHA256       string       `json:"atomic_commit_sha256,omitempty"`
	FinancialPreflightSHA256 string       `json:"financial_preflight_sha256,omitempty"`
	SimulatedPairVisible     bool         `json:"simulated_pair_visible"`
	PostingApplied           bool         `json:"posting_applied"`
	SettlementAuthorized     bool         `json:"settlement_authorized"`
	TradingAuthorized        bool         `json:"trading_authorized"`
}

type ExternalGlobalAtomicConsistency struct {
	Schema                   int                             `json:"schema"`
	Stage                    string                          `json:"stage"`
	BatchKey                 string                          `json:"batch_key"`
	RegistryChecksum         string                          `json:"registry_checksum"`
	ReservationSHA256        string                          `json:"reservation_sha256"`
	BoundedAuditSHA256       string                          `json:"bounded_audit_sha256"`
	Candidates               []ExternalGlobalAtomicCandidate `json:"candidates"`
	AllSimulatedPairsVisible bool                            `json:"all_simulated_pairs_visible"`
	SharedAccountProven      bool                            `json:"shared_account_proven"`
	CompleteHistoryProven    bool                            `json:"complete_history_proven"`
	FundingAllocated         bool                            `json:"funding_allocated"`
	PostingApplied           bool                            `json:"posting_applied"`
	SettlementAuthorized     bool                            `json:"settlement_authorized"`
	TradingAuthorized        bool                            `json:"trading_authorized"`
	DigestSHA256             string                          `json:"digest_sha256"`
}

func (r ExternalGlobalAtomicConsistency) digest() string {
	r.DigestSHA256 = ""
	return externalCloseHash(r)
}

// InspectExternalGlobalAtomicConsistency requires an exactly matching *entire*
// previously reserved batch, not a caller-provided claim or subset. Each
// event's linked atomic journal and full financial NON-POSTING preflight are
// revalidated; a missing seal is reported as incomplete, never as permission.
// Disk and exchange state is not mutated. A corrupt reservation or malformed
// upstream journal is an error, never an automatically repaired state.
func InspectExternalGlobalAtomicConsistency(
	ctx context.Context, registry *ExternalGlobalEvidenceReservationStore,
	events []ExternalFinancialEventSource,
) (ExternalGlobalAtomicConsistency, error) {
	var zero ExternalGlobalAtomicConsistency
	if ctx == nil || registry == nil || registry.root == "" {
		return zero, errors.New("context and existing offline registry required")
	}
	for _, e := range events {
		source, err := filepath.Abs(e.DataDir)
		if err != nil {
			return zero, err
		}
		source, err = filepath.EvalSymlinks(source)
		if err != nil {
			return zero, err
		}
		rel, err := filepath.Rel(source, registry.root)
		if err != nil {
			return zero, err
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
			return zero, errors.New("registry must be separate from original incident data")
		}
	}
	audit, err := InspectExternalCrossEventFinancials(ctx, events)
	if err != nil {
		return zero, err
	}
	if audit.Stage != ExternalCrossEventNoConflict || len(audit.Conflicts) != 0 ||
		audit.PostingApplied || audit.SettlementAuthorized || audit.TradingAuthorized ||
		audit.DigestSHA256 != audit.digest() {
		return zero, errors.New("invalid cross-event bounded financial audit")
	}
	state, err := registry.Inspect(ctx)
	if err != nil {
		return zero, err
	}
	archives := make(map[string]PackageAIncidentArchive, len(events))
	roots := make(map[string]string, len(events))
	for _, e := range events {
		archives[e.ClientOrderID] = e.Incident
		roots[e.ClientOrderID] = e.DataDir
	}
	derived := ExternalGlobalEvidenceReservation{Schema: 1, Stage: ExternalGlobalEvidenceReserved,
		BatchAuditSHA256: audit.DigestSHA256, Claims: make([]ExternalGlobalEvidenceClaim, 0, len(audit.Members))}
	for _, m := range audit.Members {
		derived.Claims = append(derived.Claims, externalGlobalClaimFromMember(m, archives[m.ClientOrderID]))
	}
	derived.BatchKey = derived.key()
	var selected *ExternalGlobalEvidenceReservation
	for i := range state.Reservations {
		r := &state.Reservations[i]
		if r.BatchKey == derived.BatchKey {
			if r.BatchAuditSHA256 != audit.DigestSHA256 || !reflect.DeepEqual(r.Claims, derived.Claims) {
				return zero, errors.New("reservation and revalidated original evidence differ")
			}
			selected = r
			break
		}
	}
	if selected == nil {
		return zero, errors.New("exact audited batch has not been globally reserved")
	}

	result := ExternalGlobalAtomicConsistency{Schema: 1, Stage: ExternalGlobalAtomicConsistent,
		BatchKey: selected.BatchKey, RegistryChecksum: state.LastChecksum,
		ReservationSHA256: selected.Checksum, BoundedAuditSHA256: audit.DigestSHA256,
		AllSimulatedPairsVisible: true, Candidates: make([]ExternalGlobalAtomicCandidate, 0, len(selected.Claims))}
	observedAtomic := make([]ExternalAtomicRehearsalInspection, 0, len(selected.Claims))
	observedPreflight := make([]string, 0, len(selected.Claims))
	for _, claim := range selected.Claims {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		store, err := NewExternalAtomicLinkedEventStore(roots[claim.ClientOrderID], claim.ClientOrderID)
		if err != nil {
			return zero, err
		}
		archive := archives[claim.ClientOrderID].Entry
		inspection, err := store.Inspect(ctx, claim.ClientOrderID, archive)
		if err != nil {
			return zero, fmt.Errorf("%s atomic journal: %w", claim.ClientOrderID, err)
		}
		candidate := ExternalGlobalAtomicCandidate{ClientOrderID: claim.ClientOrderID,
			ModelID: claim.ModelID, PositionSide: claim.PositionSide,
			ReservationSHA256: selected.Checksum, AtomicStage: inspection.Stage}
		if inspection.PostingApplied || inspection.LifecycleChanged || inspection.OwnershipChanged ||
			inspection.SettlementAuthorized || inspection.TradingAuthorized {
			return zero, errors.New("authorizing atomic candidate refused")
		}
		if inspection.Stage == ExternalAtomicRehearsalSealed && inspection.AtomicProjectionVisible {
			preflight, err := store.InspectFinancialCommitPreflight(ctx, claim.ClientOrderID, archive, inspection.CommitSHA256)
			if err != nil {
				return zero, fmt.Errorf("%s financial preflight: %w", claim.ClientOrderID, err)
			}
			if preflight.Stage != ExternalFinancialCommitBlocked || preflight.DigestSHA256 != preflight.digest() ||
				preflight.ClientOrderID != claim.ClientOrderID || preflight.ModelID != claim.ModelID ||
				preflight.PositionSide != claim.PositionSide || preflight.PinnedCommitSHA256 != inspection.CommitSHA256 ||
				preflight.PostingApplied || preflight.LifecycleChanged || preflight.OwnershipChanged ||
				preflight.SettlementAuthorized || preflight.TradingAuthorized {
				return zero, errors.New("financial preflight identity or safety mismatch")
			}
			candidate.AtomicCommitSHA256 = inspection.CommitSHA256
			candidate.FinancialPreflightSHA256 = preflight.DigestSHA256
			candidate.SimulatedPairVisible = true
		} else {
			result.Stage = ExternalGlobalAtomicIncomplete
			result.AllSimulatedPairsVisible = false
		}
		result.Candidates = append(result.Candidates, candidate)
		observedAtomic = append(observedAtomic, inspection)
		observedPreflight = append(observedPreflight, candidate.FinancialPreflightSHA256)
	}
	// Re-open every scoped atomic chain after reading the last event. A seal
	// becoming visible mid-scan must not produce a mixed-generation batch.
	for i, claim := range selected.Claims {
		store, err := NewExternalAtomicLinkedEventStore(roots[claim.ClientOrderID], claim.ClientOrderID)
		if err != nil {
			return zero, err
		}
		fresh, err := store.Inspect(ctx, claim.ClientOrderID, archives[claim.ClientOrderID].Entry)
		if err != nil || !reflect.DeepEqual(fresh, observedAtomic[i]) {
			return zero, errors.New("atomic rehearsal changed during global consistency scan")
		}
		if observedPreflight[i] != "" {
			gate, err := store.InspectFinancialCommitPreflight(ctx, claim.ClientOrderID, archives[claim.ClientOrderID].Entry, fresh.CommitSHA256)
			if err != nil || gate.DigestSHA256 != observedPreflight[i] {
				return zero, errors.New("financial preflight changed during global consistency scan")
			}
		}
	}
	// The entire batch is a single read model: never expose an individually
	// sealed candidate as a jointly ready pair while any peer is unfinished.
	if !result.AllSimulatedPairsVisible {
		for i := range result.Candidates {
			result.Candidates[i].SimulatedPairVisible = false
			result.Candidates[i].AtomicCommitSHA256 = ""
			result.Candidates[i].FinancialPreflightSHA256 = ""
		}
	}
	// Check both independent evidence sets once more after all scoped reads.
	freshState, err := registry.Inspect(ctx)
	if err != nil || !reflect.DeepEqual(state, freshState) {
		return zero, errors.New("global registry changed during atomic inspection")
	}
	freshAudit, err := InspectExternalCrossEventFinancials(ctx, events)
	if err != nil || !reflect.DeepEqual(audit, freshAudit) {
		return zero, errors.New("original incident evidence changed during atomic inspection")
	}
	result.DigestSHA256 = result.digest()
	return result, nil
}
