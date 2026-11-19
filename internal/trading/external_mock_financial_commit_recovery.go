package trading

// Dev.3.132: READ-ONLY crash/restart classification for the OFFLINE simulator.
// A complete immutable prefix can be resumed ONE stage at a time only after a
// separate simulation-only acknowledgement. Corrupt/torn checkpoints or changed
// source evidence NEVER cause automatic deletion, rollback, or rewrite.

import (
	"context"
	"errors"
)

const (
	ExternalMockRecoveryCold   = "OFFLINE_MOCK_COLD_START_NOT_POSTED"
	ExternalMockRecoveryResume = "OFFLINE_MOCK_VALID_PREFIX_RESUMABLE_NOT_POSTED"
	ExternalMockRecoveryFinal  = "OFFLINE_MOCK_FINAL_COMMIT_VERIFIED_NOT_POSTED"
)

type ExternalMockCommitRecovery struct {
	Schema                int    `json:"schema"`
	Status                string `json:"status"`
	BatchKey              string `json:"batch_key"`
	ConsistencySHA256     string `json:"consistency_sha256"`
	CompletedSteps        int    `json:"completed_steps"`
	LastDurableStage      string `json:"last_durable_stage,omitempty"`
	NextStage             string `json:"next_stage,omitempty"`
	ResumeAvailable       bool   `json:"resume_available"`
	SimulatedViewsVisible bool   `json:"simulated_views_visible"`
	CommitSHA256          string `json:"commit_sha256,omitempty"`
	PostingApplied        bool   `json:"posting_applied"`
	SettlementAuthorized  bool   `json:"settlement_authorized"`
	TradingAuthorized     bool   `json:"trading_authorized"`
}

// InspectRecovery revalidates both the original evidence and the mock checkpoint
// chain. It does not write anything. Errors are BLOCKED, not a request to repair
// or to assume an ambiguous COMMIT failed.
func (s *ExternalMockFinancialCommitStore) InspectRecovery(ctx context.Context, registry *ExternalGlobalEvidenceReservationStore, events []ExternalFinancialEventSource) (ExternalMockCommitRecovery, error) {
	var zero ExternalMockCommitRecovery
	i, err := s.Inspect(ctx, registry, events)
	if err != nil {
		return zero, err
	}
	if i.CompletedSteps < 0 || i.CompletedSteps > len(mockCommitKinds) || i.PostingApplied || i.SettlementAuthorized || i.TradingAuthorized {
		return zero, errors.New("invalid or authorizing simulated recovery state")
	}
	r := ExternalMockCommitRecovery{
		Schema: 1, BatchKey: i.BatchKey, ConsistencySHA256: i.ConsistencySHA256,
		CompletedSteps: i.CompletedSteps, SimulatedViewsVisible: i.SimulatedViewsVisible,
	}
	if i.CompletedSteps > 0 {
		r.LastDurableStage = mockCommitKinds[i.CompletedSteps-1]
	}
	if i.CompletedSteps == len(mockCommitKinds) {
		if i.Stage != ExternalMockCommitVisible || !i.SimulatedViewsVisible || i.CommitSHA256 == "" {
			return zero, errors.New("inconsistent final simulated commit")
		}
		r.Status = ExternalMockRecoveryFinal
		r.CommitSHA256 = i.CommitSHA256
		return r, nil
	}
	if i.SimulatedViewsVisible || i.CommitSHA256 != "" {
		return zero, errors.New("partial simulated commit leaked final views")
	}
	r.NextStage = mockCommitKinds[i.CompletedSteps]
	r.ResumeAvailable = true // OFFLINE only: requires a separate explicit acknowledgement
	if i.CompletedSteps == 0 {
		r.Status = ExternalMockRecoveryCold
	} else {
		r.Status = ExternalMockRecoveryResume
	}
	return r, nil
}

// ResumeNext requires a caller-pinned read-only recovery checkpoint, makes at
// most ONE stage of forward progress per pinned request, never retries a
// damaged checkpoint and never sends a financial posting or Binance request.
// If a crash happened AFTER the commit became durable but BEFORE the caller
// received its ACK, this re-reads the final marker instead of replaying COMMIT.
func (s *ExternalMockFinancialCommitStore) ResumeNext(ctx context.Context, registry *ExternalGlobalEvidenceReservationStore, events []ExternalFinancialEventSource, expectedSteps int, expectedConsistencySHA256, ack string) (ExternalMockCommitInspection, error) {
	var zero ExternalMockCommitInspection
	if ack != ExternalMockCommitAcknowledgement {
		return zero, errors.New("explicit simulation-only recovery acknowledgement required")
	}
	if expectedSteps < 0 || expectedSteps > len(mockCommitKinds) || len(expectedConsistencySHA256) != 64 {
		return zero, errors.New("pinned recovery step count and consistency SHA-256 required")
	}
	r, err := s.InspectRecovery(ctx, registry, events)
	if err != nil {
		return zero, err
	}
	if r.CompletedSteps != expectedSteps || r.ConsistencySHA256 != expectedConsistencySHA256 {
		return zero, errors.New("recovery progress or source SHA-256 changed; re-inspect before retry")
	}
	if !r.ResumeAvailable {
		return s.Inspect(ctx, registry, events)
	}
	// Advance targets the ORIGINAL pinned stage. If a concurrent actor already
	// wrote it, Advance will only re-read/idempotently confirm that stage;
	// it cannot move to a second stage based on a stale client request.
	return s.Advance(ctx, registry, events, mockCommitKinds[expectedSteps], ack)
}
