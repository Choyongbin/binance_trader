package trading

import (
	"context"
	"errors"
	"fmt"
)

// ArmImportedEntryProtectionSupervised connects a fully imported entry with
// the existing one-shot Demo TP/SL arming state machine. It is deliberately
// NOT exposed by the product HTTP server or any unattended trading runtime.
// The caller must separately establish operational authorization, including
// whole-account ownership and recovery of preexisting incidents; passing a
// local hash-chain review alone does not establish those conditions.
//
// After the preflight, ArmDemoProtection independently fetches fresh signed
// account state before each POST and persists MAY_HAVE_SENT before submitting.
// Any ambiguous result stays blocked; this function never retries/cancels.
func ArmImportedEntryProtectionSupervised(ctx context.Context, group string,
	reservation Reservation, lifecycle ExecutionLifecycle, owner OwnerPosition,
	coordinator *Coordinator, store *ProtectionSubmissionStore,
	transport ProtectionArmingTransport, creds DemoCredentials,
) (ProtectionArmingResult, error) {
	result := ProtectionArmingResult{GroupID: group, ManualReviewRequired: true}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if coordinator == nil || store == nil || transport == nil ||
		creds.APIKey == "" || creds.Secret == "" {
		return result, errors.New("supervised entry protection 필수 구성요소가 없습니다")
	}
	// Validate coordinator ownership before even requesting an account view.
	if err := coordinator.ownsLock(); err != nil {
		return result, fmt.Errorf("supervised entry protection coordinator 소유권 실패: %w", err)
	}
	if reservation.ClientOrderID == "" || group == "" {
		return result, errors.New("supervised entry protection 진입·보호 그룹 식별자 누락")
	}
	pre, err := transport.FetchDemoExecutionPreflight(ctx, creds.APIKey, creds.Secret, "BTCUSDT")
	if err != nil {
		return result, fmt.Errorf("supervised entry protection signed preflight 실패: %w", err)
	}
	review, err := InspectEntryProtectionContinuityGroup(store, group, reservation, lifecycle, owner, pre)
	if err != nil {
		return result, fmt.Errorf("supervised entry protection 연속성 검사 실패: %w", err)
	}
	if review.State != "ENTRY_IMPORTED_PROTECTION_UNSENT_REVIEW" || !review.SignedSnapshotMatched ||
		review.CheckpointState != "PREPARED_NEVER_DISPATCHED" || len(review.Blockers) != 0 {
		return result, fmt.Errorf("supervised entry protection 미충족: state=%s checkpoint=%s blockers=%v", review.State, review.CheckpointState, review.Blockers)
	}
	// The review itself never authorizes a POST. This explicit supervised
	// boundary is kept outside the product server pending Demo E2E approval.
	return ArmDemoProtection(ctx, group, owner, coordinator, store, transport, creds)
}
