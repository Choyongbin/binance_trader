package trading

import (
	"context"
	"errors"
	"time"

	"binance_trader/internal/exchange"
)

type DemoProtectionRuntimeCoordinator struct {
	dataDir     string
	coordinator *Coordinator
	submissions *ProtectionSubmissionStore
	settlements *ProtectionSettlementStore
	owners      *OwnershipStore
	cancels     *CancelIntentStore
	client      *exchange.Client
	credentials CredentialsProvider
}

func NewDemoProtectionRuntimeCoordinator(dataDir string, coordinator *Coordinator, client *exchange.Client, credentials CredentialsProvider) (*DemoProtectionRuntimeCoordinator, error) {
	if dataDir == "" || coordinator == nil || client == nil || credentials == nil {
		return nil, errors.New("Demo protection runtime coordinator 필수 구성요소가 없습니다")
	}
	if err := coordinator.ownsLock(); err != nil {
		return nil, err
	}
	submissions, err := OpenProtectionSubmissionStore(dataDir)
	if err != nil {
		return nil, err
	}
	settlements, err := NewProtectionSettlementStore(dataDir)
	if err != nil {
		return nil, err
	}
	owners, err := NewOwnershipStore(dataDir)
	if err != nil {
		return nil, err
	}
	cancels, err := NewCancelIntentStore(dataDir)
	if err != nil {
		return nil, err
	}
	return &DemoProtectionRuntimeCoordinator{dataDir: dataDir, coordinator: coordinator, submissions: submissions, settlements: settlements, owners: owners, cancels: cancels, client: client, credentials: credentials}, nil
}

// Arm is fail-closed until the product has independent approval, full signed
// account ownership validation and Demo E2E certification. In particular, an
// internal runtime handle must not be an implicit approval to place two live
// non-OCO Algo orders.
func (r *DemoProtectionRuntimeCoordinator) Arm(ctx context.Context, group string, owner OwnerPosition) (ProtectionArmingResult, error) {
	return ProtectionArmingResult{GroupID: group, ManualReviewRequired: true}, ErrProtectionApproval
}

// RecoverArming performs read-only recovery of MAY_HAVE_SENT protection legs.
// It never replays POST/DELETE side effects.
func (r *DemoProtectionRuntimeCoordinator) RecoverArming(ctx context.Context, group string, owner OwnerPosition) (ProtectionArmingResult, error) {
	creds, err := r.credentials(ctx)
	if err != nil {
		return ProtectionArmingResult{}, err
	}
	return RecoverDemoProtectionArming(ctx, group, owner, r.coordinator, r.submissions, r.client, creds)
}

// CancelTriggeredSibling is disabled on the product runtime. The caller-supplied
// ProtectionEvidence is NOT an approval token or a substitute for an
// independent, account-wide signed ownership audit. The offline-only 3.119
// harness exercises the separate single-DELETE recovery path without enabling
// this production exchange mutation entry point.
func (r *DemoProtectionRuntimeCoordinator) CancelTriggeredSibling(ctx context.Context, group string, evidence ProtectionEvidence) (exchange.DemoAlgoResult, error) {
	return exchange.DemoAlgoResult{}, ErrProtectionApproval
}

func (r *DemoProtectionRuntimeCoordinator) RecoverSettlement(ctx context.Context, group string) (ProtectionSettlementResult, error) {
	creds, err := r.credentials(ctx)
	if err != nil {
		return ProtectionSettlementResult{}, err
	}
	return r.settlements.RecoverProtectionSettlement(ctx, group, r.submissions, r.owners, r.client, creds)
}

var timeNowUTC = func() time.Time { return time.Now().UTC() }
