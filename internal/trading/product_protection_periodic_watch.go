package trading

import (
	"context"
	"errors"
	"sync"
	"time"

	"binance_trader/internal/exchange"
)

// ProductProtectionWatchState deliberately reports observations, not an
// authority to open trades or to issue either independent Binance Algo leg.
const (
	ProtectionWatchNotStarted = "NOT_STARTED_NO_PROTECTION_ATTESTATION"
	ProtectionWatchChecking   = "CHECKING_NOT_AUTHORIZED"
	ProtectionWatchObserved   = "LOCAL_PROTECTION_OBSERVED_NOT_APPROVED"
	ProtectionWatchEmpty      = "NO_LOCAL_ENTRIES_ACCOUNT_UNVERIFIED"
	ProtectionWatchBlocked    = "SIGNED_PROTECTION_REVIEW_BLOCKED"
	ProtectionWatchStopped    = "STOPPED_NO_CONTINUOUS_MONITORING"
)

// ProductProtectionWatchStatus is intentionally process-local and non-durable.
// An observation may become stale the instant it was read; neither this status
// nor an ACTIVE pair certifies continuous OCO protection.
type ProductProtectionWatchStatus struct {
	State                string                     `json:"state"`
	Running              bool                       `json:"running"`
	LastCheckedAt        string                     `json:"last_checked_at,omitempty"`
	LastErrorCategory    string                     `json:"last_error_category,omitempty"`
	Stale                bool                       `json:"stale"`
	Checks               uint64                     `json:"checks"`
	Inventory            PendingProtectionInventory `json:"inventory"`
	ManualReviewRequired bool                       `json:"manual_review_required"`
	TradingAuthorized    bool                       `json:"trading_authorized"`
	SubmitAuthorized     bool                       `json:"submit_authorized"`
	CancelAuthorized     bool                       `json:"cancel_authorized"`
	SettlementAuthorized bool                       `json:"settlement_authorized"`
	ContinuousProtection bool                       `json:"continuous_protection_proven"`
}

// ProductProtectionPoller is server-owned rather than auto-run-owned. STOP
// revokes the entry lease, not this independent READ-ONLY watcher. One signed
// pass is bounded and serialized; overlap or an unverified result blocks the
// watch report. This is not a substitute for a real exchange-side protector.
type ProductProtectionPoller struct {
	mu       sync.Mutex
	inspect  func(context.Context) (PendingProtectionInventory, error)
	interval time.Duration
	timeout  time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
	closed   bool
	status   ProductProtectionWatchStatus
}

func cloneProtectionInventory(in PendingProtectionInventory) PendingProtectionInventory {
	in.Entries = append([]EntryProtectionLifecycleReview(nil), in.Entries...)
	for i := range in.Entries {
		in.Entries[i].Checkpoint.Blockers = append([]string(nil), in.Entries[i].Checkpoint.Blockers...)
	}
	in.Steps = append([]ProductProtectionStep(nil), in.Steps...)
	in.Blockers = append([]string(nil), in.Blockers...)
	in.Incidents = append([]ProtectionIncident(nil), in.Incidents...)
	return in
}

func NewProductProtectionPoller(inspect func(context.Context) (PendingProtectionInventory, error), interval, timeout time.Duration) (*ProductProtectionPoller, error) {
	if inspect == nil || interval <= 0 || timeout <= 0 || timeout >= interval {
		return nil, errors.New("protection polling requires an inspector and 0 < timeout < interval")
	}
	return &ProductProtectionPoller{inspect: inspect, interval: interval, timeout: timeout,
		status: ProductProtectionWatchStatus{State: ProtectionWatchNotStarted, ManualReviewRequired: true,
			Inventory: PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true}}}, nil
}

func (p *ProductProtectionPoller) Status() ProductProtectionWatchStatus {
	if p == nil {
		return ProductProtectionWatchStatus{State: ProtectionWatchBlocked, ManualReviewRequired: true}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.status
	v.Inventory = cloneProtectionInventory(v.Inventory)
	checked, parseErr := time.Parse(time.RFC3339Nano, v.LastCheckedAt)
	v.Stale = parseErr != nil || time.Since(checked) > 2*p.interval
	if v.Stale && v.Running {
		v.State = ProtectionWatchBlocked
		v.LastErrorCategory = "STALE_SIGNED_OBSERVATION"
		v.Inventory = PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true}
	}
	return v
}

// Start is idempotent. A fresh process starts in the blocked/not-yet-checked
// state, not with a previous process' successful observation.
func (p *ProductProtectionPoller) Start() error {
	if p == nil {
		return errors.New("protection poller missing")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("protection poller is closed")
	}
	if p.cancel != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.done = make(chan struct{})
	p.status.Running = true
	p.status.State = ProtectionWatchChecking
	go p.run(ctx, p.done)
	return nil
}

func (p *ProductProtectionPoller) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer func() {
		p.mu.Lock()
		p.status.Running = false
		p.status.State = ProtectionWatchStopped
		p.mu.Unlock()
	}()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		p.check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *ProductProtectionPoller) check(parent context.Context) {
	if parent.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()
	// A failed read must not retain a previous apparently-good inventory.
	p.mu.Lock()
	p.status.State = ProtectionWatchChecking
	p.status.Inventory = PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true}
	p.mu.Unlock()

	var inventory PendingProtectionInventory
	var err error
	func() {
		defer func() {
			if recover() != nil {
				err = errors.New("protection inspection panic")
			}
		}()
		inventory, err = p.inspect(ctx)
	}()
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	// An inspector reporting any authorization is itself an incident.
	// Never propagate it through a read-only status endpoint.
	claimedAuthorization := inventory.TradingAuthorized || inventory.SubmitAuthorized ||
		inventory.CancelAuthorized || inventory.SettlementAuthorized || inventory.ContinuousMonitoring || !inventory.ManualReviewRequired
	// A step is a non-executable reconciliation instruction, never a capability.
	// A forged/misaligned step cannot become part of an observed watch status.
	if len(inventory.Steps) > 0 && len(inventory.Steps) != len(inventory.Entries) {
		claimedAuthorization = true
	}
	for i := range inventory.Steps {
		step := &inventory.Steps[i]
		if step.TradingAuthorized || step.SubmitAuthorized || step.CancelAuthorized ||
			step.SettlementAuthorized || !step.ManualReviewRequired {
			claimedAuthorization = true
		}
		if i >= len(inventory.Entries) ||
			step.ClientOrderID != inventory.Entries[i].ClientOrderID ||
			step.GroupID != inventory.Entries[i].GroupID ||
			step.LifecycleState != inventory.Entries[i].State {
			claimedAuthorization = true
		} else {
			expected, policyErr := classifyProductProtectionStep(inventory.Entries[i])
			if policyErr != nil {
				// A known incident is still a valid blocked status, but it
				// must not masquerade as a plan for an executable transition.
				if step.NextStep != ProductStepHalt {
					claimedAuthorization = true
				}
			} else if step.NextStep != expected.NextStep || step.SignedRecoveryOnly != expected.SignedRecoveryOnly {
				claimedAuthorization = true
			}
		}
		step.TradingAuthorized = false
		step.SubmitAuthorized = false
		step.CancelAuthorized = false
		step.SettlementAuthorized = false
		step.ManualReviewRequired = true
	}
	for i := range inventory.Entries {
		entry := &inventory.Entries[i]
		if entry.TradingAuthorized || entry.ProtectionSubmitAuthorized || entry.CancelAuthorized ||
			entry.RetryAuthorized || entry.SettlementAuthorized || !entry.ManualReviewRequired {
			claimedAuthorization = true
		}
		checkpoint := &entry.Checkpoint
		if checkpoint.TradingAuthorized || checkpoint.ProtectionSubmitAuthorized || checkpoint.RetryAuthorized ||
			(checkpoint.State != "" && !checkpoint.ManualReviewRequired) ||
			checkpoint.TP.LiveAuthorized || checkpoint.SL.LiveAuthorized ||
			(checkpoint.TP.Leg != "" && !checkpoint.TP.ManualReviewRequired) ||
			(checkpoint.SL.Leg != "" && !checkpoint.SL.ManualReviewRequired) ||
			entry.ExitFills.TradingAuthorized || entry.ExitFills.FinanciallySettled ||
			(entry.ExitFills.State != "" && !entry.ExitFills.ManualReviewRequired) ||
			(entry.State == EntryLifecyclePairActive && checkpoint.State != "" &&
				checkpoint.State != "TWO_LEGS_PREVIOUSLY_OBSERVED_RECHECK_REQUIRED") {
			claimedAuthorization = true
		}
		checkpoint.TradingAuthorized = false
		checkpoint.ProtectionSubmitAuthorized = false
		checkpoint.RetryAuthorized = false
		checkpoint.TP.LiveAuthorized = false
		checkpoint.SL.LiveAuthorized = false
		checkpoint.TP.ManualReviewRequired = true
		checkpoint.SL.ManualReviewRequired = true
		checkpoint.ManualReviewRequired = true
		entry.ExitFills.TradingAuthorized = false
		entry.ExitFills.FinanciallySettled = false
		entry.ExitFills.ManualReviewRequired = true
		entry.TradingAuthorized = false
		entry.ProtectionSubmitAuthorized = false
		entry.CancelAuthorized = false
		entry.RetryAuthorized = false
		entry.SettlementAuthorized = false
		entry.ManualReviewRequired = true
	}
	inventory.TradingAuthorized = false
	inventory.SubmitAuthorized = false
	inventory.CancelAuthorized = false
	inventory.SettlementAuthorized = false
	inventory.ContinuousMonitoring = false
	inventory.ManualReviewRequired = true
	state := ProtectionWatchBlocked
	category := ""
	if err != nil {
		// A real inventory scan can return a useful partial incident report
		// alongside its fail-closed error. Preserve those observations for an
		// operator, but NEVER call them observed/approved. A generic transport
		// failure without a coherent incident remains an empty blocked report.
		if inventory.State == PendingProtectionInspectionBlocked && len(inventory.Incidents) > 0 &&
			!claimedAuthorization && len(inventory.Blockers) > 0 {
			category = "PROTECTION_INCIDENT_REQUIRES_RECONCILIATION"
		} else {
			category = "SIGNED_INSPECTION_FAILED"
			inventory = PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true}
		}
	} else {
		switch inventory.State {
		case PendingProtectionInspectionObserved:
			if len(inventory.Blockers) == 0 && len(inventory.Incidents) == 0 && !claimedAuthorization && inventory.InspectedEntries > 0 && len(inventory.Entries) == inventory.InspectedEntries {
				state = ProtectionWatchObserved
			}
		case PendingProtectionInspectionEmpty:
			if len(inventory.Blockers) == 0 && len(inventory.Incidents) == 0 && !claimedAuthorization && inventory.InspectedEntries == 0 && len(inventory.Entries) == 0 {
				state = ProtectionWatchEmpty
			}
		}
		if state == ProtectionWatchBlocked {
			category = "UNSAFE_OR_UNRECOGNIZED_INVENTORY"
		}
	}
	p.mu.Lock()
	p.status.Checks++
	p.status.LastCheckedAt = time.Now().UTC().Format(time.RFC3339Nano)
	p.status.State = state
	p.status.LastErrorCategory = category
	p.status.Inventory = cloneProtectionInventory(inventory)
	p.mu.Unlock()
}

// Close terminates the polling service independently of auto STOP. The caller
// must not tear down the server's dependencies before this returns. If the
// inspector ignores cancellation, return an error instead of claiming it shut
// down cleanly (the inspect timeout cannot interrupt a blocking transport).
func (p *ProductProtectionPoller) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	p.closed = true
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(p.timeout + time.Second):
			return errors.New("protection watcher shutdown not confirmed")
		}
	}
	return nil
}

// InspectDemoOutstandingProtectionsOnce performs a bounded exchange GET-only
// review after taking the existing local coordinator lock. It deliberately
// cannot overlap an active auto-run or wallet mutation: lock contention fails
// closed. Opening/closing the coordinator changes local lock files, but never
// mutates an order, protection checkpoint or financial ledger.
func InspectDemoOutstandingProtectionsOnce(ctx context.Context, dataDir string, client *exchange.Client, credentials CredentialsProvider) (out PendingProtectionInventory, runErr error) {
	blocked := PendingProtectionInventory{State: PendingProtectionInspectionBlocked, ManualReviewRequired: true}
	if ctx == nil || dataDir == "" || client == nil || credentials == nil {
		return blocked, errors.New("missing Demo protection observation dependencies")
	}
	if err := ctx.Err(); err != nil {
		return blocked, err
	}
	creds, err := credentials(ctx)
	if err != nil {
		return blocked, err
	}
	if creds.APIKey == "" || creds.Secret == "" {
		return blocked, errors.New("Demo credentials unavailable")
	}
	// Pin the authenticated account across all signed requests in this cycle.
	// An account switch must not merge two accounts' ownership evidence.
	pinnedCredentials := func(checkCtx context.Context) (DemoCredentials, error) {
		current, readErr := credentials(checkCtx)
		if readErr != nil {
			return DemoCredentials{}, readErr
		}
		if current.APIKey != creds.APIKey || current.Secret != creds.Secret {
			return DemoCredentials{}, errors.New("Demo credentials changed during protection observation")
		}
		return current, nil
	}
	coordinator, err := OpenCoordinator(dataDir)
	if err != nil {
		return blocked, err
	}
	defer func() {
		if closeErr := coordinator.Close(); closeErr != nil {
			out = blocked
			runErr = errors.Join(runErr, errors.New("protection coordinator release failed"))
		}
	}()
	inspector, err := NewDemoProtectionRuntimeCoordinator(dataDir, coordinator, client, pinnedCredentials)
	if err != nil {
		return blocked, err
	}
	return inspector.InspectOutstandingProtections(ctx)
}
