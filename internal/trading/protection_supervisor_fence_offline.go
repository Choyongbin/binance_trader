package trading

// OFFLINE TEST HARNESS ONLY. This is a second durable fence around the
// supervisor's decision to call an already one-shot simulated POST/DELETE.
// The underlying protection and cancel stores still own the per-leg
// MAY_HAVE_SENT records. This file neither grants product authorization nor
// accepts a real exchange.Client as a mutation transport.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OfflineSupervisorFence binds a simulated one-shot decision to the immutable
// plan and durable entry, before dispatch is attempted. A restart must treat
// its existence as possibly sent even if the process died before dispatch.
// It is never a signed exchange acknowledgement or settlement proof.
type OfflineSupervisorFence struct {
	Schema        int                            `json:"schema"`
	GroupID       string                         `json:"group_id"`
	ClientOrderID string                         `json:"client_order_id"`
	PlanDigest    string                         `json:"plan_digest"`
	Action        EntryProtectionRehearsalAction `json:"action"`
	FromState     string                         `json:"from_state"`
	CreatedAt     string                         `json:"created_at"`
	Checksum      string                         `json:"checksum"`
}

func (f OfflineSupervisorFence) digest() string {
	f.Checksum = ""
	blob, _ := json.Marshal(f)
	h := sha256.Sum256(blob)
	return hex.EncodeToString(h[:])
}

func (f OfflineSupervisorFence) valid() bool {
	if f.Schema != 1 || !requestIDPattern.MatchString(f.GroupID) ||
		!requestIDPattern.MatchString(f.ClientOrderID) || len(f.PlanDigest) != 64 ||
		f.CreatedAt == "" || f.Checksum != f.digest() {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, f.CreatedAt); err != nil {
		return false
	}
	return (f.Action == EntryRehearsalArm && f.FromState == EntryLifecyclePreparedOnly) ||
		(f.Action == EntryRehearsalCancel && f.FromState == EntryLifecycleCancelReview)
}

type OfflineSupervisorFenceStore struct {
	mu  sync.Mutex
	dir string
}

// OpenOfflineSupervisorFenceStore MUST ONLY be used by the sealed local-only
// simulation. Never construct it from a live server handler or background job.
func OpenOfflineSupervisorFenceStore(dataDir string) (*OfflineSupervisorFenceStore, error) {
	if dataDir == "" {
		return nil, errors.New("offline supervisor data dir required")
	}
	dir := filepath.Join(dataDir, "trading", "offline_supervisor_fences")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("offline supervisor fence directory not trusted")
	}
	return &OfflineSupervisorFenceStore{dir: dir}, nil
}

func (s *OfflineSupervisorFenceStore) file(group string, action EntryProtectionRehearsalAction) (string, error) {
	if !requestIDPattern.MatchString(group) {
		return "", errors.New("invalid offline supervisor group")
	}
	suffix := ""
	switch action {
	case EntryRehearsalArm:
		suffix = ".arm.json"
	case EntryRehearsalCancel:
		suffix = ".cancel.json"
	default:
		return "", errors.New("non-mutating action must not receive a fence")
	}
	return filepath.Join(s.dir, group+suffix), nil
}

// ReserveOnce writes an O_EXCL + fsync fence. On any existing file, it reads
// and validates the original attempt. Existing valid records are NEVER a
// reason to issue another simulated write; callers may only signed-GET.
// Corrupt records and mismatched entry/plan are hard failures.
func (s *OfflineSupervisorFenceStore) ReserveOnce(plan ProtectionDispatchPlan, entry Reservation,
	action EntryProtectionRehearsalAction, fromState string) (bool, error) {
	if s == nil || entry.ClientOrderID == "" || entry.Intent.Action != Enter ||
		entry.Intent.Symbol != "BTCUSDT" || plan.Source != entry.Intent.Source ||
		plan.ModelID != entry.Intent.ModelID || plan.PositionSide != entry.Intent.PositionSide ||
		plan.GroupID != entryProtectionGroupID(entry) {
		return false, errors.New("offline supervisor invalid plan or entry")
	}
	if err := plan.validate(); err != nil {
		return false, err
	}
	f := OfflineSupervisorFence{Schema: 1, GroupID: plan.GroupID, ClientOrderID: entry.ClientOrderID,
		PlanDigest: plan.Digest, Action: action, FromState: fromState,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	f.Checksum = f.digest()
	if !f.valid() {
		return false, errors.New("offline supervisor attempted invalid action/state")
	}
	path, err := s.file(f.GroupID, action)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, _ := json.MarshalIndent(f, "", "  ")
	err = exclusiveDurable(path, blob)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return false, fmt.Errorf("offline fence write uncertain; do not replay: %w", err)
	}
	info, statErr := os.Lstat(path)
	if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 4096 {
		return false, errors.New("offline supervisor fence file unsafe or damaged")
	}
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		return false, readErr
	}
	var prior OfflineSupervisorFence
	if json.Unmarshal(raw, &prior) != nil || !prior.valid() ||
		prior.GroupID != f.GroupID || prior.ClientOrderID != f.ClientOrderID ||
		prior.PlanDigest != f.PlanDigest || prior.Action != f.Action || prior.FromState != f.FromState {
		return false, errors.New("offline supervisor prior fence corrupted or conflicts with plan")
	}
	return false, nil
}

// RehearseFencedProtectionSupervisorTick adds one supervisor-level durable
// decision fence. A pending fence always changes a second invocation to a
// signed GET-only inspection, even if the first execution crashed BEFORE the
// simulated exchange call. Two different actions use different fences, so an
// already armed position may be separately considered for one sibling cancel.
// Every actual POST/DELETE remains strictly within the sealed mock transport.
func RehearseFencedProtectionSupervisorTick(ctx context.Context,
	r Reservation, coordinator *Coordinator, lifecycle *ExecutionLifecycleStore,
	protection *ProtectionSubmissionStore, cancels *CancelIntentStore,
	fences *OfflineSupervisorFenceStore, transport localOnlyProtectionRehearsalTransport,
	creds DemoCredentials, evidence *ProtectionEvidence,
) (OfflineProtectionSupervisorTick, error) {
	out := OfflineProtectionSupervisorTick{State: OfflineSupervisorHalted, ManualReviewRequired: true}
	if ctx == nil || coordinator == nil || lifecycle == nil || protection == nil || cancels == nil || fences == nil || transport == nil {
		return out, errors.New("offline fenced supervisor dependencies missing")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if filepath.Clean(fences.dir) != filepath.Join(filepath.Dir(coordinator.root), "trading", "offline_supervisor_fences") ||
		filepath.Clean(lifecycle.root) != filepath.Clean(coordinator.root) ||
		filepath.Clean(protection.dir) != filepath.Join(coordinator.root, "protection_submissions") ||
		filepath.Clean(cancels.dir) != filepath.Join(coordinator.root, "algo_cancel_intents") {
		return out, errors.New("offline supervisor fence/ledger root mismatch")
	}
	if err := coordinator.ownsLock(); err != nil {
		return out, err
	}
	before, err := ReviewRecoveredEntryProtectionLifecycleSigned(ctx, r, coordinator, lifecycle, protection, cancels, transport, creds)
	out.Before = before
	if err != nil {
		out.Reason = "signed GET failed before supervisor fence"
		return out, err
	}
	action, reason, err := nextOfflineSupervisorAction(before, evidence)
	out.Reason = reason
	if err != nil {
		return out, err
	}
	if action == EntryRehearsalInspect ||
		(action == EntryRehearsalArm && before.State == EntryLifecycleUnknownSubmission) ||
		(action == EntryRehearsalCancel && before.CancelMayHaveBeenSent) {
		return rehearseEntryProtectionSupervisorCore(ctx, r, coordinator, lifecycle, protection, cancels, transport, creds, evidence)
	}
	plan, err := protection.read(entryProtectionGroupID(r))
	if err != nil {
		return out, err
	}
	if action == EntryRehearsalCancel {
		owner := OwnerPosition{Source: r.Intent.Source, ModelID: r.Intent.ModelID,
			Symbol: r.Intent.Symbol, Side: r.Intent.PositionSide, Quantity: r.Intent.Quantity}
		if evidence == nil || !sameOfflineCancelReview(*evidence, before, plan, owner) {
			return out, errors.New("independent signed cancel evidence not verified")
		}
	}
	fresh, err := fences.ReserveOnce(plan, r, action, before.State)
	if err != nil {
		out.Reason = "offline supervisor durable fence invalid or uncertain; no replay"
		return out, err
	}
	if !fresh {
		// A prior process MAY HAVE sent. Even if the current signed view still
		// says PREPARED, never attempt POST/DELETE on this path again.
		inspected, inspectErr := RehearseEntryProtectionDispatch(ctx, EntryRehearsalInspect,
			r, coordinator, lifecycle, protection, cancels, transport, creds, nil)
		out.Before, out.Dispatch, out.Reason = before, inspected, "prior durable fence: GET-only; manual review required"
		out.State = OfflineSupervisorSignedRecover
		out.ChosenAction = EntryRehearsalInspect
		if inspectErr != nil {
			out.State = OfflineSupervisorHalted
		}
		return out, inspectErr
	}
	// Underlying dispatcher verifies the signed state AGAIN and writes each
	// respective leg/cancel MAY_HAVE_SENT before simulated mutation.
	return rehearseEntryProtectionSupervisorCore(ctx, r, coordinator, lifecycle, protection, cancels, transport, creds, evidence)
}
