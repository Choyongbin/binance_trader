package trading

// Read-only cross-ledger reconciliation for a product protection inventory.
// In addition to unfinished entry reservations, an interrupted process can
// leave an Algo submission plan or a sibling DELETE marker behind. Scanning
// reservations alone would miss those durable side effects. This audit treats
// every unmatched or inconsistent local artifact as a blocking incident; it
// neither repairs files nor submits/cancels anything on Binance.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

const maxLocalProtectionArtifacts = 10000

func (r *DemoProtectionRuntimeCoordinator) auditLocalProtectionArtifacts(ctx context.Context, reservations []Reservation) ([]ProtectionIncident, error) {
	if r == nil || r.submissions == nil || r.cancels == nil || ctx == nil {
		return nil, errors.New("product protection artifact audit dependencies missing")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	incident := func(group, client, code string) ProtectionIncident {
		return ProtectionIncident{GroupID: group, ClientOrderID: client, Code: code, ManualReviewRequired: true}
	}
	// A group identifier must never silently point to two different entries.
	byGroup := make(map[string]Reservation, len(reservations))
	problems := make([]ProtectionIncident, 0)
	for _, entry := range reservations {
		if entry.ClientOrderID == "" || entry.Intent.Action != Enter {
			continue
		}
		group := entryProtectionGroupID(entry)
		if prior, exists := byGroup[group]; exists {
			problems = append(problems, incident(group, entry.ClientOrderID, "DUPLICATE_ENTRY_PROTECTION_GROUP"))
			if prior.ClientOrderID != entry.ClientOrderID {
				return problems, errors.New("conflicting protection group owner")
			}
			continue
		}
		byGroup[group] = entry
	}
	r.submissions.mu.Lock()
	plans, err := r.submissions.scanPlansLocked()
	r.submissions.mu.Unlock()
	if err != nil {
		return problems, fmt.Errorf("protection submission artifact scan failed: %w", err)
	}
	if len(plans) > maxLocalProtectionArtifacts {
		return problems, errors.New("too many durable protection plans for bounded audit")
	}
	byPlan := make(map[string]ProtectionDispatchPlan, len(plans))
	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return problems, err
		}
		byPlan[plan.GroupID] = plan
		entry, found := byGroup[plan.GroupID]
		if !found {
			problems = append(problems, incident(plan.GroupID, "", "ORPHAN_PROTECTION_PLAN_NO_ENTRY"))
			continue
		}
		if entry.Intent.Source != Model || entry.Intent.Action != Enter || entry.Intent.Symbol != "BTCUSDT" ||
			entry.Intent.Source != plan.Source || entry.Intent.ModelID != plan.ModelID ||
			entry.Intent.PositionSide != plan.PositionSide || plan.TP.Symbol != entry.Intent.Symbol ||
			plan.SL.Symbol != entry.Intent.Symbol {
			problems = append(problems, incident(plan.GroupID, entry.ClientOrderID, "PROTECTION_PLAN_ENTRY_OWNER_MISMATCH"))
		}
	}
	files, err := os.ReadDir(r.cancels.dir)
	if err != nil {
		return problems, fmt.Errorf("cancel intent directory audit failed: %w", err)
	}
	if len(files) > maxLocalProtectionArtifacts*3 {
		return problems, errors.New("too many cancel artifacts for bounded audit")
	}
	cancelGroups := make(map[string]map[string]bool)
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return problems, err
		}
		if file.IsDir() || !file.Type().IsRegular() {
			return problems, errors.New("unexpected non-regular cancel intent artifact")
		}
		name := file.Name()
		suffix := ".json"
		switch {
		case strings.HasSuffix(name, ".dispatch.json"):
			suffix = ".dispatch.json"
		case strings.HasSuffix(name, ".outcome.json"):
			suffix = ".outcome.json"
		case strings.HasSuffix(name, ".json"):
		default:
			return problems, errors.New("unexpected cancel intent artifact name")
		}
		group := strings.TrimSuffix(name, suffix)
		if !cancelGroupRE.MatchString(group) {
			return problems, errors.New("invalid cancel intent artifact group")
		}
		if cancelGroups[group] == nil {
			cancelGroups[group] = make(map[string]bool)
		}
		cancelGroups[group][suffix] = true
	}
	for group, suffixes := range cancelGroups {
		if err := ctx.Err(); err != nil {
			return problems, err
		}
		entry, knownEntry := byGroup[group]
		plan, knownPlan := byPlan[group]
		if !knownEntry || !knownPlan {
			problems = append(problems, incident(group, entry.ClientOrderID, "ORPHAN_SIBLING_CANCEL_INTENT"))
			continue
		}
		if !suffixes[".json"] || (suffixes[".outcome.json"] && !suffixes[".dispatch.json"]) {
			problems = append(problems, incident(group, entry.ClientOrderID, "INCOMPLETE_SIBLING_CANCEL_JOURNAL"))
			continue
		}
		cancel, attempted, inspectErr := r.cancels.Inspect(group)
		if inspectErr != nil {
			problems = append(problems, incident(group, entry.ClientOrderID, "CORRUPT_SIBLING_CANCEL_JOURNAL"))
			continue
		}
		if attempted != suffixes[".dispatch.json"] ||
			(cancel.OwnerClientAlgoID != plan.TP.ClientAlgoID && cancel.OwnerClientAlgoID != plan.SL.ClientAlgoID) {
			problems = append(problems, incident(group, entry.ClientOrderID, "SIBLING_CANCEL_TARGET_MISMATCH"))
			continue
		}
		leg := "tp"
		if cancel.OwnerClientAlgoID == plan.SL.ClientAlgoID {
			leg = "sl"
		}
		observed, obsErr := r.submissions.Inspect(group, leg)
		if obsErr != nil || !observed.SignedObserved || observed.ObservedAlgoID != cancel.SiblingAlgoID {
			// Never trust the caller-chosen DELETE target merely because its
			// clientAlgoId matches a prepared instruction. Its AlgoID must be
			// bound to the original sealed and signed-observed leg.
			problems = append(problems, incident(group, entry.ClientOrderID, "SIBLING_CANCEL_ALGO_ID_UNVERIFIED"))
		}
	}
	return problems, nil
}
