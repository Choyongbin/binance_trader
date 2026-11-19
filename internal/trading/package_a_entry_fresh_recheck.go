package trading

// A new, signed GET-only validation of a stored missing-entry-fill plan.
// It deliberately does NOT import fills, advance lifecycle/ownership, or
// authorize settlement. A signed HTTP request is not a Binance-signed archive.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"
)

const PackageAEntryFreshRecheckStage = "FRESH_SIGNED_ENTRY_RECHECK_REVIEW_ONLY"

type PackageAEntryFreshRecheck struct {
	Schema                      int                         `json:"schema"`
	Stage                       string                      `json:"stage"`
	CheckedAtUTC                string                      `json:"checked_at_utc"`
	ClientOrderID               string                      `json:"client_order_id"`
	EntryOrderID                int64                       `json:"entry_order_id"`
	EntryArchiveSHA256          string                      `json:"entry_archive_sha256"`
	RecoveryPlanSHA256          string                      `json:"recovery_plan_sha256"`
	OriginalLocalEvidenceSHA256 string                      `json:"original_local_evidence_sha256"`
	SignedOrderAndFillsMatch    bool                        `json:"fresh_signed_order_and_fills_match"`
	MissingLocalTradeIDs        []int64                     `json:"missing_local_trade_ids"`
	LocalLifecycleStage         string                      `json:"local_lifecycle_stage"`
	LocalLifecycleEvents        []PackageALifecycleBoundary `json:"local_lifecycle_events"`
	FailureBoundary             string                      `json:"failure_boundary"`
	RootCauseProven             bool                        `json:"root_cause_proven"`
	BlockerCodes                []string                    `json:"blocker_codes"`
	FillJournalChanged          bool                        `json:"fill_journal_changed"`
	LifecycleChanged            bool                        `json:"lifecycle_changed"`
	OwnershipChanged            bool                        `json:"ownership_changed"`
	RecoveryApplied             bool                        `json:"recovery_applied"`
	SettlementAuthorized        bool                        `json:"settlement_authorized"`
	TradingAuthorized           bool                        `json:"trading_authorized"`
	Checksum                    string                      `json:"local_sha256_checksum"`
}

type PackageALifecycleBoundary struct {
	Stage           ExecutionStage `json:"stage"`
	At              string         `json:"at"`
	ExchangeOrderID int64          `json:"exchange_order_id,omitempty"`
	EvidenceSHA256  string         `json:"local_evidence_sha256"`
}

func (r PackageAEntryFreshRecheck) digest() string {
	r.Checksum = ""
	return externalCloseHash(r)
}

// RecheckPackageAEntryRecovery requires a currently running signed-GET reader
// supplied explicitly by the operator and verifies old evidence *again* both
// before and after network activity. It is NOT an approval to apply the plan.
// The exact crash/transport failure that caused the gap is usually unknowable
// from a MAY_HAVE_SENT marker alone; report the last verified durable boundary.
func RecheckPackageAEntryRecovery(ctx context.Context, dataDir string, archive PackageAEntryArchive, plan PackageAEntryRecoveryPlan, reader DemoTradeReader, creds DemoCredentials, now time.Time) (PackageAEntryFreshRecheck, error) {
	var empty PackageAEntryFreshRecheck
	if ctx == nil || reader == nil || creds.APIKey == "" || creds.Secret == "" || now.IsZero() {
		return empty, errors.New("fresh signed GET reader, credentials and clock required")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := VerifyPackageAEntryRecoveryPlan(ctx, dataDir, archive, plan); err != nil {
		return empty, fmt.Errorf("current original plan invalid: %w", err)
	}
	before, err := snapshotExecutionEvidence(dataDir)
	if err != nil {
		return empty, err
	}
	if before != plan.LocalEvidenceSHA256 {
		return empty, errors.New("local evidence changed before signed recheck")
	}

	// Audit independently verifies two signed order reads bracketing a complete
	// userTrades response, exact order/fill identity, quantities, and local gap.
	fresh, err := AuditPackageAEntryGap(ctx, dataDir, archive.Gap.ClientOrderID, reader, creds)
	if err != nil {
		return empty, fmt.Errorf("fresh signed entry recheck failed: %w", err)
	}
	if !reflect.DeepEqual(fresh, archive.Gap) {
		return empty, errors.New("fresh signed entry evidence differs from sealed archive; no recovery")
	}
	if err := VerifyPackageAEntryRecoveryPlan(ctx, dataDir, archive, plan); err != nil {
		return empty, fmt.Errorf("original evidence drifted during signed recheck: %w", err)
	}

	// Read the complete hash-chained local lifecycle as a forensic last-known
	// boundary. Absence or corruption is not silently interpreted as a crash.
	lifecycle, err := loadExecutionLifecycleFromDir(filepathJoin(dataDir, "trading", "execution_lifecycle"), plan.ClientOrderID)
	boundaries := []PackageALifecycleBoundary{}
	if errors.Is(err, os.ErrNotExist) && plan.LifecycleStage == "MISSING_LIFECYCLE_BLOCKED" {
		// Legacy journals may predate execution lifecycle support. Do not
		// manufacture a MAY_HAVE_SENT event or assert a crash boundary.
	} else if err != nil {
		return empty, fmt.Errorf("local lifecycle boundary unavailable: %w", err)
	} else {
		if string(lifecycle.Stage) != plan.LifecycleStage {
			return empty, errors.New("lifecycle stage changed since recovery plan")
		}
		boundaries = append(boundaries, PackageALifecycleBoundary{Stage: lifecycle.Root.Stage, At: lifecycle.Root.CreatedAt, EvidenceSHA256: lifecycle.Root.Checksum})
		for _, event := range lifecycle.Events {
			boundaries = append(boundaries, PackageALifecycleBoundary{Stage: event.Stage, At: event.At, ExchangeOrderID: event.ExchangeOrderID, EvidenceSHA256: event.Checksum})
		}
	}
	after, err := snapshotExecutionEvidence(dataDir)
	if err != nil || after != before {
		return empty, errors.New("original financial evidence drifted during signed recheck")
	}
	ids := append([]int64{}, fresh.MissingLocalTradeIDs...)
	blockers := []string{"OWNED_ENTRY_FILLS_NOT_IMPORTED", "EXTERNAL_EXIT_OWNERSHIP_UNVERIFIED", "FULL_ACCOUNT_HISTORY_UNVERIFIED", "MODEL_FUNDING_ATTRIBUTION_UNVERIFIED", "ATOMIC_LEDGER_POSTING_UNIMPLEMENTED", "HTTP_423_ORDER_LOCK_REQUIRED"}
	if len(ids) == 0 {
		blockers[0] = "OWNED_ENTRY_IMPORT_REQUIRES_SEPARATE_VERIFICATION"
	}
	if len(boundaries) == 0 {
		blockers = append(blockers, "MISSING_ENTRY_LIFECYCLE")
	}
	result := PackageAEntryFreshRecheck{
		Schema: 1, Stage: PackageAEntryFreshRecheckStage, CheckedAtUTC: now.UTC().Format(time.RFC3339Nano),
		ClientOrderID: plan.ClientOrderID, EntryOrderID: plan.OrderID,
		EntryArchiveSHA256: archive.Checksum, RecoveryPlanSHA256: plan.Checksum,
		OriginalLocalEvidenceSHA256: before, SignedOrderAndFillsMatch: true,
		MissingLocalTradeIDs: ids, LocalLifecycleStage: plan.LifecycleStage,
		LocalLifecycleEvents: boundaries,
		FailureBoundary:      "ONLY_LAST_DURABLE_STAGE_KNOWN; EXACT_POST_OR_RECOVERY_FAILURE_NOT_PROVEN",
		RootCauseProven:      false, BlockerCodes: blockers,
	}
	result.Checksum = result.digest()
	return result, nil
}
