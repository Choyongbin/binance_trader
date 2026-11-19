package trading

// Unified, strictly read-only operator status assembled from immutable local
// evidence. It does not contact Binance, acquire credentials, remove locks,
// mutate evidence, or authorize trading. Its purpose is to stop operators from
// having to mentally combine several historical diagnostics with subtly
// different scopes.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	OperatorStateEmpty          = "EMPTY_HISTORICAL_ONLY"
	OperatorStateHistoricalOnly = "HISTORICAL_ONLY_MANUAL_REVIEW"
	OperatorStateBlocked        = "BLOCKED_MANUAL_RECONCILIATION"
)

type OperatorProtectionStatus struct {
	ArmingState          string `json:"arming_state"`
	PlanCount            int    `json:"plan_count"`
	SettledGroupCount    int    `json:"settled_group_count"`
	UnsettledGroupCount  int    `json:"unsettled_group_count"`
	ProtectionFillCount  int    `json:"protection_fill_count"`
	DualTriggeredHistory int    `json:"dual_triggered_history"`
}

type OperatorRecoveryStatus struct {
	ProbeCount       int    `json:"probe_count"`
	LatestProbeStage string `json:"latest_probe_stage,omitempty"`
	InterruptedCount int    `json:"interrupted_count"`
	ReviewCount      int    `json:"review_count"`
}

type OperatorReconnectStatus struct {
	WatchCount       int    `json:"watch_count"`
	LatestWatchStage string `json:"latest_watch_stage,omitempty"`
	InterruptedCount int    `json:"interrupted_count"`
}

type OperatorAccountingStatus struct {
	ReportCount               int       `json:"report_count"`
	LatestCreatedAt           time.Time `json:"latest_created_at,omitempty"`
	LatestKnownOrders         int       `json:"latest_known_orders,omitempty"`
	LatestKnownFills          int       `json:"latest_known_fills,omitempty"`
	LatestProtectionFills     int       `json:"latest_protection_fills,omitempty"`
	LatestProtectionGroups    int       `json:"latest_protection_groups,omitempty"`
	LatestDualTriggeredGroups int       `json:"latest_dual_triggered_groups,omitempty"`
}

type OperatorIntegratedStatus struct {
	ReportCount  int       `json:"report_count"`
	LatestState  string    `json:"latest_state,omitempty"`
	LatestAt     time.Time `json:"latest_at,omitempty"`
	BlockerCount int       `json:"blocker_count"`
}

type OperatorUnifiedStatus struct {
	Schema                    int                      `json:"schema"`
	CheckedAt                 time.Time                `json:"checked_at"`
	State                     string                   `json:"state"`
	EvidenceSHA256            string                   `json:"evidence_sha256"`
	RestartDisposition        string                   `json:"restart_disposition"`
	MonitorState              string                   `json:"monitor_state"`
	MonitorLockPresent        bool                     `json:"monitor_lock_present"`
	EventLockPresent          bool                     `json:"event_lock_present"`
	RecoveryProbeLockPresent  bool                     `json:"recovery_probe_lock_present"`
	ReconnectWatchLockPresent bool                     `json:"reconnect_watch_lock_present"`
	GapMarkerCount            int                      `json:"gap_marker_count"`
	StreamContinuityProven    bool                     `json:"stream_continuity_proven"`
	TradingAuthorized         bool                     `json:"trading_authorized"`
	ManualReviewRequired      bool                     `json:"manual_review_required"`
	Recovery                  OperatorRecoveryStatus   `json:"recovery"`
	Reconnect                 OperatorReconnectStatus  `json:"reconnect"`
	Integrated                OperatorIntegratedStatus `json:"integrated"`
	Accounting                OperatorAccountingStatus `json:"accounting"`
	Protection                OperatorProtectionStatus `json:"protection"`
	Blockers                  []string                 `json:"blockers"`
}

var operatorSettlementFile = regexp.MustCompile(`^([0-9a-f]{32})\.(fills|reconciled|settled)\.json$`)
var operatorProtectionFillFile = regexp.MustCompile(`^BTCUSDT_[1-9][0-9]*\.json$`)

func operatorSnapshotDir(root string, validate func(string, string, bool) bool) (string, error) {
	digest, _, _, err := snapshotDir(root, validate)
	return digest, err
}

func operatorEvidenceSHA256(dataDir string) (string, error) {
	type part struct{ name, digest string }
	parts := []part{}
	add := func(name, root string, validate func(string, string, bool) bool) error {
		d, err := operatorSnapshotDir(root, validate)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		parts = append(parts, part{name, d})
		return nil
	}
	flat := func(pattern *regexp.Regexp) func(string, string, bool) bool {
		return func(parent, name string, isDir bool) bool { return parent == "" && !isDir && pattern.MatchString(name) }
	}
	tradingRoot := filepath.Join(dataDir, "trading")
	if err := add("recovery_sessions", filepath.Join(tradingRoot, "recovery_sessions"), func(parent, name string, isDir bool) bool {
		return parent == "" && ((name == "exclusive.active" && isDir) || (!isDir && probeFile.MatchString(name))) || parent == "exclusive.active" && !isDir && name == "owner.json"
	}); err != nil {
		return "", err
	}
	if err := add("reconnect_watch_sessions", filepath.Join(tradingRoot, "reconnect_watch_sessions"), func(parent, name string, isDir bool) bool {
		return parent == "" && ((name == "exclusive.active" && isDir) || (!isDir && incidentWatchFile.MatchString(name))) || parent == "exclusive.active" && !isDir && name == "owner.json"
	}); err != nil {
		return "", err
	}
	if err := add("continuous_monitor", filepath.Join(tradingRoot, "continuous_monitor"), func(parent, name string, isDir bool) bool {
		switch {
		case parent == "" && name == "exclusive.active":
			return isDir
		case parent == "" && monitorSessionName.MatchString(name):
			return isDir
		case parent == "exclusive.active":
			return !isDir && name == "owner.json"
		case monitorSessionName.MatchString(parent):
			return !isDir && monitorCheckpointName.MatchString(name)
		default:
			return false
		}
	}); err != nil {
		return "", err
	}
	if err := add("user_events", filepath.Join(tradingRoot, "user_events"), func(parent, name string, isDir bool) bool {
		return parent == "" && ((name == "exclusive.active" && isDir) || (!isDir && eventName.MatchString(name)))
	}); err != nil {
		return "", err
	}
	if err := add("stream_discontinuities", filepath.Join(tradingRoot, "stream_discontinuities"), flat(gapMarkerName)); err != nil {
		return "", err
	}
	if err := add("recovery_reviews", filepath.Join(tradingRoot, "recovery_reviews"), flat(recoveryName)); err != nil {
		return "", err
	}
	if err := add("integrated_reviews", filepath.Join(tradingRoot, "integrated_reviews"), flat(integratedReportName)); err != nil {
		return "", err
	}
	if err := add("accounting_reports", filepath.Join(tradingRoot, "accounting_reports"), flat(accountingArchivePattern)); err != nil {
		return "", err
	}
	if err := add("cold_archives", filepath.Join(tradingRoot, "cold_archives"), flat(coldArchiveName)); err != nil {
		return "", err
	}
	if err := add("protection_submissions", filepath.Join(tradingRoot, "protection_submissions"), validProtectionSubmissionArchiveEntry); err != nil {
		return "", err
	}
	if err := add("protection_fills", filepath.Join(tradingRoot, "protection_fills"), flat(operatorProtectionFillFile)); err != nil {
		return "", err
	}
	if err := add("protection_settlements", filepath.Join(tradingRoot, "protection_settlements"), flat(operatorSettlementFile)); err != nil {
		return "", err
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].name < parts[j].name })
	h := sha256.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s:%s;", len(p.name), p.name, p.digest)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func summarizeProtectionForOperator(dataDir string, accounting []AccountingReport) (OperatorProtectionStatus, []string, error) {
	out := OperatorProtectionStatus{}
	blockers := []string{}
	dir := filepath.Join(dataDir, "trading", "protection_submissions")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		out.ArmingState = ProtectionArmingEmpty
	} else {
		if err != nil {
			return out, nil, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return out, nil, errors.New("protection submission root invalid")
		}
		store := &ProtectionSubmissionStore{dir: dir}
		plans, err := store.scanPlansLocked()
		if err != nil {
			return out, nil, err
		}
		out.PlanCount = len(plans)
		activePlans, settledGroups, err := activeProtectionPlansLocked(store, plans)
		if err != nil {
			return out, nil, err
		}
		out.SettledGroupCount = len(settledGroups)
		out.UnsettledGroupCount = len(activePlans)
		out.ArmingState = ReviewProtectionArmingPlans(activePlans).State
		ss := &ProtectionSettlementStore{root: filepath.Join(dataDir, "trading"), fillDir: filepath.Join(dataDir, "trading", "protection_fills"), stageDir: filepath.Join(dataDir, "trading", "protection_settlements")}
		known := make(map[string]bool, len(plans))
		for _, plan := range plans {
			known[plan.GroupID] = true
			if settledGroups[plan.GroupID] {
				continue
			}
			stage, _, err := ss.currentStageLocked(plan.GroupID, plan)
			if err != nil {
				return out, nil, err
			}
			blockers = append(blockers, "미완료 protection settlement: "+plan.GroupID+" ("+string(stage)+")")
		}
		entries, err := os.ReadDir(ss.stageDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
					return out, nil, errors.New("invalid protection settlement entry")
				}
				m := operatorSettlementFile.FindStringSubmatch(e.Name())
				if m == nil || !known[m[1]] {
					return out, nil, errors.New("orphan/unknown protection settlement evidence")
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return out, nil, err
		}
	}
	regular := []StoredFill{}
	fills, err := readValidatedProtectionFillsForStore(filepath.Join(dataDir, "trading"), regular)
	if err != nil {
		return out, nil, err
	}
	out.ProtectionFillCount = len(fills)
	if len(accounting) > 0 {
		out.DualTriggeredHistory = accounting[len(accounting)-1].DualTriggeredGroupCount
	}
	sort.Strings(blockers)
	return out, blockers, nil
}

// AnalyzeOperatorUnifiedStatus reads only local evidence. The before/after
// combined digest prevents a multi-directory status from mixing two different
// points in time. Any corruption or concurrent change fails closed.
func AnalyzeOperatorUnifiedStatus(dataDir string, now time.Time) (OperatorUnifiedStatus, error) {
	out := OperatorUnifiedStatus{Schema: 1, CheckedAt: now.UTC(), ManualReviewRequired: true, Blockers: []string{}, State: OperatorStateEmpty}
	if strings.TrimSpace(dataDir) == "" || now.IsZero() {
		return out, errors.New("operator status requires data directory and time")
	}
	before, err := operatorEvidenceSHA256(dataDir)
	if err != nil {
		return out, err
	}
	forensic, err := AnalyzeRestartForensics(dataDir, now)
	if err != nil {
		return out, err
	}
	probes, err := LoadRecoveryProbes(dataDir)
	if err != nil {
		return out, err
	}
	reconnects, err := LoadReconnectWatches(dataDir)
	if err != nil {
		return out, err
	}
	reviews, err := LoadRecoveryReviews(dataDir)
	if err != nil {
		return out, err
	}
	integrated, err := LoadIntegratedReviews(dataDir)
	if err != nil {
		return out, err
	}
	accounting, err := LoadAccountingReports(dataDir)
	if err != nil {
		return out, err
	}
	protection, protectionBlockers, err := summarizeProtectionForOperator(dataDir, accounting)
	if err != nil {
		return out, err
	}
	after, err := operatorEvidenceSHA256(dataDir)
	if err != nil {
		return out, err
	}
	if before != after {
		return out, errors.New("operator evidence changed during unified inspection")
	}

	out.EvidenceSHA256 = after
	out.RestartDisposition = forensic.RestartDisposition
	out.MonitorState = forensic.MonitorState
	out.MonitorLockPresent = forensic.MonitorLockPresent
	out.EventLockPresent = forensic.EventLockPresent
	out.GapMarkerCount = forensic.GapMarkerCount
	out.RecoveryProbeLockPresent, err = lockPresent(filepath.Join(dataDir, "trading", "recovery_sessions", "exclusive.active"))
	if err != nil {
		return out, err
	}
	out.ReconnectWatchLockPresent, err = lockPresent(filepath.Join(dataDir, "trading", "reconnect_watch_sessions", "exclusive.active"))
	if err != nil {
		return out, err
	}
	out.Recovery.ProbeCount = len(probes)
	out.Recovery.ReviewCount = len(reviews)
	for _, p := range probes {
		if p.Stage == "INTERRUPTED" {
			out.Recovery.InterruptedCount++
		}
	}
	if len(probes) > 0 {
		out.Recovery.LatestProbeStage = probes[len(probes)-1].Stage
	}
	out.Reconnect.WatchCount = len(reconnects)
	for _, w := range reconnects {
		if w.Stage == "INTERRUPTED" {
			out.Reconnect.InterruptedCount++
		}
	}
	if len(reconnects) > 0 {
		out.Reconnect.LatestWatchStage = reconnects[len(reconnects)-1].Stage
	}
	out.Integrated.ReportCount = len(integrated)
	if len(integrated) > 0 {
		latest := integrated[len(integrated)-1]
		out.Integrated.LatestState = latest.State
		out.Integrated.LatestAt = latest.CheckedAt
		out.Integrated.BlockerCount = len(latest.Blockers)
	}
	out.Accounting.ReportCount = len(accounting)
	if len(accounting) > 0 {
		latest := accounting[len(accounting)-1]
		out.Accounting.LatestCreatedAt = latest.CreatedAt
		out.Accounting.LatestKnownOrders = latest.KnownOrders
		out.Accounting.LatestKnownFills = latest.KnownFills
		out.Accounting.LatestProtectionFills = latest.ProtectionFillCount
		out.Accounting.LatestProtectionGroups = latest.ProtectionGroupCount
		out.Accounting.LatestDualTriggeredGroups = latest.DualTriggeredGroupCount
	}
	out.Protection = protection

	add := func(v string) {
		if strings.TrimSpace(v) != "" {
			out.Blockers = append(out.Blockers, v)
		}
	}
	if forensic.InterruptedOrActive {
		add("restart/monitor evidence is active or interrupted")
	}
	if out.RecoveryProbeLockPresent {
		add("recovery probe durable lock is present: running or crash-interrupted ownership requires manual review")
	}
	if out.ReconnectWatchLockPresent {
		add("reconnect watch durable lock is present: running or crash-interrupted ownership requires manual review")
	}
	if forensic.GapMarkerCount > 0 {
		add("historical stream gap markers remain")
	}
	if out.Recovery.InterruptedCount > 0 {
		add("interrupted recovery probe evidence remains")
	}
	if out.Reconnect.InterruptedCount > 0 {
		add("interrupted reconnect watch evidence remains")
	}
	if len(integrated) > 0 && integrated[len(integrated)-1].State == "BLOCKED" {
		add("latest integrated review is BLOCKED")
	}
	for _, b := range protectionBlockers {
		add(b)
	}
	if protection.ArmingState == ProtectionArmingPooledConflict {
		add("pooled-side protection arming conflict")
	}
	// Historical reports are useful evidence but never continuity/readiness.
	hasHistory := len(probes) > 0 || len(reconnects) > 0 || len(reviews) > 0 || len(integrated) > 0 || len(accounting) > 0 || protection.PlanCount > 0 || forensic.MonitorSessionCount > 0 || forensic.EventCount > 0 || forensic.GapMarkerCount > 0
	if len(out.Blockers) > 0 {
		out.State = OperatorStateBlocked
	} else if hasHistory {
		out.State = OperatorStateHistoricalOnly
	}
	sort.Strings(out.Blockers)
	return out, nil
}
