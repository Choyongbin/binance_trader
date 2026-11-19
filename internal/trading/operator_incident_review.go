package trading

// Two-phase, OFFLINE-COPY ONLY operator acknowledgement of interrupted account
// monitoring evidence. A receipt records that a human reviewed a particular
// immutable local snapshot; it is NOT a dead-process proof, an exchange audit,
// a stale-lock removal instruction, or a trading/rearm authorization.
//
// This code deliberately has no os.Remove on the inspected data tree and no
// entry point from live HTTP or the order execution coordinator.
import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

const incidentPlanTTL = 30 * time.Minute

var incidentID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var incidentWatchFile = regexp.MustCompile(`^[a-f0-9]{32}\.(start|end)\.json$`)

// An operator MUST inspect the full offline inventory separately before
// acknowledging a bounded plan. These categories intentionally avoid saying
// that any on-disk lock is stale: no historical PID/mtime proves that.
type IncidentReviewPlan struct {
	Schema                       int       `json:"schema"`
	ID                           string    `json:"id"`
	PreparedAt                   time.Time `json:"prepared_at"`
	ExpiresAt                    time.Time `json:"expires_at"`
	EvidenceSHA256               string    `json:"evidence_sha256"`
	ForensicDisposition          string    `json:"forensic_disposition"`
	ReviewCategory               string    `json:"review_category"`
	MonitorLockPresent           bool      `json:"monitor_lock_present"`
	EventLockPresent             bool      `json:"event_lock_present"`
	MutationGatePresent          bool      `json:"mutation_gate_present"`
	RecoveryProbeLockPresent     bool      `json:"recovery_probe_lock_present"`
	ReconnectWatchLockPresent    bool      `json:"reconnect_watch_lock_present"`
	MonitorSessions              int       `json:"monitor_sessions"`
	HistoricalGapMarkers         int       `json:"historical_gap_markers"`
	IncompleteHistoricalSessions int       `json:"incomplete_historical_sessions"`
	UnmatchedHistoricalMarkers   int       `json:"unmatched_historical_markers"`
	Confirmation                 string    `json:"confirmation"` // unpredictable operator-only acknowledgement challenge
	RequiredFollowup             []string  `json:"required_followup"`
	TradingAuthorized            bool      `json:"trading_authorized"`
	AutoRearmAllowed             bool      `json:"auto_rearm_allowed"`
	StreamContinuityProven       bool      `json:"stream_continuity_proven"`
	ManualReviewRequired         bool      `json:"manual_review_required"`
	SHA256                       string    `json:"sha256"`
}

func (p IncidentReviewPlan) digest() string {
	p.SHA256 = ""
	raw, _ := json.Marshal(p)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

// A receipt is explicitly limited to the operator's statement that an
// inventory was reviewed on an offline copy, NEVER approval of recovery.
type IncidentReviewReceipt struct {
	Schema                     int       `json:"schema"`
	PlanID                     string    `json:"plan_id"`
	PlanSHA256                 string    `json:"plan_sha256"`
	EvidenceSHA256             string    `json:"evidence_sha256"`
	AcknowledgedAt             time.Time `json:"acknowledged_at"`
	StoppedWritersSelfAttested bool      `json:"stopped_writers_self_attested"`
	ExternalBackupSelfAttested bool      `json:"external_backup_self_attested"`
	Scope                      string    `json:"scope"`
	RemainingAction            string    `json:"remaining_action"`
	TradingAuthorized          bool      `json:"trading_authorized"`
	AutoRearmAllowed           bool      `json:"auto_rearm_allowed"`
	StreamContinuityProven     bool      `json:"stream_continuity_proven"`
	ManualReviewRequired       bool      `json:"manual_review_required"`
	SHA256                     string    `json:"sha256"`
}

func (r IncidentReviewReceipt) digest() string {
	r.SHA256 = ""
	raw, _ := json.Marshal(r)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

// The evidence seal covers archive *contents* in addition to counts. A newer
// event, changed lock owner, or modified historical diagnostic invalidates an
// old plan, even if a report's short summary would look identical.
type incidentGateState struct {
	MutationGate   bool
	RecoveryProbe  bool
	ReconnectWatch bool
}

func incidentEvidenceSHA256(dataDir string, now time.Time) (RestartForensics, incidentGateState, string, error) {
	report, err := AnalyzeRestartForensics(dataDir, now)
	if err != nil || !report.EvidenceStable {
		return RestartForensics{}, incidentGateState{}, "", errors.New("offline forensic inspection failed")
	}
	gate, err := MutationGatePresent(dataDir)
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", errors.New("mutation gate unreadable")
	}
	gateDigest, _, _, err := snapshotDir(filepath.Join(dataDir, "trading", "mutation_gate.active"), func(parent, name string, dir bool) bool {
		return parent == "" && !dir && name == "owner.json"
	})
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", errors.New("mutation gate evidence invalid")
	}
	// Preserve historical START/END files too, not only the marker summary.
	probeDigest, _, _, err := snapshotDir(filepath.Join(dataDir, "trading", "recovery_sessions"), func(parent, name string, dir bool) bool {
		return parent == "" && (name == "exclusive.active" && dir || !dir && probeFile.MatchString(name)) || parent == "exclusive.active" && !dir && name == "owner.json"
	})
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", errors.New("recovery archive evidence invalid")
	}
	watchDigest, _, _, err := snapshotDir(filepath.Join(dataDir, "trading", "reconnect_watch_sessions"), func(parent, name string, dir bool) bool {
		return parent == "" && (name == "exclusive.active" && dir || !dir && incidentWatchFile.MatchString(name)) || parent == "exclusive.active" && !dir && name == "owner.json"
	})
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", errors.New("reconnect archive evidence invalid")
	}
	probeLock, err := lockPresent(filepath.Join(dataDir, "trading", "recovery_sessions", "exclusive.active"))
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", err
	}
	watchLock, err := lockPresent(filepath.Join(dataDir, "trading", "reconnect_watch_sessions", "exclusive.active"))
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", err
	}
	// No creation or deletion anywhere in the inspected tree.
	// CheckedAt changes between prepare/confirm and is not evidence.
	report.CheckedAt = time.Time{}
	report.Note = ""
	body, _ := json.Marshal(struct {
		Report           RestartForensics `json:"report"`
		MutationGate     bool             `json:"mutation_gate"`
		MutationGateTree string           `json:"mutation_gate_tree"`
		RecoveryTree     string           `json:"recovery_tree"`
		ReconnectTree    string           `json:"reconnect_tree"`
		RecoveryLock     bool             `json:"recovery_lock"`
		WatchLock        bool             `json:"watch_lock"`
	}{report, gate, gateDigest, probeDigest, watchDigest, probeLock, watchLock})
	hash := sha256.Sum256(body)
	// Detect changes while reviewing the supplemental archive trees.
	second, err := AnalyzeRestartForensics(dataDir, time.Now().UTC())
	if err != nil {
		return RestartForensics{}, incidentGateState{}, "", errors.New("evidence changed during audit")
	}
	second.CheckedAt, second.Note = time.Time{}, ""
	beforeJSON, _ := json.Marshal(report)
	afterJSON, _ := json.Marshal(second)
	if string(beforeJSON) != string(afterJSON) {
		return RestartForensics{}, incidentGateState{}, "", errors.New("forensic state changed during audit")
	}
	gate2, err := MutationGatePresent(dataDir)
	if err != nil || gate2 != gate {
		return RestartForensics{}, incidentGateState{}, "", errors.New("mutation gate changed during audit")
	}
	repeat := func(root string, validate func(string, string, bool) bool, previous string) error {
		after, _, _, e := snapshotDir(root, validate)
		if e != nil || after != previous {
			return errors.New("supplemental evidence changed during audit")
		}
		return nil
	}
	if err = repeat(filepath.Join(dataDir, "trading", "mutation_gate.active"), func(parent, name string, dir bool) bool { return parent == "" && !dir && name == "owner.json" }, gateDigest); err != nil {
		return RestartForensics{}, incidentGateState{}, "", err
	}
	if err = repeat(filepath.Join(dataDir, "trading", "recovery_sessions"), func(parent, name string, dir bool) bool {
		return parent == "" && (name == "exclusive.active" && dir || !dir && probeFile.MatchString(name)) || parent == "exclusive.active" && !dir && name == "owner.json"
	}, probeDigest); err != nil {
		return RestartForensics{}, incidentGateState{}, "", err
	}
	if err = repeat(filepath.Join(dataDir, "trading", "reconnect_watch_sessions"), func(parent, name string, dir bool) bool {
		return parent == "" && (name == "exclusive.active" && dir || !dir && incidentWatchFile.MatchString(name)) || parent == "exclusive.active" && !dir && name == "owner.json"
	}, watchDigest); err != nil {
		return RestartForensics{}, incidentGateState{}, "", err
	}
	probeLock2, pErr := lockPresent(filepath.Join(dataDir, "trading", "recovery_sessions", "exclusive.active"))
	watchLock2, wErr := lockPresent(filepath.Join(dataDir, "trading", "reconnect_watch_sessions", "exclusive.active"))
	if pErr != nil || wErr != nil || probeLock != probeLock2 || watchLock != watchLock2 {
		return RestartForensics{}, incidentGateState{}, "", errors.New("diagnostic lock changed during audit")
	}
	return report, incidentGateState{MutationGate: gate, RecoveryProbe: probeLock, ReconnectWatch: watchLock}, hex.EncodeToString(hash[:]), nil
}

// Review files must be stored in a PREEXISTING, distinct external directory.
// Requiring it to preexist prevents inadvertently creating review artifacts
// under data through a symlink or a misspelled destination.
func validateIncidentDirectories(dataDir, reviewDir string) error {
	if dataDir == "" || reviewDir == "" {
		return errors.New("explicit offline data copy and external review directory required")
	}
	a, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return errors.New("offline data copy unavailable")
	}
	b, err := filepath.EvalSymlinks(reviewDir)
	if err != nil {
		return errors.New("external review directory must already exist")
	}
	a, _ = filepath.Abs(a)
	b, _ = filepath.Abs(b)
	ai, e1 := os.Stat(a)
	bi, e2 := os.Stat(b)
	if e1 != nil || e2 != nil || !ai.IsDir() || !bi.IsDir() {
		return errors.New("invalid incident review directories")
	}
	// Never allow either tree to be inside the other, on any supported OS.
	if runtime.GOOS == "windows" {
		a = strings.ToLower(a)
		b = strings.ToLower(b)
	}
	inside := func(x, y string) bool {
		rel, e := filepath.Rel(x, y)
		return e == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
	}
	if inside(a, b) || inside(b, a) {
		return errors.New("review records must be outside inspected data")
	}
	return nil
}

func strictIncidentJSON(path string, dest any) error {
	meta, err := os.Lstat(path)
	if err != nil || !meta.Mode().IsRegular() || meta.Mode()&os.ModeSymlink != 0 || meta.Size() > 65536 {
		return errors.New("review file unavailable or unsafe")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 65537))
	d.DisallowUnknownFields()
	if d.Decode(dest) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("review record malformed")
	}
	return nil
}
func checkedIncidentPlan(p IncidentReviewPlan) error {
	if p.Schema != 1 || !incidentID.MatchString(p.ID) || p.PreparedAt.IsZero() || p.ExpiresAt.Sub(p.PreparedAt) != incidentPlanTTL || p.EvidenceSHA256 == "" || !eventDigest.MatchString(p.EvidenceSHA256) || p.Confirmation != "ACK-EVIDENCE-"+p.ID[:12] || p.RequiredFollowup == nil || p.TradingAuthorized || p.AutoRearmAllowed || p.StreamContinuityProven || !p.ManualReviewRequired || !eventDigest.MatchString(p.SHA256) || p.SHA256 != p.digest() {
		return errors.New("invalid operator review plan")
	}
	switch p.ReviewCategory {
	case "NO_MONITOR_HISTORY", "HISTORICAL_ONLY", "OWNER_OR_HISTORY_UNVERIFIED":
	default:
		return errors.New("invalid review category")
	}
	switch p.ForensicDisposition {
	case "NO_MONITOR_HISTORY", "HISTORICAL_ONLY_REARM_REQUIRES_PASSPHRASE", "OFFLINE_MANUAL_REVIEW_REQUIRED":
	default:
		return errors.New("invalid forensic disposition")
	}
	return nil
}
func checkedIncidentReceipt(r IncidentReviewReceipt) error {
	if r.Schema != 1 || !incidentID.MatchString(r.PlanID) || !eventDigest.MatchString(r.PlanSHA256) || !eventDigest.MatchString(r.EvidenceSHA256) || r.AcknowledgedAt.IsZero() || !r.StoppedWritersSelfAttested || !r.ExternalBackupSelfAttested || r.Scope != "OFFLINE_EVIDENCE_ACKNOWLEDGEMENT_ONLY" || r.RemainingAction != "EXTERNAL_SIGNED_AUDIT_AND_INDEPENDENT_OWNER_VERIFICATION_REQUIRED" || r.TradingAuthorized || r.AutoRearmAllowed || r.StreamContinuityProven || !r.ManualReviewRequired || !eventDigest.MatchString(r.SHA256) || r.SHA256 != r.digest() {
		return errors.New("invalid operator receipt")
	}
	return nil
}

// ReadIncidentReviews rejects unknown files, orphan receipts, duplicate IDs,
// torn writes and edited checksums. There is deliberately no pruning API.
func ReadIncidentReviews(reviewDir string) ([]IncidentReviewPlan, []IncidentReviewReceipt, error) {
	entries, err := os.ReadDir(reviewDir)
	if err != nil {
		return nil, nil, err
	}
	for _, f := range entries {
		if f.Name() == "exclusive.active" {
			return nil, nil, errors.New("active or stale review lock")
		}
	}
	return readIncidentRecords(entries, reviewDir)
}

func lockedIncidentReviews(reviewDir string, fn func() error) error {
	lock := filepath.Join(reviewDir, "exclusive.active")
	if err := os.Mkdir(lock, 0700); err != nil {
		return errors.New("review already active or stale: manual inspection required")
	}
	// No data-tree lock is acquired or removed here.
	err := fn()
	if e := os.Remove(lock); e != nil && err == nil {
		err = errors.New("review lock cleanup uncertain")
	}
	return err
}
func writeIncidentFile(path string, payload any) error {
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(raw, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// PrepareIncidentReview operates on an operator-provided STOPPED-SERVER COPY;
// that assertion cannot be established by a filesystem hash alone.
func PrepareIncidentReview(dataDir, externalReviewDir string, now time.Time) (IncidentReviewPlan, error) {
	p := IncidentReviewPlan{}
	if now.IsZero() || validateIncidentDirectories(dataDir, externalReviewDir) != nil {
		return p, errors.New("offline data copy and preexisting external review directory required")
	}
	err := lockedIncidentReviews(externalReviewDir, func() error {
		if _, _, e := readIncidentReviewsUnlocked(externalReviewDir); e != nil {
			return e
		}
		report, gate, seal, e := incidentEvidenceSHA256(dataDir, now)
		if e != nil {
			return e
		}
		var nonce [16]byte
		if _, e = rand.Read(nonce[:]); e != nil {
			return e
		}
		id := hex.EncodeToString(nonce[:])
		category := "HISTORICAL_ONLY"
		if report.RestartDisposition == "NO_MONITOR_HISTORY" {
			category = "NO_MONITOR_HISTORY"
		}
		if report.InterruptedOrActive || gate.MutationGate || gate.RecoveryProbe || gate.ReconnectWatch {
			category = "OWNER_OR_HISTORY_UNVERIFIED"
		}
		p = IncidentReviewPlan{Schema: 1, ID: id, PreparedAt: now.UTC(), ExpiresAt: now.UTC().Add(incidentPlanTTL), EvidenceSHA256: seal,
			ForensicDisposition: report.RestartDisposition, ReviewCategory: category, MonitorLockPresent: report.MonitorLockPresent,
			EventLockPresent: report.EventLockPresent, MutationGatePresent: gate.MutationGate, RecoveryProbeLockPresent: gate.RecoveryProbe, ReconnectWatchLockPresent: gate.ReconnectWatch, MonitorSessions: report.MonitorSessionCount,
			HistoricalGapMarkers: report.GapMarkerCount, IncompleteHistoricalSessions: report.IncompleteHistoricalSessions,
			UnmatchedHistoricalMarkers: report.TimestampUnmatchedGaps, Confirmation: "ACK-EVIDENCE-" + id[:12], ManualReviewRequired: true,
			RequiredFollowup: []string{"다른 프로세스가 원본 데이터를 사용하는지 운영자가 별도 확인", "외부 전체 백업과 독립 서명 REST 계정/체결 감사", "기존 마커·락 삭제 금지: 이 확인서는 복구 승인이 아님"}}
		p.SHA256 = p.digest()
		return writeIncidentFile(filepath.Join(externalReviewDir, "plan-"+id+".json"), p)
	})
	if err != nil {
		return IncidentReviewPlan{}, err
	}
	return p, nil
}

// readIncidentReviewsUnlocked is ONLY used while holding the review-dir mutex.
func readIncidentReviewsUnlocked(dir string) ([]IncidentReviewPlan, []IncidentReviewReceipt, error) {
	// Verify the directory's contents with a temporary sentinel exception;
	// the exclusive.active directory itself is exclusively ours.
	entries, e := os.ReadDir(dir)
	if e != nil {
		return nil, nil, e
	}
	for _, f := range entries {
		if f.Name() == "exclusive.active" {
			continue
		}
		if !f.Type().IsRegular() {
			return nil, nil, errors.New("unknown review entry")
		}
	}
	// Reuse the strict loader by validating only peer regular files inside the
	// caller-owned directory without creating another filesystem object.
	return readIncidentRecords(entries, dir)
}

func readIncidentRecords(entries []os.DirEntry, root string) ([]IncidentReviewPlan, []IncidentReviewReceipt, error) {
	plans := make(map[string]IncidentReviewPlan)
	receipts := make(map[string]IncidentReviewReceipt)
	for _, f := range entries {
		if f.Name() == "exclusive.active" {
			continue
		}
		if !f.Type().IsRegular() {
			return nil, nil, errors.New("invalid review file type")
		}
		n := f.Name()
		path := filepath.Join(root, n)
		if strings.HasPrefix(n, "plan-") && strings.HasSuffix(n, ".json") {
			id := strings.TrimSuffix(strings.TrimPrefix(n, "plan-"), ".json")
			var p IncidentReviewPlan
			if !incidentID.MatchString(id) || strictIncidentJSON(path, &p) != nil || checkedIncidentPlan(p) != nil || p.ID != id {
				return nil, nil, errors.New("invalid review plan")
			}
			plans[id] = p
		} else if strings.HasPrefix(n, "receipt-") && strings.HasSuffix(n, ".json") {
			id := strings.TrimSuffix(strings.TrimPrefix(n, "receipt-"), ".json")
			var r IncidentReviewReceipt
			if !incidentID.MatchString(id) || strictIncidentJSON(path, &r) != nil || checkedIncidentReceipt(r) != nil || r.PlanID != id {
				return nil, nil, errors.New("invalid review receipt")
			}
			receipts[id] = r
		} else {
			return nil, nil, errors.New("unknown incident review archive entry")
		}
	}
	ids := make([]string, 0, len(plans))
	for id := range plans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ps := make([]IncidentReviewPlan, 0, len(plans))
	rs := make([]IncidentReviewReceipt, 0, len(receipts))
	for _, id := range ids {
		p := plans[id]
		ps = append(ps, p)
		if r, ok := receipts[id]; ok {
			if r.PlanSHA256 != p.SHA256 || r.EvidenceSHA256 != p.EvidenceSHA256 || r.AcknowledgedAt.Before(p.PreparedAt) || r.AcknowledgedAt.After(p.ExpiresAt) {
				return nil, nil, errors.New("receipt mismatch")
			}
			rs = append(rs, r)
		}
	}
	if len(rs) != len(receipts) {
		return nil, nil, errors.New("orphan review receipt")
	}
	return ps, rs, nil
}

// AcknowledgeIncidentReview is a self-attested inventory review only. It
// NEVER modifies offline source records or the real installation, even if the
// owner is known to be dead and the caller confirmed an external backup.
func AcknowledgeIncidentReview(dataDir, externalReviewDir, planID, confirmation string, stoppedWriters, externalBackup bool, now time.Time) (IncidentReviewReceipt, error) {
	var receipt IncidentReviewReceipt
	if now.IsZero() || !incidentID.MatchString(planID) || !stoppedWriters || !externalBackup || validateIncidentDirectories(dataDir, externalReviewDir) != nil {
		return receipt, errors.New("offline review prerequisites missing")
	}
	err := lockedIncidentReviews(externalReviewDir, func() error {
		plans, receipts, e := readIncidentReviewsUnlocked(externalReviewDir)
		if e != nil {
			return e
		}
		for _, r := range receipts {
			if r.PlanID == planID {
				return errors.New("incident plan already acknowledged")
			}
		}
		var p IncidentReviewPlan
		found := false
		for _, candidate := range plans {
			if candidate.ID == planID {
				p = candidate
				found = true
				break
			}
		}
		if !found || confirmation != p.Confirmation || now.Before(p.PreparedAt) || now.After(p.ExpiresAt) {
			return errors.New("unknown, expired or unconfirmed plan")
		}
		report, gate, seal, e := incidentEvidenceSHA256(dataDir, now)
		if e != nil {
			return e
		}
		category := "HISTORICAL_ONLY"
		if report.RestartDisposition == "NO_MONITOR_HISTORY" {
			category = "NO_MONITOR_HISTORY"
		}
		if report.InterruptedOrActive || gate.MutationGate || gate.RecoveryProbe || gate.ReconnectWatch {
			category = "OWNER_OR_HISTORY_UNVERIFIED"
		}
		if seal != p.EvidenceSHA256 || p.ForensicDisposition != report.RestartDisposition || p.ReviewCategory != category || p.MonitorLockPresent != report.MonitorLockPresent || p.EventLockPresent != report.EventLockPresent || p.MutationGatePresent != gate.MutationGate || p.RecoveryProbeLockPresent != gate.RecoveryProbe || p.ReconnectWatchLockPresent != gate.ReconnectWatch || p.MonitorSessions != report.MonitorSessionCount || p.HistoricalGapMarkers != report.GapMarkerCount || p.IncompleteHistoricalSessions != report.IncompleteHistoricalSessions || p.UnmatchedHistoricalMarkers != report.TimestampUnmatchedGaps {
			return errors.New("offline evidence changed; prepare new review plan")
		}
		receipt = IncidentReviewReceipt{Schema: 1, PlanID: p.ID, PlanSHA256: p.SHA256, EvidenceSHA256: seal, AcknowledgedAt: now.UTC(), StoppedWritersSelfAttested: true, ExternalBackupSelfAttested: true, Scope: "OFFLINE_EVIDENCE_ACKNOWLEDGEMENT_ONLY", RemainingAction: "EXTERNAL_SIGNED_AUDIT_AND_INDEPENDENT_OWNER_VERIFICATION_REQUIRED", ManualReviewRequired: true}
		receipt.SHA256 = receipt.digest()
		return writeIncidentFile(filepath.Join(externalReviewDir, "receipt-"+p.ID+".json"), receipt)
	})
	if err != nil {
		return IncidentReviewReceipt{}, err
	}
	return receipt, nil
}

// Prevent a future refactor from accidentally turning this into a destructive
// API. The only unlink in this file is our OWN external review-dir mutex.
