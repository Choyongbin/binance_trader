package trading

// Read-only forensic packets for CRITICAL operator remediation steps. Packets
// contain only local evidence identifiers, states, timestamps and digests; they
// never copy credentials, signatures or raw private account events and never
// mutate the inspected data tree.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const IncidentPacketScope = "READ_ONLY_LOCAL_EVIDENCE_SUMMARY_NOT_EXCHANGE_ATTESTATION"

type OperatorIncidentEvidenceRef struct {
	Kind   string    `json:"kind"`
	ID     string    `json:"id"`
	State  string    `json:"state"`
	At     time.Time `json:"at,omitempty"`
	SHA256 string    `json:"sha256"`
	Note   string    `json:"note,omitempty"`
}

type OperatorIncidentSnapshot struct {
	OperatorState             string `json:"operator_state"`
	RestartDisposition        string `json:"restart_disposition"`
	MonitorState              string `json:"monitor_state"`
	MonitorLockPresent        bool   `json:"monitor_lock_present"`
	EventLockPresent          bool   `json:"event_lock_present"`
	RecoveryProbeLockPresent  bool   `json:"recovery_probe_lock_present"`
	ReconnectWatchLockPresent bool   `json:"reconnect_watch_lock_present"`
	GapMarkerCount            int    `json:"gap_marker_count"`
	RecoveryInterrupted       int    `json:"recovery_interrupted_count"`
	ReconnectInterrupted      int    `json:"reconnect_interrupted_count"`
	ProtectionUnsettled       int    `json:"protection_unsettled_group_count"`
	ProtectionArmingState     string `json:"protection_arming_state"`
	IntegratedLatestState     string `json:"integrated_latest_state"`
	AccountingReportCount     int    `json:"accounting_report_count"`
}

type OperatorIncidentPacket struct {
	Schema               int                           `json:"schema"`
	PacketID             string                        `json:"packet_id"`
	GeneratedAt          time.Time                     `json:"generated_at"`
	Code                 string                        `json:"code"`
	Priority             string                        `json:"priority"`
	Title                string                        `json:"title"`
	Reason               string                        `json:"reason"`
	Scope                string                        `json:"scope"`
	EvidenceSHA256       string                        `json:"evidence_sha256"`
	Snapshot             OperatorIncidentSnapshot      `json:"snapshot"`
	EvidenceRefs         []OperatorIncidentEvidenceRef `json:"evidence_refs"`
	EvidenceToInspect    []string                      `json:"evidence_to_inspect"`
	OperatorAction       []string                      `json:"operator_action"`
	ForbiddenAutomation  []string                      `json:"forbidden_automation"`
	TradingAuthorized    bool                          `json:"trading_authorized"`
	AutomaticRepair      bool                          `json:"automatic_repair"`
	MutationPerformed    bool                          `json:"mutation_performed"`
	ManualReviewRequired bool                          `json:"manual_review_required"`
	SHA256               string                        `json:"sha256"`
}

func (p OperatorIncidentPacket) digest() string {
	p.SHA256 = ""
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type OperatorIncidentPacketSet struct {
	Schema               int                      `json:"schema"`
	GeneratedAt          time.Time                `json:"generated_at"`
	EvidenceSHA256       string                   `json:"evidence_sha256"`
	SourceOperatorState  string                   `json:"source_operator_state"`
	CriticalPacketCount  int                      `json:"critical_packet_count"`
	Packets              []OperatorIncidentPacket `json:"packets"`
	TradingAuthorized    bool                     `json:"trading_authorized"`
	AutomaticRepair      bool                     `json:"automatic_repair"`
	MutationPerformed    bool                     `json:"mutation_performed"`
	ManualReviewRequired bool                     `json:"manual_review_required"`
	SHA256               string                   `json:"sha256"`
}

func (p OperatorIncidentPacketSet) digest() string {
	p.SHA256 = ""
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func digestRestartForPacket(r RestartForensics) string {
	r.CheckedAt = time.Time{}
	r.Note = ""
	raw, _ := json.Marshal(r)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func packetID(evidence, code string, refs []OperatorIncidentEvidenceRef) string {
	type stableRef struct {
		Kind   string `json:"kind"`
		ID     string `json:"id"`
		State  string `json:"state"`
		SHA256 string `json:"sha256"`
	}
	stableRefs := make([]stableRef, 0, len(refs))
	for _, ref := range refs {
		stableRefs = append(stableRefs, stableRef{ref.Kind, ref.ID, ref.State, ref.SHA256})
	}
	stable := struct {
		Evidence string      `json:"evidence"`
		Code     string      `json:"code"`
		Refs     []stableRef `json:"refs"`
	}{evidence, code, stableRefs}
	raw, _ := json.Marshal(stable)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16])
}

func buildIncidentRefs(dataDir, code string, status OperatorUnifiedStatus, now time.Time) ([]OperatorIncidentEvidenceRef, error) {
	refs := []OperatorIncidentEvidenceRef{}
	switch code {
	case "PRIVATE_DIAGNOSTIC_LOCK":
		type lockSpec struct {
			present bool
			kind    string
			root    string
			valid   func(string, string, bool) bool
		}
		specs := []lockSpec{
			{status.RecoveryProbeLockPresent, "recovery_probe_lock", filepath.Join(dataDir, "trading", "recovery_sessions"), func(parent, name string, dir bool) bool {
				return parent == "" && (name == "exclusive.active" && dir || !dir && probeFile.MatchString(name)) || parent == "exclusive.active" && !dir && name == "owner.json"
			}},
			{status.ReconnectWatchLockPresent, "reconnect_watch_lock", filepath.Join(dataDir, "trading", "reconnect_watch_sessions"), func(parent, name string, dir bool) bool {
				return parent == "" && (name == "exclusive.active" && dir || !dir && incidentWatchFile.MatchString(name)) || parent == "exclusive.active" && !dir && name == "owner.json"
			}},
		}
		for _, spec := range specs {
			if !spec.present {
				continue
			}
			digest, _, _, err := snapshotDir(spec.root, spec.valid)
			if err != nil {
				return nil, err
			}
			refs = append(refs, OperatorIncidentEvidenceRef{Kind: spec.kind, ID: "exclusive.active", State: "RUNNING_OR_CRASH_INTERRUPTED", SHA256: digest, Note: "durable lock ownership requires offline manual review"})
		}
	case "RESTART_MONITOR_EVIDENCE", "STREAM_GAP_RECONCILIATION":
		f, err := AnalyzeRestartForensics(dataDir, now)
		if err != nil || !f.EvidenceStable {
			return nil, errors.New("restart forensic evidence unavailable")
		}
		refs = append(refs, OperatorIncidentEvidenceRef{
			Kind: "restart_forensics", ID: "current-local-snapshot", State: f.RestartDisposition,
			At: now.UTC(), SHA256: digestRestartForPacket(f),
			Note: fmt.Sprintf("monitor_sessions=%d events=%d gaps=%d", f.MonitorSessionCount, f.EventCount, f.GapMarkerCount),
		})
		if code == "STREAM_GAP_RECONCILIATION" && f.GapMarkerCount > 0 {
			refs = append(refs, OperatorIncidentEvidenceRef{Kind: "stream_gap_archive", ID: "aggregate", State: "GAPS_PRESENT", SHA256: f.GapMarkerSHA256, Note: fmt.Sprintf("count=%d", f.GapMarkerCount)})
		}
	case "RECOVERY_PROBE_INTERRUPTED":
		probes, err := LoadRecoveryProbes(dataDir)
		if err != nil {
			return nil, err
		}
		for _, p := range probes {
			if p.Stage != "INTERRUPTED" && p.Stage != "STARTED" {
				continue
			}
			at := p.StartedAt
			if !p.FinishedAt.IsZero() {
				at = p.FinishedAt
			}
			digest := p.SHA256
			if p.Stage == "INTERRUPTED" {
				immutableStart := p
				immutableStart.Stage = "STARTED"
				immutableStart.Blockers = []string{}
				immutableStart.SHA256 = ""
				digest = immutableStart.digest()
			}
			refs = append(refs, OperatorIncidentEvidenceRef{Kind: "recovery_probe", ID: p.ID, State: p.Stage, At: at, SHA256: digest, Note: fmt.Sprintf("events=%d gaps=%d", p.EventCount, p.DiscontinuityCount)})
		}
	case "PROTECTION_SETTLEMENT_INCOMPLETE", "POOLED_PROTECTION_CONFLICT":
		dir := filepath.Join(dataDir, "trading", "protection_submissions")
		store := &ProtectionSubmissionStore{dir: dir}
		plans, err := store.scanPlansLocked()
		if err != nil {
			return nil, err
		}
		active, _, err := activeProtectionPlansLocked(store, plans)
		if err != nil {
			return nil, err
		}
		ss := &ProtectionSettlementStore{root: filepath.Join(dataDir, "trading"), fillDir: filepath.Join(dataDir, "trading", "protection_fills"), stageDir: filepath.Join(dataDir, "trading", "protection_settlements")}
		for _, plan := range active {
			stage, _, err := ss.currentStageLocked(plan.GroupID, plan)
			if err != nil {
				return nil, err
			}
			refs = append(refs, OperatorIncidentEvidenceRef{Kind: "protection_group", ID: plan.GroupID, State: string(stage), SHA256: plan.Digest, Note: string(plan.PositionSide) + "/" + string(plan.Source)})
		}
	default:
		return nil, errors.New("incident packet requested for non-critical or unknown remediation code")
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		return refs[i].ID < refs[j].ID
	})
	if len(refs) == 0 {
		return nil, errors.New("critical incident has no validated evidence references")
	}
	for _, ref := range refs {
		if strings.TrimSpace(ref.ID) == "" || strings.TrimSpace(ref.State) == "" || len(ref.SHA256) != 64 {
			return nil, errors.New("invalid incident evidence reference")
		}
		if _, err := hex.DecodeString(ref.SHA256); err != nil {
			return nil, errors.New("invalid incident evidence digest")
		}
	}
	return refs, nil
}

// BuildOperatorIncidentPackets creates sealed, in-memory forensic packets for
// every CRITICAL remediation step. It does not write a packet to the inspected
// data tree; callers may redirect the JSON output to an external location.
func BuildOperatorIncidentPackets(dataDir string, now time.Time) (OperatorIncidentPacketSet, error) {
	out := OperatorIncidentPacketSet{Schema: 1, GeneratedAt: now.UTC(), Packets: []OperatorIncidentPacket{}, TradingAuthorized: false, AutomaticRepair: false, MutationPerformed: false, ManualReviewRequired: true}
	if strings.TrimSpace(dataDir) == "" || now.IsZero() {
		return out, errors.New("incident packet requires data directory and time")
	}
	before, err := operatorEvidenceSHA256(dataDir)
	if err != nil {
		return out, err
	}
	status, err := AnalyzeOperatorUnifiedStatus(dataDir, now)
	if err != nil {
		return out, err
	}
	plan, err := AnalyzeOperatorRemediation(dataDir, now)
	if err != nil {
		return out, err
	}
	if status.EvidenceSHA256 != plan.EvidenceSHA256 || status.EvidenceSHA256 != before {
		return out, errors.New("incident packet source evidence mismatch")
	}
	snapshot := OperatorIncidentSnapshot{
		OperatorState: status.State, RestartDisposition: status.RestartDisposition, MonitorState: status.MonitorState,
		MonitorLockPresent: status.MonitorLockPresent, EventLockPresent: status.EventLockPresent,
		RecoveryProbeLockPresent: status.RecoveryProbeLockPresent, ReconnectWatchLockPresent: status.ReconnectWatchLockPresent, GapMarkerCount: status.GapMarkerCount,
		RecoveryInterrupted: status.Recovery.InterruptedCount, ReconnectInterrupted: status.Reconnect.InterruptedCount, ProtectionUnsettled: status.Protection.UnsettledGroupCount,
		ProtectionArmingState: status.Protection.ArmingState, IntegratedLatestState: status.Integrated.LatestState, AccountingReportCount: status.Accounting.ReportCount,
	}
	packets := make([]OperatorIncidentPacket, 0)
	for _, step := range plan.Steps {
		if step.Priority != RemediationCritical {
			continue
		}
		refs, err := buildIncidentRefs(dataDir, step.Code, status, now)
		if err != nil {
			return out, fmt.Errorf("%s: %w", step.Code, err)
		}
		p := OperatorIncidentPacket{
			Schema: 1, GeneratedAt: now.UTC(), Code: step.Code, Priority: step.Priority, Title: step.Title, Reason: step.Reason,
			Scope: IncidentPacketScope, EvidenceSHA256: status.EvidenceSHA256, Snapshot: snapshot, EvidenceRefs: refs,
			EvidenceToInspect: append([]string(nil), step.EvidenceToInspect...), OperatorAction: append([]string(nil), step.OperatorAction...),
			ForbiddenAutomation: append([]string(nil), step.ForbiddenAutomation...), TradingAuthorized: false, AutomaticRepair: false, MutationPerformed: false, ManualReviewRequired: true,
		}
		p.PacketID = packetID(p.EvidenceSHA256, p.Code, p.EvidenceRefs)
		p.SHA256 = p.digest()
		packets = append(packets, p)
	}
	sort.Slice(packets, func(i, j int) bool { return packets[i].Code < packets[j].Code })
	after, err := operatorEvidenceSHA256(dataDir)
	if err != nil {
		return out, err
	}
	if after != before {
		return out, errors.New("operator evidence changed during incident packet generation")
	}
	out.EvidenceSHA256 = after
	out.SourceOperatorState = status.State
	out.Packets = packets
	out.CriticalPacketCount = len(packets)
	out.SHA256 = out.digest()
	return out, nil
}
