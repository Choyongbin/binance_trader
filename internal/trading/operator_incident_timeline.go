package trading

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const IncidentTimelineScope = "READ_ONLY_LOCAL_EVIDENCE_TIMELINE_NOT_EXCHANGE_ATTESTATION"

type OperatorTimelineEvent struct {
	At             time.Time `json:"at"`
	Kind           string    `json:"kind"`
	ID             string    `json:"id"`
	CorrelationID  string    `json:"correlation_id"`
	State          string    `json:"state"`
	Severity       string    `json:"severity"`
	Summary        string    `json:"summary"`
	EvidenceSHA256 string    `json:"evidence_sha256,omitempty"`
}

type OperatorIncidentTimeline struct {
	Schema               int                     `json:"schema"`
	GeneratedAt          time.Time               `json:"generated_at"`
	Scope                string                  `json:"scope"`
	EvidenceSHA256       string                  `json:"evidence_sha256"`
	SourceOperatorState  string                  `json:"source_operator_state"`
	EventCount           int                     `json:"event_count"`
	Events               []OperatorTimelineEvent `json:"events"`
	ActiveBlockers       []string                `json:"active_blockers"`
	TradingAuthorized    bool                    `json:"trading_authorized"`
	AutomaticRepair      bool                    `json:"automatic_repair"`
	MutationPerformed    bool                    `json:"mutation_performed"`
	ManualReviewRequired bool                    `json:"manual_review_required"`
	SHA256               string                  `json:"sha256"`
}

func (t OperatorIncidentTimeline) digest() string {
	t.SHA256 = ""
	b, _ := json.Marshal(t)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func incidentCorrelationID(kind, id string) string {
	raw := []byte("incident-correlation:v1:" + kind + ":" + id)
	s := sha256.Sum256(raw)
	return hex.EncodeToString(s[:16])
}

func BuildOperatorIncidentTimeline(dataDir string, now time.Time) (OperatorIncidentTimeline, error) {
	out := OperatorIncidentTimeline{Schema: 1, GeneratedAt: now.UTC(), Scope: IncidentTimelineScope, Events: []OperatorTimelineEvent{}, ActiveBlockers: []string{}, ManualReviewRequired: true}
	if strings.TrimSpace(dataDir) == "" || now.IsZero() {
		return out, errors.New("incident timeline requires data directory and time")
	}
	before, err := operatorEvidenceSHA256(dataDir)
	if err != nil {
		return out, err
	}
	status, err := AnalyzeOperatorUnifiedStatus(dataDir, now)
	if err != nil {
		return out, err
	}
	probes, err := LoadRecoveryProbes(dataDir)
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
	f, err := AnalyzeRestartForensics(dataDir, now)
	if err != nil {
		return out, err
	}
	events := []OperatorTimelineEvent{}
	for _, p := range probes {
		at := p.StartedAt
		if !p.FinishedAt.IsZero() {
			at = p.FinishedAt
		}
		sev := "INFO"
		if p.Stage == "INTERRUPTED" || p.Stage == "BLOCKED" {
			sev = "CRITICAL"
		}
		events = append(events, OperatorTimelineEvent{At: at, Kind: "recovery_probe", ID: p.ID, CorrelationID: incidentCorrelationID("recovery_probe", p.ID), State: p.Stage, Severity: sev, Summary: "recovery diagnostic " + strings.ToLower(p.Stage), EvidenceSHA256: p.SHA256})
	}
	for _, r := range integrated {
		sev := "INFO"
		if r.State == "BLOCKED" {
			sev = "HIGH"
		}
		events = append(events, OperatorTimelineEvent{At: r.CheckedAt, Kind: "integrated_review", ID: r.ID, CorrelationID: incidentCorrelationID("integrated_review", r.ID), State: r.State, Severity: sev, Summary: "integrated evidence review", EvidenceSHA256: r.SHA256})
	}
	for _, r := range accounting {
		events = append(events, OperatorTimelineEvent{At: r.CreatedAt, Kind: "accounting", ID: r.ID, CorrelationID: incidentCorrelationID("accounting", r.ID), State: r.Coverage, Severity: "INFO", Summary: "accounting report archived", EvidenceSHA256: r.SHA256})
	}
	if f.MonitorSessionCount > 0 || f.EventCount > 0 || f.GapMarkerCount > 0 || f.InterruptedOrActive {
		sev := "INFO"
		if f.InterruptedOrActive || f.GapMarkerCount > 0 {
			sev = "CRITICAL"
		}
		events = append(events, OperatorTimelineEvent{At: f.CheckedAt, Kind: "restart_forensics", ID: "current-local-snapshot", CorrelationID: incidentCorrelationID("restart_forensics", "current-local-snapshot"), State: f.RestartDisposition, Severity: sev, Summary: "restart/stream forensic aggregate", EvidenceSHA256: digestRestartForPacket(f)})
	}
	dir := filepathJoin(dataDir, "trading", "protection_submissions")
	store := &ProtectionSubmissionStore{dir: dir}
	if plans, e := store.scanPlansLocked(); e == nil {
		ss := &ProtectionSettlementStore{root: filepathJoin(dataDir, "trading"), fillDir: filepathJoin(dataDir, "trading", "protection_fills"), stageDir: filepathJoin(dataDir, "trading", "protection_settlements")}
		for _, p := range plans {
			at, e := time.Parse(time.RFC3339Nano, p.CreatedAt)
			if e != nil {
				continue
			}
			st, _, e := ss.currentStageLocked(p.GroupID, p)
			if e != nil {
				return out, e
			}
			sev := "INFO"
			if st != ProtectionSettlementSettled {
				sev = "CRITICAL"
			}
			events = append(events, OperatorTimelineEvent{At: at, Kind: "protection_group", ID: p.GroupID, CorrelationID: incidentCorrelationID("protection_group", p.GroupID), State: string(st), Severity: sev, Summary: "protective order lifecycle", EvidenceSHA256: p.Digest})
		}
	}
	if audit, e := ReviewExecutionLifecycles(dataDir); e == nil {
		dir := filepathJoin(dataDir, "trading", "execution_lifecycle")
		for _, item := range audit.Items {
			lc, le := loadExecutionLifecycleFromDir(dir, item.ClientOrderID)
			if le != nil {
				return out, le
			}
			at, le := time.Parse(time.RFC3339Nano, lc.Root.CreatedAt)
			if le != nil {
				return out, le
			}
			sev := "INFO"
			if item.Stage != ExecutionSettled {
				sev = "CRITICAL"
			}
			events = append(events, OperatorTimelineEvent{At: at, Kind: "execution", ID: item.ClientOrderID, CorrelationID: incidentCorrelationID("execution", item.ClientOrderID), State: string(item.Stage), Severity: sev, Summary: "durable execution lifecycle", EvidenceSHA256: lc.Root.Checksum})
		}
	} else if !strings.Contains(e.Error(), "데이터 디렉터리") {
		return out, e
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].At.Equal(events[j].At) {
			if events[i].Kind == events[j].Kind {
				return events[i].ID < events[j].ID
			}
			return events[i].Kind < events[j].Kind
		}
		return events[i].At.Before(events[j].At)
	})
	after, err := operatorEvidenceSHA256(dataDir)
	if err != nil {
		return out, err
	}
	if before != after || status.EvidenceSHA256 != after {
		return out, errors.New("operator evidence changed during timeline construction")
	}
	out.EvidenceSHA256 = after
	out.SourceOperatorState = status.State
	out.Events = events
	out.EventCount = len(events)
	out.ActiveBlockers = append([]string(nil), status.Blockers...)
	out.SHA256 = out.digest()
	return out, nil
}

// isolated for testability and to keep path joins platform-correct.
func filepathJoin(parts ...string) string { return filepath.Join(parts...) }
