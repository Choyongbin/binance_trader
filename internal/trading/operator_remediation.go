package trading

// Structured, strictly read-only operator remediation guidance derived only
// from OperatorUnifiedStatus. It never contacts Binance, never removes locks,
// never edits evidence and never authorizes trading. Actions are instructions
// for a human operator to perform in a controlled/offline workflow.

import (
	"errors"
	"sort"
	"strings"
	"time"
)

const (
	RemediationStateNone      = "NO_ACTIVE_REMEDIATION"
	RemediationStateReview    = "HISTORICAL_REVIEW_REQUIRED"
	RemediationStateReconcile = "MANUAL_RECONCILIATION_REQUIRED"

	RemediationCritical = "CRITICAL"
	RemediationHigh     = "HIGH"
	RemediationMedium   = "MEDIUM"
	RemediationInfo     = "INFO"
)

type OperatorRemediationStep struct {
	Order               int      `json:"order"`
	Code                string   `json:"code"`
	Priority            string   `json:"priority"`
	Title               string   `json:"title"`
	Reason              string   `json:"reason"`
	EvidenceToInspect   []string `json:"evidence_to_inspect"`
	OperatorAction      []string `json:"operator_action"`
	ForbiddenAutomation []string `json:"forbidden_automation"`
	MutationPerformed   bool     `json:"mutation_performed"`
	TradingAuthorized   bool     `json:"trading_authorized"`
}

type OperatorRemediationPlan struct {
	Schema               int                       `json:"schema"`
	CheckedAt            time.Time                 `json:"checked_at"`
	State                string                    `json:"state"`
	SourceOperatorState  string                    `json:"source_operator_state"`
	EvidenceSHA256       string                    `json:"evidence_sha256"`
	StepCount            int                       `json:"step_count"`
	HighestPriority      string                    `json:"highest_priority"`
	Steps                []OperatorRemediationStep `json:"steps"`
	TradingAuthorized    bool                      `json:"trading_authorized"`
	AutomaticRepair      bool                      `json:"automatic_repair"`
	MutationPerformed    bool                      `json:"mutation_performed"`
	ManualReviewRequired bool                      `json:"manual_review_required"`
}

func remediationRank(p string) int {
	switch p {
	case RemediationCritical:
		return 0
	case RemediationHigh:
		return 1
	case RemediationMedium:
		return 2
	default:
		return 3
	}
}

func newRemediationStep(code, priority, title, reason string, evidence, action, forbidden []string) OperatorRemediationStep {
	return OperatorRemediationStep{
		Code: code, Priority: priority, Title: title, Reason: reason,
		EvidenceToInspect:   append([]string(nil), evidence...),
		OperatorAction:      append([]string(nil), action...),
		ForbiddenAutomation: append([]string(nil), forbidden...),
		MutationPerformed:   false, TradingAuthorized: false,
	}
}

// AnalyzeOperatorRemediation derives a deterministic plan from the same stable
// evidence snapshot used by AnalyzeOperatorUnifiedStatus. Unknown blockers are
// never ignored: they become an explicit high-priority manual evidence review.
func AnalyzeOperatorRemediation(dataDir string, now time.Time) (OperatorRemediationPlan, error) {
	out := OperatorRemediationPlan{
		Schema: 1, CheckedAt: now.UTC(), State: RemediationStateNone,
		HighestPriority: RemediationInfo, Steps: []OperatorRemediationStep{},
		TradingAuthorized: false, AutomaticRepair: false, MutationPerformed: false,
		ManualReviewRequired: true,
	}
	if strings.TrimSpace(dataDir) == "" || now.IsZero() {
		return out, errors.New("operator remediation requires data directory and time")
	}
	status, err := AnalyzeOperatorUnifiedStatus(dataDir, now)
	if err != nil {
		return out, err
	}
	out.SourceOperatorState = status.State
	out.EvidenceSHA256 = status.EvidenceSHA256

	steps := make([]OperatorRemediationStep, 0, 8)
	seen := map[string]bool{}
	add := func(s OperatorRemediationStep) {
		if seen[s.Code] {
			return
		}
		seen[s.Code] = true
		steps = append(steps, s)
	}

	if status.RecoveryProbeLockPresent || status.ReconnectWatchLockPresent {
		add(newRemediationStep(
			"PRIVATE_DIAGNOSTIC_LOCK", RemediationCritical,
			"비공개 진단 durable lock 조사",
			"recovery probe 또는 reconnect watch의 durable lock이 남아 있습니다. START 증적 이전 crash window일 수도 있으므로 실행 중인지 비정상 종료인지 자동 판단할 수 없습니다.",
			[]string{"trading/recovery_sessions/exclusive.active", "trading/reconnect_watch_sessions/exclusive.active", "offline operator-status/incident-review"},
			[]string{"서버/관련 writer가 완전히 중지된 data 사본에서 lock owner와 주변 START/END 증적을 확인", "독립 signed account audit로 중단 구간을 대조", "원본 lock/증적은 판단 근거가 확보되기 전 보존"},
			[]string{"durable lock 자동 삭제", "진단 자동 재시작", "거래 자동 활성화"},
		))
	}
	if status.MonitorLockPresent || status.EventLockPresent || status.RestartDisposition == "OFFLINE_MANUAL_REVIEW_REQUIRED" || status.MonitorState == MonitorUncertain || status.MonitorState == MonitorConnecting || status.MonitorState == MonitorReconciling || status.MonitorState == MonitorRecovering || status.MonitorState == MonitorBlocked {
		add(newRemediationStep(
			"RESTART_MONITOR_EVIDENCE", RemediationCritical,
			"재시작/관측기 미종료 증적 확인",
			"관측기 또는 이벤트 증적이 실행 중이거나 비정상 종료 상태로 보입니다. 락의 존재만으로 안전한 재시작을 판단할 수 없습니다.",
			[]string{"trading/continuous_monitor", "trading/user_events", "restart-forensics offline output"},
			[]string{"서버를 중지한 data 사본에서 restart-forensics/operator-status를 다시 실행", "마지막 monitor checkpoint와 event/gap 시간대를 대조", "증적이 안정된 뒤에만 다음 단계로 진행"},
			[]string{"락 디렉터리 자동 삭제", "관측기 자동 재시작", "거래 자동 활성화"},
		))
	}
	if status.GapMarkerCount > 0 {
		add(newRemediationStep(
			"STREAM_GAP_RECONCILIATION", RemediationCritical,
			"스트림 gap 구간 대조",
			"과거 stream gap marker가 남아 있어 해당 구간의 주문/체결 누락 가능성을 배제할 수 없습니다.",
			[]string{"trading/stream_discontinuities", "trading/user_events", "signed order/userTrades history", "restart-forensics"},
			[]string{"gap 전후 signed order/userTrades 범위를 독립 조회해 로컬 증적과 대조", "외부 주문·누락 tradeId·수량 차이를 수동 확인"},
			[]string{"gap marker 삭제", "timestamp만으로 연속성 인정", "자동 거래 재개"},
		))
	}
	if status.Recovery.InterruptedCount > 0 || status.Recovery.LatestProbeStage == "STARTED" {
		add(newRemediationStep(
			"RECOVERY_PROBE_INTERRUPTED", RemediationCritical,
			"중단된 recovery probe 조사",
			"START 증적에 대응하는 확정 END가 없거나 interrupted recovery가 남아 있습니다.",
			[]string{"trading/recovery_sessions", "trading/recovery_reviews", "signed full account history"},
			[]string{"중단 시점 전후의 signed order/userTrades/account snapshot을 새 읽기 전용 진단으로 대조", "기존 START/lock 증적은 보존"},
			[]string{"START/lock 파일 삭제", "동일 주문 재POST", "중단을 성공으로 간주"},
		))
	}
	if status.Protection.UnsettledGroupCount > 0 {
		add(newRemediationStep(
			"PROTECTION_SETTLEMENT_INCOMPLETE", RemediationCritical,
			"미완료 protection settlement 복구",
			"TP/SL protection group이 최종 SETTLED까지 도달하지 않았습니다. 실제 trigger/fill 여부와 pooled-side 잔량을 다시 확인해야 합니다.",
			[]string{"trading/protection_submissions", "trading/protection_settlements", "trading/protection_fills", "signed algo order", "signed actual order/userTrades", "current pooled-side position"},
			[]string{"각 leg의 signed terminal 상태와 actualOrderId 체결을 확인", "immutable protection fill 합계와 pooled-side position을 대조", "불명확하면 HALT_AND_MANUAL_RECONCILE 유지"},
			[]string{"sibling cancel 자동 재전송", "TP/SL 자동 재생성", "settlement marker 수동 생성"},
		))
	}
	if status.Protection.ArmingState == ProtectionArmingPooledConflict {
		add(newRemediationStep(
			"POOLED_PROTECTION_CONFLICT", RemediationCritical,
			"pooled-side protection 충돌 해결",
			"같은 Hedge side에 서로 양립할 수 없는 active protection plan이 존재합니다.",
			[]string{"all active protection plans for same symbol/positionSide", "signed algo inventory", "owner quantities", "pooled-side aggregate position"},
			[]string{"모든 active plan의 소유권·수량·signed 상태를 한 번에 대조", "충돌이 해소되기 전 새 protection arming 금지"},
			[]string{"임의 sibling cancel", "owner 하나만 기준으로 수량 축소", "새 protection 자동 arming"},
		))
	}
	if status.Integrated.LatestState == "BLOCKED" {
		add(newRemediationStep(
			"INTEGRATED_REVIEW_BLOCKED", RemediationHigh,
			"통합 검토 blocker 확인",
			"가장 최근 integrated review가 BLOCKED 상태입니다.",
			[]string{"latest trading/integrated_reviews report", "its blocker list", "referenced local/signed evidence"},
			[]string{"latest blocker를 원본 증적별로 분해해 확인", "새 review를 생성하더라도 과거 BLOCKED 기록은 보존"},
			[]string{"BLOCKED report 삭제", "새 PASS-like report로 과거 blocker 덮어쓰기"},
		))
	}

	// Preserve any blocker not already represented by a structured rule.
	knownFragments := []string{"durable lock", "restart/monitor", "stream gap", "interrupted recovery", "interrupted reconnect", "integrated review", "protection settlement", "pooled-side protection"}
	unknown := make([]string, 0)
	for _, b := range status.Blockers {
		matched := false
		lb := strings.ToLower(b)
		for _, f := range knownFragments {
			if strings.Contains(lb, strings.ToLower(f)) {
				matched = true
				break
			}
		}
		if !matched {
			unknown = append(unknown, b)
		}
	}
	if len(unknown) > 0 {
		add(newRemediationStep(
			"UNCLASSIFIED_BLOCKER", RemediationHigh,
			"분류되지 않은 blocker 수동 조사",
			strings.Join(unknown, " / "),
			[]string{"operator-status blockers", "referenced immutable evidence"},
			[]string{"원인 증적을 식별하고 별도 수동 reconciliation 기록으로 남김"},
			[]string{"blocker 문자열 무시", "자동 unlock", "증적 삭제"},
		))
	}

	if len(steps) == 0 && status.State == OperatorStateHistoricalOnly {
		add(newRemediationStep(
			"HISTORICAL_EVIDENCE_REVIEW", RemediationMedium,
			"과거 증적 검토",
			"활성 blocker는 없지만 과거 진단/회계/protection 이력이 존재합니다. 이는 현재 Binance 상태나 stream continuity의 증명이 아닙니다.",
			[]string{"operator-status", "latest recovery/integrated/accounting reports"},
			[]string{"실제 PC 검증 시 최신 signed read-only 상태와 과거 기록의 일관성 확인"},
			[]string{"과거 report만으로 거래 허가", "stream continuity 추정"},
		))
	}
	if len(steps) == 0 {
		add(newRemediationStep(
			"NO_HISTORICAL_EVIDENCE", RemediationInfo,
			"활성 복구 작업 없음",
			"현재 로컬 증적에서 활성 blocker는 발견되지 않았습니다. 그러나 신규/빈 기록은 현재 계정 안전성이나 stream continuity를 증명하지 않습니다.",
			[]string{"operator-status evidence hash"},
			[]string{"PC 실환경에서는 별도 read-only account/stream 검증 수행"},
			[]string{"빈 기록을 거래 허가로 해석"},
		))
	}

	sort.SliceStable(steps, func(i, j int) bool {
		ri, rj := remediationRank(steps[i].Priority), remediationRank(steps[j].Priority)
		if ri != rj {
			return ri < rj
		}
		return steps[i].Code < steps[j].Code
	})
	for i := range steps {
		steps[i].Order = i + 1
	}
	out.Steps = steps
	out.StepCount = len(steps)
	out.HighestPriority = steps[0].Priority
	switch status.State {
	case OperatorStateBlocked:
		out.State = RemediationStateReconcile
	case OperatorStateHistoricalOnly:
		out.State = RemediationStateReview
	default:
		out.State = RemediationStateNone
	}
	return out, nil
}
