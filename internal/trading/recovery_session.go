package trading

// An OPERATOR-ONLY, read-only startup/reconnect DIAGNOSTIC that overlaps an
// authenticated REST recovery observation with a private user-data socket.
// It never supplies a trading permit: neither REST history nor Binance's
// unsequenced user-data events can establish uninterrupted lifetime coverage.
// A separate real Demo integration test and future order-lifecycle policy are
// required before manual Demo orders may be enabled.
import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wsclient"
)

type RecoveryProbe struct {
	Schema                int       `json:"schema"`
	ID                    string    `json:"id"`
	Stage                 string    `json:"stage"` // STARTED, REVIEWED, BLOCKED
	StartedAt             time.Time `json:"started_at"`
	FinishedAt            time.Time `json:"finished_at,omitempty"`
	ObserverConnected     bool      `json:"observer_connected"`
	RESTReviewed          bool      `json:"rest_reviewed"`
	BaselineWindowStartMS int64     `json:"baseline_window_start_ms,omitempty"`
	BaselineWindowEndMS   int64     `json:"baseline_window_end_ms,omitempty"`
	BeforeEventDigest     string    `json:"before_event_digest,omitempty"`
	AfterEventDigest      string    `json:"after_event_digest,omitempty"`
	BeforeGapDigest       string    `json:"before_gap_digest,omitempty"`
	AfterGapDigest        string    `json:"after_gap_digest,omitempty"`
	EventCount            int       `json:"event_count"`
	DiscontinuityCount    int       `json:"discontinuity_count"`
	ReviewFile            string    `json:"review_file,omitempty"` // basename only, no absolute path
	AtStopEventDigest     string    `json:"at_stop_event_digest,omitempty"`
	AtStopGapDigest       string    `json:"at_stop_gap_digest,omitempty"`
	StaleAfterObservation bool      `json:"stale_after_observation"`
	// Optional bounded LIVE-EPOCH soak evidence (omitted from legacy reports,
	// preserving the historical schema-1 SHA256 contract).
	SoakRequestedSec     int        `json:"soak_requested_seconds,omitempty"`
	SoakCompleted        bool       `json:"soak_completed,omitempty"`
	SecondRESTReviewed   bool       `json:"second_rest_reviewed,omitempty"`
	SecondRESTAt         *time.Time `json:"second_rest_at,omitempty"`
	FirstExposureSHA256  string     `json:"first_exposure_sha256,omitempty"`
	SecondExposureSHA256 string     `json:"second_exposure_sha256,omitempty"`
	SoakEndEventDigest   string     `json:"soak_end_event_digest,omitempty"`
	SoakEndGapDigest     string     `json:"soak_end_gap_digest,omitempty"`
	Blockers             []string   `json:"blockers"`
	TradingAuthorized    bool       `json:"trading_authorized"`     // always false
	ManualReviewRequired bool       `json:"manual_review_required"` // always true
	SHA256               string     `json:"sha256"`
}

func (r RecoveryProbe) digest() string {
	r.SHA256 = ""
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (r *RecoveryProbe) seal() { r.SHA256 = r.digest() }

var probeFile = regexp.MustCompile(`^([0-9a-f]{32})\.(start|end)\.json$`)

// A dedicated exclusive directory prevents two recovery diagnostics from
// claiming the same observation interval. Crash leaves the directory behind:
// the operator must inspect orphaned work; there is NO stale PID auto-unlock.
type RecoveryProbeStore struct {
	dir, lock, id string
	closed        bool
}

func OpenRecoveryProbeStore(dataDir string) (*RecoveryProbeStore, error) {
	if dataDir == "" {
		return nil, errors.New("복구 데이터 경로가 필요합니다")
	}
	// A previous START without END cannot be overwritten with a new clean
	// diagnostic; an operator must investigate that crash first.
	prior, e := LoadRecoveryProbes(dataDir)
	if e != nil {
		return nil, e
	}
	for _, r := range prior {
		if r.Stage == "INTERRUPTED" {
			return nil, errors.New("미완료 복구 세션 증적: 수동 조사 필요")
		}
	}
	dir := filepath.Join(dataDir, "trading", "recovery_sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock := filepath.Join(dir, "exclusive.active")
	if err := os.Mkdir(lock, 0700); err != nil {
		return nil, errors.New("복구 진단이 실행 중이거나 이전 비정상 종료를 수동 확인해야 합니다")
	}
	var idRaw [16]byte
	if _, err := rand.Read(idRaw[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(idRaw[:])
	owner := filepath.Join(lock, "owner.json")
	f, e := os.OpenFile(owner, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	body, _ := json.Marshal(map[string]any{"schema": 1, "id": id})
	_, e = f.Write(body)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return nil, e
	} // retain uncertain lock
	return &RecoveryProbeStore{dir: dir, lock: lock, id: id}, nil
}
func (s *RecoveryProbeStore) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	raw, err := os.ReadFile(filepath.Join(s.lock, "owner.json"))
	if err != nil {
		return err
	}
	var owner struct {
		Schema int    `json:"schema"`
		ID     string `json:"id"`
	}
	if json.Unmarshal(raw, &owner) != nil || owner.Schema != 1 || owner.ID != s.id {
		return errors.New("복구 진단 소유자 잠금 불일치")
	}
	if err := os.Remove(filepath.Join(s.lock, "owner.json")); err != nil {
		return err
	}
	return os.Remove(s.lock)
}
func (s *RecoveryProbeStore) write(r *RecoveryProbe, phase string) error {
	if s == nil || s.closed || r == nil || r.ID != s.id || r.Schema != 1 || r.TradingAuthorized || !r.ManualReviewRequired || (phase != "start" && phase != "end") {
		return errors.New("복구 기록 형식 또는 저장소 오류")
	}
	if phase == "start" && r.Stage != "STARTED" || phase == "end" && r.Stage != "BLOCKED" && r.Stage != "REVIEWED" {
		return errors.New("복구 기록 단계 오류")
	}
	r.seal()
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(s.dir, r.ID+"."+phase+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(append(raw, '\n'))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("복구 증적 저장 불확실: %w", err)
	}
	return nil
}

// LoadRecoveryProbes validates every immutable record. A START without an END
// indicates a possible crash; it remains visible and BLOCKED. A forged
// trading_authorized flag or tampered checksum fails the entire scan.
func LoadRecoveryProbes(dataDir string) ([]RecoveryProbe, error) {
	dir := filepath.Join(dataDir, "trading", "recovery_sessions")
	fs, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []RecoveryProbe{}, nil
	}
	if err != nil {
		return nil, err
	}
	starts := map[string]RecoveryProbe{}
	ends := map[string]RecoveryProbe{}
	for _, f := range fs {
		if f.Name() == "exclusive.active" && f.IsDir() {
			// Another in-progress session is not itself a completed report.
			continue
		}
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !probeFile.MatchString(f.Name()) {
			return nil, errors.New("알 수 없는 복구 기록 파일")
		}
		m := probeFile.FindStringSubmatch(f.Name())
		b, e := os.ReadFile(filepath.Join(dir, f.Name()))
		if e != nil {
			return nil, e
		}
		var r RecoveryProbe
		if json.Unmarshal(b, &r) != nil || r.Schema != 1 || r.ID != m[1] || r.SHA256 != r.digest() || r.TradingAuthorized || !r.ManualReviewRequired || r.StartedAt.IsZero() || r.Blockers == nil {
			return nil, errors.New("복구 증적 파일 손상/위조")
		}
		if m[2] == "start" {
			if r.Stage != "STARTED" || !r.FinishedAt.IsZero() || len(r.Blockers) != 0 || r.ObserverConnected || r.RESTReviewed {
				return nil, errors.New("복구 시작 기록 손상")
			}
			starts[r.ID] = r
		} else {
			if (r.Stage != "BLOCKED" && r.Stage != "REVIEWED") || r.SoakRequestedSec < 0 || (r.SoakCompleted && (r.SoakRequestedSec == 0 || !r.SecondRESTReviewed || r.SecondRESTAt == nil || r.SecondRESTAt.IsZero() || r.FirstExposureSHA256 == "" || r.SecondExposureSHA256 == "" || r.SoakEndEventDigest == "" || r.SoakEndGapDigest == "")) || (r.Stage == "REVIEWED" && r.SoakRequestedSec > 0 && !r.SoakCompleted) || r.FinishedAt.Before(r.StartedAt) || len(r.Blockers) == 0 || r.ReviewFile != "" && filepath.Base(r.ReviewFile) != r.ReviewFile || r.Stage == "REVIEWED" && (!r.RESTReviewed || !r.ObserverConnected || r.ReviewFile == "" || !r.StaleAfterObservation) {
				return nil, errors.New("복구 종료 기록 손상")
			}
			ends[r.ID] = r
		}
	}
	out := make([]RecoveryProbe, 0, len(starts))
	for id, start := range starts {
		if end, ok := ends[id]; ok {
			if !end.StartedAt.Equal(start.StartedAt) {
				return nil, errors.New("복구 세션 시작/종료 연결 실패")
			}
			out = append(out, end)
		} else {
			start.Stage = "INTERRUPTED"
			start.Blockers = []string{"완료 기록 없는 비정상 종료/중단: 수동 복구 필요"}
			start.SHA256 = ""
			out = append(out, start)
		}
	}
	for id := range ends {
		if _, ok := starts[id]; !ok {
			return nil, errors.New("시작 기록 없는 복구 종료")
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return chronologicalIDLess(out[i].StartedAt, out[i].ID, out[j].StartedAt, out[j].ID)
	})
	return out, nil
}

type evidenceState struct {
	EventDigest, GapDigest string
	EventCount, GapCount   int
}

func hashParts(parts []string) string {
	sort.Strings(parts)
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(fmt.Sprintf("%d:", len(p))))
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Fingerprint existing event+gap evidence without trusting its file names.
// A torn concurrent write returns an error, never a clean snapshot.
func readRecoveryEvidence(dataDir string) (evidenceState, error) {
	return readRecoveryEvidenceMode(dataDir, false)
}

func readRecoveryEvidenceAllowMissing(dataDir string) (evidenceState, error) {
	return readRecoveryEvidenceMode(dataDir, true)
}

func readRecoveryEvidenceMode(dataDir string, allowMissing bool) (evidenceState, error) {
	var out evidenceState
	dir := filepath.Join(dataDir, "trading", "user_events")
	events, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		events = nil
	} else if err != nil {
		return out, err
	}
	eventParts := []string{}
	if len(events) > 100000 {
		return out, errors.New("계정 이벤트 보관 수가 진단 한도를 초과합니다")
	}
	for _, f := range events {
		if f.Name() == "exclusive.active" && f.IsDir() {
			// Another in-progress session is not itself a completed report.
			continue
		}
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !eventName.MatchString(f.Name()) {
			return out, errors.New("알 수 없는 계정 이벤트 파일")
		}
		b, er := os.ReadFile(filepath.Join(dir, f.Name()))
		if er != nil {
			return out, er
		}
		var record eventEnvelope
		if json.Unmarshal(b, &record) != nil || record.Schema != 1 || record.Digest != record.sum() || f.Name() != record.Event.Digest+".json" || record.Event.Kind == "" || record.Event.EventTime <= 0 {
			return out, errors.New("계정 이벤트 증적 손상")
		}
		eventParts = append(eventParts, f.Name()+":"+record.Digest)
	}
	out.EventCount = len(eventParts)
	out.EventDigest = hashParts(eventParts)
	gapDir := filepath.Join(dataDir, "trading", "stream_discontinuities")
	gaps, err := os.ReadDir(gapDir)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		gaps = nil
	} else if err != nil {
		return out, err
	}
	if len(gaps) > 100000 {
		return out, errors.New("스트림 불연속 마커 수가 진단 한도를 초과합니다")
	}
	gapParts := []string{}
	for _, f := range gaps {
		name := f.Name()
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || len(name) != 37 || !strings.HasSuffix(name, ".json") {
			return out, errors.New("스트림 불연속 기록 형식 오류")
		}
		if _, er := hex.DecodeString(strings.TrimSuffix(name, ".json")); er != nil {
			return out, errors.New("스트림 불연속 파일 이름 오류")
		}
		b, er := os.ReadFile(filepath.Join(gapDir, name))
		if er != nil {
			return out, er
		}
		var mark struct {
			Schema int    `json:"schema"`
			Reason string `json:"reason"`
			At     string `json:"at"`
		}
		if json.Unmarshal(b, &mark) != nil || mark.Schema != 1 || mark.Reason == "" {
			return out, errors.New("스트림 불연속 기록 손상")
		}
		if _, er := time.Parse(time.RFC3339Nano, mark.At); er != nil {
			return out, errors.New("스트림 마커 시간 오류")
		}
		sum := sha256.Sum256(b)
		gapParts = append(gapParts, name+":"+hex.EncodeToString(sum[:]))
	}
	out.GapCount = len(gapParts)
	out.GapDigest = hashParts(gapParts)
	return out, nil
}

// RecoveryObserver is an injection seam for *mock testing* the orchestration.
// Only the production wrapper below dials a real private Demo stream.
type RecoveryObserver func(ctx context.Context, store *EventStore, connected func()) error

// ProbeDemoRecovery MUST be explicitly invoked by an operator-controlled
// future integration, never on normal app startup. It STARTS an observer,
// captures an authenticated REST baseline while listening, and keeps trading
// disabled regardless of the outcome. The observer is stopped upon return;
// therefore its result is a HISTORICAL DIAGNOSTIC, not a live checkpoint.
// No keys/secret/listenKey or raw event payloads are archived here.
func ProbeDemoRecovery(ctx context.Context, reader DemoRecoveryReader, creds DemoCredentials, dataDir string, sinceMS int64, observer RecoveryObserver) (RecoveryProbe, error) {
	return probeDemoRecoveryWithSoak(ctx, reader, creds, dataDir, sinceMS, observer, 0)
}

// ProbeDemoRecoveryWithSoak performs a finite, explicit operator-triggered
// read-only observer epoch. The signed REST snapshots bracket the dwell time;
// no finite observation (including zero event epochs) proves full coverage.
// A disconnected epoch MUST NOT silently reconnect or ever authorize orders.
func ProbeDemoRecoveryWithSoak(ctx context.Context, reader DemoRecoveryReader, creds DemoCredentials, dataDir string, sinceMS int64, observer RecoveryObserver, dwell time.Duration) (RecoveryProbe, error) {
	if dwell <= 0 || dwell > 3*time.Minute {
		return RecoveryProbe{}, errors.New("유효한 관측 시간이 필요합니다")
	}
	return probeDemoRecoveryWithSoak(ctx, reader, creds, dataDir, sinceMS, observer, dwell)
}

func probeDemoRecoveryWithSoak(ctx context.Context, reader DemoRecoveryReader, creds DemoCredentials, dataDir string, sinceMS int64, observer RecoveryObserver, dwell time.Duration) (result RecoveryProbe, retErr error) {
	if reader == nil || observer == nil || creds.APIKey == "" || creds.Secret == "" || dataDir == "" {
		return result, errors.New("서명된 Demo 조회/계정 스트림 설정 필요")
	}
	if sinceMS <= 0 || time.Since(time.UnixMilli(sinceMS)) > 6*24*time.Hour || time.UnixMilli(sinceMS).After(time.Now()) {
		return result, errors.New("유효한 6일 이내 복구 시각이 필요합니다")
	}
	session, err := OpenRecoveryProbeStore(dataDir)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := session.Close(); e != nil && retErr == nil {
			retErr = e
		}
	}()
	result = RecoveryProbe{Schema: 1, ID: session.id, Stage: "STARTED", StartedAt: time.Now().UTC(), TradingAuthorized: false, ManualReviewRequired: true, Blockers: []string{}}
	if dwell > 0 {
		result.SoakRequestedSec = int((dwell + time.Second - 1) / time.Second)
	}
	if err = session.write(&result, "start"); err != nil {
		// Incomplete immutable start evidence requires human inspection.
		session.closed = true
		return result, err
	}
	// Always persist a terminal BLOCKED/REVIEWED result even after a safe error.
	defer func() {
		if result.FinishedAt.IsZero() {
			result.FinishedAt = time.Now().UTC()
		}
		if result.Stage == "STARTED" {
			result.Stage = "BLOCKED"
		}
		if result.Stage == "BLOCKED" && len(result.Blockers) == 0 {
			result.Blockers = append(result.Blockers, "진단 미완료")
		}
		if retErr != nil {
			result.Stage = "BLOCKED"
		}
		if err := session.write(&result, "end"); err != nil {
			// Preserve an uncertain exclusive lock instead of auto-clearing it.
			session.closed = true
			if retErr == nil {
				retErr = err
			}
		}
	}()
	store, err := OpenEventStore(dataDir)
	if err != nil {
		result.Blockers = append(result.Blockers, "계정 이벤트 저장소 초기화 실패")
		return result, errors.New("계정 이벤트 저장소 초기화 실패")
	}
	// Observer cancellation and CLOSE must finish before returning. An unjoined
	// observer leaves the exclusive event lock in place rather than guessing.
	obsCtx, cancel := context.WithCancel(ctx)
	connected := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- observer(obsCtx, store, func() {
			select {
			case connected <- struct{}{}:
			default:
			}
		})
	}()
	joined := false
	defer func() {
		// A bounded soak is not REVIEWED if the socket had already returned
		// before our intentional shutdown. Check before cancellation: the
		// final synchronous probe check and this handoff may race a socket drop.
		if dwell > 0 && !joined {
			select {
			case <-done:
				joined = true
				result.Stage = "BLOCKED"
				result.SoakCompleted = false
				result.Blockers = append(result.Blockers, "관측 종료 직전 계정 스트림 중단: 유한 관측 불완전")
				if retErr == nil {
					retErr = errors.New("관측 종료 전 계정 스트림 중단")
				}
			default:
			}
		}
		if dwell > 0 && ctx.Err() != nil {
			result.Stage = "BLOCKED"
			result.SoakCompleted = false
			result.Blockers = append(result.Blockers, "진단 상위 요청 취소 또는 시간 초과")
			if retErr == nil {
				retErr = errors.New("유한 진단 요청 중단")
			}
		}
		cancel()
		if !joined {
			select {
			case observerErr := <-done:
				joined = true
				// Normal shutdown returns ctx cancellation. Any other result
				// can be a dropped socket that raced our cancellation and
				// must never produce a completed soak record.
				if dwell > 0 && !errors.Is(observerErr, context.Canceled) {
					result.Stage = "BLOCKED"
					result.SoakCompleted = false
					result.Blockers = append(result.Blockers, "관측 종료 시 스트림 상태 불명확: 수동 재검증")
					if retErr == nil {
						retErr = errors.New("관측 종료 시 스트림 상태 불명확")
					}
				}
			case <-time.After(3 * time.Second):
				result.Stage = "BLOCKED"
				result.Blockers = append(result.Blockers, "계정 이벤트 관측기 종료 확인 실패: 수동 확인 필요")
				if retErr == nil {
					retErr = errors.New("계정 관측기 종료 확인 실패")
				}
			}
		}
		if joined {
			// Also mark disconnect if an injected/future observer forgets
			// to do so, to invalidate the candidate before archiving END.
			if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
				result.Blockers = append(result.Blockers, "관측 중단 마커 기록 실패")
				if retErr == nil {
					retErr = e
				}
			}
			if e := store.Close(); e != nil {
				result.Blockers = append(result.Blockers, "계정 스트림 저장소 종료 불확실")
				if retErr == nil {
					retErr = e
				}
			}
		}
		if end, e := readRecoveryEvidence(dataDir); e == nil {
			result.AtStopEventDigest = end.EventDigest
			result.AtStopGapDigest = end.GapDigest
			result.StaleAfterObservation = true
			if result.AfterEventDigest != "" && (end.EventDigest != result.AfterEventDigest || end.GapDigest != result.AfterGapDigest) {
				result.Blockers = append(result.Blockers, "진단 이후 계정 이벤트 또는 종료 불연속: 기존 리뷰는 현재 상태 증명이 아님")
			}
		} else {
			result.Blockers = append(result.Blockers, "관측 종료 후 증적 재검증 실패")
			if retErr == nil {
				retErr = e
			}
		}
	}()
	// Connection timeout deliberately bounded even if the caller has no deadline.
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-connected:
		result.ObserverConnected = true
	case obsErr := <-done:
		joined = true
		// Expose an allowlisted phase and numeric HTTP status only.
		// A legacy or injected error is never copied into immutable evidence.
		d := wsclient.DiagnoseDial(obsErr)
		if d.Phase != "UNCLASSIFIED" {
			detail := "사설 WS 접속 단계: " + d.Phase
			if d.HTTPStatus != 0 {
				detail += fmt.Sprintf(" (HTTP %d)", d.HTTPStatus)
			}
			result.Blockers = append(result.Blockers, detail)
		}
		result.Blockers = append(result.Blockers, "사설 계정 스트림 연결 전 종료")
		return result, errors.New("사설 계정 스트림 연결 실패")
	case <-timer.C:
		result.Blockers = append(result.Blockers, "사설 계정 스트림 연결 시간 초과")
		return result, errors.New("사설 계정 스트림 연결 시간 초과")
	case <-ctx.Done():
		result.Blockers = append(result.Blockers, "요청 취소")
		return result, ctx.Err()
	}
	before, err := readRecoveryEvidence(dataDir)
	if err != nil {
		result.Blockers = append(result.Blockers, "초기 이벤트 증적 불명확")
		return result, err
	}
	result.BeforeEventDigest = before.EventDigest
	result.BeforeGapDigest = before.GapDigest
	if before.GapCount == 0 {
		result.Blockers = append(result.Blockers, "STARTUP 스트림 불연속 마커 없음: 관측기 구현 확인 필요")
	}
	// Reader must be signed Demo-only. A token-less socket handshake is not
	// account authentication; the REST query independently authenticates it.
	baseline, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, sinceMS)
	if err != nil {
		result.Blockers = append(result.Blockers, "서명된 REST 전체 계정 진단 실패")
		return result, errors.New("서명된 REST 전체 계정 진단 실패")
	}
	result.BaselineWindowStartMS = baseline.WindowStartMS
	result.BaselineWindowEndMS = baseline.WindowEndMS
	if dwell > 0 {
		if err := validBoundedDriftInput(baseline, sinceMS, time.Now().UTC()); err != nil {
			result.Blockers = append(result.Blockers, "첫 서명 REST 전체 계정 관측의 유효성 검증 실패")
			return result, errors.New("첫 서명 관측 검증 실패")
		}
		result.FirstExposureSHA256, err = boundedExposureSignature(baseline)
		if err != nil {
			result.Blockers = append(result.Blockers, "첫 서명 계정 상태 서명 불명확")
			return result, errors.New("첫 서명 계정 상태 검증 실패")
		}
	}
	after, err := readRecoveryEvidence(dataDir)
	if err != nil {
		result.Blockers = append(result.Blockers, "REST 조회 이후 이벤트 증적 불명확")
		return result, err
	}
	result.AfterEventDigest = after.EventDigest
	result.AfterGapDigest = after.GapDigest
	result.EventCount = after.EventCount
	result.DiscontinuityCount = after.GapCount
	if before.EventDigest != after.EventDigest || before.GapDigest != after.GapDigest {
		result.Blockers = append(result.Blockers, "REST 조회 중 계정 이벤트 또는 스트림 불연속 발생: 스냅샷을 단일 상태로 판단할 수 없음")
	}
	if dwell > 0 && before.GapDigest != after.GapDigest {
		return result, errors.New("첫 서명 조회 중 계정 스트림 불연속")
	}
	select {
	case <-done:
		joined = true
		result.Blockers = append(result.Blockers, "REST 조회 중 사설 계정 스트림 중단")
		if dwell > 0 {
			return result, errors.New("첫 서명 조회 중 계정 스트림 종료")
		}
	default:
	}
	own, err := NewOwnershipStore(dataDir)
	if err != nil {
		result.Blockers = append(result.Blockers, "체결 장부 열기 실패")
		return result, err
	}
	review, err := ReviewRecovery(dataDir, own, baseline, time.Now().UTC())
	if err != nil {
		result.Blockers = append(result.Blockers, "독립적인 복구 리뷰 수행 실패")
		return result, err
	}
	result.RESTReviewed = true
	result.Blockers = append(result.Blockers, review.Issues...)
	archive, err := ArchiveRecoveryReview(dataDir, review)
	if err != nil {
		result.Blockers = append(result.Blockers, "복구 보고서 영구 보관 실패")
		return result, err
	}
	result.ReviewFile = filepath.Base(archive)
	final, err := readRecoveryEvidence(dataDir)
	if err != nil {
		result.Blockers = append(result.Blockers, "보고서 저장 후 이벤트 증적 불확실")
		return result, err
	}
	if final.EventDigest != after.EventDigest || final.GapDigest != after.GapDigest {
		result.Blockers = append(result.Blockers, "리뷰 수행 중에도 계정 이벤트/스트림 상태가 변경됨")
	}
	if dwell > 0 && final.GapDigest != after.GapDigest {
		return result, errors.New("독립 리뷰 중 계정 스트림 불연속")
	}
	select {
	case <-done:
		joined = true
		result.Blockers = append(result.Blockers, "리뷰 완료 전 스트림 중단")
		if dwell > 0 {
			return result, errors.New("독립 리뷰 중 계정 스트림 종료")
		}
	default:
	}
	// Optional extra dwell/REST bracket deliberately uses the SAME active socket
	// and the SAME event-store epoch. On any disconnect the entire diagnosis
	// remains BLOCKED; no reconnect and no auto-cleared historical gaps.
	if dwell > 0 {
		wait := time.NewTimer(dwell)
		select {
		case <-wait.C:
		case <-done:
			joined = true
			if !wait.Stop() {
				select {
				case <-wait.C:
				default:
				}
			}
			result.Blockers = append(result.Blockers, "추가 관측 중 비공개 WS 연결 종료: 새 서명 복구 필요")
			return result, errors.New("추가 관측 중 연결 종료")
		case <-ctx.Done():
			wait.Stop()
			result.Blockers = append(result.Blockers, "추가 관측 중 요청 취소/제한 시간 초과")
			return result, ctx.Err()
		}
		// Signed read #2 is required AFTER the dwell period. Compare both
		// complete exchange snapshots and all disk evidence, not just events.
		preSecond, err := readRecoveryEvidence(dataDir)
		if err != nil {
			result.Blockers = append(result.Blockers, "두 번째 조회 전 계정 증적 손상")
			return result, errors.New("계정 증적 검증 실패")
		}
		if preSecond.GapDigest != final.GapDigest || preSecond.EventDigest != final.EventDigest {
			result.Blockers = append(result.Blockers, "관측 유지 중 이벤트 또는 중단 기록 변화: 독립 수동 대조 필요")
		}
		if preSecond.GapDigest != final.GapDigest {
			return result, errors.New("추가 관측 중 계정 스트림 불연속")
		}
		select {
		case <-done:
			joined = true
			result.Blockers = append(result.Blockers, "두 번째 조회 전 스트림 종료")
			return result, errors.New("비공개 스트림 종료")
		default:
		}
		second, readErr := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, sinceMS)
		if readErr != nil {
			result.Blockers = append(result.Blockers, "두 번째 서명 REST 조회 실패")
			return result, errors.New("두 번째 서명 조회 실패")
		}
		if err = validBoundedDriftInput(second, sinceMS, time.Now().UTC()); err != nil || second.StartedAt.Before(baseline.FinishedAt) {
			result.Blockers = append(result.Blockers, "두 번째 서명 REST 조회 시각/범위 검증 실패")
			return result, errors.New("두 번째 서명 관측 검증 실패")
		}
		secondFinished := second.FinishedAt
		result.SecondRESTAt = &secondFinished
		result.SecondExposureSHA256, err = boundedExposureSignature(second)
		if err != nil {
			result.Blockers = append(result.Blockers, "두 번째 계정 상태 검증 실패")
			return result, errors.New("두 번째 계정 상태 검증 실패")
		}
		postSecond, err := readRecoveryEvidence(dataDir)
		if err != nil {
			result.Blockers = append(result.Blockers, "두 번째 조회 후 증적 손상")
			return result, errors.New("최종 증적 검증 실패")
		}
		result.SoakEndEventDigest, result.SoakEndGapDigest = postSecond.EventDigest, postSecond.GapDigest
		if preSecond.EventDigest != postSecond.EventDigest || preSecond.GapDigest != postSecond.GapDigest {
			result.Blockers = append(result.Blockers, "두 번째 REST 조회 중 계정 이벤트/불연속 발생: 수동 대조 필요")
		}
		if preSecond.GapDigest != postSecond.GapDigest {
			return result, errors.New("두 번째 서명 조회 중 계정 스트림 불연속")
		}
		if result.FirstExposureSHA256 != result.SecondExposureSHA256 {
			result.Blockers = append(result.Blockers, "두 서명 REST 조회의 계정 자산/주문/체결/손익 지문 불일치")
		}
		select {
		case <-done:
			joined = true
			result.Blockers = append(result.Blockers, "최종 조회까지 WS 연결 유지 실패")
			return result, errors.New("WS 연결 중단")
		default:
		}
		result.SecondRESTReviewed = true
		result.SoakCompleted = true
		result.Blockers = append(result.Blockers, "유한 기간 두 차례 서명 대조는 무누락/재연결/주문 안전성 증명이 아님")
	}
	// End of a FINITE historical observation; its teardown itself introduces
	// DISCONNECT evidence. No result ever authorizes live trading.
	result.Stage = "REVIEWED"
	result.Blockers = append(result.Blockers, "과거 진단 보고서이며 거래 허가가 아닙니다. 사설 스트림 중단 후 별도 재검증 필수")
	return result, nil
}

// Production bridge is not registered as an HTTP handler, daemon startup
// hook, or a live order gate. Private Demo endpoint compatibility is PENDING
// an operator's PC-based integration verification.
func ProbeLiveDemoRecovery(ctx context.Context, client *exchange.Client, creds DemoCredentials, dataDir string, sinceMS int64, dial exchange.UserStreamDialer) (RecoveryProbe, error) {
	if client == nil {
		return RecoveryProbe{}, errors.New("Demo 클라이언트 필요")
	}
	return ProbeDemoRecovery(ctx, client, creds, dataDir, sinceMS, func(ctx context.Context, s *EventStore, onConnected func()) error {
		return runDemoAccountObservationReady(ctx, client, creds.APIKey, s, dial, 30*time.Minute, onConnected)
	})
}

// Only invoked through an authenticated Demo read-only operator route.
// The URL-taking dialer remains explicit to prevent key-vs-URL regressions.
func ProbeLiveDemoRecoverySoak(ctx context.Context, client *exchange.Client, creds DemoCredentials, dataDir string, sinceMS int64, dial exchange.UserStreamDialer, dwell time.Duration) (RecoveryProbe, error) {
	if client == nil {
		return RecoveryProbe{}, errors.New("Demo 클라이언트 필요")
	}
	return ProbeDemoRecoveryWithSoak(ctx, client, creds, dataDir, sinceMS, func(ctx context.Context, s *EventStore, onConnected func()) error {
		return runDemoAccountObservationReady(ctx, client, creds.APIKey, s, dial, 30*time.Minute, onConnected)
	}, dwell)
}
