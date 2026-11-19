package trading

// Operator-armed, read-only Demo account supervisor. This intentionally DOES
// NOT provide a readiness/authorization gate. User-data streams lack global
// lossless sequencing: each epoch (including reconnects) stays untrusted.
// The operator must re-enter their vault password after a process restart.
import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

const (
	MonitorUncertain      = "UNCERTAIN"
	MonitorConnecting     = "CONNECTING"
	MonitorReconciling    = "RECONCILING"
	MonitorConnected      = "CONNECTED" // transport connected AND recent bounded signed REST; NOT trading ready
	MonitorRecovering     = "RECOVERING"
	MonitorBlocked        = "BLOCKED"
	MonitorStopped        = "STOPPED"
	monitorReconnectLimit = 2
)

// MonitorCheckpoint is append-only, chained and sanitized. Never store a
// passphrase, key, listenKey, account ID, complete URL or raw exchange error.
// A hash chain detects accidental alteration but is NOT an external attestation.
type MonitorCheckpoint struct {
	Schema                 int       `json:"schema"`
	SessionID              string    `json:"session_id"`
	Sequence               uint64    `json:"sequence"`
	PreviousHash           string    `json:"previous_hash"`
	At                     time.Time `json:"at"`
	State                  string    `json:"state"`
	Epoch                  int       `json:"epoch"`
	Reconnects             int       `json:"reconnects"`
	LastSignedRESTAt       time.Time `json:"last_signed_rest_at,omitempty"`
	ExposureSHA256         string    `json:"exposure_sha256,omitempty"`
	GapDigest              string    `json:"gap_digest,omitempty"`
	Issues                 []string  `json:"issues"`
	Reason                 string    `json:"reason"` // from a fixed local whitelist, never raw errors
	TradingAuthorized      bool      `json:"trading_authorized"`
	StreamContinuityProven bool      `json:"stream_continuity_proven"`
	ContinuityScope        string    `json:"continuity_scope,omitempty"`
	ContinuitySince        time.Time `json:"continuity_since,omitempty"`
	ContinuityGapDigest    string    `json:"continuity_gap_digest,omitempty"`
	ManualReviewRequired   bool      `json:"manual_review_required"`
	SHA256                 string    `json:"sha256"`
}

func (p MonitorCheckpoint) digest() string {
	p.SHA256 = ""
	var b []byte
	if p.Schema == 1 {
		// Preserve the exact v1 field set/order so dev.3.35 and older archives
		// remain verifiable after continuity fields were introduced in schema 2.
		legacy := struct {
			Schema                 int       `json:"schema"`
			SessionID              string    `json:"session_id"`
			Sequence               uint64    `json:"sequence"`
			PreviousHash           string    `json:"previous_hash"`
			At                     time.Time `json:"at"`
			State                  string    `json:"state"`
			Epoch                  int       `json:"epoch"`
			Reconnects             int       `json:"reconnects"`
			LastSignedRESTAt       time.Time `json:"last_signed_rest_at,omitempty"`
			ExposureSHA256         string    `json:"exposure_sha256,omitempty"`
			GapDigest              string    `json:"gap_digest,omitempty"`
			Issues                 []string  `json:"issues"`
			Reason                 string    `json:"reason"`
			TradingAuthorized      bool      `json:"trading_authorized"`
			StreamContinuityProven bool      `json:"stream_continuity_proven"`
			ManualReviewRequired   bool      `json:"manual_review_required"`
			SHA256                 string    `json:"sha256"`
		}{p.Schema, p.SessionID, p.Sequence, p.PreviousHash, p.At, p.State, p.Epoch, p.Reconnects,
			p.LastSignedRESTAt, p.ExposureSHA256, p.GapDigest, p.Issues, p.Reason, p.TradingAuthorized,
			p.StreamContinuityProven, p.ManualReviewRequired, ""}
		b, _ = json.Marshal(legacy)
	} else {
		b, _ = json.Marshal(p)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (p *MonitorCheckpoint) seal() { p.SHA256 = p.digest() }
func blankMonitor() MonitorCheckpoint {
	return MonitorCheckpoint{Schema: 1, State: "NOT_RUN", Issues: []string{}, ManualReviewRequired: true}
}

var monitorSessionName = regexp.MustCompile(`^[0-9a-f]{32}$`)
var monitorCheckpointName = regexp.MustCompile(`^[0-9]{8}\.json$`)
var allowedMonitorState = map[string]bool{MonitorUncertain: true, MonitorConnecting: true, MonitorReconciling: true, MonitorConnected: true, MonitorRecovering: true, MonitorBlocked: true, MonitorStopped: true}

const monitorContinuityScope = "PROCESS_LOCAL_CHECKPOINT_TO_NOW"
const MonitorContinuityFreshness = 6 * time.Minute

var monitorReasons = map[string]bool{
	"START": true, "HANDSHAKE": true, "SIGNED_REST": true, "REST_REFRESH": true, "SOCKET_DROPPED": true,
	"RECONNECT": true, "RECONNECT_LIMIT": true, "DIAL_FAILED": true, "REST_FAILED": true,
	"GAP_CHANGED": true, "EVENT_CHANGED": true, "OPERATOR_STOP": true, "REQUEST_CANCELLED": true,
	"ARCHIVE_FAILED": true, "UNVERIFIED_HISTORY": true, "ACCOUNT_CHANGED": true,
}

func monitorRoot(dataDir string) string {
	return filepath.Join(dataDir, "trading", "continuous_monitor")
}

// ReadContinuousMonitor verifies ALL sessions and every link, not merely the
// most recent snapshot. A crashed session is displayed as UNCERTAIN, never as
// the previously saved CONNECTED state. A stale lock is NEVER auto-cleared.
func ReadContinuousMonitor(dataDir string) (MonitorCheckpoint, bool, error) {
	if dataDir == "" {
		return blankMonitor(), false, errors.New("monitor data directory required")
	}
	root := monitorRoot(dataDir)
	entries, e := os.ReadDir(root)
	if errors.Is(e, os.ErrNotExist) {
		return blankMonitor(), false, nil
	}
	if e != nil {
		return blankMonitor(), false, e
	}
	latest := blankMonitor()
	have := false
	byID := make(map[string]MonitorCheckpoint)
	var olderUnclosed *MonitorCheckpoint
	var lockID string
	for _, entry := range entries {
		if entry.Name() == "exclusive.active" {
			if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				return blankMonitor(), false, errors.New("invalid monitor lock")
			}
			body, err := os.ReadFile(filepath.Join(root, entry.Name(), "owner.json"))
			if err != nil {
				return blankMonitor(), false, err
			}
			var owner struct {
				Schema int    `json:"schema"`
				ID     string `json:"id"`
			}
			if json.Unmarshal(body, &owner) != nil || owner.Schema != 1 || !monitorSessionName.MatchString(owner.ID) {
				return blankMonitor(), false, errors.New("invalid monitor lock owner")
			}
			lockID = owner.ID
			continue
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !monitorSessionName.MatchString(entry.Name()) {
			return blankMonitor(), false, errors.New("unknown monitor archive entry")
		}
		sessionDir := filepath.Join(root, entry.Name())
		parts, err := os.ReadDir(sessionDir)
		if err != nil || len(parts) == 0 {
			return blankMonitor(), false, errors.New("incomplete monitor archive")
		}
		var previous MonitorCheckpoint
		for i, part := range parts {
			expected := fmt.Sprintf("%08d.json", i+1)
			if i >= 99999999 || part.Name() != expected || !part.Type().IsRegular() || !monitorCheckpointName.MatchString(part.Name()) {
				return blankMonitor(), false, errors.New("monitor archive sequence error")
			}
			data, err := os.ReadFile(filepath.Join(sessionDir, part.Name()))
			if err != nil || len(data) > 16384 {
				return blankMonitor(), false, errors.New("monitor archive read/size error")
			}
			var cp MonitorCheckpoint
			dec := json.NewDecoder(strings.NewReader(string(data)))
			dec.DisallowUnknownFields()
			if dec.Decode(&cp) != nil || dec.Decode(new(any)) != io.EOF || (cp.Schema != 1 && cp.Schema != 2) || (cp.Schema == 1 && cp.StreamContinuityProven) || cp.SessionID != entry.Name() || cp.Sequence != uint64(i+1) || cp.PreviousHash != previous.SHA256 || cp.Issues == nil || !allowedMonitorState[cp.State] || !monitorReasons[cp.Reason] || cp.TradingAuthorized || !validMonitorContinuity(cp) || !cp.ManualReviewRequired || cp.SHA256 != cp.digest() || cp.At.IsZero() || cp.At.After(time.Now().Add(2*time.Minute)) || i > 0 && cp.At.Before(previous.At) {
				return blankMonitor(), false, errors.New("monitor checkpoint corruption")
			}
			previous = cp
		}
		byID[entry.Name()] = previous
		if previous.State != MonitorStopped && previous.State != MonitorBlocked {
			copy := previous
			if olderUnclosed == nil || copy.At.Before(olderUnclosed.At) {
				olderUnclosed = &copy
			}
		}
		if !have || previous.At.After(latest.At) {
			latest = previous
			have = true
		}
	}
	if lockID != "" {
		// Never treat another owner's live/stale lock as a healthy connection.
		active, ok := byID[lockID]
		if !ok {
			return blankMonitor(), false, errors.New("monitor lock without valid session")
		}
		return monitorStatusForInterrupted(active), true, nil
	}
	if olderUnclosed != nil {
		return monitorStatusForInterrupted(*olderUnclosed), true, nil
	}
	return monitorStatusHistorical(latest), false, nil
}
func validMonitorContinuity(p MonitorCheckpoint) bool {
	if !p.StreamContinuityProven {
		return p.ContinuityScope == "" && p.ContinuitySince.IsZero() && p.ContinuityGapDigest == ""
	}
	return p.State == MonitorConnected && p.ContinuityScope == monitorContinuityScope && !p.ContinuitySince.IsZero() &&
		eventDigest.MatchString(p.ContinuityGapDigest) && p.GapDigest == p.ContinuityGapDigest &&
		!p.LastSignedRESTAt.IsZero() && !p.LastSignedRESTAt.Before(p.ContinuitySince)
}

// LiveMonitorContinuityProven is intentionally process-local and freshness-bound.
// It never turns a historical archive into an authorization signal.
func LiveMonitorContinuityProven(p MonitorCheckpoint, now time.Time) bool {
	if !validMonitorContinuity(p) || !p.StreamContinuityProven || now.IsZero() {
		return false
	}
	now = now.UTC()
	if p.ContinuitySince.After(now.Add(2*time.Second)) || p.LastSignedRESTAt.After(now.Add(2*time.Second)) {
		return false
	}
	age := now.Sub(p.LastSignedRESTAt)
	return age >= 0 && age <= MonitorContinuityFreshness
}
func monitorStatusHistorical(p MonitorCheckpoint) MonitorCheckpoint {
	p.StreamContinuityProven = false
	p.ContinuityScope = ""
	p.ContinuitySince = time.Time{}
	p.ContinuityGapDigest = ""
	return p
}
func monitorStatusForInterrupted(p MonitorCheckpoint) MonitorCheckpoint {
	p = monitorStatusHistorical(p)
	p.State = MonitorUncertain
	p.Reason = "UNVERIFIED_HISTORY"
	p.ExposureSHA256 = ""
	p.LastSignedRESTAt = time.Time{}
	p.Issues = []string{"이전 관측 세션 종료/재시작 상태 미확인: 오프라인 수동 검증 필요"}
	p.SHA256 = ""
	return p
}

type monitorJournal struct {
	dir, lock, id, previous string
	seq                     uint64
	closed                  bool
}

func openMonitorJournal(dataDir string) (*monitorJournal, error) {
	_, uncertain, e := ReadContinuousMonitor(dataDir)
	if e != nil {
		return nil, e
	}
	if uncertain {
		return nil, errors.New("unverified previous monitor session: manual recovery required")
	}
	root := monitorRoot(dataDir)
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	lock := filepath.Join(root, "exclusive.active")
	if e := os.Mkdir(lock, 0700); e != nil {
		return nil, errors.New("monitor already active or stale lock")
	}
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return nil, e
	}
	id := hex.EncodeToString(nonce[:])
	body, _ := json.Marshal(struct {
		Schema int    `json:"schema"`
		ID     string `json:"id"`
	}{1, id})
	f, e := os.OpenFile(filepath.Join(lock, "owner.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
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
	}
	dir := filepath.Join(root, id)
	if e = os.Mkdir(dir, 0700); e != nil {
		return nil, e
	}
	return &monitorJournal{dir: dir, lock: lock, id: id}, nil
}
func (j *monitorJournal) append(p *MonitorCheckpoint) error {
	if j == nil || j.closed || p == nil || !allowedMonitorState[p.State] || !monitorReasons[p.Reason] || p.TradingAuthorized || !validMonitorContinuity(*p) || !p.ManualReviewRequired || p.Issues == nil {
		return errors.New("unsafe monitor checkpoint")
	}
	p.Schema = 2
	p.SessionID = j.id
	p.Sequence = j.seq + 1
	p.PreviousHash = j.previous
	p.At = time.Now().UTC()
	p.seal()
	body, e := json.MarshalIndent(p, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(j.dir, fmt.Sprintf("%08d.json", p.Sequence)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(append(body, '\n'))
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	j.seq = p.Sequence
	j.previous = p.SHA256
	return nil
}
func (j *monitorJournal) close() error {
	if j == nil || j.closed {
		return nil
	}
	raw, e := os.ReadFile(filepath.Join(j.lock, "owner.json"))
	if e != nil {
		return e
	}
	var owner struct {
		Schema int    `json:"schema"`
		ID     string `json:"id"`
	}
	if json.Unmarshal(raw, &owner) != nil || owner.Schema != 1 || owner.ID != j.id {
		return errors.New("monitor lock owner mismatch")
	}
	if e = os.Remove(filepath.Join(j.lock, "owner.json")); e != nil {
		return e
	}
	if e = os.Remove(j.lock); e != nil {
		return e
	}
	j.closed = true
	return nil
}

// ContinuousREST is deliberately injectable: production independently signs
// every full account/history REST baseline; tests can force API failures.
type ContinuousREST func(context.Context) (string, []string, error)
type ContinuousObserver func(context.Context, *EventStore, func()) error

// ContinuousMonitor runs until the operator cancels it. On every dropped epoch
// it revokes the prior signed REST freshness and reconnects at most twice.
// An unexpected exit cannot be hidden by a successful REST request.
func ContinuousMonitor(ctx context.Context, dataDir string, observer ContinuousObserver, verify ContinuousREST, refreshInterval time.Duration, notify func(MonitorCheckpoint)) (out MonitorCheckpoint, retErr error) {
	if dataDir == "" || observer == nil || verify == nil || refreshInterval <= 0 {
		return blankMonitor(), errors.New("invalid monitor configuration")
	}
	journal, e := openMonitorJournal(dataDir)
	if e != nil {
		return blankMonitor(), e
	}
	out = blankMonitor()
	out.State = MonitorUncertain
	out.Reason = "START"
	out.Issues = []string{"과거 불연속 기록/전체 계정 무누락 승인 미완료"}
	record := func(state, reason string) error {
		out.State = state
		out.Reason = reason
		out.ExposureSHA256 = ""
		if state == MonitorUncertain || state == MonitorRecovering || state == MonitorBlocked || state == MonitorStopped || state == MonitorConnecting || state == MonitorReconciling {
			out.LastSignedRESTAt = time.Time{}
			out.StreamContinuityProven = false
			out.ContinuityScope = ""
			out.ContinuitySince = time.Time{}
			out.ContinuityGapDigest = ""
		}
		if e := journal.append(&out); e != nil {
			return errors.New("monitor checkpoint persistence failed")
		}
		if notify != nil {
			notify(out)
		}
		return nil
	}
	if e := record(MonitorUncertain, "START"); e != nil {
		return out, e
	}
	keepLocks := false // a hung observer must not regain ownership of a new store
	defer func() {
		if retErr != nil && out.State != MonitorBlocked {
			out.Issues = append(out.Issues, "관측 실패 또는 증적 기록 검토 필요")
			if e := record(MonitorBlocked, "ARCHIVE_FAILED"); e != nil {
				retErr = e
			}
		}
		if keepLocks {
			return
		}
		if e := journal.close(); e != nil && retErr == nil {
			retErr = errors.New("monitor lock cleanup uncertain")
		}
	}()
	store, e := OpenEventStore(dataDir)
	if e != nil {
		out.Issues = append(out.Issues, "이벤트 저장소 무결성 또는 독점 잠금 실패")
		return out, errors.New("event store unavailable")
	}
	defer func() {
		if keepLocks {
			return
		}
		if e := store.Close(); e != nil && retErr == nil {
			retErr = errors.New("event store lock cleanup uncertain")
		}
	}()
	for epoch := 1; ; epoch++ {
		continuityCandidateAt := time.Time{}
		continuityCandidateGap := ""
		if ctx.Err() != nil {
			if e := record(MonitorStopped, "OPERATOR_STOP"); e != nil {
				return out, e
			}
			return out, nil
		}
		out.Epoch = epoch
		if epoch > 1 {
			if e := record(MonitorRecovering, "RECONNECT"); e != nil {
				return out, e
			}
		}
		if e := record(MonitorConnecting, "HANDSHAKE"); e != nil {
			return out, e
		}
		epochCtx, cancel := context.WithCancel(ctx)
		connected := make(chan struct{}, 1)
		done := make(chan error, 1)
		go func() {
			done <- observer(epochCtx, store, func() {
				select {
				case connected <- struct{}{}:
				default:
				}
			})
		}()
		joined := false
		join := func() bool {
			cancel()
			if joined {
				return true
			}
			select {
			case <-done:
				joined = true
				return true
			case <-time.After(3 * time.Second):
				return false
			}
		}
		dialTimer := time.NewTimer(15 * time.Second)
		dialOK := false
		select {
		case <-connected:
			dialOK = true
		case <-done:
			joined = true
		case <-dialTimer.C:
		case <-ctx.Done():
		}
		dialTimer.Stop()
		if !dialOK {
			if !join() {
				out.Issues = append(out.Issues, "관측기 종료 미확인")
				_ = record(MonitorBlocked, "DIAL_FAILED")
				store.Stop("monitor unjoined observer: lock must remain")
				keepLocks = true
				return out, errors.New("unjoined observer")
			}
			if ctx.Err() != nil {
				_ = record(MonitorStopped, "OPERATOR_STOP")
				return out, nil
			}
			if epoch > monitorReconnectLimit {
				out.Issues = append(out.Issues, "비공개 WS 접속 실패 횟수 초과")
				_ = record(MonitorBlocked, "RECONNECT_LIMIT")
				return out, errors.New("monitor dial limit")
			}
		} else {
			// The connected callback may have raced with a simultaneous socket exit.
			select {
			case <-done:
				joined = true
			default:
			}
			if !joined {
				ticker := time.NewTicker(refreshInterval)
			epochLoop:
				for {
					if e := record(MonitorReconciling, "SIGNED_REST"); e != nil {
						ticker.Stop()
						join()
						return out, e
					}
					before, be := readRecoveryEvidence(dataDir)
					sig, issues, ve := verify(epochCtx)
					if ve == nil && !eventDigest.MatchString(sig) {
						ve = errors.New("invalid signed account digest")
					}
					after, ae := readRecoveryEvidence(dataDir)
					select {
					case <-done:
						joined = true
					default:
					}
					if be != nil || ae != nil || ve != nil || joined || ctx.Err() != nil || before.GapDigest != after.GapDigest {
						if be != nil || ae != nil || ve != nil {
							out.Issues = append(out.Issues, "계정 증적 또는 서명 REST 조회 실패")
						}
						if before.GapDigest != after.GapDigest {
							out.Issues = append(out.Issues, "REST 대조 중 스트림 불연속 기록 변경")
						}
						ticker.Stop()
						break epochLoop
					}
					eventStable := before.EventDigest == after.EventDigest
					if !eventStable {
						out.Issues = append(out.Issues, "REST 대조 중 이벤트 발생: 다음 안정된 signed refresh까지 continuity checkpoint 보류")
						continuityCandidateAt = time.Time{}
						continuityCandidateGap = ""
						out.StreamContinuityProven = false
						out.ContinuityScope = ""
						out.ContinuitySince = time.Time{}
						out.ContinuityGapDigest = ""
					}
					nowREST := time.Now().UTC()
					out.LastSignedRESTAt = nowREST
					out.GapDigest = after.GapDigest
					out.ExposureSHA256 = sig
					if eventStable {
						if continuityCandidateGap == "" {
							continuityCandidateAt = nowREST
							continuityCandidateGap = after.GapDigest
							out.StreamContinuityProven = false
						} else if continuityCandidateGap == after.GapDigest {
							out.StreamContinuityProven = true
							out.ContinuityScope = monitorContinuityScope
							out.ContinuitySince = continuityCandidateAt
							out.ContinuityGapDigest = continuityCandidateGap
						}
					}
					if out.StreamContinuityProven {
						out.Issues = append([]string{"현재 프로세스의 signed checkpoint 이후 로컬 연속성 확인됨; 과거 전체 이력/거래 허가는 별도 검증 필요"}, issues...)
					} else {
						out.Issues = append([]string{"읽기 전용 계정 감시: continuity checkpoint 승인 대기/거래 허가 미증명"}, issues...)
					}
					// Only this state carries the ephemeral snapshot fingerprint. It is
					// immediately erased on STOP/RECOVERING/UNCERTAIN/BLOCKED.
					out.State = MonitorConnected
					out.Reason = "REST_REFRESH"
					if e := journal.append(&out); e != nil {
						ticker.Stop()
						join()
						return out, errors.New("monitor checkpoint write failed")
					}
					if notify != nil {
						notify(out)
					}
					select {
					case <-ticker.C:
						continue
					case <-done:
						joined = true
						ticker.Stop()
						break epochLoop
					case <-ctx.Done():
						ticker.Stop()
						break epochLoop
					}
				}
			}
			if !join() {
				out.Issues = append(out.Issues, "관측기 종료 미확인")
				_ = record(MonitorBlocked, "SOCKET_DROPPED")
				store.Stop("monitor unjoined observer: lock must remain")
				keepLocks = true
				return out, errors.New("unjoined observer")
			}
			if ctx.Err() != nil {
				if e := record(MonitorStopped, "OPERATOR_STOP"); e != nil {
					return out, e
				}
				return out, nil
			}
			if e := record(MonitorUncertain, "SOCKET_DROPPED"); e != nil {
				return out, e
			}
			if epoch > monitorReconnectLimit {
				out.Issues = append(out.Issues, "재연결 횟수 초과: 수동 검토 필요")
				_ = record(MonitorBlocked, "RECONNECT_LIMIT")
				return out, errors.New("monitor reconnect limit")
			}
		}
		out.Reconnects++
		select {
		case <-time.After(time.Duration(epoch) * 300 * time.Millisecond):
		case <-ctx.Done():
			if e := record(MonitorStopped, "OPERATOR_STOP"); e != nil {
				return out, e
			}
			return out, nil
		}
	}
}

// WatchLiveDemoContinuously uses the full independently signed Demo REST
// source, wallet/ownership risk review and existing single-epoch WS adapter.
// Bounded REST cannot certify events missed during a disconnect.
func WatchLiveDemoContinuously(ctx context.Context, client *exchange.Client, creds DemoCredentials, dataDir string, lookback time.Duration, notify func(MonitorCheckpoint)) (MonitorCheckpoint, error) {
	if client == nil || creds.APIKey == "" || creds.Secret == "" || lookback < time.Hour || lookback > 5*24*time.Hour {
		return blankMonitor(), errors.New("invalid signed Demo monitor config")
	}
	verify := func(ctx context.Context) (string, []string, error) {
		signedCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
		defer cancel()
		since := time.Now().UTC().Add(-lookback).UnixMilli()
		baseline, e := client.FetchDemoRecoveryBaseline(signedCtx, creds.APIKey, creds.Secret, since)
		if e != nil {
			return "", nil, errors.New("signed Demo REST failed")
		}
		if e = validBoundedDriftInput(baseline, since, time.Now().UTC()); e != nil {
			return "", nil, errors.New("signed Demo REST freshness invalid")
		}
		sig, e := boundedExposureSignature(baseline)
		if e != nil {
			return "", nil, errors.New("signed Demo account digest invalid")
		}
		owner, e := NewOwnershipStore(dataDir)
		if e != nil {
			return "", nil, errors.New("local ownership integrity invalid")
		}
		review, e := ReviewRecovery(dataDir, owner, baseline, time.Now().UTC())
		if e != nil {
			return "", nil, errors.New("independent review invalid")
		}
		f := sha256.Sum256([]byte(creds.APIKey))
		walletIssues, e := ReviewContinuousWalletExposure(dataDir, hex.EncodeToString(f[:]), baseline)
		if e != nil {
			return "", nil, errors.New("independent local wallet/account review invalid")
		}
		if _, e = ArchiveRecoveryReview(dataDir, review); e != nil {
			return "", nil, errors.New("independent review archive invalid")
		}
		return sig, append(review.Issues, walletIssues...), nil
	}
	obs := func(ctx context.Context, store *EventStore, onConnected func()) error {
		return runDemoAccountObservationReady(ctx, client, creds.APIKey, store, exchange.DialDemoUserStreamURL, 30*time.Minute, onConnected)
	}
	return ContinuousMonitor(ctx, dataDir, obs, verify, 5*time.Minute, notify)
}
