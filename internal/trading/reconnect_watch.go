package trading

// Read-only, operator-initiated Demo reconnection engineering test. Every
// unexpected stream exit invalidates its REST snapshot; a NEW successful
// handshake is required before a NEW independent signed REST reconciliation.
// Binance user-data events have no global lossless sequence number: neither
// this test nor a successful reconnect is a trading permit or a gap clearance.
import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

type ReconnectEpoch struct {
	Number             int       `json:"number"`
	Connected          bool      `json:"connected"`
	SignedRESTReviewed bool      `json:"signed_rest_reviewed"`
	RESTFinishedAt     time.Time `json:"rest_finished_at,omitempty"`
	ExposureSHA256     string    `json:"exposure_sha256,omitempty"`
	RecoveryReviewFile string    `json:"recovery_review_file,omitempty"`
	EndReason          string    `json:"end_reason"` // FINAL_STOP, SOCKET_DROPPED, CONNECT_FAILED, REST_FAILED
}

type ReconnectWatchReport struct {
	Schema                 int              `json:"schema"`
	ID                     string           `json:"id"`
	Stage                  string           `json:"stage"` // STARTED, OBSERVED (historical only), BLOCKED
	StartedAt              time.Time        `json:"started_at"`
	FinishedAt             time.Time        `json:"finished_at,omitempty"`
	RequestedSeconds       int              `json:"requested_seconds"`
	Epochs                 []ReconnectEpoch `json:"epochs"`
	Reconnects             int              `json:"reconnects"`
	FinalRESTReviewed      bool             `json:"final_rest_reviewed"`
	LastExposureSHA256     string           `json:"last_exposure_sha256,omitempty"`
	Blockers               []string         `json:"blockers"`
	HistoricalOnly         bool             `json:"historical_only"`
	TradingAuthorized      bool             `json:"trading_authorized"`
	StreamContinuityProven bool             `json:"stream_continuity_proven"`
	ManualReviewRequired   bool             `json:"manual_review_required"`
	SHA256                 string           `json:"sha256"`
}

func (r ReconnectWatchReport) digest() string {
	r.SHA256 = ""
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (r *ReconnectWatchReport) seal() { r.SHA256 = r.digest() }

// ReconnectObserver opens exactly ONE epoch, calls connected only after the
// handshake, and returns at the first disconnect. The production adapter also
// handles listenKey expiry and keepalive failure without silently redialing.
type ReconnectObserver RecoveryObserver

// A fixed maximum prevents an unintended automatic infinite credential loop.
const maxWatchReconnects = 2

// WatchDemoReconnect is an injectable pure orchestration seam for deterministic
// forced-disconnect tests. Production uses WatchLiveDemoReconnect below.
func WatchDemoReconnect(ctx context.Context, reader DemoRecoveryReader, creds DemoCredentials, dataDir string, sinceMS int64, observer ReconnectObserver, duration time.Duration) (ReconnectWatchReport, error) {
	return watchDemoReconnect(ctx, reader, creds, dataDir, sinceMS, observer, duration, nil)
}

// epochReady is a TEST-ONLY synchronization hook. A mock observer can drop a
// socket exactly after signed reconciliation without relying on wall clocks;
// production passes nil and is never influenced by test fault injection.
func watchDemoReconnect(ctx context.Context, reader DemoRecoveryReader, creds DemoCredentials, dataDir string, sinceMS int64, observer ReconnectObserver, duration time.Duration, epochReady func(int)) (out ReconnectWatchReport, retErr error) {
	if reader == nil || observer == nil || creds.APIKey == "" || creds.Secret == "" || dataDir == "" || duration <= 0 || duration > 3*time.Minute || sinceMS <= 0 || time.Since(time.UnixMilli(sinceMS)) > 6*24*time.Hour || time.UnixMilli(sinceMS).After(time.Now()) {
		return out, errors.New("유효한 Demo 관측/서명 조회 설정 필요")
	}
	sess, err := openReconnectArchive(dataDir)
	if err != nil {
		return out, err
	}
	out = ReconnectWatchReport{Schema: 1, ID: sess.id, Stage: "STARTED", StartedAt: time.Now().UTC(), RequestedSeconds: int((duration + time.Second - 1) / time.Second), Epochs: []ReconnectEpoch{}, Blockers: []string{}, HistoricalOnly: true, TradingAuthorized: false, StreamContinuityProven: false, ManualReviewRequired: true}
	if err := sess.write(&out, "start"); err != nil {
		sess.closed = true
		return out, err
	}
	defer func() {
		out.FinishedAt = time.Now().UTC()
		if out.Stage == "STARTED" || retErr != nil {
			out.Stage = "BLOCKED"
		}
		if out.Stage == "BLOCKED" && len(out.Blockers) == 0 {
			out.Blockers = append(out.Blockers, "관측 미완료")
		}
		out.Blockers = append(out.Blockers, "유한 Demo 재연결 진단은 이벤트 무누락 및 거래 허가 증명이 아닙니다")
		if e := sess.write(&out, "end"); e != nil {
			sess.closed = true
			if retErr == nil {
				retErr = errors.New("재연결 증적 종료 기록 실패; 잠금 보존")
			}
		} else if e := sess.close(); e != nil && retErr == nil {
			retErr = errors.New("재연결 증적 잠금 종료 실패")
		}
	}()
	store, err := OpenEventStore(dataDir)
	if err != nil {
		out.Blockers = append(out.Blockers, "계정 이벤트 저장소 잠금 또는 검증 실패")
		return out, errors.New("계정 이벤트 저장소 이용 불가")
	}
	// Never let an observer outlive its event store; each epoch is joined below.
	closeStoreSafely := true
	defer func() {
		if !closeStoreSafely {
			store.Stop("관측기 종료 확인 실패: 영속 잠금 보존")
			return
		}
		if e := store.Close(); e != nil {
			out.Blockers = append(out.Blockers, "계정 이벤트 저장소 종료 잠금 불확실")
			if retErr == nil {
				retErr = errors.New("계정 이벤트 저장소 종료 불확실")
			}
		}
	}()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	for epochNo := 1; ; epochNo++ {
		if ctx.Err() != nil {
			out.Blockers = append(out.Blockers, "상위 요청 취소")
			return out, errors.New("요청 취소")
		}
		ep := ReconnectEpoch{Number: epochNo, EndReason: "CONNECT_FAILED"}
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
		unexpectedStop := false
		// Every path MUST wait for this epoch to terminate before another begins.
		join := func(expected bool) bool {
			if !joined && expected {
				// An observer may have exited just before intentional cancellation.
				select {
				case <-done:
					joined = true
					unexpectedStop = true
				default:
				}
			}
			cancel()
			if !joined {
				select {
				case <-done:
					joined = true
				case <-time.After(3 * time.Second):
				}
			}
			if !joined {
				out.Blockers = append(out.Blockers, "이전 WS 관측기 종료 미확인: 다음 연결 금지")
				closeStoreSafely = false
				return false
			}
			return true
		}
		connectionTimer := time.NewTimer(15 * time.Second)
		timedOut := false
		select {
		case <-connected:
			ep.Connected = true
		case <-done:
			joined = true
		case <-connectionTimer.C:
			timedOut = true
		case <-deadline.C:
			timedOut = true
		case <-ctx.Done():
			timedOut = true
		}
		connectionTimer.Stop()
		if !ep.Connected {
			if !join(false) {
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("계정 관측기 종료 불확실")
			}
			if timedOut {
				ep.EndReason = "CONNECT_TIMEOUT"
			}
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "비공개 WS 재연결 또는 초기 접속 실패: 현재 계정 상태 불확실")
			return out, errors.New("계정 스트림 연결 실패")
		}
		ep.EndReason = "REST_FAILED"
		// Gap digest includes THIS epoch's STARTUP. A new gap before the signed
		// baseline is finalized invalidates this epoch (never auto-clear it).
		pre, preErr := readRecoveryEvidence(dataDir)
		if preErr != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "초기 증적 검증 실패")
			return out, errors.New("계정 증적 오류")
		}
		baseline, readErr := reader.FetchDemoRecoveryBaseline(obsCtx, creds.APIKey, creds.Secret, sinceMS)
		post, postErr := readRecoveryEvidence(dataDir)
		if readErr != nil || postErr != nil || ctx.Err() != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "재접속 후 서명 REST 조회 실패 또는 중단")
			return out, errors.New("서명 계정 재조회 실패")
		}
		// Observer return and an appended DISCONNECT must NOT be consumed and
		// mistaken for a healthy connection, even if REST completed normally.
		select {
		case <-done:
			joined = true
		default:
		}
		if joined || pre.GapDigest != post.GapDigest {
			join(false)
			ep.EndReason = "SOCKET_DROPPED"
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "서명 REST 조회 도중 계정 스트림 종료/불연속")
			return out, errors.New("서명 재조회와 WS 동시성 불명확")
		}
		if e := validBoundedDriftInput(baseline, sinceMS, time.Now().UTC()); e != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "서명 계정 조회 범위/신선도 검증 실패")
			return out, errors.New("서명 조회 정합성 실패")
		}
		exposure, e := boundedExposureSignature(baseline)
		if e != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "계정 상태 서명 지문 생성 실패")
			return out, errors.New("계정 상태 검증 실패")
		}
		own, e := NewOwnershipStore(dataDir)
		if e != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "소유권 원장 검증 실패")
			return out, errors.New("소유권 원장 검사 실패")
		}
		review, e := ReviewRecovery(dataDir, own, baseline, time.Now().UTC())
		if e != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "서명 이력 독립 대조 실패")
			return out, errors.New("서명 이력 대조 실패")
		}
		reviewPath, e := ArchiveRecoveryReview(dataDir, review)
		if e != nil {
			join(false)
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "계정 대조 결과 보관 실패")
			return out, errors.New("대조 결과 보관 실패")
		}
		ep.SignedRESTReviewed = true
		ep.RESTFinishedAt = baseline.FinishedAt
		ep.ExposureSHA256 = exposure
		ep.RecoveryReviewFile = filepath.Base(reviewPath)
		if out.LastExposureSHA256 != "" && out.LastExposureSHA256 != exposure {
			out.Blockers = append(out.Blockers, "재연결 전후 전체 계정 지문 변화: 독립 수동 검토 필요")
		}
		out.LastExposureSHA256 = exposure
		if post.EventDigest != pre.EventDigest {
			out.Blockers = append(out.Blockers, "서명 REST 중 이벤트 발생: 순서/동시성 수동 검토 필요")
		}
		out.Blockers = append(out.Blockers, review.Issues...)
		select {
		case <-done:
			joined = true
		default:
		}
		if joined {
			join(false)
			ep.EndReason = "SOCKET_DROPPED"
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "계정 조회 직후 WS 종료: 재검증 필요")
			return out, errors.New("계정 WS 종료 불확실")
		}
		// No auto-trading: every epoch is untrusted even after a fresh signed REST
		// baseline. A lost socket starts an entirely NEW listenKey and REST review.
		if epochReady != nil {
			epochReady(epochNo)
		}
		select {
		case <-deadline.C:
			finalPre, e := readRecoveryEvidence(dataDir)
			if e != nil {
				join(false)
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("최종 증적 검사 실패")
			}
			final, fe := reader.FetchDemoRecoveryBaseline(obsCtx, creds.APIKey, creds.Secret, sinceMS)
			finalPost, pe := readRecoveryEvidence(dataDir)
			select {
			case <-done:
				joined = true
			default:
			}
			if fe != nil || pe != nil || joined || finalPre.GapDigest != finalPost.GapDigest || ctx.Err() != nil {
				join(false)
				ep.EndReason = "REST_FAILED"
				out.Epochs = append(out.Epochs, ep)
				out.Blockers = append(out.Blockers, "마지막 서명 REST/WS 동시 검증 실패")
				return out, errors.New("마지막 계정 정합성 검사 실패")
			}
			if e := validBoundedDriftInput(final, sinceMS, time.Now().UTC()); e != nil || final.StartedAt.Before(baseline.FinishedAt) {
				join(false)
				out.Epochs = append(out.Epochs, ep)
				out.Blockers = append(out.Blockers, "마지막 REST 신선도 검증 실패")
				return out, errors.New("마지막 REST 시간 오류")
			}
			finalSig, se := boundedExposureSignature(final)
			if se != nil {
				join(false)
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("마지막 계정 서명 지문 오류")
			}
			if exposure != finalSig {
				out.Blockers = append(out.Blockers, "마지막 서명 REST의 계정 상태가 관측 시작과 다름")
			}
			if finalPre.EventDigest != finalPost.EventDigest {
				out.Blockers = append(out.Blockers, "마지막 REST 도중 이벤트 발생: 수동 검토")
			}
			out.LastExposureSHA256 = finalSig
			out.FinalRESTReviewed = true
			ep.EndReason = "FINAL_STOP"
			// A deliberately stopped observer is not proof that all events arrived.
			if !join(true) {
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("종료 관측기 합류 실패")
			}
			if unexpectedStop {
				out.Epochs = append(out.Epochs, ep)
				out.Blockers = append(out.Blockers, "마지막 REST 이후 관측기 비정상 조기 종료")
				return out, errors.New("관측기 조기 종료")
			}
			if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("종료 불연속 기록 실패")
			}
			out.Epochs = append(out.Epochs, ep)
			out.Stage = "OBSERVED"
			return out, nil
		case <-done:
			joined = true
			ep.EndReason = "SOCKET_DROPPED"
			if unexpectedStop {
				out.Epochs = append(out.Epochs, ep)
				out.Blockers = append(out.Blockers, "마지막 REST 이후 관측기 비정상 조기 종료")
				return out, errors.New("관측기 조기 종료")
			}
			if e := store.RecordDiscontinuity("DISCONNECT"); e != nil {
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("계정 중단 증적 기록 실패")
			}
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "사설 WS 중단: 이전 계정 조회 지문은 무효, 새 서명 REST 대조 필수")
			if epochNo > maxWatchReconnects {
				out.Blockers = append(out.Blockers, "재연결 상한 도달")
				return out, errors.New("자동 재연결 횟수 초과")
			}
			out.Reconnects++
			// If duration already elapsed after a drop, do not claim OBSERVED.
			select {
			case <-ctx.Done():
				return out, errors.New("관측 취소")
			case <-time.After(time.Duration(epochNo) * 100 * time.Millisecond):
			}
		case <-ctx.Done():
			if !join(false) {
				out.Epochs = append(out.Epochs, ep)
				return out, errors.New("계정 관측기 합류 실패")
			}
			ep.EndReason = "CANCELED"
			out.Epochs = append(out.Epochs, ep)
			out.Blockers = append(out.Blockers, "요청 취소")
			return out, errors.New("관측 취소")
		}
	}
}

func WatchLiveDemoReconnect(ctx context.Context, client *exchange.Client, creds DemoCredentials, dataDir string, sinceMS int64, dial exchange.UserStreamDialer, duration time.Duration) (ReconnectWatchReport, error) {
	if client == nil {
		return ReconnectWatchReport{}, errors.New("Demo 클라이언트 필요")
	}
	return WatchDemoReconnect(ctx, client, creds, dataDir, sinceMS, func(ctx context.Context, store *EventStore, connected func()) error {
		return runDemoAccountObservationReady(ctx, client, creds.APIKey, store, dial, 30*time.Minute, connected)
	}, duration)
}

// SHA-256-sealed START and END files and a crash-preserving exclusive lock.
// These files contain only finite, sanitized observations, never credentials.
type reconnectArchive struct {
	dir, lock, id string
	closed        bool
}

func openReconnectArchive(dataDir string) (*reconnectArchive, error) {
	old, e := LoadReconnectWatches(dataDir)
	if e != nil {
		return nil, e
	}
	for _, r := range old {
		if r.Stage == "INTERRUPTED" {
			return nil, errors.New("이전 미완료 재연결 진단은 수동 조사 필요")
		}
	}
	dir := filepath.Join(dataDir, "trading", "reconnect_watch_sessions")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	lock := filepath.Join(dir, "exclusive.active")
	if e := os.Mkdir(lock, 0700); e != nil {
		return nil, errors.New("재연결 진단 중이거나 이전 충돌 잠금 수동 확인 필요")
	}
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return nil, e
	}
	id := hex.EncodeToString(nonce[:])
	body, _ := json.Marshal(map[string]any{"schema": 1, "id": id})
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
	return &reconnectArchive{dir: dir, lock: lock, id: id}, nil
}
func (a *reconnectArchive) write(r *ReconnectWatchReport, phase string) error {
	if a == nil || a.closed || r == nil || r.ID != a.id || r.Schema != 1 || r.TradingAuthorized || r.StreamContinuityProven || !r.HistoricalOnly || !r.ManualReviewRequired || phase != "start" && phase != "end" {
		return errors.New("재연결 보관 기록 검증 실패")
	}
	r.seal()
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(a.dir, r.ID+"."+phase+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(append(b, '\n'))
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	return e
}
func (a *reconnectArchive) close() error {
	if a.closed {
		return nil
	}
	raw, e := os.ReadFile(filepath.Join(a.lock, "owner.json"))
	if e != nil {
		return e
	}
	var owner struct {
		Schema int    `json:"schema"`
		ID     string `json:"id"`
	}
	if json.Unmarshal(raw, &owner) != nil || owner.ID != a.id || owner.Schema != 1 {
		return errors.New("재연결 잠금 소유권 검증 실패")
	}
	if e := os.Remove(filepath.Join(a.lock, "owner.json")); e != nil {
		return e
	}
	if e := os.Remove(a.lock); e != nil {
		return e
	}
	a.closed = true
	return nil
}
func LoadReconnectWatches(dataDir string) ([]ReconnectWatchReport, error) {
	dir := filepath.Join(dataDir, "trading", "reconnect_watch_sessions")
	fs, e := os.ReadDir(dir)
	if errors.Is(e, os.ErrNotExist) {
		return []ReconnectWatchReport{}, nil
	}
	if e != nil {
		return nil, e
	}
	starts := map[string]ReconnectWatchReport{}
	ends := map[string]ReconnectWatchReport{}
	for _, f := range fs {
		if f.Name() == "exclusive.active" && f.IsDir() {
			continue
		}
		name := f.Name()
		parts := strings.Split(name, ".")
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || len(parts) != 3 || len(parts[0]) != 32 || parts[2] != "json" || (parts[1] != "start" && parts[1] != "end") {
			return nil, errors.New("알 수 없는 재연결 기록 파일")
		}
		if _, e := hex.DecodeString(parts[0]); e != nil {
			return nil, errors.New("재연결 기록 이름 오류")
		}
		b, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			return nil, e
		}
		var r ReconnectWatchReport
		decoder := json.NewDecoder(strings.NewReader(string(b)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&r) != nil || decoder.Decode(new(any)) != io.EOF || r.Schema != 1 || r.ID != parts[0] || r.SHA256 != r.digest() || r.TradingAuthorized || r.StreamContinuityProven || !r.HistoricalOnly || !r.ManualReviewRequired || r.RequestedSeconds <= 0 || r.Blockers == nil || r.Epochs == nil || r.StartedAt.IsZero() {
			return nil, errors.New("재연결 증적 무결성 실패")
		}
		if parts[1] == "start" {
			if r.Stage != "STARTED" || len(r.Epochs) != 0 || !r.FinishedAt.IsZero() {
				return nil, errors.New("재연결 시작 상태 오류")
			}
			starts[r.ID] = r
		} else {
			if (r.Stage != "BLOCKED" && r.Stage != "OBSERVED") || r.FinishedAt.Before(r.StartedAt) || r.FinishedAt.IsZero() || (r.Stage == "OBSERVED" && !r.FinalRESTReviewed) {
				return nil, errors.New("재연결 종료 상태 오류")
			}
			ends[r.ID] = r
		}
	}
	out := make([]ReconnectWatchReport, 0, len(starts))
	for id, start := range starts {
		if end, ok := ends[id]; ok {
			if !end.StartedAt.Equal(start.StartedAt) {
				return nil, errors.New("재연결 START/END 불일치")
			}
			out = append(out, end)
		} else {
			start.Stage = "INTERRUPTED"
			start.SHA256 = ""
			start.Blockers = []string{"미완료 재연결 검사: 수동 조사 필요"}
			out = append(out, start)
		}
	}
	for id := range ends {
		if _, ok := starts[id]; !ok {
			return nil, errors.New("시작 없는 재연결 종료 기록")
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return chronologicalIDLess(out[i].StartedAt, out[i].ID, out[j].StartedAt, out[j].ID)
	})
	return out, nil
}
