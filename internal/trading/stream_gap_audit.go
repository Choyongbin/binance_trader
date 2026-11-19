package trading

// Stream gap forensics describes immutable local marker provenance without
// claiming that a marker has been reconciled. Existing legacy marker schema 1
// contains no session ID, so timestamp association is only a CANDIDATE.
// This diagnostic must NEVER be used to grant a wallet edit or trading permit.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var gapMarkerName = regexp.MustCompile(`^[0-9a-f]{32}\.json$`)
var allowedGapReasons = map[string]bool{
	"STARTUP": true, "DISCONNECT": true, "ROTATE": true,
	"EXPIRED": true, "KEEPALIVE_FAILED": true, "UNKNOWN_EVENT": true,
	"CANCEL_UNCERTAIN": true, "STREAM_DISCONTINUITY": true,
}

type gapMarker struct {
	reason      string
	at          time.Time
	fingerprint string
}

type GapSessionSummary struct {
	StartedAt                   time.Time      `json:"started_at"`
	FinishedAt                  time.Time      `json:"finished_at"`
	Stage                       string         `json:"stage"`
	MarkerCount                 int            `json:"marker_count"`
	Reasons                     map[string]int `json:"reasons"`
	RepeatedDisconnectCandidate bool           `json:"repeated_disconnect_candidate"`
	// Strictly indicates timestamp overlap with a validated historical
	// review, NOT proof that the gap is expected, harmless, or repaired.
	Attribution string `json:"attribution"`
}

type GapAudit struct {
	Schema                    int                 `json:"schema"`
	CheckedAt                 time.Time           `json:"checked_at"`
	MarkerCount               int                 `json:"marker_count"`
	Reasons                   map[string]int      `json:"reasons"`
	MarkerDigest              string              `json:"marker_digest"`
	SessionCount              int                 `json:"session_count"`
	Sessions                  []GapSessionSummary `json:"sessions"`
	OutsideHistoricalSessions int                 `json:"outside_historical_sessions"`
	IncompleteSessionCount    int                 `json:"incomplete_session_count"`
	AmbiguousMatchCount       int                 `json:"ambiguous_match_count"`
	TradingAuthorized         bool                `json:"trading_authorized"`
	StreamContinuityProven    bool                `json:"stream_continuity_proven"`
	ManualReviewRequired      bool                `json:"manual_review_required"`
	HistoricalOnly            bool                `json:"historical_only"`
	Note                      string              `json:"note"`
}

func scanGapMarkers(dataDir string) ([]gapMarker, string, error) {
	dir := filepath.Join(dataDir, "trading", "stream_discontinuities")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		empty := sha256.Sum256(nil)
		return []gapMarker{}, hex.EncodeToString(empty[:]), nil
	}
	if err != nil {
		return nil, "", err
	}
	if len(files) > 100000 {
		return nil, "", errors.New("계정 스트림 기록 개수가 안전 한도를 초과했습니다")
	}
	out := make([]gapMarker, 0, len(files))
	digest := sha256.New()
	for _, f := range files {
		if f.Type()&os.ModeSymlink != 0 || !f.Type().IsRegular() || !gapMarkerName.MatchString(f.Name()) {
			return nil, "", errors.New("알 수 없는 불연속 기록 파일: 수동 확인 필요")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, "", err
		}
		if len(raw) > 4096 {
			return nil, "", errors.New("불연속 기록 길이가 비정상입니다")
		}
		var m struct {
			Schema int    `json:"schema"`
			Reason string `json:"reason"`
			At     string `json:"at"`
		}
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.DisallowUnknownFields()
		if err := d.Decode(&m); err != nil {
			return nil, "", errors.New("불연속 기록 JSON/스키마 손상")
		}
		var extra any
		if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, "", errors.New("불연속 기록에 추가 JSON이 포함되었습니다")
		}
		at, err := time.Parse(time.RFC3339Nano, m.At)
		if err != nil || m.Schema != 1 || !allowedGapReasons[m.Reason] || at.After(time.Now().Add(2*time.Minute)) {
			return nil, "", errors.New("불연속 기록의 시각/사유/스키마가 유효하지 않습니다")
		}
		sum := sha256.Sum256(raw)
		part := fmt.Sprintf("%s:%x", f.Name(), sum[:])
		// Length-prefix to avoid ambiguous concatenation.
		fmt.Fprintf(digest, "%d:%s", len(part), part)
		out = append(out, gapMarker{reason: m.Reason, at: at.UTC(), fingerprint: part})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].at.Equal(out[j].at) {
			return out[i].fingerprint < out[j].fingerprint
		}
		return out[i].at.Before(out[j].at)
	})
	return out, hex.EncodeToString(digest.Sum(nil)), nil
}

// AnalyzeStreamGaps does not mutate data, close a WS, trigger a REST call,
// suppress a blocker or reuse any history as authorization.
func AnalyzeStreamGaps(dataDir string, now time.Time) (GapAudit, error) {
	report := GapAudit{Schema: 1, CheckedAt: now.UTC(), Reasons: map[string]int{}, Sessions: []GapSessionSummary{},
		ManualReviewRequired: true, HistoricalOnly: true,
		Note: "기존 불연속 기록은 해제되지 않았습니다. 기간 겹침은 진단 연관 후보일 뿐 무누락/실주문 안전성 증거가 아닙니다."}
	if dataDir == "" || now.IsZero() {
		return report, errors.New("유효한 데이터 경로와 진단 시각이 필요합니다")
	}
	markers, before, e := scanGapMarkers(dataDir)
	if e != nil {
		return report, e
	}
	sessions, e := LoadRecoveryProbes(dataDir) // validates immutable start/end SHA
	if e != nil {
		return report, e
	}
	watches, e := LoadReconnectWatches(dataDir) // independent immutable watch START/END evidence
	if e != nil {
		return report, e
	}
	_, after, e := scanGapMarkers(dataDir)
	if e != nil {
		return report, e
	}
	if before != after {
		return report, errors.New("진단 중 불연속 기록이 변경되었습니다: 다시 조회하세요")
	}
	report.MarkerDigest = before
	report.MarkerCount = len(markers)
	report.SessionCount = len(sessions) + len(watches)
	for _, s := range sessions {
		item := GapSessionSummary{StartedAt: s.StartedAt, FinishedAt: s.FinishedAt, Stage: s.Stage, Reasons: map[string]int{}, Attribution: "HISTORICAL_TIMESTAMP_OVERLAP_ONLY"}
		if s.Stage == "INTERRUPTED" {
			report.IncompleteSessionCount++
			item.Attribution = "INTERRUPTED_UNVERIFIED"
		}
		report.Sessions = append(report.Sessions, item)
	}
	for _, w := range watches {
		item := GapSessionSummary{StartedAt: w.StartedAt, FinishedAt: w.FinishedAt, Stage: w.Stage, Reasons: map[string]int{}, Attribution: "HISTORICAL_TIMESTAMP_OVERLAP_ONLY"}
		if w.Stage == "INTERRUPTED" {
			report.IncompleteSessionCount++
			item.Attribution = "INTERRUPTED_UNVERIFIED"
		}
		report.Sessions = append(report.Sessions, item)
	}
	for _, m := range markers {
		report.Reasons[m.reason]++
		matches := []int{}
		for i, s := range report.Sessions {
			if s.Stage == "INTERRUPTED" {
				continue
			}
			// Legacy marker has no session ID; require exact inclusion with
			// a validated start/end pair rather than inventing provenance.
			if !m.at.Before(s.StartedAt) && !m.at.After(s.FinishedAt) {
				matches = append(matches, i)
			}
		}
		if len(matches) != 1 {
			report.OutsideHistoricalSessions++
			if len(matches) > 1 {
				report.AmbiguousMatchCount++
			}
			continue
		}
		s := &report.Sessions[matches[0]]
		s.MarkerCount++
		s.Reasons[m.reason]++
	}
	for i := range report.Sessions {
		// Two disconnects inside the same verified diagnostic interval can
		// have other causes. Treat them as a candidate, not an excused gap.
		s := &report.Sessions[i]
		s.RepeatedDisconnectCandidate = s.Reasons["DISCONNECT"] > 1
	}
	return report, nil
}
