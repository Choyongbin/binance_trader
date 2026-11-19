package trading

// Immutable local read-only evidence summaries, never order authorization.
// Files contain normalized status, hashes and locally generated order IDs only;
// no API key, listenKey, raw WebSocket URL or entire exchange payload.
import (
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
	"sort"
	"strings"
	"time"
)

type StreamReviewReport struct {
	Schema            int                     `json:"schema"`
	ID                string                  `json:"id"`
	ReadAt            time.Time               `json:"read_at"`
	BeforeEventDigest string                  `json:"before_event_digest"`
	AfterEventDigest  string                  `json:"after_event_digest"`
	BeforeGapDigest   string                  `json:"before_gap_digest"`
	AfterGapDigest    string                  `json:"after_gap_digest"`
	EventCount        int                     `json:"event_count"`
	GapCount          int                     `json:"gap_count"`
	Review            StreamCorrelationReview `json:"review"`
	TradingAuthorized bool                    `json:"trading_authorized"` // always false
	SHA256            string                  `json:"sha256"`
}

func (r StreamReviewReport) digest() string {
	r.SHA256 = ""
	b, _ := json.Marshal(r)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

var streamReviewFileRE = regexp.MustCompile(`^[a-f0-9]{32}\.json$`)

// LoadLocalOrderRecords never silently ignores unknown/partial source files.
// Existing request state sidecars are validated by readRecord's contiguous
// sequence check. The caller must NOT read a simultaneously mutating journal.
func LoadLocalOrderRecords(dataDir string) ([]Record, error) {
	dir := filepath.Join(dataDir, "trading", "requests")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []Record{}, nil
	}
	if err != nil {
		return nil, err
	}
	base := regexp.MustCompile(`^bt_[a-f0-9]{28}\.json$`)
	state := regexp.MustCompile(`^bt_[a-f0-9]{28}\.state\.[0-9]{6}\.json$`)
	records := make([]Record, 0)
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 {
			return nil, errors.New("주문 저널에서 폴더/심볼릭 링크 감지")
		}
		name := f.Name()
		if state.MatchString(name) {
			continue
		}
		if !base.MatchString(name) {
			return nil, errors.New("주문 저널의 예상하지 못한 파일")
		}
		r, e := readRecord(filepath.Join(dir, name))
		if e != nil {
			return nil, e
		}
		if r.Plan.ClientOrderID+".json" != name {
			return nil, errors.New("주문 기록 파일 이름 불일치")
		}
		records = append(records, r)
	}
	for _, f := range files {
		if state.MatchString(f.Name()) {
			orig := strings.Split(f.Name(), ".state.")[0] + ".json"
			found := false
			for _, r := range records {
				if r.Plan.ClientOrderID+".json" == orig {
					found = true
					break
				}
			}
			if !found {
				return nil, errors.New("고아 주문 상태 파일 감지")
			}
		}
	}
	return records, nil
}

// DiagnoseStoredStream is an operator/offline reconciliation stage only. A
// stable before/after event-file fingerprint is *not* an unbroken stream:
// historical gap markers and unseen external activity remain possible.
// Its output is immutable, hash-checked and always denies trading.
func DiagnoseStoredStream(dataDir string, store *EventStore, signed []SignedOrderEvidence, now time.Time) (StreamReviewReport, error) {
	if store == nil || dataDir == "" || now.IsZero() {
		return StreamReviewReport{}, errors.New("계정 이벤트 저장소와 유효한 진단 시간이 필요합니다")
	}
	before, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return StreamReviewReport{}, err
	}
	events, err := store.ReadAll()
	if err != nil {
		return StreamReviewReport{}, err
	}
	records, err := LoadLocalOrderRecords(dataDir)
	if err != nil {
		return StreamReviewReport{}, err
	}
	report := StreamReviewReport{Schema: 1, ReadAt: now.UTC(), BeforeEventDigest: before.EventDigest, BeforeGapDigest: before.GapDigest,
		EventCount: before.EventCount, GapCount: before.GapCount}
	report.Review = ReviewStreamOrderCorrelation(events, records, signed, now)
	after, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return StreamReviewReport{}, err
	}
	report.AfterEventDigest, report.AfterGapDigest = after.EventDigest, after.GapDigest
	if before.EventDigest != after.EventDigest || before.GapDigest != after.GapDigest || before.EventCount != after.EventCount || before.GapCount != after.GapCount || len(events) != before.EventCount {
		report.Review.State = "BLOCKED"
		report.Review.Blockers = append(report.Review.Blockers, "진단 도중 계정 이벤트 또는 불연속 기록이 바뀌었습니다")
	}
	if before.GapCount > 0 {
		report.Review.Blockers = append(report.Review.Blockers, "과거 스트림 불연속 마커가 있습니다: 연속성 검증 불가")
	}
	// Even fully matching signals are a historical non-executable diagnostic.
	report.Review.TradingAuthorized = false
	report.Review.StreamContinuityProven = false
	report.Review.ManualReviewRequired = true
	if err := archiveStreamReview(dataDir, &report); err != nil {
		return StreamReviewReport{}, err
	}
	return report, nil
}

func archiveStreamReview(dataDir string, r *StreamReviewReport) error {
	dir := filepath.Join(dataDir, "trading", "stream_reviews")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if r == nil || r.Schema != 1 || r.TradingAuthorized || r.Review.TradingAuthorized || !r.Review.ManualReviewRequired || r.Review.StreamContinuityProven {
		return errors.New("거래 권한이 포함된 진단 보고서 저장 거부")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	r.ID = hex.EncodeToString(id[:])
	r.SHA256 = r.digest()
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, r.ID+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(body, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("이벤트 대조 기록 보관 불확실: %w", err)
	}
	return nil
}

func LoadStreamReviews(dataDir string) ([]StreamReviewReport, error) {
	dir := filepath.Join(dataDir, "trading", "stream_reviews")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []StreamReviewReport{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(files) > 100000 {
		return nil, errors.New("이벤트 대조 기록 개수 한도 초과")
	}
	reports := make([]StreamReviewReport, 0, len(files))
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !streamReviewFileRE.MatchString(f.Name()) {
			return nil, errors.New("알 수 없는 이벤트 대조 기록 파일")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		var r StreamReviewReport
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&r); err != nil {
			return nil, errors.New("이벤트 대조 기록 JSON 손상")
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) || r.Schema != 1 || r.ID+".json" != f.Name() ||
			r.ReadAt.IsZero() || r.SHA256 == "" || r.SHA256 != r.digest() || r.TradingAuthorized ||
			r.Review.TradingAuthorized || !r.Review.ManualReviewRequired || !r.Review.RequiresFullAccountReview || r.Review.StreamContinuityProven ||
			r.Review.State != "BLOCKED" && r.Review.State != "OBSERVED_SIGNALS_MATCH" && r.Review.State != "NO_ORDER_SIGNALS" {
			return nil, errors.New("이벤트 대조 기록 무결성/거래 잠금 오류")
		}
		reports = append(reports, r)
	}
	sort.Slice(reports, func(i, j int) bool {
		return chronologicalIDLess(reports[i].ReadAt, reports[i].ID, reports[j].ReadAt, reports[j].ID)
	})
	return reports, nil
}
