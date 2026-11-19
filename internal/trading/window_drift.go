package trading

// Bounded, signed, read-only dual-sample drift detection. This is NOT
// continuous account synchronization: an external flat round trip between
// samples, expired REST history, silent user-data-stream loss and asynchronous
// exchange settlement remain possible. No HTTP handler or order path calls it.
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
	"sort"
	"strings"
	"time"

	"binance_trader/internal/exchange"
)

type WindowDemoReader interface {
	FetchDemoRecoveryBaseline(context.Context, string, string, int64) (exchange.RecoveryBaseline, error)
}

type BoundedDriftReview struct {
	Schema                   int       `json:"schema"`
	ID                       string    `json:"id"`
	CheckedAt                time.Time `json:"checked_at"`
	SHA256                   string    `json:"sha256"`
	State                    string    `json:"state"` // BLOCKED / OBSERVED_STABLE_UNPROVEN
	FirstAt                  time.Time `json:"first_at"`
	SecondAt                 time.Time `json:"second_at"`
	InitialExposureSHA256    string    `json:"initial_exposure_sha256"`
	FinalExposureSHA256      string    `json:"final_exposure_sha256"`
	BeforeEvidenceSHA256     string    `json:"before_evidence_sha256"`
	AfterEvidenceSHA256      string    `json:"after_evidence_sha256"`
	EventCount               int       `json:"event_count"`
	GapCount                 int       `json:"gap_count"`
	LocalOrderCount          int       `json:"local_order_count"`
	Blockers                 []string  `json:"blockers"`
	TradingAuthorized        bool      `json:"trading_authorized"`         // NEVER true
	ContinuousCoverageProven bool      `json:"continuous_coverage_proven"` // NEVER true
	ManualReviewRequired     bool      `json:"manual_review_required"`     // ALWAYS true
}

// Account-exposure signature compares signed raw wallet balances, *position
// sizes*, both kinds of pending orders, and bounded recent trade/income
// history. It intentionally excludes changing mark-to-market equity and
// timestamps, but a matching digest NEVER establishes an uninterrupted stream.
func boundedExposureSignature(b exchange.RecoveryBaseline) (string, error) {
	if err := exchange.ValidatePortfolioSnapshot(b.Portfolio); err != nil {
		return "", err
	}
	if b.OpenOrders == nil || b.OpenAlgos == nil || b.RecentBTCOrders == nil || b.RecentBTCAlgos == nil || b.RecentBTCTrades == nil || b.RecentIncome == nil {
		return "", errors.New("서명된 전체 계정 데이터 일부 누락")
	}
	type asset struct{ Code, Wallet string }
	type position struct {
		Symbol, Side, Quantity string
		Isolated               bool
	}
	c := struct {
		CanTrade     bool
		MultiAssets  bool
		Assets       []asset
		Positions    []position
		Orders       []exchange.Order
		Algos        []exchange.AlgoOrder
		RecentOrders []exchange.Order
		RecentAlgos  []exchange.AlgoOrder
		Trades       []exchange.AccountTrade
		Income       []exchange.IncomeRecord
	}{CanTrade: b.Portfolio.CanTrade, MultiAssets: b.Portfolio.MultiAssetsMargin}
	for _, a := range b.Portfolio.Assets {
		c.Assets = append(c.Assets, asset{a.Asset, string(a.WalletBalance)})
	}
	for _, p := range b.Portfolio.Positions {
		c.Positions = append(c.Positions, position{p.Symbol, p.PositionSide, string(p.PositionAmt), p.Isolated})
	}
	sort.Slice(c.Assets, func(i, j int) bool { return c.Assets[i].Code < c.Assets[j].Code })
	sort.Slice(c.Positions, func(i, j int) bool {
		a, d := c.Positions[i], c.Positions[j]
		return a.Symbol+":"+a.Side < d.Symbol+":"+d.Side
	})
	c.Orders = append([]exchange.Order{}, b.OpenOrders...)
	c.Algos = append([]exchange.AlgoOrder{}, b.OpenAlgos...)
	c.RecentOrders = append([]exchange.Order{}, b.RecentBTCOrders...)
	c.RecentAlgos = append([]exchange.AlgoOrder{}, b.RecentBTCAlgos...)
	c.Trades = append([]exchange.AccountTrade{}, b.RecentBTCTrades...)
	c.Income = append([]exchange.IncomeRecord{}, b.RecentIncome...)
	sort.Slice(c.Orders, func(i, j int) bool { return c.Orders[i].OrderID < c.Orders[j].OrderID })
	sort.Slice(c.Algos, func(i, j int) bool { return c.Algos[i].AlgoID < c.Algos[j].AlgoID })
	sort.Slice(c.RecentOrders, func(i, j int) bool { return c.RecentOrders[i].OrderID < c.RecentOrders[j].OrderID })
	sort.Slice(c.RecentAlgos, func(i, j int) bool { return c.RecentAlgos[i].AlgoID < c.RecentAlgos[j].AlgoID })
	sort.Slice(c.Trades, func(i, j int) bool { return c.Trades[i].TradeID < c.Trades[j].TradeID })
	sort.Slice(c.Income, func(i, j int) bool {
		a, d := c.Income[i], c.Income[j]
		if a.TimeMS != d.TimeMS {
			return a.TimeMS < d.TimeMS
		}
		if a.TransactionID != d.TransactionID {
			return a.TransactionID < d.TransactionID
		}
		if a.Type != d.Type {
			return a.Type < d.Type
		}
		if a.Symbol != d.Symbol {
			return a.Symbol < d.Symbol
		}
		return a.Asset < d.Asset
	})
	// Duplicate IDs make the data ambiguous even if both snapshots repeat it.
	for _, orders := range [][]exchange.Order{c.Orders, c.RecentOrders} {
		for i := 1; i < len(orders); i++ {
			if orders[i].OrderID <= 0 || orders[i-1].OrderID == orders[i].OrderID {
				return "", errors.New("서명된 주문 ID 중복/오류")
			}
		}
		if len(orders) > 0 && orders[0].OrderID <= 0 {
			return "", errors.New("서명된 주문 ID 오류")
		}
	}
	for _, algos := range [][]exchange.AlgoOrder{c.Algos, c.RecentAlgos} {
		for i := 1; i < len(algos); i++ {
			if algos[i].AlgoID <= 0 || algos[i-1].AlgoID == algos[i].AlgoID {
				return "", errors.New("서명된 Algo ID 중복/오류")
			}
		}
		if len(algos) > 0 && algos[0].AlgoID <= 0 {
			return "", errors.New("서명된 Algo ID 오류")
		}
	}
	for i, t := range c.Trades {
		if t.TradeID <= 0 || i > 0 && c.Trades[i-1].TradeID == t.TradeID {
			return "", errors.New("서명된 체결 ID 중복/오류")
		}
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	d := sha256.Sum256(raw)
	return hex.EncodeToString(d[:]), nil
}

func validBoundedDriftInput(b exchange.RecoveryBaseline, since int64, now time.Time) error {
	if b.Environment != "demo" || b.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" ||
		b.WindowStartMS != since || b.WindowEndMS != b.StartedAt.UnixMilli() ||
		b.StartedAt.IsZero() || b.WindowEndMS <= since || b.FinishedAt.Before(b.StartedAt) ||
		b.FinishedAt.Sub(b.StartedAt) > 30*time.Second || b.FinishedAt.After(now.Add(time.Second)) ||
		now.Sub(b.FinishedAt) > 5*time.Second {
		return errors.New("오래되거나 불완전한 Demo 서명 관측")
	}
	_, err := boundedExposureSignature(b)
	return err
}

// DiagnoseBoundedDrift performs two sequential authenticated REST bracketing
// reads and checks that *locally persisted* event/gap and original journal
// contents did not change during either read. This function never creates a
// stream, unlocks trading, or archives secret-bearing REST payloads.
func DiagnoseBoundedDrift(ctx context.Context, dataDir string, reader WindowDemoReader,
	creds DemoCredentials, accountFingerprint string, fromMS int64, clock func() time.Time) (BoundedDriftReview, error) {
	r := BoundedDriftReview{Schema: 1, State: "BLOCKED", ManualReviewRequired: true, Blockers: []string{}}
	if reader == nil || dataDir == "" || creds.APIKey == "" || creds.Secret == "" || clock == nil || fromMS <= 0 || ctx == nil {
		return r, errors.New("읽기 전용 Demo 진단 입력값이 부족합니다")
	}
	keySum := sha256.Sum256([]byte(creds.APIKey))
	if hex.EncodeToString(keySum[:]) != accountFingerprint {
		return r, errors.New("연결된 Demo 계정 지문이 다릅니다")
	}
	now := clock().UTC()
	if now.IsZero() || fromMS >= now.UnixMilli() || now.Sub(time.UnixMilli(fromMS)) > 6*24*time.Hour {
		return r, errors.New("최근 6일 이내 진단 범위가 필요합니다")
	}
	before, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return r, err
	}
	recordsBefore, err := LoadLocalOrderRecords(dataDir)
	if err != nil {
		return r, err
	}
	r.BeforeEvidenceSHA256 = digestEvidence(before)
	r.EventCount = before.EventCount
	r.GapCount = before.GapCount
	r.LocalOrderCount = len(recordsBefore)
	if err = ctx.Err(); err != nil {
		return r, err
	}
	first, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return r, errors.New("첫 서명 계정 조회 실패: 수동 재검증 필요")
	}
	if err = validBoundedDriftInput(first, fromMS, clock().UTC()); err != nil {
		return r, err
	}
	r.FirstAt = first.FinishedAt
	r.InitialExposureSHA256, err = boundedExposureSignature(first)
	if err != nil {
		return r, err
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	second, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return r, errors.New("두 번째 서명 계정 조회 실패: 수동 재검증 필요")
	}
	if err = validBoundedDriftInput(second, fromMS, clock().UTC()); err != nil {
		return r, err
	}
	r.SecondAt = second.FinishedAt
	r.FinalExposureSHA256, err = boundedExposureSignature(second)
	if err != nil {
		return r, err
	}
	after, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return r, err
	}
	r.AfterEvidenceSHA256 = digestEvidence(after)
	recordsAfter, err := LoadLocalOrderRecords(dataDir)
	if err != nil {
		return r, err
	}
	jr1, _ := json.Marshal(recordsBefore)
	jr2, _ := json.Marshal(recordsAfter)
	if before != after {
		r.Blockers = append(r.Blockers, "두 번의 서명 계정 조회 도중 스트림 이벤트 또는 중단 기록 변경")
	}
	if string(jr1) != string(jr2) {
		r.Blockers = append(r.Blockers, "서명 계정 조회 도중 로컬 주문 저널 변경")
	}
	if first.FinishedAt.After(second.StartedAt) {
		r.Blockers = append(r.Blockers, "서명 조회 기간이 겹치거나 순서가 뒤바뀌었습니다")
	}
	if r.InitialExposureSHA256 != r.FinalExposureSHA256 {
		r.Blockers = append(r.Blockers, "서명 계정의 지갑/포지션/주문/체결/수익 상태 변동 감지")
	}
	if before.GapCount > 0 {
		r.Blockers = append(r.Blockers, fmt.Sprintf("과거 스트림 불연속 %d건: 독립적인 사용자 검토 필요", before.GapCount))
	}
	if before.EventCount > 0 {
		r.Blockers = append(r.Blockers, "보존된 계정 이벤트는 서명 체결 장부와 별도 대조 전까지 미검증")
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	if len(r.Blockers) == 0 {
		r.State = "OBSERVED_STABLE_UNPROVEN"
		r.Blockers = append(r.Blockers, "두 시점 관측 일치: 그 사이의 외부 왕복 거래나 사설 스트림 무손실은 보증하지 않습니다")
	}
	r.CheckedAt = clock().UTC()
	if err := ArchiveBoundedDrift(dataDir, &r); err != nil {
		return r, err
	}
	return r, nil
}

// ArchiveBoundedDrift stores ONLY summary fingerprints and constant-format
// safety diagnostics, never signed payloads, credentials, listenKeys or URLs.
// Its files are immutable, checksummed and NOT order-authorization tokens.
var boundedDriftReportPattern = regexp.MustCompile(`^[a-f0-9]{32}\.json$`)

func (r BoundedDriftReview) digest() string {
	r.SHA256 = ""
	raw, _ := json.Marshal(r)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func validDriftArchive(r BoundedDriftReview) bool {
	return r.Schema == 1 && !r.CheckedAt.IsZero() &&
		(r.State == "BLOCKED" || r.State == "OBSERVED_STABLE_UNPROVEN") &&
		!r.TradingAuthorized && !r.ContinuousCoverageProven && r.ManualReviewRequired &&
		r.Blockers != nil && len(r.Blockers) > 0 && !r.FirstAt.IsZero() && !r.SecondAt.IsZero() &&
		!r.FirstAt.After(r.SecondAt) && !r.SecondAt.After(r.CheckedAt.Add(time.Second)) &&
		fingerprintPattern046.MatchString(r.InitialExposureSHA256) &&
		fingerprintPattern046.MatchString(r.FinalExposureSHA256) &&
		fingerprintPattern046.MatchString(r.BeforeEvidenceSHA256) &&
		fingerprintPattern046.MatchString(r.AfterEvidenceSHA256) &&
		r.EventCount >= 0 && r.GapCount >= 0 && r.LocalOrderCount >= 0 &&
		(r.State != "OBSERVED_STABLE_UNPROVEN" || (r.InitialExposureSHA256 == r.FinalExposureSHA256 &&
			r.BeforeEvidenceSHA256 == r.AfterEvidenceSHA256 && r.EventCount == 0 && r.GapCount == 0))
}
func ArchiveBoundedDrift(dataDir string, r *BoundedDriftReview) error {
	if dataDir == "" || r == nil || !validDriftArchive(*r) {
		return errors.New("서명 이중 관측 기록은 읽기 전용으로만 저장할 수 있습니다")
	}
	dir := filepath.Join(dataDir, "trading", "bounded_windows")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	r.ID = hex.EncodeToString(nonce[:])
	r.SHA256 = r.digest()
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return exclusiveDurable(filepath.Join(dir, r.ID+".json"), raw)
}
func LoadBoundedDriftReviews(dataDir string) ([]BoundedDriftReview, error) {
	dir := filepath.Join(dataDir, "trading", "bounded_windows")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []BoundedDriftReview{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(files) > 100000 {
		return nil, errors.New("이중 관측 기록 파일 수 제한 초과")
	}
	reports := make([]BoundedDriftReview, 0, len(files))
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !boundedDriftReportPattern.MatchString(f.Name()) {
			return nil, errors.New("알 수 없는 이중 관측 기록 파일")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.DisallowUnknownFields()
		var r BoundedDriftReview
		if err = d.Decode(&r); err != nil {
			return nil, errors.New("이중 관측 기록 JSON 손상")
		}
		var tail any
		if err = d.Decode(&tail); !errors.Is(err, io.EOF) {
			return nil, errors.New("이중 관측 기록 후행 데이터")
		}
		if !validDriftArchive(r) || r.ID+".json" != f.Name() || r.SHA256 == "" || r.SHA256 != r.digest() {
			return nil, errors.New("이중 관측 기록 변조 또는 비정상 거래 승인 플래그")
		}
		reports = append(reports, r)
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].CheckedAt.Equal(reports[j].CheckedAt) {
			return reports[i].ID < reports[j].ID
		}
		return reports[i].CheckedAt.Before(reports[j].CheckedAt)
	})
	return reports, nil
}
