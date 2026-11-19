package trading

// This is an OFFLINE, READ-ONLY join of existing durable evidence. It does
// not start a private stream, certify full lifetime coverage or release any
// HTTP/real/Demo trading routes. Binance pools owners on the same Hedge side;
// an observed TP+SL pair is NOT an atomic OCO order.
import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

// IntegratedReview contains ONLY computed counts, local configuration amounts,
// blockers and fingerprints of files. No API keys, listenKeys, signed REST
// payloads or raw account events are copied to an archived report.
type IntegratedReview struct {
	Schema                   int       `json:"schema"`
	ID                       string    `json:"id"`
	CheckedAt                time.Time `json:"checked_at"`
	State                    string    `json:"state"` // BLOCKED / OBSERVED_FLAT_NOT_CONTINUOUS
	AccountHistoryCoverage   string    `json:"account_history_coverage"`
	KnownOrderCount          int       `json:"known_order_count"`
	SignedOrderCount         int       `json:"signed_order_count"`
	StoredEventCount         int       `json:"stored_event_count"`
	StreamGapCount           int       `json:"stream_gap_count"`
	OwnerCount               int       `json:"owner_count"`
	LongBTC                  string    `json:"long_btc"`
	ShortBTC                 string    `json:"short_btc"`
	ActiveModelReservations  int       `json:"active_model_reservations"`
	ActiveManualReservations int       `json:"active_manual_reservations"`
	AutoWalletAssignedUSDT   string    `json:"auto_wallet_assigned_usdt"`
	AutoOwnedMarginUSDT      string    `json:"estimated_auto_owned_margin_usdt"`
	PendingAutoMarginUSDT    string    `json:"pending_auto_margin_usdt"`
	PendingManualMarginUSDT  string    `json:"pending_manual_margin_usdt"`
	AutoFreeBudgetUSDT       string    `json:"estimated_auto_free_usdt"`
	StreamReviewState        string    `json:"stream_review_state"`
	ProtectionReviewState    string    `json:"protection_review_state"`
	EvidenceBefore           string    `json:"evidence_before_sha256"`
	EvidenceAfter            string    `json:"evidence_after_sha256"`
	Blockers                 []string  `json:"blockers"`
	TradingAuthorized        bool      `json:"trading_authorized"`       // always false
	StreamContinuityProven   bool      `json:"stream_continuity_proven"` // always false
	ManualReviewRequired     bool      `json:"manual_review_required"`   // always true
	SHA256                   string    `json:"sha256"`
}

func (r IntegratedReview) digest() string {
	r.SHA256 = ""
	raw, _ := json.Marshal(r)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

// ceilTo8 never rounds *down* local collateral. All decimal arithmetic in this
// diagnostic is exact big.Rat; only reporting uses this conservative ceiling.
func ceilTo8(x *big.Rat) string {
	n := new(big.Rat).Mul(x, big.NewRat(100000000, 1))
	q, rem := new(big.Int).QuoRem(n.Num(), n.Denom(), new(big.Int))
	if rem.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return wallet.Format(new(big.Rat).SetFrac(q, big.NewInt(100000000)))
}

func floorTo8(x *big.Rat) string {
	if x.Sign() < 0 {
		return "0"
	}
	n := new(big.Rat).Mul(x, big.NewRat(100000000, 1))
	q := new(big.Int).Quo(n.Num(), n.Denom())
	return wallet.Format(new(big.Rat).SetFrac(q, big.NewInt(100000000)))
}

func digestEvidence(x evidenceState) string {
	raw, _ := json.Marshal(x)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

// ReviewIntegratedOffline may ONLY be called with an independently acquired
// authenticated Demo RecoveryBaseline and independently acquired signed per-
// order REST evidence; they must never be decoded from an HTTP/browser body.
// Full private-stream epochs and past external trades cannot be proven from
// these two inputs. Output ALWAYS denies order authorization.
func ReviewIntegratedOffline(dataDir string, budget *wallet.Store, fingerprint string,
	baseline exchange.RecoveryBaseline, signed []SignedOrderEvidence,
	expected []ExpectedProtection, now time.Time) (IntegratedReview, error) {
	r := IntegratedReview{Schema: 1, CheckedAt: now.UTC(), State: "BLOCKED",
		AccountHistoryCoverage: "BOUNDED_REST_AND_STORED_EVENTS_ONLY", Blockers: []string{},
		ManualReviewRequired: true, AutoWalletAssignedUSDT: "0", AutoOwnedMarginUSDT: "0",
		PendingAutoMarginUSDT: "0", PendingManualMarginUSDT: "0", AutoFreeBudgetUSDT: "0",
		LongBTC: "0", ShortBTC: "0"}
	add := func(s string) { r.Blockers = append(r.Blockers, s) }
	if dataDir == "" || budget == nil || !budget.BelongsTo(dataDir) ||
		!fingerprintPattern046.MatchString(fingerprint) || now.IsZero() {
		return r, errors.New("동일 경로 지갑·인증된 계정 지문·검사 시각이 필요합니다")
	}
	if baseline.Environment != "demo" || baseline.FinishedAt.IsZero() || baseline.FinishedAt.After(now.Add(time.Second)) ||
		now.Sub(baseline.FinishedAt) > 5*time.Second || baseline.Coverage != "BOUNDED_HISTORY_ONLY_NOT_CONTINUOUS" {
		return r, errors.New("최신 독립 Demo 서명 복구 관측이 필요합니다")
	}
	before, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return r, fmt.Errorf("계정 이벤트·불연속 기록이 없어 재검증 필요: %w", err)
	}
	r.EvidenceBefore, r.StoredEventCount, r.StreamGapCount = digestEvidence(before), before.EventCount, before.GapCount
	events, err := (&EventStore{dir: filepath.Join(dataDir, "trading", "user_events")}).ReadAll()
	if err != nil {
		return r, err
	}
	records, err := LoadLocalOrderRecords(dataDir)
	if err != nil {
		return r, err
	}
	r.KnownOrderCount, r.SignedOrderCount = len(records), len(signed)
	stream := ReviewStreamOrderCorrelation(events, records, signed, now)
	r.StreamReviewState = stream.State
	if stream.State == "BLOCKED" {
		add("저장된 계정 이벤트와 독립 서명 주문 내역 충돌")
	}
	if len(records) != len(signed) {
		add("모든 로컬 주문의 독립 서명 증적이 준비되지 않았습니다")
	}
	if len(events) != before.EventCount {
		add("계정 이벤트 읽기 중 파일 수 변화")
	}
	if before.GapCount > 0 {
		add("계정 스트림 불연속 기록이 남아 있습니다: 수동 복구 필요")
	}
	for _, v := range stream.Blockers {
		add("계정 이벤트: " + v)
	}

	store, err := NewOwnershipStore(dataDir)
	if err != nil {
		return r, err
	}
	recovery, err := ReviewRecovery(dataDir, store, baseline, now)
	if err != nil {
		return r, err
	}
	// ReviewRecovery intentionally always reports a missing *continuous*
	// stream checkpoint; do not confuse a matching REST window with readiness.
	for _, v := range recovery.Issues {
		add("전체 계정: " + v)
	}
	var owned []OwnerPosition
	if recovery.Ownership == nil {
		add("체결 소유권 감사 결과 누락")
	} else {
		owned = recovery.Ownership.Owned
		if !recovery.Ownership.ReadyForAssessment {
			add("실제 체결·가상 포지션 소유권 재검증 필요")
		}
	}
	r.OwnerCount = len(owned)
	sideCounts := map[PositionSide]int{Long: 0, Short: 0}
	sideQty := map[PositionSide]*big.Rat{Long: new(big.Rat), Short: new(big.Rat)}
	modelMargin := new(big.Rat)
	for _, p := range owned {
		q, qe := signedRat(p.Quantity)
		if qe != nil || q.Sign() < 0 {
			return r, errors.New("소유 수량/소수점 오류")
		}
		if q.Sign() == 0 {
			continue
		}
		if p.Symbol != "BTCUSDT" || (p.Side != Long && p.Side != Short) {
			add("지원하지 않는 소유자 포지션")
			continue
		}
		sideCounts[p.Side]++
		sideQty[p.Side].Add(sideQty[p.Side], q)
		if p.Source == Model {
			avg, e := positiveRat(p.AverageEntry)
			if e != nil {
				return r, e
			}
			// 15x theoretical entry margin is only a budget ESTIMATE; real
			// Binance isolated margin may vary with fees, transfers or mark.
			modelMargin.Add(modelMargin, new(big.Rat).Quo(new(big.Rat).Mul(q, avg), big.NewRat(15, 1)))
		}
	}
	r.LongBTC, r.ShortBTC = formatOwnershipDecimal(sideQty[Long]), formatOwnershipDecimal(sideQty[Short])
	if sideCounts[Long] > 1 || sideCounts[Short] > 1 {
		add("동일 방향 복수 소유자: 개별 TP/SL은 거래소 원자적 OCO/손실 격리가 아닙니다")
	}

	protection := ReviewProtectionPortfolio(baseline, owned, expected,
		recovery.Ownership != nil && recovery.Ownership.ReadyForAssessment, now)
	r.ProtectionReviewState = protection.State
	if protection.State == "BLOCKED" {
		for _, v := range protection.Blockers {
			add("보호 주문: " + v)
		}
	}
	if protection.State == "OBSERVED_MATCH_NOT_OCO" {
		add("현재 보호 주문은 원자적 OCO가 아닙니다: 자동 실행 승인 금지")
	}

	assigned, err := budget.SnapshotFor(fingerprint)
	if err != nil {
		return r, err
	}
	if !assigned.Verified {
		add("자동매매 지갑과 Demo 인증 키가 일치하지 않습니다")
	}
	r.AutoWalletAssignedUSDT = assigned.AssignedUSDT
	pendingModel, pendingManual := new(big.Rat), new(big.Rat)
	dir := filepath.Join(dataDir, "trading", "reservations")
	if _, statErr := os.Stat(dir); statErr == nil {
		co := &Coordinator{reserveDir: dir}
		reservations, er := co.scan()
		if er != nil {
			return r, er
		}
		for _, v := range reservations {
			if co.isDone(v) || v.Intent.Action != Enter {
				continue
			}
			n, er := signedRat(v.MarginUSDT)
			if er != nil || n.Sign() < 0 {
				return r, errors.New("예약 금액 손상")
			}
			if v.Intent.Source == Model {
				pendingModel.Add(pendingModel, n)
				r.ActiveModelReservations++
			} else if v.Intent.Source == Manual {
				pendingManual.Add(pendingManual, n)
				r.ActiveManualReservations++
			} else {
				return r, errors.New("알 수 없는 예약 소유자")
			}
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return r, statErr
	}
	r.AutoOwnedMarginUSDT = ceilTo8(modelMargin)
	r.PendingAutoMarginUSDT = ceilTo8(pendingModel)
	r.PendingManualMarginUSDT = ceilTo8(pendingManual)

	// A LOCAL manual pending order must be deducted from *actual* available
	// as Binance may not have received/locked collateral for it yet.
	actual, err := signedRat(string(baseline.Portfolio.AvailableBalance))
	if err != nil {
		return r, err
	}
	actual.Sub(actual, pendingManual)
	if actual.Sign() < 0 {
		actual.SetInt64(0)
		add("수동 예약이 실제 가용 USDT보다 큽니다")
	}
	risk, err := wallet.Assess(assigned.AssignedUSDT, r.AutoOwnedMarginUSDT,
		r.PendingAutoMarginUSDT, "0", floorTo8(actual))
	if err != nil {
		return r, err
	}
	if assigned.Verified {
		r.AutoFreeBudgetUSDT = risk.FreeBudgetUSDT
	} else {
		r.AutoFreeBudgetUSDT = "0" // do not display spendable value for another account
	}
	if risk.Reason != "" {
		add("배정 지갑: " + risk.Reason)
	}
	if !risk.Fits {
		add("배정 지갑 계산이 새 주문을 허용하지 않습니다")
	}
	if r.ActiveModelReservations > 0 || r.ActiveManualReservations > 0 {
		add("미완료 자금 예약이 존재합니다: 신규 주문 금지")
	}

	after, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return r, err
	}
	r.EvidenceAfter = digestEvidence(after)
	if r.EvidenceBefore != r.EvidenceAfter {
		add("통합 진단 중 계정 이벤트/불연속 기록이 변경되었습니다")
	}
	// Two baseline advisories are intentionally permanent in the pre-live
	// recovery module. When nothing else is abnormal, make an observed flat
	// diagnostic visibly distinct from confirmed corruption; NEVER grant a
	// trading permit or claim continuous account-stream coverage.
	onlyKnownRecoveryAdvisories := true
	for _, issue := range recovery.Issues {
		if issue != "계정 자산 위험: 자동매매 전용 배정 지갑과 실제 Demo 계정 일치 검증이 필요합니다" &&
			issue != "과거 bounded review만으로는 live continuity 승인 체크포인트를 대체할 수 없음: 현재 프로세스의 연속 관측과 별도 거래 직전 preflight 필요" {
			onlyKnownRecoveryAdvisories = false
		}
	}
	if onlyKnownRecoveryAdvisories && assigned.Verified && risk.Fits &&
		len(records) == 0 && len(signed) == 0 && len(owned) == 0 && len(expected) == 0 &&
		before.EventCount == 0 && before.GapCount == 0 && protection.State == "EMPTY" &&
		r.ActiveModelReservations == 0 && r.ActiveManualReservations == 0 &&
		stream.State == "NO_ORDER_SIGNALS" && r.EvidenceBefore == r.EvidenceAfter {
		r.State = "OBSERVED_FLAT_NOT_CONTINUOUS"
	}
	return r, nil
}

var fingerprintPattern046 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var integratedReportName = regexp.MustCompile(`^[0-9a-f]{32}\.json$`)

// ArchiveIntegratedReview is immutable and safe to show as historical status;
// malformed or privilege-bearing reports are rejected when reloaded.
func ArchiveIntegratedReview(dataDir string, r *IntegratedReview) error {
	if dataDir == "" || r == nil || r.Schema != 1 || r.TradingAuthorized ||
		r.StreamContinuityProven || !r.ManualReviewRequired ||
		(r.State != "BLOCKED" && r.State != "OBSERVED_FLAT_NOT_CONTINUOUS") ||
		r.CheckedAt.IsZero() || r.Blockers == nil || r.EvidenceBefore == "" || r.EvidenceAfter == "" {
		return errors.New("읽기 전용 통합 검토 보고서 형식 오류")
	}
	dir := filepath.Join(dataDir, "trading", "integrated_reviews")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	r.ID = hex.EncodeToString(b[:])
	r.SHA256 = r.digest()
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return exclusiveDurable(filepath.Join(dir, r.ID+".json"), raw)
}

func LoadIntegratedReviews(dataDir string) ([]IntegratedReview, error) {
	dir := filepath.Join(dataDir, "trading", "integrated_reviews")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []IntegratedReview{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(files) > 100000 {
		return nil, errors.New("통합 검토 파일 개수 한도 초과")
	}
	out := make([]IntegratedReview, 0, len(files))
	seen := map[string]bool{}
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !integratedReportName.MatchString(f.Name()) {
			return nil, errors.New("알 수 없는 통합 검토 기록")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.DisallowUnknownFields()
		var v IntegratedReview
		if err := d.Decode(&v); err != nil {
			return nil, err
		}
		var tail any
		if err = d.Decode(&tail); err == nil {
			return nil, errors.New("통합 검토 후행 데이터")
		} else if !errors.Is(err, io.EOF) {
			return nil, err
		}
		if v.Schema != 1 || v.ID+".json" != f.Name() || v.CheckedAt.IsZero() ||
			v.SHA256 == "" || v.SHA256 != v.digest() || v.TradingAuthorized || v.StreamContinuityProven ||
			!v.ManualReviewRequired || v.Blockers == nil || !fingerprintPattern046.MatchString(v.EvidenceBefore) ||
			!fingerprintPattern046.MatchString(v.EvidenceAfter) ||
			(v.State != "BLOCKED" && v.State != "OBSERVED_FLAT_NOT_CONTINUOUS") {
			return nil, errors.New("통합 검토 보고서 변조/승인 플래그 감지")
		}
		seen[f.Name()] = true
		out = append(out, v)
	}
	cold, err := coldEntries(dataDir, ColdKindIntegrated)
	if err != nil {
		return nil, err
	}
	for name, raw := range cold {
		if seen[name] {
			continue
		}
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.DisallowUnknownFields()
		var v IntegratedReview
		if err := d.Decode(&v); err != nil {
			return nil, err
		}
		var tail any
		if err = d.Decode(&tail); err == nil {
			return nil, errors.New("통합 검토 cold archive 후행 데이터")
		} else if !errors.Is(err, io.EOF) {
			return nil, err
		}
		if !integratedReportName.MatchString(name) || v.Schema != 1 || v.ID+".json" != name || v.CheckedAt.IsZero() || v.SHA256 == "" || v.SHA256 != v.digest() || v.TradingAuthorized || v.StreamContinuityProven || !v.ManualReviewRequired || v.Blockers == nil || !fingerprintPattern046.MatchString(v.EvidenceBefore) || !fingerprintPattern046.MatchString(v.EvidenceAfter) || (v.State != "BLOCKED" && v.State != "OBSERVED_FLAT_NOT_CONTINUOUS") {
			return nil, errors.New("통합 검토 cold archive 손상")
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CheckedAt.Equal(out[j].CheckedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CheckedAt.Before(out[j].CheckedAt)
	})
	return out, nil
}
