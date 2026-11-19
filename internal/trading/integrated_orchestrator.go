package trading

// Explicit INTERNAL diagnostic sequence for future operator integration.
// Signed REST is bracketed by independently verified local event/gap evidence
// and immutable order journal scans. It never obtains a trading permit. No
// HTTP route invokes this code: real Demo private WS is not yet validated.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/wallet"
)

type IntegratedDemoReader interface {
	DemoTradeReader
	FetchDemoRecoveryBaseline(context.Context, string, string, int64) (exchange.RecoveryBaseline, error)
}

func DiagnoseIntegratedDemo(ctx context.Context, dataDir string, budget *wallet.Store,
	reader IntegratedDemoReader, creds DemoCredentials, fingerprint string, fromMS int64,
	expected []ExpectedProtection, clock func() time.Time) (IntegratedReview, error) {
	if reader == nil || creds.APIKey == "" || creds.Secret == "" || dataDir == "" || budget == nil || !budget.BelongsTo(dataDir) {
		return IntegratedReview{}, errors.New("동일한 Demo 인증 조회·배정 지갑 데이터 경로가 필요합니다")
	}
	h := sha256.Sum256([]byte(creds.APIKey))
	if hex.EncodeToString(h[:]) != fingerprint {
		return IntegratedReview{}, errors.New("배정 지갑 계정 지문과 조회 API 키가 다릅니다")
	}
	if clock == nil {
		clock = time.Now
	}
	now := clock().UTC()
	if fromMS <= 0 || now.Sub(time.UnixMilli(fromMS)) > 6*24*time.Hour || !time.UnixMilli(fromMS).Before(now) {
		return IntegratedReview{}, errors.New("복구 조회 범위는 최근 6일 이내여야 합니다")
	}
	before, err := readRecoveryEvidence(dataDir)
	if err != nil {
		return IntegratedReview{}, err
	}
	records, err := LoadLocalOrderRecords(dataDir)
	if err != nil {
		return IntegratedReview{}, err
	}
	signed := make([]SignedOrderEvidence, 0, len(records))
	for _, record := range records {
		if err = ctx.Err(); err != nil {
			return IntegratedReview{}, err
		}
		ev, err := FetchSignedOrderEvidence(ctx, reader, creds, record)
		if err != nil {
			return IntegratedReview{}, err
		}
		signed = append(signed, ev)
	}
	if err = ctx.Err(); err != nil {
		return IntegratedReview{}, err
	}
	baseline, err := reader.FetchDemoRecoveryBaseline(ctx, creds.APIKey, creds.Secret, fromMS)
	if err != nil {
		return IntegratedReview{}, err
	}
	after, err := readRecoveryEvidence(dataDir)
	if err != nil || digestEvidence(before) != digestEvidence(after) {
		return IntegratedReview{}, errors.New("인증 조회 중 계정 이벤트/연결 증적이 변경되었습니다: 다시 시도 및 수동 검토 필요")
	}
	// A new local request/sidecar during REST reads also invalidates this
	// historical observation. Never silently omit a submitted order.
	refreshed, err := LoadLocalOrderRecords(dataDir)
	if err != nil {
		return IntegratedReview{}, err
	}
	a, _ := json.Marshal(records)
	b, _ := json.Marshal(refreshed)
	if string(a) != string(b) {
		return IntegratedReview{}, errors.New("서명 조회 중 로컬 주문 저널이 변경되었습니다")
	}
	review, err := ReviewIntegratedOffline(dataDir, budget, fingerprint, baseline, signed, expected, clock().UTC())
	if err != nil {
		return review, err
	}
	if err = ArchiveIntegratedReview(dataDir, &review); err != nil {
		return review, err
	}
	return review, nil
}
