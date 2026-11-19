package main

// Package A offline triage deliberately transforms an integrity-checked bounded
// archive into a human review worksheet, NEVER into settlement authorization.
// Archive digests protect against accidental/local drift, not against an actor
// able to rewrite both an archive and its hash. A saved Binance GET response
// is not a Binance-signed document, and a past flat account is not fresh proof.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"binance_trader/internal/trading"
)

const discoveryDiagnosticStage = "OFFLINE_INCIDENT_TRIAGE_NO_POSTING"

type incidentDiagnosticCheck struct {
	Code     string `json:"code"`
	Status   string `json:"status"`
	Detail   string `json:"detail"`
	NextStep string `json:"next_step"`
}

type packageADiscoveryDiagnostic struct {
	Schema                     int                       `json:"schema"`
	Stage                      string                    `json:"stage"`
	ClientOrderID              string                    `json:"client_order_id"`
	ModelID                    string                    `json:"model_id"`
	PositionSide               trading.PositionSide      `json:"position_side"`
	EntryOrderID               int64                     `json:"entry_order_id"`
	ExitOrderID                int64                     `json:"exit_order_id"`
	ArchiveChecksum            string                    `json:"verified_archive_checksum"`
	OriginalEvidenceSHA256     string                    `json:"original_local_evidence_sha256"`
	EntryTradeIDs              []int64                   `json:"signed_entry_trade_ids"`
	ExitTradeIDs               []int64                   `json:"signed_exit_trade_ids"`
	MissingEntryLocalTradeIDs  []int64                   `json:"missing_local_entry_trade_ids"`
	BoundedFundingRows         int                       `json:"bounded_funding_rows"`
	BoundedFundingNetUSDT      string                    `json:"bounded_funding_net_usdt"`
	LocalAuditState            string                    `json:"local_lifecycle_audit_state"`
	LocalLifecycleStage        string                    `json:"local_entry_lifecycle_stage"`
	Checks                     []incidentDiagnosticCheck `json:"checks"`
	BlockerCodes               []string                  `json:"blocker_codes"`
	ArchiveIntegrityVerified   bool                      `json:"archive_integrity_verified"`
	FullHistoryVerified        bool                      `json:"full_history_verified"`
	FundingAttributionVerified bool                      `json:"funding_attribution_verified"`
	PostingAuthorized          bool                      `json:"posting_authorized"`
	SettlementAuthorized       bool                      `json:"settlement_authorized"`
	TradingAuthorized          bool                      `json:"trading_authorized"`
	LifecycleChanged           bool                      `json:"lifecycle_changed"`
	OwnershipChanged           bool                      `json:"ownership_changed"`
	Checksum                   string                    `json:"local_sha256_checksum"`
}

func (d packageADiscoveryDiagnostic) digest() string {
	d.Checksum = ""
	b, _ := json.Marshal(d)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func createDiscoveryDiagnostic(b packageADiscoveryArchive, audit trading.ExecutionLifecycleAudit) (packageADiscoveryDiagnostic, error) {
	// Caller must have verified the complete archive against the live local
	// journals already. Revalidate its internal identity to prevent an unsafe
	// caller from constructing a report on an obviously contradictory bundle.
	if err := bundleIdentityCheck(b); err != nil {
		return packageADiscoveryDiagnostic{}, err
	}
	if audit.TradingAuthorized || audit.State == trading.ExecutionAuditBlocked || audit.Total < 0 || audit.Pending < 0 || audit.Settled < 0 || audit.Total != len(audit.Items) || audit.Pending+audit.Settled != audit.Total {
		return packageADiscoveryDiagnostic{}, errors.New("inconsistent or authorizing lifecycle audit")
	}
	gap := b.Incident.Entry.Gap
	if gap.ClientOrderID != b.ClientOrderID || gap.ModelID == "" || len(gap.SignedTradeIDs) == 0 || b.Incident.Close.ExitOrderID <= 0 {
		return packageADiscoveryDiagnostic{}, errors.New("missing signed/local incident identities")
	}
	d := packageADiscoveryDiagnostic{Schema: 1, Stage: discoveryDiagnosticStage,
		ClientOrderID: b.ClientOrderID, ModelID: gap.ModelID, PositionSide: gap.PositionSide,
		EntryOrderID: b.Incident.Entry.Order.OrderID, ExitOrderID: b.Incident.Close.ExitOrderID,
		ArchiveChecksum: b.Checksum, OriginalEvidenceSHA256: b.Incident.OriginalEvidenceSHA256,
		EntryTradeIDs: append([]int64{}, gap.SignedTradeIDs...), ExitTradeIDs: append([]int64{}, b.Incident.Close.ExitTradeIDs...),
		MissingEntryLocalTradeIDs: append([]int64{}, gap.MissingLocalTradeIDs...),
		BoundedFundingRows:        b.Incident.Close.BoundedFundingRows,
		BoundedFundingNetUSDT:     b.Incident.Close.BoundedFundingNetUSDT,
		LocalAuditState:           audit.State, ArchiveIntegrityVerified: true,
	}
	sort.Slice(d.EntryTradeIDs, func(i, j int) bool { return d.EntryTradeIDs[i] < d.EntryTradeIDs[j] })
	sort.Slice(d.ExitTradeIDs, func(i, j int) bool { return d.ExitTradeIDs[i] < d.ExitTradeIDs[j] })
	sort.Slice(d.MissingEntryLocalTradeIDs, func(i, j int) bool { return d.MissingEntryLocalTradeIDs[i] < d.MissingEntryLocalTradeIDs[j] })
	add := func(code, status, detail, next string) {
		d.Checks = append(d.Checks, incidentDiagnosticCheck{Code: code, Status: status, Detail: detail, NextStep: next})
		if status != "EVIDENCED_BOUNDED" {
			d.BlockerCodes = append(d.BlockerCodes, code)
		}
	}
	add("BOUNDED_ARCHIVE_INTEGRITY", "EVIDENCED_BOUNDED", "보존된 조회구간의 진입·청산 주문/체결 및 원본 저널 지문이 일치함 (거래소의 파일 서명 아님)", "보존 JSON과 해시를 독립된 안전한 저장소에도 보관")
	if len(d.MissingEntryLocalTradeIDs) > 0 {
		add("MISSING_OWNED_ENTRY_FILLS", "BLOCKED", fmt.Sprintf("로컬 진입 체결 %d건 누락", len(d.MissingEntryLocalTradeIDs)), "기존 owned-order signed 복구 절차로 누락 체결을 멱등 복원; 주문 재전송 금지")
	} else {
		add("ENTRY_FILLS_PRESENT", "EVIDENCED_BOUNDED", "보존 당시 로컬 진입 체결 ID와 signed 조회가 일치함", "다음 실검증 시 원본 로컬 저널 다시 비교")
	}
	count := 0
	for _, item := range audit.Items {
		if item.ClientOrderID != b.ClientOrderID {
			continue
		}
		count++
		d.LocalLifecycleStage = string(item.Stage)
		if item.PositionSide != gap.PositionSide || item.Action != trading.Enter || item.Stage == trading.ExecutionSettled {
			return packageADiscoveryDiagnostic{}, errors.New("entry lifecycle inconsistent, wrong side/action or already settled")
		}
	}
	if count > 1 {
		return packageADiscoveryDiagnostic{}, errors.New("duplicate entry lifecycle IDs")
	}
	if count == 0 {
		d.LocalLifecycleStage = "MISSING_LIFECYCLE_BLOCKED"
	}
	add("UNSETTLED_LIFECYCLE", "BLOCKED", fmt.Sprintf("원래 진입 Lifecycle=%s (전체 %d건, 미종료 %d건)", d.LocalLifecycleStage, audit.Total, audit.Pending), "원래 clientOrderID로 signed 상태/체결 복구; 임의 SETTLED·장부 삭제 금지")
	if audit.Total > 1 {
		add("OTHER_OWNED_ORDERS", "BLOCKED", fmt.Sprintf("로컬 Lifecycle 총 %d건 존재; 다른 모델의 소유권 충돌 가능", audit.Total), "전체 예약·소유 수량과 실제 Hedge LONG/SHORT 합계 독립 대조")
	}
	add("HISTORICAL_COVERAGE", "BLOCKED", "진입 첫 체결부터 청산 마지막 체결까지 한정된 구간; 계정 전 기간 검증 아님", "원장 원본과 거래소 전체 관련 이력의 추가 독립 검증 필요")
	add("FUNDING_MODEL_ATTRIBUTION", "BLOCKED", fmt.Sprintf("조회구간 펀딩 %d건, 합계 %s USDT; 동일 Hedge 방향에서 모델별 귀속 미확정", d.BoundedFundingRows, d.BoundedFundingNetUSDT), "signed income/수수료와 해당 시점 모델별 노출 비중을 독립 검증")
	add("FRESH_EXCHANGE_STATE", "BLOCKED", "보존된 과거 flat signed 스냅샷은 현재 잔고·미체결 주문·Algo 주문의 증명이 아님", "실제 Binance Demo에 재접속해 신선한 signed 계정/주문 재조회")
	add("OPERATOR_EXIT_OWNERSHIP", "BLOCKED", "외부 주문의 signed 존재는 누가 청산을 실행했는지와 어느 모델 소유였는지의 증명이 아님", "운영자가 Binance Demo 주문 이력에서 외부 청산 주문 ID·방향 확인")
	add("ATOMIC_LEDGER_POSTING", "BLOCKED", "Lifecycle와 Ownership 원자적 적용/중단 복구 및 독립 정산 승인 미구현", "영구 멱등 금융 정산 프로토콜 구현·장애 복구 검증 전 적용 금지")
	d.Checksum = d.digest()
	return d, nil
}

func validateDiscoveryDiagnostic(d packageADiscoveryDiagnostic) error {
	if d.Schema != 1 || d.Stage != discoveryDiagnosticStage || !d.ArchiveIntegrityVerified || d.FullHistoryVerified || d.FundingAttributionVerified || d.PostingAuthorized || d.SettlementAuthorized || d.TradingAuthorized || d.LifecycleChanged || d.OwnershipChanged ||
		d.Checksum != d.digest() || d.ArchiveChecksum == "" || len(d.Checks) < 7 || len(d.BlockerCodes) < 6 {
		return errors.New("unsafe or corrupted incident diagnostic")
	}
	known := map[string]string{}
	gotBlockers := make([]string, 0, len(d.Checks))
	for _, c := range d.Checks {
		if c.Code == "" || c.Detail == "" || c.NextStep == "" || known[c.Code] != "" ||
			(c.Status != "BLOCKED" && c.Status != "EVIDENCED_BOUNDED") {
			return errors.New("invalid diagnostic check")
		}
		known[c.Code] = c.Status
		if c.Status == "BLOCKED" {
			gotBlockers = append(gotBlockers, c.Code)
		}
	}
	if !reflect.DeepEqual(gotBlockers, d.BlockerCodes) {
		return errors.New("diagnostic blocker codes do not match checks")
	}
	if known["BOUNDED_ARCHIVE_INTEGRITY"] != "EVIDENCED_BOUNDED" || known["UNSETTLED_LIFECYCLE"] != "BLOCKED" ||
		known["HISTORICAL_COVERAGE"] != "BLOCKED" || known["FUNDING_MODEL_ATTRIBUTION"] != "BLOCKED" ||
		known["FRESH_EXCHANGE_STATE"] != "BLOCKED" || known["OPERATOR_EXIT_OWNERSHIP"] != "BLOCKED" ||
		known["ATOMIC_LEDGER_POSTING"] != "BLOCKED" {
		return errors.New("required posting blocker was removed or misrepresented")
	}
	if len(d.MissingEntryLocalTradeIDs) > 0 && known["MISSING_OWNED_ENTRY_FILLS"] != "BLOCKED" {
		return errors.New("missing entry fills incorrectly cleared")
	}
	if len(d.MissingEntryLocalTradeIDs) == 0 && known["ENTRY_FILLS_PRESENT"] != "EVIDENCED_BOUNDED" {
		return errors.New("inconsistent local entry report")
	}
	return nil
}

func verifyThenDiagnoseDiscoveryArchive(ctx context.Context, dataDir string, b packageADiscoveryArchive) (packageADiscoveryDiagnostic, error) {
	// Current original journal must be intact before AND after the lifecycle
	// audit; both readers never create a directory or rewrite source files.
	if err := verifyPackageADiscoveryArchive(ctx, dataDir, b); err != nil {
		return packageADiscoveryDiagnostic{}, err
	}
	audit, err := trading.ReviewExecutionLifecycles(dataDir)
	if err != nil {
		return packageADiscoveryDiagnostic{}, err
	}
	d, err := createDiscoveryDiagnostic(b, audit)
	if err != nil {
		return packageADiscoveryDiagnostic{}, err
	}
	if err := verifyPackageADiscoveryArchive(ctx, dataDir, b); err != nil {
		return packageADiscoveryDiagnostic{}, fmt.Errorf("local evidence changed during diagnostic: %w", err)
	}
	return d, validateDiscoveryDiagnostic(d)
}
