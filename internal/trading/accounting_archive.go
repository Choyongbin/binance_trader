package trading

// Append-only, integrity-checked summaries. This is NOT a SQLite execution
// ledger. The existing immutable signed-order/fill records remain the source
// of truth. No API endpoint generates or modifies these diagnostics.

import (
	"crypto/rand"
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

var accountingArchivePattern = regexp.MustCompile(`^[a-f0-9]{32}\.json$`)

func validAccountingEvidenceCoverage(v string) bool {
	switch v {
	case "BOUNDED_DEMO_REST_MATCHED_LOCAL_NOT_CONTINUOUS",
		"BOUNDED_DEMO_REST_MATCHED_LOCAL_WITH_UNATTESTED_OPENING_LOTS_NOT_CONTINUOUS":
		return true
	default:
		return false
	}
}

func validateAccountingArchive(r AccountingReport) bool {
	if r.Schema != 1 || r.CreatedAt.IsZero() || r.CreatedAt.After(time.Now().UTC().Add(time.Minute)) ||
		r.WindowStartMS <= 0 || r.WindowEndMS <= r.WindowStartMS ||
		r.WindowEndMS-r.WindowStartMS > int64((6*24*time.Hour)/time.Millisecond) ||
		!accountingArchivePattern.MatchString(r.ID+".json") ||
		((r.EvidenceCoverage == "") != (r.EvidenceSHA256 == "")) ||
		(r.EvidenceCoverage != "" && (!validAccountingEvidenceCoverage(r.EvidenceCoverage) || !fingerprintPattern046.MatchString(r.EvidenceSHA256))) ||
		r.TradingAuthorized || r.ActualNetVerified ||
		r.FundingAttribution != "PROVISIONAL_OR_UNALLOCATED" ||
		r.Coverage != "BOUNDED_SIGNED_INCOME_AND_LOCAL_VERIFIED_FILLS_NOT_CONTINUOUS" ||
		r.Owners == nil || r.AccountFundingByAsset == nil || r.UnallocatedFundingByAsset == nil || r.OtherIncomeByTypeAsset == nil ||
		r.Warnings == nil || len(r.Warnings) < 2 || r.KnownOrders < 0 || r.KnownFills < 0 || r.IncomeRows < 0 {
		return false
	}
	ownerProtectionFills, ownerProtectionGroups, ownerDualGroups := 0, 0, 0
	for _, o := range r.Owners {
		if o.ActualNetVerified || o.Symbol != "BTCUSDT" || (o.Source != Manual && o.Source != Model) ||
			(o.PositionSide != Long && o.PositionSide != Short) || o.CommissionByAsset == nil {
			return false
		}
		if o.Protection != nil {
			p := o.Protection
			if p.FillCount <= 0 || p.GroupCount <= 0 || p.DualTriggeredGroupCount < 0 || p.DualTriggeredGroupCount > p.GroupCount ||
				p.TakeProfitFillCount < 0 || p.StopLossFillCount < 0 || p.TakeProfitFillCount+p.StopLossFillCount != p.FillCount || p.CommissionByAsset == nil {
				return false
			}
			for _, v := range []string{p.TakeProfitQuantity, p.StopLossQuantity} {
				if _, err := decimalRat(v, true); err != nil {
					return false
				}
			}
			for _, v := range []string{p.VirtualGrossRealizedUSDT, p.ExchangeReportedGrossUSDT} {
				if _, err := signedRat(v); err != nil {
					return false
				}
			}
			ownerProtectionFills += p.FillCount
			ownerProtectionGroups += p.GroupCount
			ownerDualGroups += p.DualTriggeredGroupCount
		}
	}
	if r.ProtectionFillCount < 0 || r.ProtectionGroupCount < 0 || r.DualTriggeredGroupCount < 0 || r.DualTriggeredGroupCount > r.ProtectionGroupCount ||
		ownerProtectionFills != r.ProtectionFillCount || ownerProtectionGroups != r.ProtectionGroupCount || ownerDualGroups != r.DualTriggeredGroupCount {
		return false
	}
	return true
}

// ArchiveAccounting is an INTERNAL operator/offline function only. A summary
// is never permission to enter a position and cannot replace signed history.
func ArchiveAccounting(dataDir string, r *AccountingReport) error {
	if dataDir == "" || r == nil {
		return errors.New("회계 보고서와 데이터 경로가 필요합니다")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	r.ID = hex.EncodeToString(nonce[:])
	r.SHA256 = r.digest()
	if !validateAccountingArchive(*r) {
		return errors.New("읽기 전용 회계 보고서 조건 불일치")
	}
	dir := filepath.Join(dataDir, "trading", "accounting_reports")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, r.ID+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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
		return fmt.Errorf("회계 요약 보고서 쓰기 불확실: %w", err)
	}
	return nil
}

func LoadAccountingReports(dataDir string) ([]AccountingReport, error) {
	if dataDir == "" {
		return nil, errors.New("회계 보고서 경로가 필요합니다")
	}
	dir := filepath.Join(dataDir, "trading", "accounting_reports")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []AccountingReport{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(files) > 100000 {
		return nil, errors.New("회계 기록이 너무 많아 수동 조사가 필요합니다")
	}
	out := make([]AccountingReport, 0, len(files))
	seen := map[string]bool{}
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !accountingArchivePattern.MatchString(f.Name()) {
			return nil, errors.New("알 수 없는 회계 보고서 파일")
		}
		raw, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		var r AccountingReport
		if err = dec.Decode(&r); err != nil {
			return nil, errors.New("회계 보고서 JSON 손상")
		}
		var tail any
		if err = dec.Decode(&tail); !errors.Is(err, io.EOF) {
			return nil, errors.New("회계 보고서의 추가 데이터 발견")
		}
		if !validateAccountingArchive(r) || r.ID+".json" != f.Name() || r.SHA256 == "" || r.SHA256 != r.digest() {
			return nil, errors.New("회계 보고서 체크섬/권한 또는 내용 변조")
		}
		seen[f.Name()] = true
		out = append(out, r)
	}
	cold, err := coldEntries(dataDir, ColdKindAccounting)
	if err != nil {
		return nil, err
	}
	for name, raw := range cold {
		if seen[name] {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		var r AccountingReport
		if err := dec.Decode(&r); err != nil {
			return nil, errors.New("회계 cold archive JSON 손상")
		}
		var tail any
		if err = dec.Decode(&tail); !errors.Is(err, io.EOF) {
			return nil, errors.New("회계 cold archive 후행 데이터")
		}
		if !accountingArchivePattern.MatchString(name) || !validateAccountingArchive(r) || r.ID+".json" != name || r.SHA256 == "" || r.SHA256 != r.digest() {
			return nil, errors.New("회계 cold archive 손상")
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}
