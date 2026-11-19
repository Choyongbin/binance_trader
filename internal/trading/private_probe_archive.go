package trading

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"binance_trader/internal/wsclient"
)

// Historical diagnostic logs deliberately contain only allowlisted metadata;
// the data is NOT a stream replay and never becomes an execution permit.
type PrivateProbeRecord struct {
	ID     string                 `json:"id"`
	Result PrivateTransportResult `json:"result"`
	SHA256 string                 `json:"sha256"`
}

func (r PrivateProbeRecord) sum() string {
	r.SHA256 = ""
	b, _ := json.Marshal(r)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

var privateProbeFilename = regexp.MustCompile(`^[0-9a-f]{32}\.json$`)

func validatePrivateResult(r PrivateTransportResult) error {
	if r.Schema != 1 || r.TradingAuthorized || r.StreamContinuityProven || !r.HistoricalOnly || r.Note != privateProbeDisclaimer ||
		r.StartedAt.IsZero() || r.FinishedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) || r.EventCount < 0 || r.EventCount > 100000 || len(r.EventTypes) > 16 ||
		r.Status == "" || !privateProbeStatus(r.Status) ||
		!wsclient.ValidDialDiagnostic(wsclient.SafeDialDiagnostic{Phase: r.DialPhase, HTTPStatus: r.DialHTTPStatus}) || r.DialElapsedMS < 0 || r.DialElapsedMS > 120000 ||
		(r.DialPhase == "CONNECTED" && !r.HandshakeOK) || (r.DialPhase != "" && r.DialPhase != "CONNECTED" && r.DialPhase != "TIMEOUT" && r.HandshakeOK) ||
		(r.Status == "HANDSHAKE_ONLY" && (!r.HandshakeOK || r.EventCount != 0)) ||
		(r.Status == "EVENTS_OBSERVED" && (!r.HandshakeOK || r.EventCount == 0)) {
		return errors.New("비공개 스트림 진단 데이터가 잘못되었습니다")
	}
	allowed := map[string]bool{"ORDER_TRADE_UPDATE": true, "ACCOUNT_UPDATE": true, "ACCOUNT_CONFIG_UPDATE": true, "MARGIN_CALL": true, "TRADE_LITE": true, "STRATEGY_UPDATE": true, "ALGO_UPDATE": true, "CONDITIONAL_ORDER_TRIGGER_REJECT": true}
	for i, k := range r.EventTypes {
		if !allowed[k] || i > 0 && k <= r.EventTypes[i-1] {
			return errors.New("진단 이벤트 목록 오류")
		}
	}
	if r.Status == "HANDSHAKE_ONLY" && len(r.EventTypes) != 0 || r.Status == "EVENTS_OBSERVED" && len(r.EventTypes) == 0 {
		return errors.New("진단 이벤트 수와 유형이 일치하지 않습니다")
	}
	return nil
}
func privateProbeStatus(s string) bool {
	switch s {
	case "INVALID_CONFIGURATION", "LISTEN_KEY_FAILED", "WS_HANDSHAKE_FAILED", "WS_HANDSHAKE_TIMEOUT", "STREAM_INTERRUPTED_OR_INVALID_EVENT", "OBSERVER_NOT_STOPPED", "CANCELLED", "HANDSHAKE_ONLY", "EVENTS_OBSERVED", "SESSION_CHANGED":
		return true
	}
	return false
}
func privateProbeDir(dataDir string) string {
	return filepath.Join(dataDir, "diagnostics", "private_stream")
}
func ArchivePrivateProbe(dataDir string, r PrivateTransportResult) (PrivateProbeRecord, error) {
	if dataDir == "" {
		return PrivateProbeRecord{}, errors.New("진단 데이터 경로 누락")
	}
	if e := validatePrivateResult(r); e != nil {
		return PrivateProbeRecord{}, e
	}
	var id [16]byte
	if _, e := rand.Read(id[:]); e != nil {
		return PrivateProbeRecord{}, e
	}
	rec := PrivateProbeRecord{ID: hex.EncodeToString(id[:]), Result: r}
	rec.SHA256 = rec.sum()
	dir := privateProbeDir(dataDir)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return PrivateProbeRecord{}, e
	}
	file, e := os.OpenFile(filepath.Join(dir, rec.ID+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return PrivateProbeRecord{}, e
	}
	b, _ := json.MarshalIndent(rec, "", "  ")
	_, e = file.Write(append(b, '\n'))
	if e == nil {
		e = file.Sync()
	}
	closeErr := file.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return PrivateProbeRecord{}, errors.New("진단 기록 저장 불확실: 수동 조사 필요")
	}
	return rec, nil
}
func LoadPrivateProbes(dataDir string) ([]PrivateProbeRecord, error) {
	files, e := os.ReadDir(privateProbeDir(dataDir))
	if errors.Is(e, os.ErrNotExist) {
		return []PrivateProbeRecord{}, nil
	}
	if e != nil {
		return nil, e
	}
	if len(files) > 10000 {
		return nil, errors.New("진단 기록이 허용량을 초과합니다")
	}
	out := make([]PrivateProbeRecord, 0, len(files))
	for _, f := range files {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !privateProbeFilename.MatchString(f.Name()) {
			return nil, errors.New("알 수 없는 진단 기록")
		}
		raw, e := os.ReadFile(filepath.Join(privateProbeDir(dataDir), f.Name()))
		if e != nil {
			return nil, e
		}
		var rec PrivateProbeRecord
		if json.Unmarshal(raw, &rec) != nil || rec.ID+".json" != f.Name() || rec.SHA256 != rec.sum() || validatePrivateResult(rec.Result) != nil {
			return nil, errors.New("비공개 스트림 진단 기록 무결성 오류")
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Result.StartedAt.Equal(out[j].Result.StartedAt) {
			if out[i].Result.FinishedAt.Equal(out[j].Result.FinishedAt) {
				return out[i].ID < out[j].ID
			}
			return out[i].Result.FinishedAt.After(out[j].Result.FinishedAt)
		}
		return out[i].Result.StartedAt.After(out[j].Result.StartedAt)
	})
	// There is no expiry-to-live transition; all archived probes are historical.
	return out, nil
}
