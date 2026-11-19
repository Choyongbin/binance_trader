package server

// History endpoints never create SQLite files, fetch signed exchange data or
// grant an order permission. The operator-only report-index CLI builds the
// disposable projection from validated, immutable original reports.
import (
	"errors"
	"net/http"
	"strconv"

	"binance_trader/internal/reportindex"
)

func (s *Server) accountingIndexStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "조회 매개변수를 지원하지 않습니다")
		return
	}
	status, e := reportindex.StatusOnly(s.dataDir)
	if e != nil {
		respondError(w, 503, "회계 색인 또는 불변 원본 검증 실패: 수동 조사 필요")
		return
	}
	respondJSON(w, 200, status)
}
func (s *Server) accountingHistory(w http.ResponseWriter, r *http.Request) {
	allowed := map[string]bool{"limit": true, "offset": true, "source": true, "model_id": true}
	query := r.URL.Query()
	for k, values := range query {
		if !allowed[k] || len(values) != 1 {
			respondError(w, 400, "허용하지 않는 조회 매개변수")
			return
		}
	}
	limit, offset := 20, 0
	var e error
	if v := query.Get("limit"); v != "" {
		limit, e = strconv.Atoi(v)
		if e != nil {
			respondError(w, 400, "limit 값 오류")
			return
		}
	}
	if v := query.Get("offset"); v != "" {
		offset, e = strconv.Atoi(v)
		if e != nil {
			respondError(w, 400, "offset 값 오류")
			return
		}
	}
	if limit < 1 || limit > 100 || offset < 0 || offset > 100000 {
		respondError(w, 400, "조회 범위 초과")
		return
	}
	source, modelID := query.Get("source"), query.Get("model_id")
	if source != "" && source != "manual" && source != "model" {
		respondError(w, 400, "소유자 종류 오류")
		return
	}
	if modelID != "" && (source != "model" || len(modelID) > 80) {
		respondError(w, 400, "모델 식별자 오류")
		return
	}
	status, e := reportindex.StatusOnly(s.dataDir)
	if e != nil {
		respondError(w, 503, "불변 회계 원본 또는 SQLite 손상: 수동 조사 필요")
		return
	}
	if status.State != "CURRENT" {
		if status.State == "NOT_BUILT" && status.ReportCount == 0 {
			respondJSON(w, 200, map[string]any{"index": status, "reports": []any{}, "total": 0, "limit": limit, "offset": offset})
			return
		}
		respondJSON(w, 409, map[string]any{"error": "과거 회계 색인이 없거나 오래되었습니다. 보고서 원본을 검증한 다음 운영자 CLI로 갱신해야 합니다.", "index": status})
		return
	}
	page, e := reportindex.ReadHistory(s.dataDir, limit, offset, reportindex.Filter{Source: source, ModelID: modelID})
	if errors.Is(e, reportindex.ErrStale) || errors.Is(e, reportindex.ErrNotBuilt) {
		respondError(w, 409, "조회 중 회계 원본이 바뀌었습니다. 색인을 검증해 주세요.")
		return
	}
	if e != nil {
		respondError(w, 503, "과거 회계 색인 조회 실패: 수동 조사 필요")
		return
	}
	respondJSON(w, 200, page)
}
