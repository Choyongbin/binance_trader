package server

import (
	"binance_trader/internal/app"
	"binance_trader/internal/model"
	"errors"
	"net/http"
	"time"
)

// selectedRuntimeModel is the SINGLE source of model identity used by the
// controller factory. Preferences do not grant trading authorization.
func selectedRuntimeModel(ids []string, registry *model.Registry) (string, error) {
	if len(ids) != 1 {
		return "", errors.New("자동매매 실행 모델을 정확히 하나 선택해야 합니다")
	}
	if _, err := registry.Resolve(ids[0]); err != nil {
		return "", err
	}
	return ids[0], nil
}
func modelSchedulerMode(ids []string) string {
	if len(ids) > 1 {
		return "MULTI_READ_ONLY_PREVIEW"
	}
	if len(ids) == 1 {
		return "SINGLE_MODEL_SELECTED"
	}
	return "NOT_CONFIGURED"
}
func selectedModelPreview(ids []string, registry *model.Registry) string {
	id, err := selectedRuntimeModel(ids, registry)
	if err != nil {
		return ""
	}
	return id
}
func (s *Server) modelRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "쿼리 파라미터를 지원하지 않습니다")
		return
	}
	ids := s.store.Snapshot().SelectedModelIDs
	audit := app.AuditModelConfiguration(s.strategyRegistry, ids)
	id, err := selectedRuntimeModel(ids, s.strategyRegistry)
	issue := ""
	if err != nil {
		issue = err.Error()
	}
	if len(audit.Blockers) != 0 {
		issue = audit.Blockers[0]
	} else if len(ids) > 1 {
		issue = "다중 모델 팩터리는 첫 주문 시도 후 전체 중단되는 감독형이며, 제품 주문/자동 시작은 HTTP 423으로 잠겨 있습니다"
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"scheduler_mode":               modelSchedulerMode(ids),
		"scheduler_selected_ids":       ids,
		"scheduler_running":            s.modelSignalStatusAt(time.Now().UTC()).SchedulerRunning && len(ids) > 1,
		"preview_configuration_ready":  audit.MultiModelConfigured,                                                        // no validated market/account cycle yet
		"internal_multi_factory_wired": audit.MultiModelConfigured && allRegisteredRuntimeModels(ids, s.strategyRegistry), // NOT trading authorization
		"execution_enabled":            false,
		"registered_model_ids":         s.strategyRegistry.IDs(),
		"selected_model_ids":           ids,
		"runtime_model_id":             id,
		"runtime_selection_ready":      err == nil && audit.SingleModelConfigured,
		"model_registry_audit":         audit,
		"trading_authorized":           false,
		"auto_start_locked":            true,
		"reason":                       issue,
	})
}

// allRegisteredRuntimeModels validates the same IDs as the controller factory
// without constructing any order engine or creating durable state.
func allRegisteredRuntimeModels(ids []string, registry *model.Registry) bool {
	if registry == nil || len(ids) < 2 || len(ids) > 16 {
		return false
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return false
		}
		seen[id] = true
		if _, err := registry.Resolve(id); err != nil {
			return false
		}
	}
	return true
}
