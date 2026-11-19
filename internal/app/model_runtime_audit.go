package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"binance_trader/internal/model"
)

// ModelRuntimeAudit is a read-only report of exactly what the compiled model
// registry and the UI catalog can actually select. It is NOT a trading
// readiness assessment: no backtest, recovered ownership, Binance credentials,
// live orders, or final model PnL are attested here.
type ModelRuntimeAudit struct {
	State                        string   `json:"state"`
	RegisteredModelIDs           []string `json:"registered_model_ids"`
	CatalogModelIDs              []string `json:"catalog_model_ids"`
	SelectedModelIDs             []string `json:"selected_model_ids"`
	RegisteredCount              int      `json:"registered_count"`
	CatalogRegistryConsistent    bool     `json:"catalog_registry_consistent"`
	SelectionResolved            bool     `json:"selection_resolved"`
	SingleModelConfigured        bool     `json:"single_model_configured"`
	MultiModelConfigured         bool     `json:"multi_model_configured"`
	SharedAccountBudget          bool     `json:"shared_account_budget"`
	SupervisedOneOrderPerRun     bool     `json:"supervised_one_order_per_run"`
	IndependentModelPositions    bool     `json:"independent_model_positions"`
	LegacyLogisticModelsVerified bool     `json:"legacy_logistic_models_verified"`
	ModelFinalPnLVerified        bool     `json:"model_final_pnl_verified"`
	LiveMultiOrderTradingEnabled bool     `json:"live_multi_order_trading_enabled"`
	TradingAuthorized            bool     `json:"trading_authorized"`
	Blockers                     []string `json:"blockers"`
}

// AuditModelConfiguration never writes settings, orders, reservations, or
// ledgers and never queries Binance. Both the configured selection and the
// catalog MUST correspond exactly to frozen, registered strategies. A plan for
// five Logistic models is not a registry entry or proof of model artifacts.
func AuditModelConfiguration(registry *model.Registry, selected []string) ModelRuntimeAudit {
	return auditModelConfiguration(registry, ModelCatalog, selected)
}

func auditModelConfiguration(registry *model.Registry, catalog []Model, selected []string) ModelRuntimeAudit {
	out := ModelRuntimeAudit{
		State: "BLOCKED", RegisteredModelIDs: []string{}, CatalogModelIDs: []string{},
		SelectedModelIDs: append([]string{}, selected...), Blockers: []string{},
		SharedAccountBudget: true, SupervisedOneOrderPerRun: true,
		// Binance uses pooled Hedge-side quantities. Independent per-model
		// exchange positions cannot be asserted even if local lots are labeled.
		IndependentModelPositions: false,
	}
	if registry == nil {
		out.Blockers = append(out.Blockers, "등록된 모델 registry가 없습니다")
		return out
	}
	out.RegisteredModelIDs = registry.IDs()
	out.RegisteredCount = len(out.RegisteredModelIDs)
	seenCatalog := map[string]bool{}
	for _, item := range catalog {
		if item.ID == "" || seenCatalog[item.ID] || !item.Implemented ||
			strings.TrimSpace(item.Name) == "" || strings.TrimSpace(item.Description) == "" {
			out.Blockers = append(out.Blockers, "UI 모델 catalog 항목이 중복·누락·미구현 상태입니다")
		}
		seenCatalog[item.ID] = true
		out.CatalogModelIDs = append(out.CatalogModelIDs, item.ID)
	}
	sort.Strings(out.CatalogModelIDs)
	if len(out.CatalogModelIDs) != len(out.RegisteredModelIDs) {
		out.Blockers = append(out.Blockers, "UI catalog와 실제 registry 모델 개수가 다릅니다")
	}
	for _, id := range out.RegisteredModelIDs {
		if !seenCatalog[id] {
			out.Blockers = append(out.Blockers, "실행 registry에 UI catalog가 모르는 모델 존재: "+id)
		}
	}
	for _, id := range out.CatalogModelIDs {
		strategy, err := registry.Resolve(id)
		if err != nil || strategy.ID() != id {
			out.Blockers = append(out.Blockers, "UI 모델이 동결된 실행 registry에 없음: "+id)
		}
	}
	out.CatalogRegistryConsistent = len(out.Blockers) == 0
	if len(selected) > 16 {
		out.Blockers = append(out.Blockers, "동시 선택 모델이 16개를 초과했습니다")
	}
	seenSelection := map[string]bool{}
	for _, id := range selected {
		if seenSelection[id] {
			out.Blockers = append(out.Blockers, "선택 모델 ID 중복: "+id)
		}
		seenSelection[id] = true
		if !seenCatalog[id] {
			out.Blockers = append(out.Blockers, "선택 모델이 UI catalog에 없음: "+id)
		}
		strategy, err := registry.Resolve(id)
		if err != nil || strategy.ID() != id {
			out.Blockers = append(out.Blockers, "선택 모델이 실행 registry에 없음: "+id)
		}
	}
	out.SelectionResolved = len(out.Blockers) == 0 && len(selected) != 0
	out.SingleModelConfigured = out.SelectionResolved && len(selected) == 1
	out.MultiModelConfigured = out.SelectionResolved && len(selected) >= 2 && len(selected) <= 16
	switch {
	case len(out.Blockers) != 0:
		out.State = "BLOCKED_CATALOG_OR_SELECTION_MISMATCH"
	case len(selected) == 0:
		out.State = "NO_MODEL_SELECTED"
	default:
		out.State = "CONFIG_CONSISTENT_TRADING_LOCKED"
	}
	// Never infer that historical Logistic models are available just because
	// old research mentioned them. All permissions remain false by design.
	return out
}

// ReadModelSettingsOnly reads an explicitly selected settings file without
// invoking OpenStore (which may create directories and write defaults).
// It is intended solely for the offline audit CLI.
func ReadModelSettingsOnly(path string) (Settings, error) {
	if path == "" {
		return Settings{}, errors.New("settings JSON 경로가 비어 있습니다")
	}
	st, err := os.Lstat(path)
	if err != nil {
		return Settings{}, err
	}
	if !st.Mode().IsRegular() || st.Size() > 32<<10 {
		return Settings{}, errors.New("settings는 일반 파일이어야 하며 32KiB를 초과할 수 없습니다")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return Settings{}, err
	}
	if len(content) > 32<<10 {
		return Settings{}, errors.New("settings 크기 한도 초과")
	}
	var s Settings
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&s); err != nil {
		return Settings{}, fmt.Errorf("settings JSON 형식 오류: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Settings{}, errors.New("settings JSON 뒤에 추가 데이터가 있습니다")
	}
	if s.Version != 1 || s.Environment != Demo && s.Environment != Real {
		return Settings{}, errors.New("settings 버전 또는 환경 오류")
	}
	return s, nil
}
