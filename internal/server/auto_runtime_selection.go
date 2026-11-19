package server

import (
	"errors"
	"fmt"

	"binance_trader/internal/model"
	"binance_trader/internal/trading"
)

// chooseAutoRuntime connects the existing shared multi-model coordinator to
// the controller factory without granting trade permission. It rejects an
// invalid model set before opening any durable order state. Production HTTP
// start/stop/order routes remain 423 until protection and settlement E2E.
func chooseAutoRuntime(ids []string, registry *model.Registry,
	single func(string) (trading.AutoRuntime, error),
	multi func([]string) (trading.AutoRuntime, error),
) (trading.AutoRuntime, error) {
	if registry == nil || single == nil || multi == nil || len(ids) == 0 || len(ids) > 16 {
		return nil, errors.New("등록된 자동매매 모델 1~16개가 필요합니다")
	}
	selected := append([]string(nil), ids...)
	seen := make(map[string]struct{}, len(selected))
	for _, id := range selected {
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("자동매매 중복 모델 ID: %s", id)
		}
		seen[id] = struct{}{}
		if _, err := registry.Resolve(id); err != nil {
			return nil, err
		}
	}
	if len(selected) == 1 {
		return single(selected[0])
	}
	return multi(selected)
}
