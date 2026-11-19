package trading

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
)

// PreviewCycleEvidence is a trusted INTERNAL read-only snapshot. In particular,
// neither account/owner evidence nor quantities may come from a browser POST.
// The caller must independently source durable ACTIVE reservations and a
// reconciled OwnershipReport. No Binance mutations are performed here.
type PreviewCycleEvidence struct {
	Market             exchange.LiveMarketSnapshot
	Signed             exchange.Snapshot
	Ownership          OwnershipReport
	ActiveReservations []Reservation
	Rules              exchange.SymbolRules
	AssignedUSDT       string
	Now                time.Time
}

// PreviewCycle is NOT an executable order plan. It never authorizes an order;
// the same candle cannot produce a second fresh scheduling decision.
type PreviewCycle struct {
	State               string            `json:"state"`
	CandleOpenMS        int64             `json:"candle_open_ms"`
	SelectedModelIDs    []string          `json:"selected_model_ids"`
	Preview             MultiModelPreview `json:"preview"`
	DuplicateSuppressed bool              `json:"duplicate_suppressed"`
	TradingAuthorized   bool              `json:"trading_authorized"`
	ExecutionEnabled    bool              `json:"execution_enabled"`
}

// ReadOnlyMultiModelScheduler is an in-memory, serialized *evaluation* driver.
// It deliberately has no executor, coordinator, persistence or HTTP order
// interface. An immutable selection/order fixes model priority for the life of
// this instance; a new runtime requires its own verified switch.
type ReadOnlyMultiModelScheduler struct {
	mu         sync.Mutex
	registry   *model.Registry
	ids        []string
	lastCandle int64
}

func NewReadOnlyMultiModelScheduler(registry *model.Registry, ids []string) (*ReadOnlyMultiModelScheduler, error) {
	if registry == nil || len(ids) < 2 || len(ids) > 16 {
		return nil, errors.New("읽기 전용 다중 모델 scheduler는 2~16개의 등록 모델을 요구합니다")
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return nil, fmt.Errorf("다중 모델 scheduler: 중복 ID %q", id)
		}
		seen[id] = true
		if _, err := registry.Resolve(id); err != nil {
			return nil, err
		}
	}
	return &ReadOnlyMultiModelScheduler{registry: registry, ids: append([]string(nil), ids...)}, nil
}

// Cycle checks signed account and ownership evidence on EVERY call, including
// duplicate-candle requests. Reusing a previous successful result without
// rechecking evidence could hide a newly opened external position/order.
// A rollback of the 1m candle timestamp is rejected rather than re-evaluated.
func (s *ReadOnlyMultiModelScheduler) Cycle(ctx context.Context, evidence PreviewCycleEvidence) (PreviewCycle, error) {
	out := PreviewCycle{TradingAuthorized: false, ExecutionEnabled: false}
	if s == nil {
		return out, errors.New("nil 다중 모델 scheduler")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out.SelectedModelIDs = append([]string(nil), s.ids...)
	if evidence.Now.IsZero() || len(evidence.Market.Candles) == 0 {
		return out, errors.New("다중 모델 scheduler: 1분봉 또는 검증 시각이 없습니다")
	}
	candle := evidence.Market.Candles[len(evidence.Market.Candles)-1].OpenTime
	if candle <= 0 || candle%60000 != 0 || candle > evidence.Now.UnixMilli() {
		return out, errors.New("다중 모델 scheduler: 잘못된 1분봉 시작 시각")
	}
	// The websocket receive timestamp being fresh does NOT establish that the
	// underlying candle is current. Refuse stale/resynced old 1m data.
	if evidence.Now.Sub(time.UnixMilli(candle)) > 65*time.Second {
		return out, errors.New("다중 모델 scheduler: 시장 캔들이 오래되었습니다")
	}
	out.CandleOpenMS = candle
	if s.lastCandle != 0 && candle < s.lastCandle {
		return out, errors.New("다중 모델 scheduler: 캔들 시간이 역행했습니다; 재동기화 필요")
	}
	// PreviewMultiModelSignals does not write reservations, fills, order
	// journals, or exchange state, and always returns authorization=false.
	preview, err := PreviewMultiModelSignals(s.registry, s.ids, evidence.Market,
		evidence.Signed, evidence.Ownership, evidence.ActiveReservations,
		evidence.Rules, evidence.AssignedUSDT, evidence.Now)
	if err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if preview.TradingAuthorized || preview.ExecutionEnabled {
		return out, errors.New("다중 모델 scheduler: preview가 실행 권한을 주장했습니다")
	}
	if s.lastCandle == candle {
		// Return NO new candidates or provisional margin. The signed/owner
		// snapshot was still revalidated above, but the candle was consumed.
		out.State = "DUPLICATE_CANDLE_SUPPRESSED"
		out.DuplicateSuppressed = true
		out.Preview = MultiModelPreview{Candidates: []ModelCandidatePreview{},
			Exposures: preview.Exposures, GlobalBudget: preview.GlobalBudget,
			ProvisionalTotalUSDT: "0", TradingAuthorized: false, ExecutionEnabled: false}
		return out, nil
	}
	s.lastCandle = candle
	out.State = "PREVIEW_ONLY"
	out.Preview = preview
	return out, nil
}

// LastCandle is diagnostic state only, not a restart-safe execution cursor.
func (s *ReadOnlyMultiModelScheduler) LastCandle() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCandle
}
