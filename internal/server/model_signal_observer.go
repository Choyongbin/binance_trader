package server

import (
	"errors"
	"net/http"
	"slices"
	"sync"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/trading"
)

// modelSignalObserver runs only on the server and only while its public market
// service is running. This is market-data observation, not the signed ownership
// scheduler, and has no executor/coordinator/credential access. No disk writes.
type modelSignalObserver struct {
	sampleMu   sync.Mutex // serialize background and explicit samples; late success cannot overwrite a newer failure
	mu         sync.RWMutex
	startOnce  sync.Once
	stopOnce   sync.Once
	stop       chan struct{}
	done       chan struct{}
	running    bool
	selected   []string
	snapshot   trading.MarketSignalCycle
	reason     string
	observedAt time.Time
}

func newModelSignalObserver() *modelSignalObserver {
	return &modelSignalObserver{stop: make(chan struct{}), done: make(chan struct{}), snapshot: trading.MarketSignalCycle{State: "WAITING", Signals: []trading.MarketSignal{}, TradingAuthorized: false, ExecutionEnabled: false}}
}

func (o *modelSignalObserver) Start(tick func()) {
	o.startOnce.Do(func() {
		select {
		case <-o.stop:
			close(o.done)
			return
		default:
		}
		o.mu.Lock()
		o.running = true
		o.mu.Unlock()
		go func() {
			defer close(o.done)
			defer func() { o.mu.Lock(); o.running = false; o.mu.Unlock() }()
			timer := time.NewTicker(1500 * time.Millisecond)
			defer timer.Stop()
			tick()
			for {
				select {
				case <-o.stop:
					return
				case <-timer.C:
					tick()
				}
			}
		}()
	})
}

func (o *modelSignalObserver) Close() {
	o.stopOnce.Do(func() { close(o.stop) })
	o.startOnce.Do(func() { close(o.done) })
	<-o.done
}

func (s *Server) sampleModelSignals() {
	o := s.modelObserver
	if o == nil {
		return
	}
	// Background initial tick and explicit on-demand sampling can race. A
	// late successful tick must never overwrite the newer unavailable state.
	// Serialize the entire sample, including provider read and publication.
	o.sampleMu.Lock()
	defer o.sampleMu.Unlock()
	now := time.Now().UTC()
	config := s.store.Snapshot()
	out := trading.MarketSignalCycle{State: "WAITING_FOR_SELECTION", Signals: []trading.MarketSignal{}, TradingAuthorized: false, ExecutionEnabled: false}
	reason := "두 모델을 선택하면 Demo 공개 1분봉으로 가상 FLAT 진입 조건을 평가합니다"
	if config.Environment != app.Demo {
		out.State = "BLOCKED_REAL_ENVIRONMENT"
		reason = "Real 환경에서는 시장 신호 감시를 실행하지 않습니다"
	} else if len(config.SelectedModelIDs) != 2 {
		out.State = "WAITING_FOR_MULTI_SELECTION"
	} else {
		m, err := s.modelSignalMarketProvider()
		if err != nil {
			out.State = "WAITING_FOR_FRESH_DEMO_WS"
			reason = "Demo 실시간 1분봉 준비/연속성 검증 대기"
		} else {
			preview, e := trading.PreviewMarketSignals(s.strategyRegistry, config.SelectedModelIDs, m, now)
			if e != nil {
				out.State = "BLOCKED_MARKET_EVIDENCE"
				reason = "실시간 시장 데이터 또는 모델 평가 검증 실패"
			} else {
				out = preview
				reason = "가상의 빈 포지션으로 계산한 시장 지표 신호입니다. 실제 계정·소유권을 대조한 거래 신호가 아닙니다"
			}
		}
	}
	// A late market sample must not be attributed to a changed model preference.
	if !slices.Equal(s.store.Snapshot().SelectedModelIDs, config.SelectedModelIDs) || s.store.Snapshot().Environment != config.Environment {
		out = trading.MarketSignalCycle{State: "CONFIG_CHANGED", Signals: []trading.MarketSignal{}, TradingAuthorized: false, ExecutionEnabled: false}
		reason = "모델 선택 또는 환경이 변경되어 새 평가를 기다립니다"
	}
	o.mu.Lock()
	o.selected = slices.Clone(config.SelectedModelIDs)
	o.snapshot = out
	o.reason = reason
	o.observedAt = now
	o.mu.Unlock()
}

type modelSignalStatus struct {
	State               string                 `json:"state"`
	SelectedModelIDs    []string               `json:"selected_model_ids"`
	SchedulerRunning    bool                   `json:"scheduler_running"`
	ObservedAt          string                 `json:"observed_at,omitempty"`
	CandleOpenMS        int64                  `json:"candle_open_ms,omitempty"`
	Signals             []trading.MarketSignal `json:"signals"`
	Assumption          string                 `json:"assumption"`
	Reason              string                 `json:"reason"`
	SignedAccountStatus string                 `json:"signed_account_status"`
	SignedAccountFresh  bool                   `json:"signed_account_fresh"`
	OwnershipVerified   bool                   `json:"ownership_verified"`
	RiskPreviewReady    bool                   `json:"risk_preview_ready"`
	TradingAuthorized   bool                   `json:"trading_authorized"`
	ExecutionEnabled    bool                   `json:"execution_enabled"`
}

func (s *Server) modelSignalStatusAt(now time.Time) modelSignalStatus {
	config := s.store.Snapshot()
	account := s.demo.Snapshot()
	signedFresh := false
	if at, e := time.Parse(time.RFC3339Nano, account.AsOf); e == nil && !at.After(now.Add(time.Second)) && now.Sub(at) <= 5*time.Second && account.Environment == "demo" && account.Status == "connected" && account.Balances != nil && account.Positions != nil && account.Orders != nil && account.AlgoOrders != nil && config.Environment == app.Demo {
		signedFresh = true
	}
	out := modelSignalStatus{State: "WAITING", SelectedModelIDs: slices.Clone(config.SelectedModelIDs), Signals: []trading.MarketSignal{}, Assumption: "HYPOTHETICAL_FLAT_NO_ACCOUNT_OR_OWNER_ATTESTATION", SignedAccountStatus: account.Status, SignedAccountFresh: signedFresh, TradingAuthorized: false, ExecutionEnabled: false, OwnershipVerified: false, RiskPreviewReady: false}
	o := s.modelObserver
	if o == nil {
		return out
	}
	o.mu.RLock()
	running := o.running
	ids := slices.Clone(o.selected)
	view := o.snapshot
	observed := o.observedAt
	reason := o.reason
	o.mu.RUnlock()
	out.SchedulerRunning = running
	if config.Environment != app.Demo {
		out.State = "BLOCKED_REAL_ENVIRONMENT"
		out.Reason = "실거래 환경 차단"
		return out
	}
	if len(config.SelectedModelIDs) != 2 {
		out.State = "WAITING_FOR_MULTI_SELECTION"
		out.Reason = "두 모델을 선택해야 시장 신호 평가를 시작합니다"
		return out
	}
	if !running || !slices.Equal(ids, config.SelectedModelIDs) || observed.IsZero() || observed.After(now.Add(time.Second)) || now.Sub(observed) > 4*time.Second {
		out.State = "WAITING_FOR_FRESH_OBSERVATION"
		out.Reason = "스케줄러의 최근 검증된 관측 결과가 없습니다"
		return out
	}
	out.State = view.State
	out.Reason = reason
	out.ObservedAt = observed.Format(time.RFC3339Nano)
	if view.State == "MARKET_SIGNALS_ONLY" {
		// Even if the background ticker stalls while reading public WS, a stale
		// result cannot be served as fresh. In-progress candle age is rechecked.
		if view.CandleOpenMS <= 0 || view.CandleOpenMS > now.UnixMilli() || view.CandleOpenMS != now.UnixMilli()/60000*60000 || len(view.Signals) != len(config.SelectedModelIDs) {
			out.State = "STALE_CANDLE"
			out.Reason = "관측된 캔들이 현재 1분봉이 아닙니다"
			return out
		}
		out.CandleOpenMS = view.CandleOpenMS
		out.Signals = append([]trading.MarketSignal(nil), view.Signals...)
	}
	return out
}

func (s *Server) modelMarketSignals(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "쿼리 파라미터를 지원하지 않습니다")
		return
	}
	if r.Method != "GET" {
		respondError(w, http.StatusMethodNotAllowed, "읽기 전용 상태만 조회할 수 있습니다")
		return
	}
	if s.modelObserver == nil {
		respondError(w, http.StatusServiceUnavailable, errors.New("모델 신호 감시가 준비되지 않았습니다").Error())
		return
	}
	respondJSON(w, http.StatusOK, s.modelSignalStatusAt(time.Now().UTC()))
}
