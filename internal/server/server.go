package server

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"binance_trader/internal/app"
	"binance_trader/internal/credentials"
	"binance_trader/internal/exchange"
	"binance_trader/internal/model"
	"binance_trader/internal/trading"
	"binance_trader/internal/version"
	"binance_trader/internal/wallet"
)

//go:embed web/*
var assets embed.FS

type Server struct {
	dataDir                   string
	store                     *app.Store
	wallet                    *wallet.Store
	credentialDir             string
	vault                     *credentials.Vault
	demo                      *exchange.Manager
	market                    *exchange.MarketService
	live                      *exchange.StreamService
	manualQuote               func() (trading.Quote, error) // live backend only; tests inject a fake
	csrfToken                 string
	privateProbeBusy          atomic.Bool
	accountDiagnosticBusy     atomic.Bool
	configMutationBusy        atomic.Bool
	recoveryBusy              atomic.Bool
	recoveryRunner            func(context.Context, trading.DemoCredentials, string, int64) (trading.RecoveryProbe, error)
	soakRunner                func(context.Context, trading.DemoCredentials, string, int64, time.Duration) (trading.RecoveryProbe, error)
	reconnectRunner           func(context.Context, trading.DemoCredentials, string, int64, time.Duration) (trading.ReconnectWatchReport, error)
	monitor                   monitorControl
	monitorRunner             monitorRunner
	monitorLifecycleMu        sync.Mutex
	accountingBusy            atomic.Bool
	accountingReader          trading.AccountingWindowReader
	privateProbeRunner        func(context.Context, string) trading.PrivateTransportResult
	readinessCapability       func() (known bool, canTrade bool)
	autoControl               *trading.AutoRunControl
	autoController            *trading.AutoController
	protectionWatcher         *trading.ProductProtectionPoller
	strategyRegistry          *model.Registry
	modelObserver             *modelSignalObserver
	modelSignalMarketProvider func() (exchange.LiveMarketSnapshot, error)
	started                   time.Time
}

func New(dataDir string) (*Server, error) { return newWithFetcher(dataDir, exchange.NewDemoClient()) }
func newWithFetcher(dataDir string, fetcher exchange.Fetcher) (*Server, error) {
	store, err := app.OpenStore(dataDir)
	if err != nil {
		return nil, err
	}
	virtualWallet, err := wallet.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("자동매매 배정 지갑 검증 오류: %w", err)
	}
	credentialDir := filepath.Join(dataDir, "credentials")
	vault, err := credentials.NewVault(credentialDir)
	if err != nil {
		return nil, err
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return nil, err
	}
	market := exchange.NewDemoMarketService()
	live := exchange.NewDemoStream(market)
	live.SetEnabled(store.Snapshot().Environment == app.Demo)
	srv := &Server{dataDir: dataDir, store: store, wallet: virtualWallet, credentialDir: credentialDir, vault: vault, demo: exchange.NewManager(fetcher), market: market, live: live, csrfToken: base64.RawURLEncoding.EncodeToString(random), started: time.Now()}
	autoControl, err := trading.OpenAutoRunControl(dataDir)
	if err != nil {
		srv.demo.Close()
		return nil, fmt.Errorf("자동매매 run control 검증 오류: %w", err)
	}
	srv.autoControl = autoControl
	srv.strategyRegistry = model.DefaultRegistry()
	srv.modelObserver = newModelSignalObserver()
	srv.modelSignalMarketProvider = srv.live.Get
	autoClient := exchange.NewDemoClient()
	credentialsProvider := func(ctx context.Context) (trading.DemoCredentials, error) {
		if err := ctx.Err(); err != nil {
			return trading.DemoCredentials{}, err
		}
		key, secret, err := srv.demo.InternalCredentials()
		if err != nil {
			return trading.DemoCredentials{}, err
		}
		return trading.DemoCredentials{APIKey: key, Secret: secret}, nil
	}
	marketProvider := func(ctx context.Context) (exchange.LiveMarketSnapshot, error) {
		if err := ctx.Err(); err != nil {
			return exchange.LiveMarketSnapshot{}, err
		}
		return srv.live.Get()
	}
	srv.autoController, err = trading.NewAutoController(autoControl, func(lease trading.AutoRunLease) (trading.AutoRuntime, error) {
		if srv.store.Snapshot().Environment != app.Demo {
			return nil, errors.New("Real 환경에서는 자동매매 실행 런타임을 구성할 수 없습니다")
		}
		return chooseAutoRuntime(srv.store.Snapshot().SelectedModelIDs, srv.strategyRegistry,
			func(id string) (trading.AutoRuntime, error) {
				return trading.NewDemoAutoExecutionRuntimeWithRegistry(srv.strategyRegistry, dataDir, id, autoClient, credentialsProvider, marketProvider, autoControl, lease)
			},
			func(ids []string) (trading.AutoRuntime, error) {
				return trading.NewDemoMultiModelAutoRuntimeWithRegistry(srv.strategyRegistry, dataDir, ids, autoClient, credentialsProvider, marketProvider, autoControl, lease)
			},
		)
	}, time.Second)
	if err != nil {
		srv.demo.Close()
		return nil, fmt.Errorf("자동매매 controller 구성 오류: %w", err)
	}
	// Process-lifetime, GET-only protection observation. The watcher is
	// independent of the auto-run lease, so STOP does not stop inspecting
	// existing exposure. It never authorizes a Binance POST or DELETE.
	srv.protectionWatcher, err = trading.NewProductProtectionPoller(func(ctx context.Context) (trading.PendingProtectionInventory, error) {
		if srv.store.Snapshot().Environment != app.Demo || srv.accountDiagnosticBusy.Load() ||
			srv.recoveryBusy.Load() || srv.autoController.Status().Running {
			return trading.PendingProtectionInventory{State: trading.PendingProtectionInspectionBlocked, ManualReviewRequired: true},
				errors.New("protection inspection deferred: environment or exclusive operation")
		}
		return trading.InspectDemoOutstandingProtectionsOnce(ctx, dataDir, autoClient, credentialsProvider)
	}, 90*time.Second, 15*time.Second)
	if err != nil {
		srv.demo.Close()
		return nil, fmt.Errorf("protection read-only watch configuration: %w", err)
	}
	srv.accountingReader = exchange.NewDemoClient()
	srv.recoveryRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64) (trading.RecoveryProbe, error) {
		return trading.ProbeLiveDemoRecovery(ctx, exchange.NewDemoClient(), creds, dir, since, nil)
	}
	srv.soakRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64, dwell time.Duration) (trading.RecoveryProbe, error) {
		return trading.ProbeLiveDemoRecoverySoak(ctx, exchange.NewDemoClient(), creds, dir, since, exchange.DialDemoUserStreamURL, dwell)
	}
	srv.reconnectRunner = func(ctx context.Context, creds trading.DemoCredentials, dir string, since int64, dwell time.Duration) (trading.ReconnectWatchReport, error) {
		return trading.WatchLiveDemoReconnect(ctx, exchange.NewDemoClient(), creds, dir, since, exchange.DialDemoUserStreamURL, dwell)
	}
	srv.readinessCapability = func() (bool, bool) { return false, false }
	srv.privateProbeRunner = func(ctx context.Context, key string) trading.PrivateTransportResult {
		return trading.ProbePrivateTransport(ctx, exchange.NewDemoClient(), key, exchange.DialDemoUserStreamURL, 5*time.Second)
	}
	srv.manualQuote = func() (trading.Quote, error) {
		m, e := srv.live.Get()
		if e != nil {
			return trading.Quote{}, e
		}
		if len(m.Candles) == 0 {
			return trading.Quote{}, errors.New("시세 데이터가 없습니다")
		}
		receivedAt, err := time.Parse(time.RFC3339Nano, m.LastReceivedAt)
		if err != nil || receivedAt.IsZero() {
			return trading.Quote{}, errors.New("WS 수신 시각 확인 실패")
		}
		return trading.Quote{Symbol: m.Symbol, Price: strconv.FormatFloat(m.Candles[len(m.Candles)-1].Close, 'f', 8, 64), ReceivedAt: receivedAt}, nil
	}
	return srv, nil
}

func (s *Server) StartMarket() {
	s.live.Start()
	s.modelObserver.Start(s.sampleModelSignals)
	if s.protectionWatcher != nil {
		_ = s.protectionWatcher.Start()
	}
}
func (s *Server) Close() {
	if s.protectionWatcher != nil {
		_ = s.protectionWatcher.Close()
	}
	if s.modelObserver != nil {
		s.modelObserver.Close()
	}
	if s.autoController != nil && s.autoController.Status().Running {
		_ = s.autoController.Stop()
	}
	s.monitorLifecycleMu.Lock()
	_ = s.stopContinuousMonitor()
	s.monitorLifecycleMu.Unlock()
	s.live.Close()
	s.demo.Close()
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", s.health)
	mux.HandleFunc("GET /api/v1/version", s.version)
	mux.HandleFunc("GET /api/v1/market/klines", s.marketCandles)
	mux.HandleFunc("GET /api/v1/market/live", s.liveMarket)
	mux.HandleFunc("GET /api/v1/market/status", s.liveStatus)
	mux.HandleFunc("GET /api/v1/bootstrap", s.bootstrap)
	mux.HandleFunc("GET /api/v1/account/balances", s.balances)
	mux.HandleFunc("GET /api/v1/account/positions", s.positions)
	mux.HandleFunc("GET /api/v1/account/orders", s.orders)
	mux.HandleFunc("GET /api/v1/account/snapshot", s.accountSnapshot)
	mux.HandleFunc("GET /api/v1/wallet/auto", s.autoWalletStatus)
	mux.HandleFunc("GET /api/v1/trading/multi-model/evidence", s.multiModelEvidence)
	mux.HandleFunc("GET /api/v1/trading/local-readiness", s.localReadiness)
	mux.HandleFunc("GET /api/v1/trading/readiness", s.tradingReadiness)
	mux.HandleFunc("POST /api/v1/trading/execution-preflight", s.mutation(s.executionPreflightDiagnostic))
	mux.HandleFunc("GET /api/v1/trading/auto/status", s.autoControlStatus)
	mux.HandleFunc("GET /api/v1/trading/protection/watch-status", s.protectionWatchStatus)
	mux.HandleFunc("POST /api/v1/wallet/auto/change", s.mutation(s.changeAutoWallet))
	mux.HandleFunc("GET /api/v1/trading/config", s.tradingConfig)
	mux.HandleFunc("POST /api/v1/trading/config/apply-demo-v1", s.mutation(s.applyDemoTradingConfig))
	mux.HandleFunc("POST /api/v1/trading/manual/preview", s.mutation(s.manualPreview))
	mux.HandleFunc("GET /api/v1/recovery/status", s.recoveryStatus)
	mux.HandleFunc("GET /api/v1/trading/stream-gaps/status", s.streamGapsStatus)
	mux.HandleFunc("POST /api/v1/recovery/probe", s.mutation(s.runRecoveryProbe))
	mux.HandleFunc("POST /api/v1/recovery/soak", s.mutation(s.runRecoverySoak))
	mux.HandleFunc("GET /api/v1/recovery/soak/status", s.recoverySoakStatus)
	mux.HandleFunc("POST /api/v1/recovery/reconnect-watch", s.mutation(s.runReconnectWatch))
	mux.HandleFunc("GET /api/v1/recovery/reconnect-watch/status", s.reconnectWatchStatus)
	mux.HandleFunc("GET /api/v1/recovery/continuous-monitor/status", s.monitorStatus)
	mux.HandleFunc("GET /api/v1/recovery/restart-forensics/status", s.restartForensicsStatus)
	mux.HandleFunc("POST /api/v1/recovery/continuous-monitor/start", s.mutation(s.startContinuousMonitor))
	mux.HandleFunc("POST /api/v1/recovery/continuous-monitor/stop", s.mutation(s.stopContinuousMonitorHTTP))
	mux.HandleFunc("GET /api/v1/trading/stream-review/status", s.streamReviewStatus)
	mux.HandleFunc("GET /api/v1/trading/integrated-review/status", s.integratedReviewStatus)
	mux.HandleFunc("GET /api/v1/trading/operator-status", s.operatorUnifiedStatus)
	mux.HandleFunc("GET /api/v1/trading/operator-remediation", s.operatorRemediation)
	mux.HandleFunc("GET /api/v1/trading/operator-incident-packets", s.operatorIncidentPackets)
	mux.HandleFunc("GET /api/v1/trading/operator-incident-timeline", s.operatorIncidentTimeline)
	mux.HandleFunc("GET /api/v1/trading/bounded-drift/status", s.boundedDriftStatus)
	mux.HandleFunc("GET /api/v1/trading/accounting/status", s.accountingStatus)
	mux.HandleFunc("POST /api/v1/trading/accounting/generate", s.mutation(s.generateAccounting))
	mux.HandleFunc("GET /api/v1/trading/accounting/index/status", s.accountingIndexStatus)
	mux.HandleFunc("GET /api/v1/trading/accounting/history", s.accountingHistory)
	mux.HandleFunc("GET /api/v1/trading/private-probe/status", s.privateProbeStatus)
	mux.HandleFunc("POST /api/v1/trading/private-probe/run", s.mutation(s.runPrivateProbe))
	mux.HandleFunc("POST /api/v1/account/refresh", s.mutation(s.refresh))
	mux.HandleFunc("POST /api/v1/credentials/demo/save", s.mutation(s.saveDemo))
	mux.HandleFunc("POST /api/v1/credentials/demo/unlock", s.mutation(s.unlockDemo))
	mux.HandleFunc("POST /api/v1/credentials/demo/lock", s.mutation(s.lockDemo))
	mux.HandleFunc("POST /api/v1/settings/environment", s.mutation(s.changeEnvironment))
	mux.HandleFunc("PUT /api/v1/models/selection", s.mutation(s.changeModels))
	mux.HandleFunc("GET /api/v1/models/runtime", s.modelRuntimeStatus)
	mux.HandleFunc("GET /api/v1/trading/model-dashboard", s.modelDashboard)
	mux.HandleFunc("GET /api/v1/models/market-signals", s.modelMarketSignals)
	mux.HandleFunc("POST /api/v1/trading/auto/start", s.mutation(s.locked))
	mux.HandleFunc("POST /api/v1/trading/auto/stop", s.mutation(s.locked))
	mux.HandleFunc("POST /api/v1/trading/manual/orders", s.mutation(s.locked))
	webFS, err := fs.Sub(assets, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(webFS)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.securityHeaders(w)
		// Restrict browser and DNS-rebinding access to loopback Host values.
		host := r.Host
		hostname := host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			hostname = parsed
		}
		if hostname != "localhost" && hostname != "127.0.0.1" && hostname != "::1" {
			respondError(w, 403, "로컬 호스트에서만 접근할 수 있습니다")
			return
		}
		// Prevent stale HTML/JS from persisting across manual local upgrades.
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
}

func respondJSON(w http.ResponseWriter, status int, obj any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(obj)
}
func respondError(w http.ResponseWriter, code int, message string) {
	respondJSON(w, code, map[string]any{"error": message})
}
func (s *Server) marketCandles(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 화면에서는 데모 시세를 표시하지 않습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "시장 데이터 종목과 주기는 BTCUSDT/1분봉으로 고정되어 있습니다")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
	defer cancel()
	snapshot, err := s.market.Get(ctx)
	if err != nil {
		respondError(w, 502, err.Error())
		return
	}
	respondJSON(w, 200, snapshot)
}

func (s *Server) liveStatus(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 화면은 잠겨 있습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "쿼리 파라미터를 지원하지 않습니다")
		return
	}
	respondJSON(w, 200, s.live.Status())
}

func (s *Server) liveMarket(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 화면은 잠겨 있습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, 400, "종목은 BTCUSDT/1분봉으로 고정되어 있습니다")
		return
	}
	snapshot, err := s.live.Get()
	// An environment switch may race with an already-started request.
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 화면은 잠겨 있습니다")
		return
	}
	if err != nil {
		respondJSON(w, 503, snapshot.LiveMarketStatus)
		return
	}
	respondJSON(w, 200, snapshot)
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, 200, map[string]any{"release": version.Release, "phase": version.Phase, "parent": version.Parent, "trading": version.Trading})
}
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	status := s.demo.Snapshot().Status
	if s.store.Snapshot().Environment != app.Demo {
		status = "locked"
	}
	respondJSON(w, 200, map[string]any{"server": "running", "release": version.Release, "exchange": status, "trading": "disabled", "uptime_seconds": int(time.Since(s.started).Seconds()), "market_stream": s.live.Status()})
}
func (s *Server) bootstrap(w http.ResponseWriter, _ *http.Request) {
	current := s.store.Snapshot()
	respondJSON(w, 200, map[string]any{
		"product": "바이낸스 트레이더", "phase": version.PhaseMajor, "phase_name": version.Phase, "release": version.Release,
		"environment": current.Environment, "csrf_token": s.csrfToken,
		"models": app.ModelCatalog, "selected_model_ids": current.SelectedModelIDs,
		"registered_model_ids": s.strategyRegistry.IDs(),
		"connection":           s.demoStatus(current.Environment), "automation": "controller_connected_protection_arming_locked",
		"trading_enabled":          false,
		"account":                  s.filteredAccount(current.Environment),
		"risk":                     map[string]any{"entry_margin_fraction": 0.10, "aggregate_auto_margin_policy": "INTERNAL_DEMO_ALLOCATION", "default_auto_leverage": 15, "margin_mode": "ISOLATED", "position_mode": "HEDGE", "take_profit_roi": 0.05, "stop_loss_roi": -0.05},
		"credentials":              credentials.Inspect(s.credentialDir),
		"refresh_interval_seconds": 10,
		"server_time":              time.Now().UTC().Format(time.RFC3339),
	})
}
func (s *Server) demoStatus(env app.Environment) string {
	if env != app.Demo {
		return "real_locked"
	}
	return s.demo.Snapshot().Status
}
func (s *Server) filteredAccount(env app.Environment) exchange.Snapshot {
	if env != app.Demo {
		return exchange.Snapshot{Status: "real_locked", Environment: "real"}
	}
	return s.demo.Snapshot()
}

// tradingConfig is an explicit, READ-ONLY user-triggered signed lookup. It is
// not part of periodic polling: the account-wide position-mode endpoint has
// a nontrivial request weight. Configuration POSTs and order routes remain
// inaccessible to the browser during this phase.
func (s *Server) tradingConfig(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 설정 조회는 잠겨 있습니다")
		return
	}
	if r.URL.RawQuery != "" {
		respondError(w, http.StatusBadRequest, "지원하지 않는 조회 매개변수입니다")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	config, err := s.demo.InspectTradingConfiguration(ctx, "BTCUSDT")
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "환경이 변경되어 이전 응답을 버렸습니다")
		return
	}
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, config)
}

func (s *Server) executionPreflightDiagnostic(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 execution preflight는 잠겨 있습니다")
		return
	}
	if s.autoController != nil && s.autoController.Status().Running {
		respondError(w, http.StatusConflict, "자동매매 controller가 실행 중입니다")
		return
	}
	var input struct {
		Confirmation string `json:"confirmation"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, http.StatusBadRequest, "요청 형식이 잘못되었습니다")
		return
	}
	if input.Confirmation != "RUN READ ONLY DEMO EXECUTION PREFLIGHT" {
		respondError(w, http.StatusBadRequest, "명시적인 read-only execution preflight 확인 문구가 필요합니다")
		return
	}
	if held, err := trading.MutationGatePresent(s.dataDir); err != nil || held {
		respondError(w, http.StatusLocked, "mutation gate가 활성 상태이거나 안전하게 확인할 수 없습니다")
		return
	}
	if _, err := os.Lstat(filepath.Join(s.dataDir, "trading", "coordinator.active")); err == nil {
		respondError(w, http.StatusLocked, "거래 coordinator가 활성 상태라 preflight를 실행할 수 없습니다")
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		respondError(w, http.StatusLocked, "거래 coordinator 상태를 안전하게 확인할 수 없습니다")
		return
	}
	fingerprint := s.demo.KeyFingerprint()
	if fingerprint == "" || s.demo.Snapshot().Status != "connected" {
		respondError(w, http.StatusServiceUnavailable, "Demo 계정 인증을 먼저 해제하세요")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	snap, err := s.demo.InspectExecutionPreflight(ctx, "BTCUSDT")
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "signed execution preflight 실패: "+err.Error())
		return
	}
	if s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != fingerprint {
		respondError(w, http.StatusLocked, "execution preflight 도중 환경 또는 Demo 인증 상태가 변경되었습니다")
		return
	}
	nonZeroPositions := 0
	for _, p := range snap.Portfolio.Positions {
		amt, ok := new(big.Rat).SetString(string(p.PositionAmt))
		if !ok {
			respondError(w, http.StatusServiceUnavailable, "signed execution preflight 포지션 수량 형식 오류")
			return
		}
		if amt.Sign() != 0 {
			nonZeroPositions++
		}
	}
	elapsed := snap.FinishedAt.Sub(snap.StartedAt)
	respondJSON(w, http.StatusOK, map[string]any{
		"environment":            snap.Environment,
		"symbol":                 "BTCUSDT",
		"started_at":             snap.StartedAt,
		"finished_at":            snap.FinishedAt,
		"elapsed_ms":             elapsed.Milliseconds(),
		"can_trade":              snap.Portfolio.CanTrade,
		"nonzero_position_count": nonZeroPositions,
		"open_order_count":       len(snap.OpenOrders),
		"open_algo_count":        len(snap.OpenAlgos),
		"settings_match_v1":      snap.Configuration.SettingsMatchV1,
		"configuration":          snap.Configuration,
		"mutation_performed":     false,
		"trading_authorized":     false,
	})
}

func (s *Server) applyDemoTradingConfig(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, http.StatusLocked, "실거래 계정 설정 변경은 잠겨 있습니다")
		return
	}
	if s.autoController != nil && s.autoController.Status().Running {
		respondError(w, http.StatusConflict, "자동매매 controller가 실행 중입니다")
		return
	}
	if !s.configMutationBusy.CompareAndSwap(false, true) {
		respondError(w, http.StatusConflict, "Demo 거래 설정 변경이 이미 진행 중입니다")
		return
	}
	defer s.configMutationBusy.Store(false)
	var input struct {
		Confirmation string `json:"confirmation"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, http.StatusBadRequest, "요청 형식이 잘못되었습니다")
		return
	}
	if input.Confirmation != "APPLY DEMO BTCUSDT HEDGE ISOLATED 15X" {
		respondError(w, http.StatusBadRequest, "명시적인 Demo 설정 변경 확인 문구가 필요합니다")
		return
	}
	if _, err := os.Lstat(filepath.Join(s.dataDir, "trading", "coordinator.active")); err == nil {
		respondError(w, http.StatusLocked, "거래 coordinator가 활성 상태라 설정을 변경할 수 없습니다")
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		respondError(w, http.StatusLocked, "거래 coordinator 상태를 안전하게 확인할 수 없습니다")
		return
	}
	gate, err := trading.AcquireMutationGate(s.dataDir)
	if err != nil {
		respondError(w, http.StatusLocked, err.Error())
		return
	}
	defer gate.Close()
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	config, err := s.demo.ApplyDemoTradingSetup(ctx, "BTCUSDT")
	if err != nil {
		respondError(w, http.StatusConflict, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"applied": true, "mutation_performed": true, "config": config})
}

func (s *Server) accountSnapshot(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, 200, s.filteredAccount(s.store.Snapshot().Environment))
}
func (s *Server) connected(w http.ResponseWriter) (exchange.Snapshot, bool) {
	current := s.filteredAccount(s.store.Snapshot().Environment)
	if current.Status != "connected" {
		respondJSON(w, 503, current)
		return exchange.Snapshot{}, false
	}
	return current, true
}
func (s *Server) balances(w http.ResponseWriter, _ *http.Request) {
	if current, ok := s.connected(w); ok {
		respondJSON(w, 200, map[string]any{"status": "connected", "items": current.Balances, "as_of": current.AsOf})
	}
}
func (s *Server) positions(w http.ResponseWriter, _ *http.Request) {
	if current, ok := s.connected(w); ok {
		respondJSON(w, 200, map[string]any{"status": "connected", "items": current.Positions, "as_of": current.AsOf})
	}
}
func (s *Server) orders(w http.ResponseWriter, _ *http.Request) {
	if current, ok := s.connected(w); ok {
		respondJSON(w, 200, map[string]any{"status": "connected", "items": current.Orders, "algo_orders": current.AlgoOrders, "as_of": current.AsOf})
	}
}
func (s *Server) stopAutoForAccountMutation() error {
	if s.autoController == nil {
		return nil
	}
	// Always issue an explicit durable STOP before credential/environment/model
	// mutation. Running=false is not sufficient: the controller may be in a
	// STOPPING/fail-closed transition or restart-blocked by an interrupted run.
	return s.autoController.Stop()
}

func (s *Server) saveDemo(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 화면에서는 Demo 인증 정보를 변경할 수 없습니다")
		return
	}
	var input struct {
		Key        string `json:"api_key"`
		Secret     string `json:"secret_key"`
		Passphrase string `json:"passphrase"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, 400, "입력 형식을 확인하세요")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.demo.ValidateCredentials(ctx, input.Key, input.Secret); err != nil {
		respondError(w, 502, "Demo 연결 검증 실패: "+err.Error())
		return
	}
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	if err := s.stopAutoForAccountMutation(); err != nil {
		respondError(w, 503, "자동매매 controller를 안전하게 중지하지 못했습니다: "+err.Error())
		return
	}
	if !s.stopContinuousMonitor() {
		respondError(w, 503, "active account observer did not stop; credentials unchanged")
		return
	}
	if err := s.vault.SaveDemo(input.Key, input.Secret, input.Passphrase); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	if err := s.demo.Connect(ctx, input.Key, input.Secret); err != nil {
		respondError(w, 503, "인증 정보 저장 후 Demo 재연결 실패: "+err.Error())
		return
	}
	respondJSON(w, 200, map[string]any{"status": "connected", "credentials": credentials.Inspect(s.credentialDir)})
}
func (s *Server) unlockDemo(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "데모 환경에서만 연결할 수 있습니다")
		return
	}
	var input struct {
		Passphrase string `json:"passphrase"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, 400, "입력 형식을 확인하세요")
		return
	}
	key, secret, err := s.vault.ReadDemo(input.Passphrase)
	if err != nil {
		respondError(w, 400, "복호화 실패: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	if err := s.stopAutoForAccountMutation(); err != nil {
		respondError(w, 503, "자동매매 controller를 안전하게 중지하지 못했습니다: "+err.Error())
		return
	}
	if !s.stopContinuousMonitor() {
		respondError(w, 503, "active monitor did not stop; account unchanged")
		return
	}
	if err := s.demo.Connect(ctx, key, secret); err != nil {
		respondError(w, 502, err.Error())
		return
	}
	respondJSON(w, 200, map[string]any{"status": "connected"})
}
func (s *Server) lockDemo(w http.ResponseWriter, _ *http.Request) {
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	if err := s.stopAutoForAccountMutation(); err != nil {
		respondError(w, 503, "자동매매 controller를 안전하게 중지하지 못했습니다: "+err.Error())
		return
	}
	if !s.stopContinuousMonitor() {
		respondError(w, 503, "active monitor did not stop; account unchanged")
		return
	}
	s.demo.Clear()
	respondJSON(w, 200, map[string]any{"status": "locked"})
}
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 계정 조회는 잠겨 있습니다")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.demo.Refresh(ctx); err != nil {
		respondError(w, 503, err.Error())
		return
	}
	respondJSON(w, 200, s.demo.Snapshot())
}
func (s *Server) locked(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusLocked, map[string]any{"error": "2단계에서는 모든 주문 및 자동매매가 비활성화되어 있습니다"})
}
func (s *Server) mutation(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			originURL := strings.TrimPrefix(strings.TrimPrefix(origin, "http://"), "https://")
			// Reject origins from non-local hosts and require exact Host match.
			if !strings.HasPrefix(origin, "http://") || originURL != r.Host {
				respondError(w, 403, "허용되지 않은 요청 출처입니다")
				return
			}
		}
		if r.Header.Get("X-CSRF-Token") != s.csrfToken {
			respondError(w, 403, "CSRF 검증에 실패했습니다")
			return
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			respondError(w, 415, "JSON 요청만 허용됩니다")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		next(w, r)
	}
}
func decodeStrict(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("JSON 본문은 하나만 허용됩니다")
	}
	return nil
}
func (s *Server) changeEnvironment(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Environment app.Environment `json:"environment"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, 400, "잘못된 요청 형식입니다")
		return
	}
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	if err := s.stopAutoForAccountMutation(); err != nil {
		respondError(w, 503, "자동매매 controller를 안전하게 중지하지 못했습니다: "+err.Error())
		return
	}
	if !s.stopContinuousMonitor() {
		respondError(w, 503, "active monitor did not stop; environment unchanged")
		return
	}
	changed, err := s.store.UpdateEnvironment(input.Environment)
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	s.demo.Clear() // switching environments always terminates authenticated sessions
	s.live.SetEnabled(changed.Environment == app.Demo)
	respondJSON(w, 200, map[string]any{"environment": changed.Environment, "trading_enabled": false})
}
func (s *Server) changeModels(w http.ResponseWriter, r *http.Request) {
	var input struct {
		IDs []string `json:"ids"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, 400, "잘못된 요청 형식입니다")
		return
	}
	if input.IDs == nil {
		respondError(w, 400, "모델 ID 목록이 필요합니다")
		return
	}
	s.monitorLifecycleMu.Lock()
	defer s.monitorLifecycleMu.Unlock()
	current := s.store.Snapshot()
	// Up to two registered models may be saved for READ-ONLY scheduling.
	// selectedRuntimeModel still strictly rejects multi-selection for the
	// executable single-model AutoController factory. HTTP orders stay 423.
	if len(input.IDs) > 2 {
		respondError(w, 400, "읽기 전용 미리보기 모델은 최대 두 개까지 선택할 수 있습니다")
		return
	}
	seenModelIDs := map[string]bool{}
	for _, id := range input.IDs {
		if seenModelIDs[id] {
			respondError(w, 400, "중복 모델 ID는 선택할 수 없습니다")
			return
		}
		seenModelIDs[id] = true
	}
	for _, id := range input.IDs {
		if _, err := s.strategyRegistry.Resolve(id); err != nil {
			respondError(w, 400, err.Error())
			return
		}
	}
	if slices.Equal(current.SelectedModelIDs, input.IDs) {
		respondJSON(w, 200, map[string]any{"selected_model_ids": current.SelectedModelIDs, "runtime_model_id": selectedModelPreview(current.SelectedModelIDs, s.strategyRegistry), "scheduler_mode": modelSchedulerMode(current.SelectedModelIDs), "trading_enabled": false, "mutation_performed": false})
		return
	}
	if current.Environment != app.Demo {
		respondError(w, 423, "Real 환경에서는 모델 실행 전환을 지원하지 않습니다")
		return
	}
	if err := s.stopAutoForAccountMutation(); err != nil {
		respondError(w, 503, "자동매매 controller를 안전하게 중지하지 못했습니다: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.demo.Refresh(ctx); err != nil {
		respondError(w, 423, "모델 전환에는 연결된 Demo signed 계정의 최신 조회가 필요합니다")
		return
	}
	if err := trading.VerifyModelSelectionSafe(s.dataDir, s.demo.Snapshot(), time.Now().UTC()); err != nil {
		respondError(w, 423, err.Error())
		return
	}
	changed, err := s.store.UpdateSelection(slices.Clone(input.IDs))
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	respondJSON(w, 200, map[string]any{"selected_model_ids": changed.SelectedModelIDs, "runtime_model_id": selectedModelPreview(changed.SelectedModelIDs, s.strategyRegistry), "scheduler_mode": modelSchedulerMode(changed.SelectedModelIDs), "trading_enabled": false, "mutation_performed": true})
}

// recoveryStatus is strictly read-only. It never starts a private stream,
// decrypts credentials, or interprets a report as trading authorization.
func (s *Server) recoveryStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		respondError(w, 400, "파라미터를 지원하지 않습니다")
		return
	}
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 환경의 복구 진단을 지원하지 않습니다")
		return
	}
	records, err := trading.LoadRecoveryProbes(s.dataDir)
	if err != nil {
		respondError(w, 503, "복구 증적 손상 또는 접근 실패: 거래는 잠금 상태입니다")
		return
	}
	response := map[string]any{"trading_authorized": false, "status": "대기", "report_count": len(records), "manual_review_required": true, "diagnostic_busy": s.recoveryBusy.Load(), "stream_continuity_proven": false}
	// A durable exclusive lock can represent a running or crashed probe;
	// either way it is NOT a ready-to-trade state.
	if _, err := os.Stat(filepath.Join(s.dataDir, "trading", "recovery_sessions", "exclusive.active")); err == nil {
		response["diagnostic_running_or_stale_lock"] = true
		response["status"] = "복구 진단 실행 중 또는 비정상 종료: 거래 잠금"
	} else if !errors.Is(err, os.ErrNotExist) {
		respondError(w, 503, "복구 진단 잠금 상태 검사 실패")
		return
	}
	if len(records) > 0 {
		latest := records[len(records)-1]
		// A previously saved report is necessarily historic (no live stream is
		// tied to this endpoint); do not expose it as CURRENT or READY.
		if response["diagnostic_running_or_stale_lock"] != true {
			response["status"] = "수동 확인 필요"
		}
		response["last_report"] = map[string]any{"stage": latest.Stage, "started_at": latest.StartedAt, "finished_at": latest.FinishedAt, "rest_reviewed": latest.RESTReviewed, "observer_connected": latest.ObserverConnected, "blockers": latest.Blockers, "stale_after_observation": latest.StaleAfterObservation}
	}
	respondJSON(w, 200, response)
}

// autoWalletStatus exposes ONLY a local allocation. It never represents a
// segregated Binance subaccount or guarantees that other orders cannot draw
// down the same actual USD-M collateral.
func (s *Server) autoWalletStatus(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "실거래 지갑은 잠겨 있습니다")
		return
	}
	status, err := s.wallet.SnapshotFor(s.demo.KeyFingerprint())
	if err != nil {
		respondError(w, 503, err.Error())
		return
	}
	snap := s.demo.Snapshot()
	_, historical, localEditErr := trading.CheckWalletEditHistory(s.dataDir)
	response := map[string]any{"wallet": status, "connected": false, "account_free_usdt": "", "unallocated_usdt": "", "can_adjust": false, "can_allocate": false, "can_return": false, "requires_reconciliation": historical, "requires_signed_audit": true, "warning": "자동매매 증거금 지갑은 실제 이체가 아닌 내부 배정 한도입니다. 동일 Binance 선물 계정의 수동/외부 주문은 실제 잔액에 영향을 줍니다."}
	when, e := time.Parse(time.RFC3339Nano, snap.AsOf)
	if snap.Status == "connected" && e == nil && time.Since(when) <= 15*time.Second && len(snap.Positions) == 0 && len(snap.Orders) == 0 && len(snap.AlgoOrders) == 0 {
		response["connected"] = true
		for _, b := range snap.Balances {
			if b.Asset != "USDT" {
				continue
			}
			actual, err := wallet.Decimal(b.Available, true)
			if err != nil {
				break
			}
			full, err := wallet.Decimal(b.Wallet, true)
			if err != nil {
				break
			}
			assigned, err := wallet.Decimal(status.AssignedUSDT, true)
			if err != nil {
				break
			}
			response["account_free_usdt"] = wallet.Format(actual)
			unallocated := new(big.Rat).Sub(full, assigned)
			if unallocated.Sign() >= 0 {
				response["unallocated_usdt"] = wallet.Format(unallocated)
			}
			response["can_allocate"] = status.Verified && full.Cmp(assigned) >= 0 && actual.Cmp(assigned) >= 0
			response["can_return"] = status.Verified && assigned.Sign() > 0
			response["can_adjust"] = response["can_allocate"].(bool) || response["can_return"].(bool)
		}
	}
	if held, gateErr := trading.MutationGatePresent(s.dataDir); gateErr != nil || held {
		response["can_adjust"] = false
		response["can_allocate"] = false
		response["can_return"] = false
		response["warning"] = "지갑 변경/조정기 공통 잠금이 진행 중이거나 비정상 종료되었습니다: 수동 점검 필요"
	} else if localEditErr != nil {
		response["can_adjust"] = false
		response["can_allocate"] = false
		response["can_return"] = false
		response["warning"] = "로컬 거래/이벤트 기록 또는 비정상 잠금 확인: 독립 계정 감사 전 배정 변경 금지"
	} else if historical {
		response["warning"] = "배정 변경 시 과거 기록 유무와 관계없이 저장 암호와 전체 Demo 계정 상태를 다시 검증합니다. 실제 주문은 잠겨 있습니다."
	}
	respondJSON(w, 200, response)
}
func (s *Server) changeAutoWallet(w http.ResponseWriter, r *http.Request) {
	if s.store.Snapshot().Environment != app.Demo {
		respondError(w, 423, "Demo 전용 배정 지갑입니다")
		return
	}
	var input struct {
		RequestID  string `json:"request_id"`
		Operation  string `json:"operation"`
		AmountUSDT string `json:"amount_usdt"`
		Passphrase string `json:"passphrase,omitempty"`
	}
	if err := decodeStrict(r, &input); err != nil {
		respondError(w, 400, "잘못된 배정 요청 형식")
		return
	}
	// Snapshot must be from the currently unlocked session, fresh, and have no
	// observed BTC exposure. Full-account baseline still gates future trades.
	snap := s.demo.Snapshot()
	keyID := s.demo.KeyFingerprint()
	at, err := time.Parse(time.RFC3339Nano, snap.AsOf)
	if err != nil || snap.Status != "connected" || keyID == "" || at.After(time.Now().Add(time.Second)) || time.Since(at) > 15*time.Second || len(snap.Positions) > 0 || len(snap.Orders) > 0 || len(snap.AlgoOrders) > 0 {
		respondError(w, 423, "최신 Demo 인증/계정 조회가 필요하며 관측된 포지션과 대기 주문이 없어야 배정액을 바꿀 수 있습니다")
		return
	}
	gate, gateErr := trading.AcquireMutationGate(s.dataDir)
	if gateErr != nil {
		respondError(w, 423, gateErr.Error())
		return
	}
	defer gate.Close() // crash leaves durable fail-closed gate for manual review
	before, historical, err := trading.CheckWalletEditHistory(s.dataDir)
	if err != nil {
		respondError(w, http.StatusLocked, err.Error())
		return
	}
	// Signed ALL-account audit is mandatory even on a fresh, empty local
	// ledger: BTC display snapshots cannot detect another symbol's exposure.
	if input.Passphrase == "" {
		respondError(w, 400, "첫 배정을 포함하여 모든 변경에 Demo 저장 암호 및 전체 계정 서명 검증이 필요합니다")
		return
	}
	var auditBalance walletAuditBalance
	{
		if !s.accountDiagnosticBusy.CompareAndSwap(false, true) {
			respondError(w, 409, "다른 전체 계정 진단이 실행 중입니다")
			return
		}
		defer s.accountDiagnosticBusy.Store(false)
		ctx, cancel := context.WithTimeout(r.Context(), 40*time.Second)
		defer cancel()
		auditBalance, err = s.auditedWalletAvailable(ctx, input.Passphrase, keyID)
		if err != nil {
			respondError(w, 423, err.Error())
			return
		}
		after, stillHistorical, auditErr := trading.CheckWalletEditHistory(s.dataDir)
		if auditErr != nil || stillHistorical != historical || after != before || time.Since(auditBalance.FinishedAt) > 10*time.Second || auditBalance.FinishedAt.After(time.Now().Add(time.Second)) || s.store.Snapshot().Environment != app.Demo || s.demo.Snapshot().Status != "connected" || s.demo.KeyFingerprint() != keyID || ctx.Err() != nil {
			respondError(w, 409, "전체 계정 조회 중 로컬 증적 또는 Demo 연결이 변경되었습니다")
			return
		}
	}
	// The fresh signed whole-account audit is authoritative for wallet mutation.
	// Do not re-depend on the older display snapshot after the audit completed.
	result, err := s.wallet.Change(input.RequestID, input.Operation, input.AmountUSDT, auditBalance.AvailableUSDT, keyID)
	if err != nil {
		respondError(w, 409, err.Error())
		return
	}
	if s.demo.KeyFingerprint() != keyID {
		respondError(w, 503, "계정 인증 상태가 변경됨: 지갑 기록을 다시 확인하세요")
		return
	}
	respondJSON(w, 200, result)
}
