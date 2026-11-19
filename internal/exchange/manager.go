package exchange

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

type Fetcher interface {
	Fetch(context.Context, string, string) (Snapshot, error)
}

// Manager keeps decrypted credentials in RAM only during an authenticated
// session. Failed fetches clear both data and credentials (fail closed).
type Manager struct {
	mu         sync.RWMutex
	fetchMu    sync.Mutex
	remote     Fetcher
	key        []byte
	secret     []byte
	generation uint64
	snapshot   Snapshot
	stop       chan struct{}
	done       chan struct{}
	closeOnce  sync.Once
}

func NewManager(remote Fetcher) *Manager {
	m := &Manager{remote: remote, stop: make(chan struct{}), done: make(chan struct{}), snapshot: Snapshot{Status: "locked", Environment: "demo"}}
	go m.run()
	return m
}
func (m *Manager) run() {
	defer close(m.done)
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-timer.C:
			m.mu.RLock()
			ok := m.snapshot.Status == "connected"
			m.mu.RUnlock()
			if ok {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_ = m.Refresh(ctx)
				cancel()
			}
		}
	}
}
func (m *Manager) Clear() { m.mu.Lock(); defer m.mu.Unlock(); m.clearLocked("locked", "") }
func (m *Manager) clearLocked(status, reason string) {
	m.generation++
	for i := range m.key {
		m.key[i] = 0
	}
	for i := range m.secret {
		m.secret[i] = 0
	}
	m.key = nil
	m.secret = nil
	m.snapshot = Snapshot{Status: status, Environment: "demo", Error: reason}
}
func (m *Manager) Close() { m.closeOnce.Do(func() { close(m.stop); <-m.done; m.Clear() }) }
func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := m.snapshot
	if s.Balances != nil {
		s.Balances = append(make([]Balance, 0, len(s.Balances)), s.Balances...)
	}
	if s.Positions != nil {
		s.Positions = append(make([]Position, 0, len(s.Positions)), s.Positions...)
	}
	if s.Orders != nil {
		s.Orders = append(make([]Order, 0, len(s.Orders)), s.Orders...)
	}
	if s.AlgoOrders != nil {
		s.AlgoOrders = append(make([]AlgoOrder, 0, len(s.AlgoOrders)), s.AlgoOrders...)
	}
	return s
}
func (m *Manager) Connect(ctx context.Context, key, secret string) error {
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()
	m.mu.Lock()
	m.clearLocked("connecting", "")
	gen := m.generation
	m.mu.Unlock()
	result, err := m.remote.Fetch(ctx, key, secret)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.generation != gen {
		return errors.New("연결 요청이 취소되었습니다")
	}
	if err != nil {
		m.clearLocked("error", err.Error())
		return err
	}
	m.key = []byte(key)
	m.secret = []byte(secret)
	m.snapshot = result
	return nil
}

// ValidateCredentials checks a proposed Demo key without replacing the
// currently unlocked session. A failed replacement must preserve old keys.
func (m *Manager) ValidateCredentials(ctx context.Context, key, secret string) error {
	result, err := m.remote.Fetch(ctx, key, secret)
	if err != nil {
		return err
	}
	if result.Status != "connected" || result.Environment != "demo" {
		return errors.New("Demo 계정 인증 응답이 유효하지 않습니다")
	}
	return nil
}
func (m *Manager) Refresh(ctx context.Context) error {
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()
	m.mu.RLock()
	gen := m.generation
	key, secret := string(m.key), string(m.secret)
	status := m.snapshot.Status
	m.mu.RUnlock()
	if status != "connected" {
		return errors.New("먼저 데모 계정을 연결하세요")
	}
	result, err := m.remote.Fetch(ctx, key, secret)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.generation != gen {
		return errors.New("연결 상태가 변경되어 조회 결과를 버렸습니다")
	}
	if err != nil {
		m.clearLocked("error", err.Error())
		return err
	}
	m.snapshot = result
	return nil
}

// InspectTradingConfiguration reads the live account settings on demand. This
// is deliberately NOT polled every 10 seconds (the position-mode GET is costly).
// It never exposes the unlocked key/secret and discards a result if locking or
// environment selection changes while the request is in flight.
func (m *Manager) InspectTradingConfiguration(ctx context.Context, symbol string) (TradingConfiguration, error) {
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()
	inspector, ok := m.remote.(ConfigInspector)
	if !ok {
		return TradingConfiguration{}, errors.New("설정 조회를 지원하지 않는 거래소 연결입니다")
	}
	m.mu.RLock()
	gen, status := m.generation, m.snapshot.Status
	key, secret := string(m.key), string(m.secret)
	m.mu.RUnlock()
	if status != "connected" || key == "" || secret == "" {
		return TradingConfiguration{}, errors.New("데모 계정 인증을 먼저 해제하세요")
	}
	result, err := inspector.InspectTradingConfiguration(ctx, key, secret, symbol)
	m.mu.RLock()
	valid := m.generation == gen && m.snapshot.Status == "connected"
	m.mu.RUnlock()
	if !valid {
		return TradingConfiguration{}, errors.New("요청 도중 인증 상태가 변경되어 결과를 버렸습니다")
	}
	return result, err
}

// KeyFingerprint is a local-only budget binding to the currently unlocked
// API KEY (not proof that two different keys belong to different accounts).
// This digest MUST NOT be sent to the browser or recorded in application logs.

// InternalCredentials returns a short-lived copy of the currently unlocked
// Demo credentials for internal execution components only. Callers must not
// log, persist, or expose the returned strings. A locked/changed session fails
// closed.
func (m *Manager) InternalCredentials() (string, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.snapshot.Status != "connected" || len(m.key) == 0 || len(m.secret) == 0 {
		return "", "", errors.New("Demo 계정 인증을 먼저 해제하세요")
	}
	return string(m.key), string(m.secret), nil
}

// InspectExecutionPreflight runs the same bracketed signed Demo preflight used
// at the execution boundary without exposing credentials or granting trading
// authorization. The result is discarded if the unlocked Demo session changes
// while the request is in flight.
func (m *Manager) InspectExecutionPreflight(ctx context.Context, symbol string) (ExecutionPreflightSnapshot, error) {
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()
	reader, ok := m.remote.(interface {
		FetchDemoExecutionPreflight(context.Context, string, string, string) (ExecutionPreflightSnapshot, error)
	})
	if !ok {
		return ExecutionPreflightSnapshot{}, errors.New("execution preflight를 지원하지 않는 거래소 연결입니다")
	}
	m.mu.RLock()
	gen, status := m.generation, m.snapshot.Status
	key, secret := string(m.key), string(m.secret)
	m.mu.RUnlock()
	if status != "connected" || key == "" || secret == "" {
		return ExecutionPreflightSnapshot{}, errors.New("데모 계정 인증을 먼저 해제하세요")
	}
	result, err := reader.FetchDemoExecutionPreflight(ctx, key, secret, symbol)
	m.mu.RLock()
	valid := m.generation == gen && m.snapshot.Status == "connected"
	m.mu.RUnlock()
	if !valid {
		return ExecutionPreflightSnapshot{}, errors.New("요청 도중 인증 상태가 변경되어 execution preflight 결과를 버렸습니다")
	}
	return result, err
}

func (m *Manager) KeyFingerprint() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.snapshot.Status != "connected" || len(m.key) == 0 {
		return ""
	}
	sum := sha256.Sum256(m.key)
	return hex.EncodeToString(sum[:])
}

// InspectSymbolRules is an explicit user-triggered PUBLIC Demo read bound to
// the currently unlocked Demo session. This prevents a delayed result from
// being displayed after the user locks or changes credentials. It does not
// establish any authorization for submitting a trade.
func (m *Manager) InspectSymbolRules(ctx context.Context, symbol string) (SymbolRules, error) {
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()
	reader, ok := m.remote.(interface {
		FetchSymbolRules(context.Context, string) (SymbolRules, error)
	})
	if !ok {
		return SymbolRules{}, errors.New("거래소 필터 조회 기능이 없습니다")
	}
	m.mu.RLock()
	gen, status := m.generation, m.snapshot.Status
	m.mu.RUnlock()
	if status != "connected" || symbol != "BTCUSDT" {
		return SymbolRules{}, errors.New("인증된 Demo BTC 계정이 필요합니다")
	}
	result, err := reader.FetchSymbolRules(ctx, symbol)
	m.mu.RLock()
	valid := m.generation == gen && m.snapshot.Status == "connected"
	m.mu.RUnlock()
	if !valid {
		return SymbolRules{}, errors.New("조회 중 Demo 인증이 변경되어 시세 정보를 버렸습니다")
	}
	return result, err
}

// ApplyDemoTradingSetup applies the narrow first-release BTCUSDT Demo setup.
// It is intentionally serialized with all Manager fetches and requires a fresh,
// empty account before each mutation. No order is created by this method.
func (m *Manager) ApplyDemoTradingSetup(ctx context.Context, symbol string) (TradingConfiguration, error) {
	if symbol != "BTCUSDT" {
		return TradingConfiguration{}, errors.New("1차 지원 종목은 BTCUSDT뿐입니다")
	}
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()
	mutator, ok := m.remote.(ConfigMutator)
	if !ok {
		return TradingConfiguration{}, errors.New("설정 변경을 지원하지 않는 거래소 연결입니다")
	}
	m.mu.RLock()
	gen, status := m.generation, m.snapshot.Status
	key, secret := string(m.key), string(m.secret)
	m.mu.RUnlock()
	if status != "connected" || key == "" || secret == "" {
		return TradingConfiguration{}, errors.New("데모 계정 인증을 먼저 해제하세요")
	}
	freshEmpty := func() (Snapshot, error) {
		snap, err := m.remote.Fetch(ctx, key, secret)
		if err != nil {
			return Snapshot{}, err
		}
		if snap.Environment != "demo" || snap.Status != "connected" || snap.Balances == nil || snap.Positions == nil || snap.Orders == nil || snap.AlgoOrders == nil {
			return Snapshot{}, errors.New("Demo 계정 전체 조회 결과가 불완전합니다")
		}
		if len(snap.Positions) != 0 || len(snap.Orders) != 0 || len(snap.AlgoOrders) != 0 {
			return Snapshot{}, errors.New("포지션 또는 미체결 주문이 있어 Demo 거래 설정을 변경할 수 없습니다")
		}
		return snap, nil
	}
	if _, err := freshEmpty(); err != nil {
		return TradingConfiguration{}, err
	}
	conf, err := mutator.InspectTradingConfiguration(ctx, key, secret, symbol)
	if err != nil {
		return TradingConfiguration{}, err
	}
	if conf.AutoAddMargin {
		return TradingConfiguration{}, errors.New("자동 격리 증거금 추가가 켜져 있어 자동 설정 변경을 중단합니다")
	}
	verifySession := func() error {
		m.mu.RLock()
		defer m.mu.RUnlock()
		if m.generation != gen || m.snapshot.Status != "connected" {
			return errors.New("설정 변경 도중 인증 상태가 변경되었습니다")
		}
		return nil
	}
	if !conf.HedgeMode {
		if err := verifySession(); err != nil {
			return TradingConfiguration{}, err
		}
		if err := mutator.SetHedgeModeForDemo(ctx, key, secret, true); err != nil {
			return TradingConfiguration{}, errors.New("Hedge Mode 변경 결과가 불확실합니다. signed 설정을 다시 조회하세요: " + err.Error())
		}
		if _, err := freshEmpty(); err != nil {
			return TradingConfiguration{}, err
		}
		conf, err = mutator.InspectTradingConfiguration(ctx, key, secret, symbol)
		if err != nil || !conf.HedgeMode {
			return TradingConfiguration{}, errors.New("Hedge Mode 변경 후 signed 재검증에 실패했습니다")
		}
	}
	if conf.MarginType != "ISOLATED" {
		if _, err := freshEmpty(); err != nil {
			return TradingConfiguration{}, err
		}
		if err := mutator.SetIsolatedForDemo(ctx, key, secret); err != nil {
			return TradingConfiguration{}, errors.New("ISOLATED 변경 결과가 불확실합니다. signed 설정을 다시 조회하세요: " + err.Error())
		}
		conf, err = mutator.InspectTradingConfiguration(ctx, key, secret, symbol)
		if err != nil || conf.MarginType != "ISOLATED" {
			return TradingConfiguration{}, errors.New("ISOLATED 변경 후 signed 재검증에 실패했습니다")
		}
	}
	if conf.Leverage != 15 {
		if _, err := freshEmpty(); err != nil {
			return TradingConfiguration{}, err
		}
		if err := mutator.SetLeverageForDemo(ctx, key, secret, 15); err != nil {
			return TradingConfiguration{}, errors.New("15배 레버리지 변경 결과가 불확실합니다. signed 설정을 다시 조회하세요: " + err.Error())
		}
		conf, err = mutator.InspectTradingConfiguration(ctx, key, secret, symbol)
		if err != nil || conf.Leverage != 15 {
			return TradingConfiguration{}, errors.New("레버리지 변경 후 signed 재검증에 실패했습니다")
		}
	}
	if _, err := freshEmpty(); err != nil {
		return TradingConfiguration{}, err
	}
	conf, err = mutator.InspectTradingConfiguration(ctx, key, secret, symbol)
	if err != nil {
		return TradingConfiguration{}, err
	}
	if !conf.HedgeMode || conf.MarginType != "ISOLATED" || conf.Leverage != 15 || conf.AutoAddMargin {
		return TradingConfiguration{}, errors.New("최종 Demo 거래 설정이 안전 계약과 일치하지 않습니다")
	}
	if err := verifySession(); err != nil {
		return TradingConfiguration{}, err
	}
	// Publish a fresh account snapshot only after all signed verification succeeded.
	finalSnap, err := m.remote.Fetch(ctx, key, secret)
	if err != nil {
		return TradingConfiguration{}, err
	}
	m.mu.Lock()
	if m.generation != gen || m.snapshot.Status != "connected" {
		m.mu.Unlock()
		return TradingConfiguration{}, errors.New("설정 변경 완료 직전 인증 상태가 변경되었습니다")
	}
	m.snapshot = finalSnap
	m.mu.Unlock()
	return conf, nil
}
