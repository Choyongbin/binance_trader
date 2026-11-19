// Package app manages local, non-secret application preferences.
// No Binance positions, balances, or successful trade state are synthesized here.
package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	TestModelID   = "ema_rsi_macd_v1"
	StrictModelID = "ema_rsi_macd_strict_v1"
)

type Environment string

const (
	Demo Environment = "demo"
	Real Environment = "real"
)

type Model struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Implemented bool   `json:"implemented"`
}

// The model catalog is UI metadata only. The first model now has a complete
// internal execution path, but HTTP start/order routes remain locked until Demo E2E.
var ModelCatalog = []Model{
	{ID: TestModelID, Name: "EMA · RSI · MACD 기본 모델", Description: "EMA25/99 · RSI14 진입 50 기준 · MACD/Signal · 내부 실행 경로 연결, 주문 잠금 유지", Implemented: true},
	{ID: StrictModelID, Name: "EMA · RSI · MACD 보수형 검증 모델", Description: "기본 모델과 동일한 청산 조건 · 진입 RSI14 LONG > 60 / SHORT < 40 · 오프라인 검증용 · 주문 잠금 유지", Implemented: true},
}

type Settings struct {
	Version          int         `json:"version"`
	Environment      Environment `json:"environment"`
	SelectedModelIDs []string    `json:"selected_model_ids"`
}

func defaults() Settings {
	return Settings{Version: 1, Environment: Demo, SelectedModelIDs: []string{}}
}

func validate(s Settings) error {
	if s.Version != 1 {
		return fmt.Errorf("지원하지 않는 설정 버전: %d", s.Version)
	}
	if s.Environment != Demo && s.Environment != Real {
		return errors.New("거래 환경은 demo 또는 real이어야 합니다")
	}
	known := map[string]bool{}
	for _, m := range ModelCatalog {
		known[m.ID] = true
	}
	seen := map[string]bool{}
	for _, id := range s.SelectedModelIDs {
		if !known[id] || seen[id] {
			return errors.New("존재하지 않거나 중복된 모델 ID입니다")
		}
		seen[id] = true
	}
	return nil
}

type Store struct {
	mu      sync.RWMutex
	path    string
	current Settings
}

func OpenStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	store := &Store{path: filepath.Join(dataDir, "settings.json"), current: defaults()}
	content, err := os.ReadFile(store.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := store.saveLocked(store.current); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		decoder := json.NewDecoder(bytesReader(content))
		decoder.DisallowUnknownFields()
		var settings Settings
		if err := decoder.Decode(&settings); err != nil {
			return nil, fmt.Errorf("설정 파일 읽기 실패: %w", err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, errors.New("설정 파일에 추가 JSON 값이 있습니다")
		}
		if err := validate(settings); err != nil {
			return nil, err
		}
		if settings.SelectedModelIDs == nil {
			settings.SelectedModelIDs = []string{}
		}
		store.current = settings
	}
	return store, nil
}

// bytesReader is defined here so JSON decoding uses a bounded in-memory file.
func bytesReader(content []byte) io.Reader { return &readOnlyBytes{b: content} }

type readOnlyBytes struct{ b []byte }

func (r *readOnlyBytes) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

func (s *Store) Snapshot() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value := s.current
	value.SelectedModelIDs = append([]string{}, value.SelectedModelIDs...)
	return value
}

func (s *Store) UpdateEnvironment(env Environment) (Settings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.current
	next.Environment = env
	if err := validate(next); err != nil {
		return Settings{}, err
	}
	if err := s.saveLocked(next); err != nil {
		return Settings{}, err
	}
	s.current = next
	return s.snapshotLocked(), nil
}

func (s *Store) UpdateSelection(ids []string) (Settings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.current
	next.SelectedModelIDs = append([]string{}, ids...)
	if err := validate(next); err != nil {
		return Settings{}, err
	}
	if err := s.saveLocked(next); err != nil {
		return Settings{}, err
	}
	s.current = next
	return s.snapshotLocked(), nil
}

func (s *Store) snapshotLocked() Settings {
	value := s.current
	value.SelectedModelIDs = append([]string{}, value.SelectedModelIDs...)
	return value
}

func (s *Store) saveLocked(value Settings) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	temp, err := os.CreateTemp(dir, ".settings-*.tmp")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(append(body, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	// os.Rename supports replacement on supported target platforms; any error
	// aborts state publication rather than silently ignoring a failed write.
	return os.Rename(name, s.path)
}
