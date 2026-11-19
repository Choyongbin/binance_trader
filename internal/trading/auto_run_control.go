package trading

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	AutoRunStopped        = "STOPPED"
	AutoRunRunning        = "RUNNING"
	AutoRunRestartBlocked = "RESTART_BLOCKED"
)

var ErrAutoRunStopped = errors.New("자동매매가 STOP 상태입니다")
var ErrAutoRunRestartBlocked = errors.New("이전 자동매매 RUNNING 증적이 남아 있습니다: 명시적 STOP 및 상태 재검증이 필요합니다")

var autoControlName = regexp.MustCompile(`^state\.([0-9]{20})\.json$`)

type AutoRunLease struct {
	Generation uint64 `json:"generation"`
	RunID      string `json:"run_id"`
}

type AutoRunStatus struct {
	Schema       int    `json:"schema"`
	State        string `json:"state"`
	Generation   uint64 `json:"generation"`
	RunID        string `json:"run_id,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
	RestartBlock bool   `json:"restart_block"`
}

type autoRunRecord struct {
	Schema     int    `json:"schema"`
	Running    bool   `json:"running"`
	Generation uint64 `json:"generation"`
	RunID      string `json:"run_id,omitempty"`
	UpdatedAt  string `json:"updated_at"`
}

type AutoRunControl struct {
	mu         sync.RWMutex
	dataDir    string
	dir        string
	ownedRunID string
}

func OpenAutoRunControl(dataDir string) (*AutoRunControl, error) {
	if dataDir == "" {
		return nil, errors.New("자동매매 제어 데이터 경로가 필요합니다")
	}
	c := &AutoRunControl{dataDir: filepath.Clean(dataDir), dir: filepath.Join(dataDir, "trading", "auto_control")}
	if _, err := c.readLatest(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return c, nil
}

func decodeAutoRunRecord(path string, wantGeneration uint64) (autoRunRecord, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return autoRunRecord{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return autoRunRecord{}, errors.New("자동매매 제어 파일 형식이 안전하지 않습니다")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return autoRunRecord{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var r autoRunRecord
	if err := dec.Decode(&r); err != nil {
		return r, err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return r, errors.New("자동매매 제어 파일에 뒤따르는 데이터가 있습니다")
	}
	if r.Schema != 1 || r.Generation != wantGeneration || r.Generation == 0 || r.UpdatedAt == "" {
		return r, errors.New("자동매매 제어 파일이 손상되었습니다")
	}
	at, err := time.Parse(time.RFC3339Nano, r.UpdatedAt)
	if err != nil || at.After(time.Now().UTC().Add(time.Second)) {
		return r, errors.New("자동매매 제어 시각이 유효하지 않습니다")
	}
	if r.Running {
		if len(r.RunID) != 48 {
			return r, errors.New("자동매매 RUNNING run_id가 손상되었습니다")
		}
	} else if r.RunID != "" {
		return r, errors.New("STOPPED 상태에 run_id가 남아 있습니다")
	}
	return r, nil
}

func (c *AutoRunControl) readLatest() (autoRunRecord, error) {
	info, err := os.Lstat(c.dir)
	if err != nil {
		return autoRunRecord{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return autoRunRecord{}, errors.New("자동매매 제어 디렉터리 형식이 안전하지 않습니다")
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return autoRunRecord{}, err
	}
	var latest uint64
	seen := make(map[uint64]bool, len(entries))
	for _, entry := range entries {
		m := autoControlName.FindStringSubmatch(entry.Name())
		if len(m) != 2 || entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return autoRunRecord{}, fmt.Errorf("알 수 없거나 안전하지 않은 자동매매 제어 증적: %s", entry.Name())
		}
		g, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil || g == 0 {
			return autoRunRecord{}, errors.New("자동매매 제어 세대 번호가 손상되었습니다")
		}
		if _, err := decodeAutoRunRecord(filepath.Join(c.dir, entry.Name()), g); err != nil {
			return autoRunRecord{}, err
		}
		if seen[g] {
			return autoRunRecord{}, errors.New("자동매매 제어 세대가 중복되었습니다")
		}
		seen[g] = true
		if g > latest {
			latest = g
		}
	}
	if latest == 0 {
		return autoRunRecord{}, os.ErrNotExist
	}
	if uint64(len(seen)) != latest {
		return autoRunRecord{}, errors.New("자동매매 제어 세대 기록이 누락되었습니다")
	}
	return decodeAutoRunRecord(filepath.Join(c.dir, fmt.Sprintf("state.%020d.json", latest)), latest)
}

func (c *AutoRunControl) appendRecord(r autoRunRecord) error {
	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(c.dir); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		if err != nil {
			return err
		}
		return errors.New("자동매매 제어 디렉터리 형식이 안전하지 않습니다")
	}
	path := filepath.Join(c.dir, fmt.Sprintf("state.%020d.json", r.Generation))
	raw, _ := json.MarshalIndent(r, "", "  ")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(raw, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return syncAutoControlDirectory(c.dir)
}

func randomRunID() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (c *AutoRunControl) Start() (AutoRunLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, err := c.readLatest()
	if errors.Is(err, os.ErrNotExist) {
		old = autoRunRecord{}
	} else if err != nil {
		return AutoRunLease{}, err
	}
	if old.Running {
		if c.ownedRunID == old.RunID {
			return AutoRunLease{Generation: old.Generation, RunID: old.RunID}, nil
		}
		return AutoRunLease{}, ErrAutoRunRestartBlocked
	}
	id, err := randomRunID()
	if err != nil {
		return AutoRunLease{}, err
	}
	next := autoRunRecord{Schema: 1, Running: true, Generation: old.Generation + 1, RunID: id, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := c.appendRecord(next); err != nil {
		return AutoRunLease{}, err
	}
	c.ownedRunID = id
	return AutoRunLease{Generation: next.Generation, RunID: id}, nil
}

func (c *AutoRunControl) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, err := c.readLatest()
	if errors.Is(err, os.ErrNotExist) {
		old = autoRunRecord{}
	} else if err != nil {
		return err
	}
	next := autoRunRecord{Schema: 1, Running: false, Generation: old.Generation + 1, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := c.appendRecord(next); err != nil {
		return err
	}
	c.ownedRunID = ""
	return nil
}

func (c *AutoRunControl) Status() (AutoRunStatus, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, err := c.readLatest()
	if errors.Is(err, os.ErrNotExist) {
		return AutoRunStatus{Schema: 1, State: AutoRunStopped}, nil
	}
	if err != nil {
		return AutoRunStatus{}, err
	}
	state := AutoRunStopped
	blocked := false
	if r.Running {
		if c.ownedRunID == r.RunID {
			state = AutoRunRunning
		} else {
			state = AutoRunRestartBlocked
			blocked = true
		}
	}
	return AutoRunStatus{Schema: 1, State: state, Generation: r.Generation, UpdatedAt: r.UpdatedAt, RestartBlock: blocked}, nil
}

func (c *AutoRunControl) checkLocked(lease AutoRunLease) error {
	r, err := c.readLatest()
	if errors.Is(err, os.ErrNotExist) {
		return ErrAutoRunStopped
	}
	if err != nil {
		return err
	}
	if !r.Running {
		return ErrAutoRunStopped
	}
	if c.ownedRunID == "" || c.ownedRunID != r.RunID {
		return ErrAutoRunRestartBlocked
	}
	if lease.Generation != r.Generation || lease.RunID == "" || lease.RunID != r.RunID {
		return errors.New("자동매매 run lease가 현재 세대와 일치하지 않습니다")
	}
	return nil
}

func (c *AutoRunControl) Guard(lease AutoRunLease) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.checkLocked(lease)
	}
}

// withLease serializes only the local reservation/lifecycle creation window
// with STOP. It never holds the lock over signed REST or an exchange POST.
func (c *AutoRunControl) withLease(lease AutoRunLease, fn func() error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.checkLocked(lease); err != nil {
		return err
	}
	return fn()
}
