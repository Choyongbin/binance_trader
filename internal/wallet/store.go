// Package wallet implements a durable, locally allocated Demo AUTO budget.
// It is NOT a Binance subaccount, actual fund transfer or risk isolation.
// It never sends an exchange request and never grants order permission.
package wallet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var amountPattern = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,12})(?:\.[0-9]{1,8})?$`)
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{8,80}$`)
var fingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func Decimal(s string, allowZero bool) (*big.Rat, error) {
	if !amountPattern.MatchString(s) {
		return nil, errors.New("USDT 금액 형식 오류: 소수 최대 8자리")
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || r.Sign() < 0 || (!allowZero && r.Sign() == 0) {
		return nil, errors.New("양수 USDT 금액이 필요합니다")
	}
	return r, nil
}
func Format(r *big.Rat) string {
	s := r.FloatString(8)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" {
		return "0"
	}
	return s
}

type Event struct {
	Schema             int    `json:"schema"`
	Seq                uint64 `json:"seq"`
	RequestID          string `json:"request_id"`
	AccountFingerprint string `json:"account_fingerprint"`
	Operation          string `json:"operation"` // ALLOCATE / RETURN
	AmountUSDT         string `json:"amount_usdt"`
	BalanceUSDT        string `json:"balance_usdt"`
	PrevHash           string `json:"previous_sha256"`
	At                 string `json:"at"`
	Hash               string `json:"sha256"`
}

func (e Event) hash() string {
	e.Hash = ""
	raw, _ := json.Marshal(e)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type Status struct {
	Kind         string `json:"kind"`
	AssignedUSDT string `json:"assigned_usdt"`
	Revision     uint64 `json:"revision"`
	Verified     bool   `json:"verified"`
	Note         string `json:"note"`
}
type Store struct {
	mu  sync.Mutex
	dir string
}

func Open(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 경로 누락")
	}
	dir := filepath.Join(dataDir, "wallet_demo", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}
	if _, _, _, err := s.read(); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Store) read() (*big.Rat, uint64, string, error) {
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, 0, "", err
	}
	bal := new(big.Rat)
	prev := ""
	seq := uint64(0)
	ids := make(map[string]bool)
	accountID := ""
	for _, f := range files {
		if f.IsDir() || f.Name() != fmt.Sprintf("%020d.json", seq+1) {
			return nil, 0, "", errors.New("자동매매 배정 기록이 누락/손상되었거나 수정 중입니다")
		}
		raw, err := os.ReadFile(filepath.Join(s.dir, f.Name()))
		if err != nil {
			return nil, 0, "", err
		}
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.DisallowUnknownFields()
		var e Event
		if err = d.Decode(&e); err != nil {
			return nil, 0, "", errors.New("지갑 거래 기록 JSON 손상")
		}
		if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, 0, "", errors.New("지갑 거래 기록 후행 데이터 오류")
		}
		if !fingerprintPattern.MatchString(e.AccountFingerprint) || (accountID != "" && e.AccountFingerprint != accountID) {
			return nil, 0, "", errors.New("자동매매 배정 지갑의 계정 연결 정보가 손상되었습니다")
		}
		accountID = e.AccountFingerprint
		if e.Schema != 1 || e.Seq != seq+1 || !idPattern.MatchString(e.RequestID) || ids[e.RequestID] || e.PrevHash != prev || e.Hash != e.hash() {
			return nil, 0, "", errors.New("지갑 거래 기록 순서/서명 불일치")
		}
		amt, err := Decimal(e.AmountUSDT, false)
		if err != nil {
			return nil, 0, "", err
		}
		switch e.Operation {
		case "ALLOCATE":
			bal.Add(bal, amt)
		case "RETURN":
			bal.Sub(bal, amt)
		default:
			return nil, 0, "", errors.New("지갑 작업 종류 오류")
		}
		stored, err := Decimal(e.BalanceUSDT, true)
		if err != nil || bal.Sign() < 0 || bal.Cmp(stored) != 0 {
			return nil, 0, "", errors.New("지갑 잔액 기록 불일치")
		}
		if _, err = time.Parse(time.RFC3339Nano, e.At); err != nil {
			return nil, 0, "", errors.New("지갑 기록 시각 오류")
		}
		ids[e.RequestID] = true
		seq++
		prev = e.Hash
	}
	return bal, seq, prev, nil
}
func (s *Store) BelongsTo(dataDir string) bool {
	return s != nil && filepath.Clean(s.dir) == filepath.Join(filepath.Clean(dataDir), "wallet_demo", "events")
}
func (s *Store) firstFingerprint() (string, error) {
	raw, err := os.ReadFile(filepath.Join(s.dir, "00000000000000000001.json"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var e Event
	if err = json.Unmarshal(raw, &e); err != nil {
		return "", err
	}
	return e.AccountFingerprint, nil
}

// SnapshotFor holds the same mutex during balance verification and account
// fingerprint binding; a concurrent allocation must never change the result
// between these two checks.
func (s *Store) SnapshotFor(accountFingerprint string) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, err := s.snapshotLocked()
	if err != nil {
		return status, err
	}
	expected, err := s.firstFingerprint()
	if err != nil {
		return Status{}, err
	}
	status.Verified = expected == "" || (accountFingerprint != "" && expected == accountFingerprint)
	if !status.Verified {
		status.Note = "배정한 Demo API 키와 현재 인증 키가 다릅니다. 자동매매 예산을 사용할 수 없습니다."
	}
	return status, nil
}
func (s *Store) Snapshot() (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}
func (s *Store) snapshotLocked() (Status, error) {
	if _, err := os.Stat(filepath.Join(filepath.Dir(s.dir), "mutation.active")); err == nil {
		return Status{}, errors.New("비정상 종료된 지갑 변경 잠금: 수동 점검 필요")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Status{}, err
	}
	bal, rev, _, err := s.read()
	if err != nil {
		return Status{}, err
	}
	return Status{Kind: "INTERNAL_ALLOCATION_DEMO", AssignedUSDT: Format(bal), Revision: rev, Verified: true, Note: "실제 바이낸스 계정 내 자금 이동이나 손실 격리가 아닌 내부 사용 한도입니다."}, nil
}

// Change requires a caller-verified fresh, authenticated Binance available
// USDT balance and guarantees an allocation never exceeds it. The calling UI
// additionally blocks edits on its known positions/orders, but full exchange
// authorization remains disabled until complete account-wide reconciliation.
func (s *Store) Change(requestID, operation, amount, freshAvailable, accountFingerprint string) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !fingerprintPattern.MatchString(accountFingerprint) {
		return Status{}, errors.New("인증된 Demo 계정 연결 정보 누락")
	}
	if !idPattern.MatchString(requestID) || (operation != "ALLOCATE" && operation != "RETURN") {
		return Status{}, errors.New("잘못된 지갑 변경 요청")
	}
	amt, err := Decimal(amount, false)
	if err != nil {
		return Status{}, err
	}
	avail, err := Decimal(freshAvailable, true)
	if err != nil {
		return Status{}, errors.New("신뢰할 수 있는 실계정 가용 USDT 값이 필요합니다")
	}
	// Durable per-operation exclusion across multiple processes: crash leaves a
	// lock and blocks further changes pending explicit human reconciliation.
	lock := filepath.Join(filepath.Dir(s.dir), "mutation.active")
	if err = os.Mkdir(lock, 0700); err != nil {
		return Status{}, errors.New("다른 지갑 변경 중이거나 비정상 종료됨: 수동 점검 필요")
	}
	unlocked := false
	defer func() {
		if unlocked {
			_ = os.Remove(lock)
		}
	}()
	bal, seq, prev, err := s.read()
	if err == nil {
		var existing string
		existing, err = s.firstFingerprint()
		if err == nil && existing != "" && existing != accountFingerprint {
			unlocked = true
			return Status{}, errors.New("다른 Demo API 키로 배정된 자금입니다. 계정 변경 시 수동 이관 필요")
		}
	}
	if err != nil {
		return Status{}, err
	}
	// Reject duplicate request IDs across restarts (idempotency key never replayed).
	files, _ := os.ReadDir(s.dir)
	for _, f := range files {
		raw, e := os.ReadFile(filepath.Join(s.dir, f.Name()))
		if e != nil {
			return Status{}, e
		}
		var old Event
		if json.Unmarshal(raw, &old) != nil {
			return Status{}, errors.New("기존 기록 손상")
		}
		if old.RequestID == requestID {
			unlocked = true
			return Status{}, errors.New("이미 적용된 요청 ID: 재실행하지 않았습니다")
		}
	}
	newBal := new(big.Rat).Set(bal)
	if operation == "ALLOCATE" {
		newBal.Add(newBal, amt)
	} else {
		newBal.Sub(newBal, amt)
	}
	if newBal.Sign() < 0 {
		unlocked = true
		return Status{}, errors.New("자동매매 지갑 배정액보다 반환 금액이 큽니다")
	}
	if newBal.Cmp(avail) > 0 {
		unlocked = true
		return Status{}, errors.New("배정액이 최신 거래소 가용 USDT를 초과합니다")
	}
	e := Event{Schema: 1, Seq: seq + 1, RequestID: requestID, AccountFingerprint: accountFingerprint, Operation: operation, AmountUSDT: Format(amt), BalanceUSDT: Format(newBal), PrevHash: prev, At: time.Now().UTC().Format(time.RFC3339Nano)}
	e.Hash = e.hash()
	raw, _ := json.MarshalIndent(e, "", "  ")
	file, err := os.OpenFile(filepath.Join(s.dir, fmt.Sprintf("%020d.json", e.Seq)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Status{}, err
	}
	_, err = file.Write(append(raw, '\n'))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return Status{}, errors.New("지갑 기록 저장 결과 불명확: 거래 중단 및 수동 검사")
	}
	unlocked = true
	return Status{Kind: "INTERNAL_ALLOCATION_DEMO", AssignedUSDT: Format(newBal), Revision: e.Seq, Verified: true, Note: "실제 바이낸스 계정 내 자금 이동이나 손실 격리가 아닌 내부 사용 한도입니다."}, nil
}
