package trading

// MutationGate serializes the entire wallet signed-audit/commit interval with
// coordinator startup, across processes using the SAME local filesystem.
// A crash deliberately retains the lock for manual inspection. NEVER remove
// an abandoned lock automatically based only on its PID or age.
import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

var ErrMutationGateBusy = errors.New("지갑 검증 또는 거래 조정기 시작이 진행 중이거나 중단되었습니다: 잠금 수동 점검 필요")

type MutationGate struct {
	dir, nonce string
}

func AcquireMutationGate(dataDir string) (*MutationGate, error) {
	if dataDir == "" {
		return nil, errors.New("데이터 경로 누락")
	}
	root := filepath.Join(dataDir, "trading")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "mutation_gate.active")
	if err := os.Mkdir(dir, 0700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, ErrMutationGateBusy
		}
		return nil, err
	}
	// A crash before owner.json is committed also intentionally leaves a lock.
	nonceBytes := make([]byte, 24)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, err
	}
	gate := &MutationGate{dir: dir, nonce: hex.EncodeToString(nonceBytes)}
	payload, _ := json.Marshal(struct {
		Schema    int    `json:"schema"`
		Nonce     string `json:"nonce"`
		PID       int    `json:"pid"`
		CreatedAt string `json:"created_at"`
	}{1, gate.nonce, os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano)})
	f, err := os.OpenFile(filepath.Join(dir, "owner.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	_, err = f.Write(payload)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return gate, nil
}

func (g *MutationGate) Close() error {
	if g == nil || g.dir == "" {
		return errors.New("잠금이 없거나 이미 종료되었습니다")
	}
	marker := filepath.Join(g.dir, "owner.json")
	raw, err := os.ReadFile(marker)
	if err != nil {
		return err
	}
	var owner struct {
		Schema int    `json:"schema"`
		Nonce  string `json:"nonce"`
	}
	if json.Unmarshal(raw, &owner) != nil || owner.Schema != 1 || owner.Nonce != g.nonce {
		return errors.New("공유 잠금 소유권 확인 실패: 수동 점검 필요")
	}
	if err := os.Remove(marker); err != nil {
		return err
	}
	if err := os.Remove(g.dir); err != nil {
		return err
	}
	g.dir = ""
	return nil
}

func MutationGatePresent(dataDir string) (bool, error) {
	_, err := os.Lstat(filepath.Join(dataDir, "trading", "mutation_gate.active"))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return true, err
}
