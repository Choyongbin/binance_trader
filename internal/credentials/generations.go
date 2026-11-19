package credentials

// Immutable encrypted credential generations plus a SINGLE atomically replaced
// active manifest. Neither partial writes nor a failed manifest rename can
// turn an in-progress generation into the active key/secret pair.
import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

const activeManifest = "demo_active.json"
const generationRoot = "demo_generations"

var generationPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

type generationManifest struct {
	Schema       int    `json:"schema"`
	Active       string `json:"active"`
	Previous     string `json:"previous,omitempty"`
	KeySHA256    string `json:"key_sha256"`
	SecretSHA256 string `json:"secret_sha256"`
	Checksum     string `json:"checksum"`
}

func (m generationManifest) checksum() string {
	m.Checksum = ""
	b, _ := json.Marshal(m)
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}

func (v *Vault) manifestPath() string           { return filepath.Join(v.Dir, activeManifest) }
func (v *Vault) generationDir(id string) string { return filepath.Join(v.Dir, generationRoot, id) }

func (v *Vault) activeManifestRead() (generationManifest, bool, error) {
	var m generationManifest
	raw, err := os.ReadFile(v.manifestPath())
	if errors.Is(err, os.ErrNotExist) {
		return m, false, nil
	}
	if err != nil {
		return m, true, err
	}
	if json.Unmarshal(raw, &m) != nil || m.Schema != 1 || !generationPattern.MatchString(m.Active) ||
		(m.Previous != "" && (!generationPattern.MatchString(m.Previous) || m.Previous == m.Active)) ||
		len(m.KeySHA256) != 64 || len(m.SecretSHA256) != 64 || m.Checksum != m.checksum() {
		return generationManifest{}, true, errors.New("활성 Demo 인증 매니페스트 손상: 이전 버전으로 자동 후퇴 금지")
	}
	return m, true, nil
}
func fileSHA(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (v *Vault) readGeneration(id, pass string, keyDigest, secretDigest string) (string, string, error) {
	if !generationPattern.MatchString(id) {
		return "", "", errors.New("인증 버전 ID 손상")
	}
	names := []string{"demo_api_key.enc", "demo_secret_key.enc"}
	values := make([]string, 2)
	generation := ""
	for i, name := range names {
		payload, err := os.ReadFile(filepath.Join(v.generationDir(id), name))
		if err != nil {
			return "", "", err
		}
		expected := keyDigest
		if i == 1 {
			expected = secretDigest
		}
		if expected != "" && fileSHA(payload) != expected {
			return "", "", errors.New("인증 파일 체크섬 불일치")
		}
		if len(payload) < len(magic)+16 || string(payload[:len(magic)]) != string(magic) {
			return "", "", errors.New("인증 파일 형식 오류")
		}
		g := hex.EncodeToString(payload[len(magic) : len(magic)+16])
		if g != id || generation != "" && generation != g {
			return "", "", errors.New("인증 파일 쌍의 버전 불일치")
		}
		generation = g
		value, err := decode(name, payload, pass)
		if err != nil {
			return "", "", err
		}
		values[i] = value
	}
	return values[0], values[1], nil
}

func writeDurableExclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func (v *Vault) replaceManifest(m generationManifest) error {
	m.Checksum = m.checksum()
	body, _ := json.MarshalIndent(m, "", "  ")
	f, err := os.CreateTemp(v.Dir, ".demo-manifest-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	_, err = f.Write(append(body, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	// os.Rename may fail (e.g. Windows file access conflict): leave existing
	// manifest intact; NEVER remove/backup the old manifest before rename.
	if err := os.Rename(name, v.manifestPath()); err != nil {
		return fmt.Errorf("활성 매니페스트 교체 실패, 이전 키 유지: %w", err)
	}
	// Directory fsync is best-effort (unsupported for Windows directories).
	if dir, err := os.Open(v.Dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (v *Vault) saveGeneration(key, secret, pass string) error {
	current, present, err := v.activeManifestRead()
	if err != nil {
		return err
	}
	// First migration must not discard or mask foreign/partly corrupted legacy
	// files; they remain recoverable until explicitly inspected.
	if !present {
		for _, name := range []string{"demo_api_key.enc", "demo_secret_key.enc"} {
			raw, err := os.ReadFile(filepath.Join(v.Dir, name))
			if err != nil {
				return err
			}
			if len(raw) > 0 && (len(raw) < len(magic)+16 || string(raw[:len(magic)]) != string(magic)) {
				return fmt.Errorf("%s 형식이 알려지지 않아 덮어쓸 수 없습니다", name)
			}
		}
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	id := hex.EncodeToString(nonce)
	root := filepath.Join(v.Dir, generationRoot)
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	dir := v.generationDir(id)
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	// Do not clean incomplete generation on failure: forensic evidence. It is
	// unreachable because there is no manifest pointing to it.
	encrypted := make([][]byte, 2)
	for i, p := range []struct{ name, value string }{{"demo_api_key.enc", key}, {"demo_secret_key.enc", secret}} {
		payload, err := encoded(p.name, p.value, pass, nonce)
		if err != nil {
			return err
		}
		if err := writeDurableExclusive(filepath.Join(dir, p.name), payload); err != nil {
			return err
		}
		encrypted[i] = payload
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	next := generationManifest{Schema: 1, Active: id, KeySHA256: fileSHA(encrypted[0]), SecretSHA256: fileSHA(encrypted[1])}
	if present {
		next.Previous = current.Active
	}
	return v.replaceManifest(next)
}

// RestorePreviousDemo is an internal MANUAL recovery operation, never exposed
// to HTTP. The operator needs the PREVIOUS password; no automatic fallback on
// active corruption or incorrect passwords is permitted.
func (v *Vault) RestorePreviousDemo(previousPassphrase string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	m, present, err := v.activeManifestRead()
	if err != nil {
		return err
	}
	if !present || m.Previous == "" {
		return errors.New("복구할 이전 인증 버전이 없습니다")
	}
	if _, _, err := v.readGeneration(m.Previous, previousPassphrase, "", ""); err != nil {
		return err
	}
	payloads := make([][]byte, 2)
	for i, name := range []string{"demo_api_key.enc", "demo_secret_key.enc"} {
		payloads[i], err = os.ReadFile(filepath.Join(v.generationDir(m.Previous), name))
		if err != nil {
			return err
		}
	}
	previous := generationManifest{Schema: 1, Active: m.Previous, Previous: m.Active, KeySHA256: fileSHA(payloads[0]), SecretSHA256: fileSHA(payloads[1])}
	return v.replaceManifest(previous)
}
