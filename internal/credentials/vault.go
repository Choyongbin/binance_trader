package credentials

// Vault encrypts each key separately with AES-256-GCM using a fresh salt and
// nonce per file. It is never backed by environment variables or plain files.
// A passphrase is supplied by the local user and is NOT written to disk.
import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	saltSize   = 16
	nonceSize  = 12
	iterations = 250000
)

var magic = []byte("BTR2AESGCM1")

// Derive an encryption key using PBKDF2-HMAC-SHA256 with a fixed, documented cost.
// A random salt is generated for every file, even within the same environment.
func derive(passphrase string, salt []byte) []byte {
	var counter [4]byte
	binary.BigEndian.PutUint32(counter[:], 1)
	h := hmac.New(sha256.New, []byte(passphrase))
	h.Write(salt)
	h.Write(counter[:])
	u := h.Sum(nil)
	result := append([]byte(nil), u...)
	for i := 1; i < iterations; i++ {
		h = hmac.New(sha256.New, []byte(passphrase))
		h.Write(u)
		u = h.Sum(u[:0])
		for j := range result {
			result[j] ^= u[j]
		}
	}
	for i := range u {
		u[i] = 0
	}
	return result
}
func encoded(name, value, passphrase string, generation []byte) ([]byte, error) {
	if len(passphrase) < 12 {
		return nil, errors.New("암호는 12자 이상이어야 합니다")
	}
	if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n") {
		return nil, errors.New("API 키를 확인하세요")
	}
	salt := make([]byte, saltSize)
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	key := derive(passphrase, salt)
	defer zero(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), magic...)
	out = append(out, generation...)
	out = append(out, salt...)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, []byte(value), append([]byte(name), generation...)), nil
}
func decode(name string, payload []byte, passphrase string) (string, error) {
	header := len(magic) + 16 + saltSize + nonceSize
	if len(payload) <= header || string(payload[:len(magic)]) != string(magic) {
		return "", errors.New("암호화 파일 형식이 올바르지 않습니다")
	}
	salt := payload[len(magic)+16 : len(magic)+16+saltSize]
	nonce := payload[len(magic)+16+saltSize : header]
	key := derive(passphrase, salt)
	defer zero(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, nonce, payload[header:], append([]byte(name), payload[len(magic):len(magic)+16]...))
	if err != nil {
		return "", errors.New("암호가 틀렸거나 암호화 파일이 손상되었습니다")
	}
	result := string(plain)
	zero(plain)
	return result, nil
}
func zero(v []byte) {
	for i := range v {
		v[i] = 0
	}
}
func (v *Vault) read(name, passphrase string) (string, error) {
	data, err := os.ReadFile(filepath.Join(v.Dir, name))
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", errors.New("API 인증 정보가 비어 있습니다")
	}
	return decode(name, data, passphrase)
}

type Vault struct {
	Dir string
	mu  sync.Mutex
}

func NewVault(dir string) (*Vault, error) {
	if err := Initialize(dir); err != nil {
		return nil, err
	}
	return &Vault{Dir: dir}, nil
}
func (v *Vault) ReadDemo(passphrase string) (string, string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	m, present, err := v.activeManifestRead()
	if err != nil {
		return "", "", err
	}
	if present {
		return v.readGeneration(m.Active, passphrase, m.KeySHA256, m.SecretSHA256)
	}
	// Backward compatibility: original pair layout is read-only after first
	// successful manifest commit. Never fall back when a manifest is damaged.
	key, err := v.read("demo_api_key.enc", passphrase)
	if err != nil {
		return "", "", err
	}
	keyBytes, err := os.ReadFile(filepath.Join(v.Dir, "demo_api_key.enc"))
	if err != nil {
		return "", "", err
	}
	secretBytes, err := os.ReadFile(filepath.Join(v.Dir, "demo_secret_key.enc"))
	if err != nil {
		return "", "", err
	}
	if len(keyBytes) < len(magic)+16 || len(secretBytes) < len(magic)+16 || string(keyBytes[len(magic):len(magic)+16]) != string(secretBytes[len(magic):len(magic)+16]) {
		return "", "", errors.New("기존 인증 파일 쌍이 일치하지 않습니다: 수동 점검 필요")
	}
	secret, err := v.read("demo_secret_key.enc", passphrase)
	if err != nil {
		return "", "", err
	}
	return key, secret, nil
}
func (v *Vault) SaveDemo(key, secret, passphrase string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(passphrase) < 12 {
		return errors.New("암호는 12자 이상이어야 합니다")
	}
	if len(key) == 0 || len(secret) == 0 || len(key) > 512 || len(secret) > 512 {
		return errors.New("인증 키 길이를 확인하세요")
	}
	return v.saveGeneration(key, secret, passphrase)
}
