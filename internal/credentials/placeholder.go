// Package credentials creates blank placeholders and encrypts demo keys.
// Real credentials remain empty and real account requests are locked in Phase 2.
package credentials

import (
	"errors"
	"os"
	"path/filepath"
)

type Status struct {
	Name  string `json:"name"`
	Label string `json:"label"`
	State string `json:"state"`
}

var names = []struct{ name, label string }{
	{"demo_api_key.enc", "데모 API 키"},
	{"demo_secret_key.enc", "데모 비밀 키"},
	{"real_api_key.enc", "실거래 API 키"},
	{"real_secret_key.enc", "실거래 비밀 키"},
}

func Initialize(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for _, item := range names {
		path := filepath.Join(dir, item.name)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func Inspect(dir string) []Status {
	statuses := make([]Status, 0, len(names))
	for _, item := range names {
		state := "비어 있음 · 설정 전"
		if item.name == "demo_api_key.enc" || item.name == "demo_secret_key.enc" {
			vault := &Vault{Dir: dir}
			m, present, err := vault.activeManifestRead()
			if err != nil {
				statuses = append(statuses, Status{Name: item.name, Label: item.label, State: "활성 인증 매니페스트 손상 · 수동 점검"})
				continue
			}
			if present {
				expected := m.KeySHA256
				if item.name == "demo_secret_key.enc" {
					expected = m.SecretSHA256
				}
				raw, e := os.ReadFile(filepath.Join(vault.generationDir(m.Active), item.name))
				if e != nil || fileSHA(raw) != expected {
					state = "활성 인증 파일 손상 · 수동 점검"
				} else {
					state = "버전별 암호화 저장됨 · 잠금 상태"
				}
				statuses = append(statuses, Status{Name: item.name, Label: item.label, State: state})
				continue
			}
		}
		fi, err := os.Stat(filepath.Join(dir, item.name))
		if err != nil {
			state = "파일 확인 불가"
		} else if fi.Size() != 0 {
			// A matching file header proves format only; correct passphrase
			// and integrity are validated when the user unlocks it.
			if lenContent, e := os.ReadFile(filepath.Join(dir, item.name)); e == nil && len(lenContent) >= len(magic) && string(lenContent[:len(magic)]) == string(magic) {
				state = "암호화 저장됨 · 잠금 상태"
			} else {
				state = "알 수 없는 파일 · 덮어쓰기 보호"
			}
		}
		statuses = append(statuses, Status{Name: item.name, Label: item.label, State: state})
	}
	return statuses
}
