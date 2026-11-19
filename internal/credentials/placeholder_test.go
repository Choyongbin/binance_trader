package credentials

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBlankPlaceholdersAndDoNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := Initialize(dir); err != nil {
		t.Fatal(err)
	}
	if got := Inspect(dir); len(got) != 4 {
		t.Fatalf("want 4 placeholders; got %d", len(got))
	}
	keyfile := filepath.Join(dir, "demo_api_key.enc")
	fi, err := os.Stat(keyfile)
	if err != nil || fi.Size() != 0 {
		t.Fatal("credential placeholder should be blank")
	}
	if err := os.WriteFile(keyfile, []byte("do-not-overwrite"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Initialize(dir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(keyfile)
	if err != nil || string(got) != "do-not-overwrite" {
		t.Fatal("existing file was overwritten")
	}
	if Inspect(dir)[0].State != "알 수 없는 파일 · 덮어쓰기 보호" {
		t.Fatal("non-empty file incorrectly marked configured")
	}
}
