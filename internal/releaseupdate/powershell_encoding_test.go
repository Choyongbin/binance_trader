package releaseupdate

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPowerShellScriptsHaveAtMostOneUTF8BOM(t *testing.T) {
	root := filepath.Join("..", "..", "scripts")
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Skip("scripts unavailable in updater staging layout")
		}
		t.Fatal(err)
	}
	bom := []byte{0xef, 0xbb, 0xbf}
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.EqualFold(filepath.Ext(path), ".ps1") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		count := 0
		rest := raw
		for bytes.HasPrefix(rest, bom) {
			count++
			rest = rest[len(bom):]
		}
		if count > 1 {
			t.Errorf("PowerShell script has %d leading UTF-8 BOMs: %s", count, filepath.ToSlash(path))
		}
		if bytes.HasPrefix(raw, []byte{0xff, 0xfe}) || bytes.HasPrefix(raw, []byte{0xfe, 0xff}) {
			t.Errorf("UTF-16 PowerShell script is not allowed: %s", filepath.ToSlash(path))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
