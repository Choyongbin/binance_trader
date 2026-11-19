//go:build windows

package trading

import (
	"errors"
	"os"
)

// Windows does not reliably permit FlushFileBuffers on directory handles opened
// through os.Open; Go's File.Sync can therefore return ERROR_ACCESS_DENIED even
// though the append-only state file itself was successfully flushed. The state
// record is already O_EXCL-created and File.Sync'ed before this helper runs.
// Validate that the directory is still a real directory, then rely on the
// flushed state file for the durable control record.
func syncAutoControlDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("자동매매 제어 디렉터리 형식이 안전하지 않습니다")
	}
	return nil
}
