package wallet

// ReadBoundAccountFingerprint is intentionally read-only. It never creates a
// wallet directory or an allocation event. An unbound/empty wallet cannot be
// used to assert an independently established Demo account identity.
import (
	"errors"
	"os"
	"path/filepath"
)

func ReadBoundAccountFingerprint(dataDir string) (fingerprint string, revision uint64, ledgerTailSHA256 string, err error) {
	if dataDir == "" {
		return "", 0, "", errors.New("missing data directory")
	}
	dir := filepath.Join(dataDir, "wallet_demo", "events")
	for _, parent := range []string{dataDir, filepath.Join(dataDir, "wallet_demo")} {
		info, statErr := os.Lstat(parent)
		if statErr != nil {
			return "", 0, "", statErr
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", 0, "", errors.New("wallet parent is not a regular directory")
		}
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", 0, "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", 0, "", errors.New("wallet event directory is not a regular directory")
	}
	s := &Store{dir: dir}
	_, revision, ledgerTailSHA256, err = s.read()
	if err != nil {
		return "", 0, "", err
	}
	if revision == 0 || ledgerTailSHA256 == "" {
		return "", 0, "", errors.New("account has no independently bound wallet events")
	}
	fingerprint, err = s.firstFingerprint()
	if err != nil || !fingerprintPattern.MatchString(fingerprint) {
		return "", 0, "", errors.New("invalid wallet account binding")
	}
	return fingerprint, revision, ledgerTailSHA256, nil
}
