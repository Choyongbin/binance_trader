package trading

// Read-only, whole-directory protection-plan audit. This is suitable for an
// offline copy or a stopped server. It fingerprints every local protection
// submission file before and after parsing so a concurrent writer cannot be
// mistaken for a stable review. It never creates, deletes, or rewrites files.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func validProtectionSubmissionArchiveEntry(parent, name string, isDir bool) bool {
	if parent != "" || isDir || !strings.HasSuffix(name, ".json") {
		return false
	}
	for _, suffix := range []string{".tp.attempt.json", ".tp.observed.json", ".sl.attempt.json", ".sl.observed.json"} {
		if strings.HasSuffix(name, suffix) {
			return requestIDPattern.MatchString(strings.TrimSuffix(name, suffix))
		}
	}
	return requestIDPattern.MatchString(strings.TrimSuffix(name, ".json"))
}

// ReviewProtectionSubmissionDirectory performs no mkdir and no mutation. A
// missing directory is an empty review, not proof of safe trading.
func ReviewProtectionSubmissionDirectory(dataDir string) (ProtectionArmingReview, error) {
	if dataDir == "" {
		return ProtectionArmingReview{}, errors.New("데이터 폴더 없음")
	}
	dir := filepath.Join(dataDir, "trading", "protection_submissions")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return ReviewProtectionArmingPlans(nil), nil
	}
	if err != nil {
		return ProtectionArmingReview{}, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ProtectionArmingReview{}, errors.New("보호 제출 경로가 안전한 디렉터리가 아닙니다")
	}
	before, _, _, err := snapshotDir(dir, validProtectionSubmissionArchiveEntry)
	if err != nil {
		return ProtectionArmingReview{}, err
	}
	store := &ProtectionSubmissionStore{dir: dir}
	plans, err := store.scanPlansLocked()
	if err != nil {
		return ProtectionArmingReview{}, err
	}
	review := ReviewProtectionArmingPlans(plans)
	after, _, _, err := snapshotDir(dir, validProtectionSubmissionArchiveEntry)
	if err != nil {
		return ProtectionArmingReview{}, err
	}
	if before != after {
		return ProtectionArmingReview{}, errors.New("보호 계획 감사 중 파일이 변경되었습니다: 중지된 오프라인 사본에서 다시 검토하십시오")
	}
	return review, nil
}
