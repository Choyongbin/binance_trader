// Package releaseupdate installs an independently unpacked, hash-verified
// cumulative source release. It NEVER modifies an existing data/ or .git/ tree,
// and retains a COMPLETE external backup before modifying an old installation.
// This is an OFFLINE tool: no exchange credentials or orders are accessed.
package releaseupdate

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const manifestFile = "docs/RELEASE_MANIFEST_SHA256.txt"
const maxSourceFiles = 6000
const maxSourceFileBytes int64 = 64 << 20

type Plan struct {
	Source               string
	Target               string
	NewInstall           bool
	Files                int
	BackedUp             bool
	Backup               string
	Installed            bool
	VersionMarkerMissing bool
}

type Options struct {
	Source     string
	Target     string
	Apply      bool
	NewInstall bool
	// Injected only by package tests, never exposed to operators.
	afterBackup func() error
	portCheck   func() error
}

func cleanRoot(p string) (string, error) {
	if p == "" {
		return "", errors.New("소스와 설치 경로가 필요합니다")
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	// Refuse a symlink/junction as the root, too.
	info, err := os.Lstat(abs)
	if err == nil && !info.IsDir() {
		return "", errors.New("경로가 실제 디렉터리가 아닙니다")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("경로 심볼릭 링크 금지")
	}
	return abs, nil
}

func beneath(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func safeRel(raw string) (string, error) {
	if raw == "" || strings.Contains(raw, "\\") || strings.Contains(raw, ":") || strings.Contains(raw, "\x00") || strings.HasPrefix(raw, "/") || strings.Contains(raw, "//") {
		return "", errors.New("허용되지 않은 파일 경로")
	}
	parts := strings.Split(raw, "/")
	for _, p := range parts {
		if p == "." || p == ".." || p == "" || strings.HasSuffix(p, ".") || strings.HasSuffix(p, " ") {
			return "", errors.New("위험한 상대 경로")
		}
	}
	rel := filepath.FromSlash(raw)
	if !filepath.IsLocal(rel) || filepath.ToSlash(filepath.Clean(rel)) != raw {
		return "", errors.New("상대 경로가 아닙니다")
	}
	head := strings.ToLower(parts[0])
	if head == "data" || head == ".git" || head == "bin" {
		return "", errors.New("데이터·Git·빌드 경로는 릴리스에 포함하지 않습니다")
	}
	return rel, nil
}

type listed struct{ rel, digest string }

func parseManifest(root string) ([]listed, error) {
	raw, err := os.Open(filepath.Join(root, filepath.FromSlash(manifestFile)))
	if err != nil {
		return nil, fmt.Errorf("누적 릴리스 체크섬 목록 없음: %w", err)
	}
	defer raw.Close()
	var entries []listed
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(io.LimitReader(raw, 2<<20))
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(line) < 67 || line[64:66] != "  " {
			return nil, errors.New("체크섬 목록 형식 오류")
		}
		hash, relRaw := line[:64], line[66:]
		d, err := hex.DecodeString(hash)
		if err != nil || len(d) != sha256.Size {
			return nil, errors.New("SHA-256 형식 오류")
		}
		rel, err := safeRel(relRaw)
		if err != nil || relRaw == manifestFile {
			return nil, errors.New("금지된 체크섬 항목")
		}
		folded := strings.ToLower(relRaw) // NTFS case-insensitive collision guard
		if seen[folded] {
			return nil, errors.New("중복 또는 대소문자 충돌 릴리스 경로")
		}
		seen[folded] = true
		entries = append(entries, listed{rel: rel, digest: hash})
		if len(entries) > maxSourceFiles {
			return nil, errors.New("릴리스 파일 개수 초과")
		}
	}
	if scanner.Err() != nil || len(entries) == 0 {
		return nil, errors.New("체크섬 목록이 비었거나 손상되었습니다")
	}
	return entries, nil
}

func safeRegular(root, path string, capBytes int64) (os.FileInfo, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, errors.New("설치 경로 이탈")
	}
	cursor := root
	for _, seg := range strings.Split(rel, string(filepath.Separator)) {
		cursor = filepath.Join(cursor, seg)
		st, err := os.Lstat(cursor)
		if err != nil {
			return nil, err
		}
		if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() && !st.IsDir() {
			return nil, errors.New("릴리스/설치 경로에 특수 파일 또는 링크가 있습니다")
		}
	}
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > capBytes {
		return nil, errors.New("비정상 파일 또는 허용 크기 초과")
	}
	return st, nil
}

func checksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifyList(root string, entries []listed) error {
	for _, item := range entries {
		p := filepath.Join(root, item.rel)
		if _, err := safeRegular(root, p, maxSourceFileBytes); err != nil {
			return fmt.Errorf("릴리스 파일 오류: %s: %w", item.rel, err)
		}
		sum, err := checksum(p)
		if err != nil || sum != item.digest {
			return fmt.Errorf("릴리스 파일 SHA-256 불일치: %s", item.rel)
		}
	}
	return nil
}

func expectedVersionMarker(root string, entries []listed) (string, bool, error) {
	var want string
	for _, item := range entries {
		if filepath.ToSlash(item.rel) == "VERSION" {
			want = item.digest
			break
		}
	}
	if want == "" {
		return "", false, errors.New("기존 매니페스트에 VERSION 항목이 없습니다")
	}
	path := filepath.Join(root, "internal", "version", "version.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false, err
	}
	const marker = `Release = "`
	text := string(raw)
	i := strings.Index(text, marker)
	if i < 0 {
		return "", false, errors.New("internal/version/version.go에서 Release를 찾지 못했습니다")
	}
	rest := text[i+len(marker):]
	j := strings.IndexByte(rest, '"')
	if j <= 0 {
		return "", false, errors.New("Release 문자열 형식 오류")
	}
	body := rest[:j] + "\n"
	h := sha256.Sum256([]byte(body))
	if hex.EncodeToString(h[:]) != want {
		return "", false, errors.New("누락된 VERSION을 신뢰 가능한 소스에서 재구성할 수 없습니다")
	}
	return body, true, nil
}

// verifyExistingList is intentionally stricter than a generic "missing file"
// repair. The only tolerated historical installer defect is a missing top-level
// VERSION marker. Every other tracked file must still match its old manifest.
// VERSION is reconstructible only when the manifest digest agrees with the
// Release constant in the already-manifested internal/version/version.go.
func verifyExistingList(root string, entries []listed) (bool, error) {
	missingVersion := false
	for _, item := range entries {
		p := filepath.Join(root, item.rel)
		if _, err := safeRegular(root, p, maxSourceFileBytes); err != nil {
			if filepath.ToSlash(item.rel) == "VERSION" && errors.Is(err, os.ErrNotExist) {
				missingVersion = true
				continue
			}
			return false, fmt.Errorf("릴리스 파일 오류: %s: %w", item.rel, err)
		}
		sum, err := checksum(p)
		if err != nil || sum != item.digest {
			return false, fmt.Errorf("릴리스 파일 SHA-256 불일치: %s", item.rel)
		}
	}
	if missingVersion {
		if _, ok, err := expectedVersionMarker(root, entries); err != nil || !ok {
			if err == nil {
				err = errors.New("VERSION 재구성 검증 실패")
			}
			return false, err
		}
	}
	return missingVersion, nil
}

func verifySource(root string) ([]listed, error) {
	entries, err := parseManifest(root)
	if err != nil {
		return nil, err
	}
	// Every source file must be covered; never copy an unlisted executable.
	expected := make(map[string]bool, len(entries)+1)
	expected[strings.ToLower(filepath.FromSlash(manifestFile))] = true
	for _, x := range entries {
		expected[strings.ToLower(x.rel)] = true
	}
	count := 0
	err = filepath.Walk(root, func(p string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("릴리스 링크/접합점 금지")
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return errors.New("릴리스 특수 파일 금지")
		}
		rel, err := filepath.Rel(root, p)
		if err != nil || !expected[strings.ToLower(rel)] {
			return errors.New("목록에 없는 파일 존재 또는 잘못된 릴리스 경로")
		}
		count++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if count != len(entries)+1 {
		return nil, errors.New("릴리스 파일 수 불일치")
	}
	if err := verifyList(root, entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func checkPort() error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", 350*time.Millisecond)
	if err == nil {
		conn.Close()
		return errors.New("127.0.0.1:8080이 사용 중입니다: 서버 중지 후 진행하세요")
	}
	return nil // Unknown listener on another port is explicitly not covered.
}

// These exact SHA-256 digests identify the read-only signed inspector that was
// distributed as an out-of-band dev.3.55 addon. It was intentionally NOT in
// the dev.3.55 manifest. Dev.3.56 includes the same files in the signed source
// tree. The historical addon may be safely replaced ONLY if the prior VERSION
// and original bytes match. Any edited or other untracked Go file stays locked.
var legacyDev355SignedInspectorGo = map[string]string{
	"cmd/demo-signed-inspect/main.go":      "cdc9eab9330348df8f9cc024bb229363d84a4d16787c2798d794fdf464045568",
	"cmd/demo-signed-inspect/main_test.go": "3c3b9d79c2934adddeb0604af120adb376416c45247cdbf0c11a16b25ac4d3cc",
}

// A stray .go file in the old working tree may be compiled alongside the
// new release despite having no old checksum. Never silently merge source.
func rejectUntrackedGo(root string, old []listed) error {
	versionBytes, versionErr := os.ReadFile(filepath.Join(root, "VERSION"))
	isDev355 := versionErr == nil && strings.TrimSpace(string(versionBytes)) == "v0.4.11-dev.3.55"
	known := make(map[string]bool, len(old)+1)
	known[strings.ToLower(filepath.FromSlash(manifestFile))] = true
	for _, e := range old {
		known[strings.ToLower(e.rel)] = true
	}
	return filepath.Walk(root, func(path string, st os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return errors.New("기존 설치에 링크/접합점이 존재합니다: 자동 통합 금지")
		}
		if st.IsDir() {
			if rel != "." {
				switch strings.ToLower(strings.Split(filepath.ToSlash(rel), "/")[0]) {
				case "data", ".git", "bin", "pc-results":
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(rel), ".go") && !known[strings.ToLower(rel)] {
			if isDev355 {
				if expected, ok := legacyDev355SignedInspectorGo[filepath.ToSlash(rel)]; ok {
					actual, err := checksum(path)
					if err == nil && actual == expected {
						return nil // exact immutable legacy read-only inspector addon
					}
				}
			}
			return fmt.Errorf("기존 설치에 체크섬 관리 밖의 Go 파일이 있습니다: %s", rel)
		}
		return nil
	})
}

func validateOld(root string) ([]listed, bool, error) {
	old, err := parseManifest(root)
	if err != nil {
		return nil, false, fmt.Errorf("기존 설치의 매니페스트를 읽지 못했습니다: %w. 새 폴더 설치(-new)를 사용하세요", err)
	}
	missingVersion, err := verifyExistingList(root, old)
	if err != nil {
		return nil, false, fmt.Errorf("기존 설치 파일이 수정되거나 손상되었습니다 (자동 덮어쓰기 금지): %w", err)
	}
	if err := rejectUntrackedGo(root, old); err != nil {
		return nil, false, err
	}
	return old, missingVersion, nil
}

func copyOne(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Perm())
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func copyTree(from, to string) error {
	// A complete copy is safer than omitting even apparently unimportant data.
	if err := os.Mkdir(to, 0700); err != nil {
		return err
	}
	return filepath.Walk(from, func(path string, st os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if st.Mode()&os.ModeSymlink != 0 {
			return errors.New("기존 설치에 링크/접합점이 있어 전체 백업을 안전하게 검증할 수 없습니다")
		}
		if st.IsDir() {
			if rel == "." {
				return nil
			}
			return os.Mkdir(target, st.Mode().Perm())
		}
		if !st.Mode().IsRegular() {
			return errors.New("기존 설치에 특수 파일이 있어 백업 중단")
		}
		if err := copyOne(path, target, st.Mode()); err != nil {
			return err
		}
		before, err := checksum(path)
		if err != nil {
			return err
		}
		after, err := checksum(target)
		if err != nil || before != after {
			return errors.New("백업 파일 검증 실패")
		}
		return nil
	})
}

func installOne(source, target string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(target), ".binance-stage-*")
	if err != nil {
		return err
	}
	path := temp.Name()
	temp.Close()
	defer os.Remove(path)
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_TRUNC|os.O_WRONLY, mode.Perm())
	if err != nil {
		in.Close()
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	in.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Chmod(path, mode.Perm()); err != nil {
		return err
	}
	// os.Rename replaces a regular file on supported NTFS systems, or fails
	// closed if Windows antivirus/process handles forbid replacement.
	return os.Rename(path, target)
}

func Run(o Options) (Plan, error) {
	portCheck := o.portCheck
	if portCheck == nil {
		portCheck = checkPort
	}
	src, err := cleanRoot(o.Source)
	if err != nil {
		return Plan{}, err
	}
	dst, err := cleanRoot(o.Target)
	if err != nil {
		return Plan{}, err
	}
	if beneath(src, dst) || beneath(dst, src) {
		return Plan{}, errors.New("원본과 설치 경로는 포함 관계가 아닌 별도 폴더여야 합니다")
	}
	entries, err := verifySource(src)
	if err != nil {
		return Plan{}, err
	}
	st, e := os.Lstat(dst)
	exists := e == nil
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return Plan{}, e
	}
	if exists && (!st.IsDir() || st.Mode()&os.ModeSymlink != 0) {
		return Plan{}, errors.New("설치 경로가 실제 폴더가 아닙니다")
	}
	if exists == o.NewInstall {
		return Plan{}, errors.New("신규 폴더는 -new, 기존 설치는 -new 없이 실행하세요")
	}
	var old []listed
	versionMarkerMissing := false
	if exists {
		if _, err := os.Stat(filepath.Join(dst, "data", "runtime", "server-process.json")); err == nil {
			return Plan{}, errors.New("기존 실행 PID 기록이 남아 있습니다. stop.ps1로 종료·확인하세요")
		} else if !errors.Is(err, os.ErrNotExist) {
			return Plan{}, err
		}
		old, versionMarkerMissing, err = validateOld(dst)
		if err != nil {
			return Plan{}, err
		}

		newSet := make(map[string]bool, len(entries))
		for _, item := range entries {
			newSet[strings.ToLower(item.rel)] = true
		}
		for _, item := range old {
			if !newSet[strings.ToLower(item.rel)] {
				return Plan{}, fmt.Errorf("이 릴리스가 기존 추적 파일을 삭제합니다: %s; 수동 검토 필요", item.rel)
			}
		}
		if err := portCheck(); err != nil {
			return Plan{}, err
		}
	}
	plan := Plan{Source: src, Target: dst, NewInstall: o.NewInstall, Files: len(entries) + 1, VersionMarkerMissing: versionMarkerMissing}
	if !o.Apply {
		return plan, nil
	}
	if exists {
		backup := dst + ".backup-" + time.Now().UTC().Format("20060102T150405.000000000Z")
		if err := copyTree(dst, backup); err != nil {
			return plan, fmt.Errorf("백업 미완료 (원본 불변): %s: %w", backup, err)
		}
		plan.BackedUp = true
		plan.Backup = backup
		if _, err := verifyExistingList(backup, old); err != nil {
			return plan, fmt.Errorf("백업된 설치 검증 실패: %w", err)
		}
		if _, err := verifyExistingList(dst, old); err != nil {
			return plan, fmt.Errorf("백업 중 기존 파일 변경 감지: %w", err)
		}
		if err := portCheck(); err != nil {
			return plan, err
		}
		if o.afterBackup != nil {
			if err := o.afterBackup(); err != nil {
				return plan, err
			}
		}
	} else if err := os.Mkdir(dst, 0700); err != nil {
		return plan, err
	}
	// Preserve source order for reproducibility; manifest MUST be committed LAST.
	sort.Slice(entries, func(i, j int) bool { return entries[i].rel < entries[j].rel })
	for _, f := range entries {
		orig := filepath.Join(src, f.rel)
		target := filepath.Join(dst, f.rel)
		if _, err := safeRegular(src, orig, maxSourceFileBytes); err != nil {
			return plan, err
		}
		if actual, err := checksum(orig); err != nil || actual != f.digest {
			return plan, errors.New("복사 직전 소스 해시 변경")
		}
		if _, err := os.Lstat(target); err == nil {
			if _, err := safeRegular(dst, target, maxSourceFileBytes); err != nil {
				return plan, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return plan, err
		}
		st, err := os.Stat(orig)
		if err != nil {
			return plan, err
		}
		if err := installOne(orig, target, st.Mode()); err != nil {
			return plan, fmt.Errorf("업데이트 도중 중단됨 (백업으로 수동 복원): %s: %w", f.rel, err)
		}
		if after, err := checksum(target); err != nil || after != f.digest {
			return plan, errors.New("업데이트 파일 해시 검증 실패: " + f.rel)
		}
	}
	manifest := filepath.Join(src, filepath.FromSlash(manifestFile))
	targetManifest := filepath.Join(dst, filepath.FromSlash(manifestFile))
	st, _ = os.Stat(manifest)
	if err := installOne(manifest, targetManifest, st.Mode()); err != nil {
		return plan, err
	}
	if err := verifyList(dst, entries); err != nil {
		return plan, err
	}
	a, err := checksum(manifest)
	if err != nil {
		return plan, err
	}
	b, err := checksum(targetManifest)
	if err != nil || a != b {
		return plan, errors.New("최종 매니페스트 불일치")
	}
	plan.Installed = true
	return plan, nil
}

// RestoreCompleteBackup restores a complete sibling backup created by Run.
// It is intentionally a separate explicit operation used only after a failed
// post-install verification while the server is stopped. The failed target is
// retained as a sibling quarantine for forensic inspection.
func RestoreCompleteBackup(backupPath, targetPath string) (string, error) {
	return restoreCompleteBackup(backupPath, targetPath, checkPort)
}

func restoreCompleteBackup(backupPath, targetPath string, portCheck func() error) (string, error) {
	backup, err := cleanRoot(backupPath)
	if err != nil {
		return "", err
	}
	target, err := cleanRoot(targetPath)
	if err != nil {
		return "", err
	}
	if beneath(backup, target) || beneath(target, backup) {
		return "", errors.New("백업과 설치 경로는 포함 관계일 수 없습니다")
	}
	if _, err := os.Stat(filepath.Join(target, "data", "runtime", "server-process.json")); err == nil {
		return "", errors.New("실행 PID 기록이 남아 있어 자동 복구하지 않습니다")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if portCheck == nil {
		portCheck = checkPort
	}
	if err := portCheck(); err != nil {
		return "", err
	}
	entries, _, err := validateOld(backup)
	if err != nil {
		return "", fmt.Errorf("복구 백업 검증 실패: %w", err)
	}
	// The backup was originally created by copyTree, which rejects links and
	// special files. Re-check before trusting it after a failed install.
	if err := filepath.Walk(backup, func(path string, st os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return errors.New("복구 백업에 링크/접합점이 있습니다")
		}
		if !st.IsDir() && !st.Mode().IsRegular() {
			return errors.New("복구 백업에 특수 파일이 있습니다")
		}
		return nil
	}); err != nil {
		return "", err
	}
	if _, err := os.Lstat(target); err != nil {
		return "", fmt.Errorf("복구 대상 설치를 찾을 수 없습니다: %w", err)
	}
	quarantine := target + ".failed-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	if err := os.Rename(target, quarantine); err != nil {
		return "", fmt.Errorf("실패 설치 격리 실패: %w", err)
	}
	if err := os.Rename(backup, target); err != nil {
		_ = os.Rename(quarantine, target)
		return "", fmt.Errorf("백업 원위치 복구 실패: %w", err)
	}
	if _, err := verifyExistingList(target, entries); err != nil {
		return quarantine, fmt.Errorf("복구 후 설치 검증 실패: %w", err)
	}
	return quarantine, nil
}
