// Package reportindex makes a disposable, read-only SQLite search projection
// from validated immutable accounting reports. It is NOT the source ledger,
// an order engine, an audited final net P&L, or proof of income completeness.
package reportindex

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/sqlitebridge"
	"binance_trader/internal/trading"
)

var (
	ErrNotBuilt  = errors.New("회계 SQLite 읽기 색인이 생성되지 않았습니다")
	ErrStale     = errors.New("회계 원본이 변경되어 SQLite 색인이 오래되었습니다")
	ErrUnsafe    = errors.New("회계 SQLite 색인 파일 손상 또는 미완료 작업: 수동 확인 필요")
	indexMu      sync.Mutex
	filesPattern = regexp.MustCompile(`^[0-9a-f]{64}\.(?:sqlite3|seal\.json)$`)
)

// Every SQLite file is keyed to the exact full set of already validated
// immutable report SHA256 digests. No silent fallback to a stale projection.
type Seal struct {
	Schema            int       `json:"schema"`
	SourceFingerprint string    `json:"source_fingerprint"`
	SourceCount       int       `json:"source_count"`
	DatabaseSHA256    string    `json:"database_sha256"`
	CreatedAt         time.Time `json:"created_at"`
	SHA256            string    `json:"sha256"`
}

func (s Seal) digest() string {
	s.SHA256 = ""
	b, _ := json.Marshal(s)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type Status struct {
	State             string    `json:"state"`
	ReportCount       int       `json:"report_count"`
	SourceFingerprint string    `json:"source_fingerprint,omitempty"`
	SnapshotCreatedAt time.Time `json:"snapshot_created_at,omitempty"`
	HistoricalOnly    bool      `json:"historical_only"`
	TradingAuthorized bool      `json:"trading_authorized"`
	ActualNetVerified bool      `json:"actual_net_verified"`
}

type Filter struct {
	Source  string
	ModelID string
}
type History struct {
	Status  Status                     `json:"index"`
	Total   int                        `json:"total"`
	Limit   int                        `json:"limit"`
	Offset  int                        `json:"offset"`
	Reports []trading.AccountingReport `json:"reports"`
}

func dirOf(dataDir string) string { return filepath.Join(dataDir, "trading", "report_index") }
func sourceFingerprint(reports []trading.AccountingReport) string {
	h := sha256.New()
	io.WriteString(h, "binance-trader-accounting-projection-v1\n")
	for _, r := range reports {
		io.WriteString(h, r.ID+":"+r.SHA256+"\n")
	}
	return hex.EncodeToString(h.Sum(nil))
}
func hashFile(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	if st, e := f.Stat(); e != nil {
		return "", e
	} else if st.Size() > 256<<20 {
		return "", fmt.Errorf("SQLite 조회 색인 허용 크기 초과: %w", ErrUnsafe)
	}
	h := sha256.New()
	if _, e := io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func strictFiles(dir string) (int, error) {
	list, e := os.ReadDir(dir)
	if os.IsNotExist(e) {
		return 0, nil
	}
	if e != nil {
		return 0, e
	}
	if len(list) > 2000 {
		return 0, fmt.Errorf("SQLite 색인 파일 상한 초과: %w", ErrUnsafe)
	}
	for _, f := range list {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 || !filesPattern.MatchString(f.Name()) {
			return 0, fmt.Errorf("예상되지 않은 색인 파일 또는 미완료 작업 %q: %w", f.Name(), ErrUnsafe)
		}
	}
	for _, f := range list {
		if strings.HasSuffix(f.Name(), ".sqlite3") {
			if _, e := os.Stat(filepath.Join(dir, strings.TrimSuffix(f.Name(), ".sqlite3")+".seal.json")); e != nil {
				return 0, fmt.Errorf("서명되지 않은 색인 파일 발견: %w", ErrUnsafe)
			}
		} else {
			if _, e := os.Stat(filepath.Join(dir, strings.TrimSuffix(f.Name(), ".seal.json")+".sqlite3")); e != nil {
				return 0, fmt.Errorf("SQLite 파일이 누락된 기록 발견: %w", ErrUnsafe)
			}
		}
	}
	return len(list) / 2, nil
}
func openVerified(dir string, reports []trading.AccountingReport) (*sqlitebridge.DB, Seal, error) {
	fingerprint := sourceFingerprint(reports)
	count, e := strictFiles(dir)
	if e != nil {
		return nil, Seal{}, e
	}
	dbPath := filepath.Join(dir, fingerprint+".sqlite3")
	sealPath := filepath.Join(dir, fingerprint+".seal.json")
	if _, e := os.Stat(sealPath); os.IsNotExist(e) {
		if count == 0 {
			return nil, Seal{}, ErrNotBuilt
		}
		return nil, Seal{}, ErrStale
	} else if e != nil {
		return nil, Seal{}, e
	}
	b, e := os.ReadFile(sealPath)
	if e != nil {
		return nil, Seal{}, e
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	var s Seal
	if e = d.Decode(&s); e != nil {
		return nil, Seal{}, fmt.Errorf("색인 영수증 JSON 손상: %w", ErrUnsafe)
	}
	var tail any
	if e = d.Decode(&tail); !errors.Is(e, io.EOF) {
		return nil, Seal{}, fmt.Errorf("색인 영수증 후행 데이터: %w", ErrUnsafe)
	}
	if s.Schema != 1 || s.SourceFingerprint != fingerprint || s.SourceCount != len(reports) || len(s.DatabaseSHA256) != 64 ||
		s.CreatedAt.IsZero() || s.CreatedAt.After(time.Now().Add(time.Minute)) || s.SHA256 != s.digest() {
		return nil, Seal{}, fmt.Errorf("색인 영수증 검증 실패: %w", ErrUnsafe)
	}
	h, e := hashFile(dbPath)
	if e != nil {
		return nil, Seal{}, fmt.Errorf("SQLite 해시 조회 실패: %w", e)
	}
	if h != s.DatabaseSHA256 {
		return nil, Seal{}, fmt.Errorf("SQLite 데이터가 원본 확인 후 수정되었습니다: %w", ErrUnsafe)
	}
	db, e := sqlitebridge.Open(dbPath, true)
	if e != nil {
		return nil, Seal{}, fmt.Errorf("SQLite 읽기 실패: %w", ErrUnsafe)
	}
	check := func(sql, want string) error {
		rows, e := db.Query(sql)
		if e != nil || len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != want {
			return fmt.Errorf("SQLite 무결성/버전 확인 실패: %w", ErrUnsafe)
		}
		return nil
	}
	if e = check("PRAGMA quick_check", "ok"); e != nil {
		db.Close()
		return nil, Seal{}, e
	}
	if e = check("PRAGMA user_version", "1"); e != nil {
		db.Close()
		return nil, Seal{}, e
	}
	meta, e := db.Query("SELECT source_fingerprint, source_count FROM projection_meta WHERE id = 1")
	if e != nil || len(meta) != 1 || len(meta[0]) != 2 || meta[0][0] != fingerprint || meta[0][1] != strconv.Itoa(len(reports)) {
		db.Close()
		return nil, Seal{}, fmt.Errorf("SQLite 원본 지문 불일치: %w", ErrUnsafe)
	}
	countRows, e := db.Query("SELECT count(*) FROM reports")
	if e != nil || len(countRows) != 1 || countRows[0][0] != strconv.Itoa(len(reports)) {
		db.Close()
		return nil, Seal{}, fmt.Errorf("색인 보고서 개수 불일치: %w", ErrUnsafe)
	}
	return db, s, nil
}

// StatusOnly does not create a database or repair damaged records. Only the
// CLI Build function is able to create new read-only projection snapshots.
func StatusOnly(dataDir string) (Status, error) {
	r, e := trading.LoadAccountingReports(dataDir)
	if e != nil {
		return Status{}, e
	}
	s := Status{State: "NOT_BUILT", ReportCount: len(r), HistoricalOnly: true}
	dir := dirOf(dataDir)
	db, seal, e := openVerified(dir, r)
	if errors.Is(e, ErrNotBuilt) {
		return s, nil
	}
	if errors.Is(e, ErrStale) {
		s.State = "STALE"
		return s, nil
	}
	if e != nil {
		return Status{}, e
	}
	defer db.Close()
	s.State = "CURRENT"
	s.SourceFingerprint = seal.SourceFingerprint
	s.SnapshotCreatedAt = seal.CreatedAt
	return s, nil
}

// Build writes a *disposable* materialization of reports already checksum-
// verified by LoadAccountingReports. The original append-only source isn't
// modified. An interrupted build leaves a lock and triggers manual review.
func Build(dataDir string) (Status, error) {
	indexMu.Lock()
	defer indexMu.Unlock()
	if dataDir == "" {
		return Status{}, errors.New("데이터 경로 누락")
	}
	if !sqlitebridge.Available() {
		return Status{}, errors.New("이 환경에서 SQLite 네이티브 색인을 실행할 수 없습니다. 원본 자료는 변경하지 않습니다")
	}
	reports, e := trading.LoadAccountingReports(dataDir)
	if e != nil {
		return Status{}, e
	}
	dir := dirOf(dataDir)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return Status{}, e
	}
	if _, e = strictFiles(dir); e != nil {
		return Status{}, e
	}
	fingerprint := sourceFingerprint(reports)
	if _, e = os.Stat(filepath.Join(dir, fingerprint+".seal.json")); e == nil {
		return StatusOnly(dataDir)
	} else if !os.IsNotExist(e) {
		return Status{}, e
	}
	if e = os.Mkdir(filepath.Join(dir, "build.lock"), 0700); e != nil {
		return Status{}, fmt.Errorf("다른 색인 작성/중단 감지: %w", ErrUnsafe)
	}
	// Leave lock on any unexpected error: partial projection requires review.
	var nonce [16]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return Status{}, e
	}
	temp := filepath.Join(dir, ".building-"+hex.EncodeToString(nonce[:])+".sqlite3")
	db, e := sqlitebridge.Open(temp, false)
	if e != nil {
		return Status{}, e
	}
	if e = populate(db, reports, fingerprint); e != nil {
		db.Close()
		return Status{}, e
	}
	if e = db.Close(); e != nil {
		return Status{}, e
	}
	if e = os.Chmod(temp, 0600); e != nil {
		return Status{}, e
	}
	h, e := hashFile(temp)
	if e != nil {
		return Status{}, e
	}
	final := filepath.Join(dir, fingerprint+".sqlite3")
	if e = os.Rename(temp, final); e != nil {
		return Status{}, e
	}
	seal := Seal{Schema: 1, SourceFingerprint: fingerprint, SourceCount: len(reports), DatabaseSHA256: h, CreatedAt: time.Now().UTC()}
	seal.SHA256 = seal.digest()
	b, _ := json.MarshalIndent(seal, "", "  ")
	f, e := os.OpenFile(filepath.Join(dir, fingerprint+".seal.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return Status{}, e
	}
	_, e = f.Write(append(b, '\n'))
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return Status{}, e
	}
	if e = os.Remove(filepath.Join(dir, "build.lock")); e != nil {
		return Status{}, e
	}
	return StatusOnly(dataDir)
}

func populate(db *sqlitebridge.DB, reports []trading.AccountingReport, fingerprint string) error {
	schema := []string{
		"PRAGMA foreign_keys = ON",

		"PRAGMA synchronous = FULL",
		"PRAGMA user_version = 1",
		"CREATE TABLE projection_meta (id INTEGER PRIMARY KEY CHECK (id=1), source_fingerprint TEXT NOT NULL, source_count INTEGER NOT NULL)",
		"CREATE TABLE reports (report_id TEXT PRIMARY KEY, created_ns INTEGER NOT NULL, window_start_ms INTEGER NOT NULL, window_end_ms INTEGER NOT NULL, sha256 TEXT NOT NULL, report_json TEXT NOT NULL)",
		"CREATE TABLE owners (report_id TEXT NOT NULL REFERENCES reports(report_id), owner_idx INTEGER NOT NULL, source TEXT NOT NULL, model_id TEXT NOT NULL, symbol TEXT NOT NULL, position_side TEXT NOT NULL, owner_json TEXT NOT NULL, PRIMARY KEY(report_id,owner_idx))",
		"CREATE INDEX owners_source_model ON owners(source,model_id,report_id)",
		"CREATE TABLE commissions (report_id TEXT NOT NULL,owner_idx INTEGER NOT NULL,asset TEXT NOT NULL,amount_decimal TEXT NOT NULL,PRIMARY KEY(report_id,owner_idx,asset),FOREIGN KEY(report_id,owner_idx) REFERENCES owners(report_id,owner_idx))",
	}
	mode, e := db.Query("PRAGMA journal_mode = DELETE")
	if e != nil || len(mode) != 1 || len(mode[0]) != 1 || mode[0][0] != "delete" {
		return fmt.Errorf("SQLite 롤백 저널 안전 모드 실패: %w", ErrUnsafe)
	}
	for _, sql := range schema {
		if e := db.Exec(sql); e != nil {
			return e
		}
	}
	if e := db.Exec("BEGIN IMMEDIATE"); e != nil {
		return e
	}
	committed := false
	defer func() {
		if !committed {
			_ = db.Exec("ROLLBACK")
		}
	}()
	if e := db.Exec("INSERT INTO projection_meta (id,source_fingerprint,source_count) VALUES (1,?,?)", fingerprint, strconv.Itoa(len(reports))); e != nil {
		return e
	}
	for _, r := range reports {
		raw, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if e = db.Exec("INSERT INTO reports (report_id,created_ns,window_start_ms,window_end_ms,sha256,report_json) VALUES (?,?,?,?,?,?)", r.ID, strconv.FormatInt(r.CreatedAt.UnixNano(), 10), strconv.FormatInt(r.WindowStartMS, 10), strconv.FormatInt(r.WindowEndMS, 10), r.SHA256, string(raw)); e != nil {
			return e
		}
		for j, owner := range r.Owners {
			raw, e := json.Marshal(owner)
			if e != nil {
				return e
			}
			if e = db.Exec("INSERT INTO owners (report_id,owner_idx,source,model_id,symbol,position_side,owner_json) VALUES (?,?,?,?,?,?,?)", r.ID, strconv.Itoa(j), string(owner.Source), owner.ModelID, owner.Symbol, string(owner.PositionSide), string(raw)); e != nil {
				return e
			}
			for asset, value := range owner.CommissionByAsset {
				if e = db.Exec("INSERT INTO commissions (report_id,owner_idx,asset,amount_decimal) VALUES (?,?,?,?)", r.ID, strconv.Itoa(j), asset, value); e != nil {
					return e
				}
			}
		}
	}
	if e := db.Exec("COMMIT"); e != nil {
		return e
	}
	committed = true
	foreign, e := db.Query("PRAGMA foreign_key_check")
	if e != nil || len(foreign) > 0 {
		return fmt.Errorf("SQLite foreign key 검사 실패: %w", ErrUnsafe)
	}
	return nil
}

func ReadHistory(dataDir string, limit, offset int, f Filter) (History, error) {
	if limit < 1 || limit > 100 || offset < 0 || offset > 100000 {
		return History{}, errors.New("조회 범위 오류")
	}
	if f.Source != "" && f.Source != "manual" && f.Source != "model" {
		return History{}, errors.New("소유자 유형 오류")
	}
	if len(f.ModelID) > 80 || (f.Source != "model" && f.ModelID != "") {
		return History{}, errors.New("모델 선택 오류")
	}
	reports, e := trading.LoadAccountingReports(dataDir)
	if e != nil {
		return History{}, e
	}
	db, seal, e := openVerified(dirOf(dataDir), reports)
	if e != nil {
		return History{}, e
	}
	defer db.Close()
	where, args := "", []string{}
	if f.Source != "" {
		where = " WHERE EXISTS (SELECT 1 FROM owners o WHERE o.report_id=r.report_id AND o.source=?)"
		args = append(args, f.Source)
		if f.ModelID != "" {
			where = " WHERE EXISTS (SELECT 1 FROM owners o WHERE o.report_id=r.report_id AND o.source=? AND o.model_id=?)"
			args = append(args, f.ModelID)
		}
	}
	count, e := db.Query("SELECT count(*) FROM reports r"+where, args...)
	if e != nil || len(count) != 1 || len(count[0]) != 1 {
		return History{}, fmt.Errorf("SQLite 행 수 조회 실패: %w", ErrUnsafe)
	}
	n, e := strconv.Atoi(count[0][0])
	if e != nil || n < 0 || n > len(reports) {
		return History{}, ErrUnsafe
	}
	argsPage := append(append([]string(nil), args...), strconv.Itoa(limit), strconv.Itoa(offset))
	rows, e := db.Query("SELECT report_json FROM reports r"+where+" ORDER BY created_ns DESC,report_id DESC LIMIT ? OFFSET ?", argsPage...)
	if e != nil {
		return History{}, e
	}
	out := History{Status: Status{State: "CURRENT", ReportCount: len(reports), SourceFingerprint: seal.SourceFingerprint, SnapshotCreatedAt: seal.CreatedAt, HistoricalOnly: true}, Limit: limit, Offset: offset, Total: n, Reports: make([]trading.AccountingReport, 0, len(rows))}
	for _, row := range rows {
		var report trading.AccountingReport
		d := json.NewDecoder(strings.NewReader(row[0]))
		d.DisallowUnknownFields()
		if d.Decode(&report) != nil || report.SHA256 == "" {
			return History{}, ErrUnsafe
		}
		original := false
		for _, r := range reports {
			if r.ID == report.ID && r.SHA256 == report.SHA256 {
				verified, _ := json.Marshal(r)
				if bytes.Equal(verified, []byte(row[0])) {
					original = true
				}
				break
			}
		}
		if !original {
			return History{}, ErrUnsafe
		}
		if f.Source != "" {
			filtered := make([]trading.OwnerAccounting, 0)
			for _, owner := range report.Owners {
				if string(owner.Source) == f.Source && (f.ModelID == "" || owner.ModelID == f.ModelID) {
					filtered = append(filtered, owner)
				}
			}
			if len(filtered) == 0 {
				return History{}, ErrUnsafe
			}
			report.Owners = filtered
		}
		out.Reports = append(out.Reports, report)
	}
	return out, nil
}
