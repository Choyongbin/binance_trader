//go:build windows

package sqlitebridge

// WinSQLite is supplied by Windows 10/11. Do not ship foreign DLLs or use CGO.
// All SQL statements originate from fixed source templates and values use
// transient-copy sqlite3_bind_text, never string interpolation.
import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"unsafe"
)

var dll = syscall.NewLazyDLL(filepath.Join(os.Getenv("SystemRoot"), "System32", "winsqlite3.dll"))

// SQLite owns returned TEXT/error message memory. Do not turn a raw uintptr
// from a DLL callback into a Go unsafe.Pointer: its lifetime is controlled
// by SQLite, and such conversions correctly trigger go vet's unsafeptr check.
// Copy foreign bytes while the SQLite statement/connection is still valid.
var kernel32 = syscall.NewLazyDLL("kernel32.dll")
var (
	pReadProcessMemory = kernel32.NewProc("ReadProcessMemory")
	pLstrlenA          = kernel32.NewProc("lstrlenA")
)
var (
	pOpen       = dll.NewProc("sqlite3_open_v2")
	pClose      = dll.NewProc("sqlite3_close")
	pErr        = dll.NewProc("sqlite3_errmsg")
	pBusy       = dll.NewProc("sqlite3_busy_timeout")
	pPrep       = dll.NewProc("sqlite3_prepare_v2")
	pParamCount = dll.NewProc("sqlite3_bind_parameter_count")
	pBind       = dll.NewProc("sqlite3_bind_text")
	pStep       = dll.NewProc("sqlite3_step")
	pFinalize   = dll.NewProc("sqlite3_finalize")
	pColCount   = dll.NewProc("sqlite3_column_count")
	pColText    = dll.NewProc("sqlite3_column_text")
	pColBytes   = dll.NewProc("sqlite3_column_bytes")
	pColType    = dll.NewProc("sqlite3_column_type")
)

func Available() bool { return filepath.IsAbs(os.Getenv("SystemRoot")) && dll.Load() == nil }

type DB struct {
	mu     sync.Mutex
	handle uintptr
}

func cbytes(s string) []byte { b := append([]byte(s), 0); return b }

// copyNativeBytes copies an SQLite-owned region into Go memory through the
// documented Windows ReadProcessMemory API, using a current-process pseudo
// handle. Unlike uintptr -> unsafe.Pointer casting, this neither manufactures
// a Go pointer to native memory nor assumes native memory is Go-heap managed.
// The caller must hold the SQLite connection lock and must not advance or
// finalize the SQLite statement before this function returns.
func copyNativeBytes(source uintptr, length uintptr) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if source == 0 {
		return nil, errors.New("SQLite native pointer is null")
	}
	// The indexer only reads text metadata; reject pathological sizes rather
	// than allocating arbitrary memory from a corrupt/native length.
	const maxNativeTextBytes = 64 << 20
	if length > maxNativeTextBytes {
		return nil, fmt.Errorf("SQLite native text exceeds %d bytes", maxNativeTextBytes)
	}
	b := make([]byte, int(length))
	self, err := syscall.GetCurrentProcess()
	if err != nil {
		return nil, fmt.Errorf("GetCurrentProcess: %w", err)
	}
	var copied uintptr
	ok, _, callErr := pReadProcessMemory.Call(
		uintptr(self), source, uintptr(unsafe.Pointer(&b[0])),
		length, uintptr(unsafe.Pointer(&copied)),
	)
	runtime.KeepAlive(b)
	if ok == 0 || copied != length {
		return nil, fmt.Errorf("SQLite native buffer copy failed (read %d/%d bytes): %v", copied, length, callErr)
	}
	return b, nil
}

func errmsg(handle uintptr) string {
	p, _, _ := pErr.Call(handle)
	if p == 0 {
		return "unknown"
	}
	// sqlite3_errmsg returns a valid NUL-terminated native UTF-8 buffer while
	// the DB handle is alive. lstrlenA counts bytes until NUL (no conversion).
	n, _, _ := pLstrlenA.Call(p)
	if n > 1024 {
		n = 1024
	}
	if n == 0 {
		return "unknown"
	}
	b, err := copyNativeBytes(p, n)
	if err != nil {
		return fmt.Sprintf("SQLite error text unreadable: %v", err)
	}
	return string(b)
}

func Open(path string, readonly bool) (*DB, error) {
	if !filepath.IsAbs(os.Getenv("SystemRoot")) {
		return nil, errors.New("Windows SystemRoot 환경 경로 확인 실패")
	}
	if err := dll.Load(); err != nil {
		return nil, fmt.Errorf("Windows 내장 winsqlite3.dll을 사용할 수 없습니다: %w", err)
	}
	p := cbytes(path)
	flags := uintptr(0x6) // SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
	if readonly {
		flags = 1
	}
	var handle uintptr
	rc, _, _ := pOpen.Call(uintptr(unsafe.Pointer(&p[0])), uintptr(unsafe.Pointer(&handle)), flags, 0)
	runtime.KeepAlive(p)
	if rc != 0 {
		msg := errmsg(handle)
		if handle != 0 {
			pClose.Call(handle)
		}
		return nil, fmt.Errorf("SQLite 열기 실패 (code=%d): %s", rc, msg)
	}
	pBusy.Call(handle, 2000)
	return &DB{handle: handle}, nil
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.handle == 0 {
		return nil
	}
	rc, _, _ := pClose.Call(d.handle)
	if rc != 0 {
		return fmt.Errorf("SQLite 종료 실패 (code=%d)", rc)
	}
	d.handle = 0
	return nil
}
func (d *DB) prepare(sql string, args []string) (uintptr, error) {
	if d.handle == 0 {
		return 0, errors.New("SQLite 연결이 종료되었습니다")
	}
	b := cbytes(sql)
	var st uintptr
	rc, _, _ := pPrep.Call(d.handle, uintptr(unsafe.Pointer(&b[0])), ^uintptr(0), uintptr(unsafe.Pointer(&st)), 0)
	runtime.KeepAlive(b)
	if rc != 0 {
		return 0, fmt.Errorf("SQLite 준비 실패 (code=%d): %s", rc, errmsg(d.handle))
	}
	if st == 0 {
		return 0, errors.New("SQLite 빈 SQL은 허용되지 않습니다")
	}
	n, _, _ := pParamCount.Call(st)
	if int(n) != len(args) {
		pFinalize.Call(st)
		return 0, errors.New("SQLite 파라미터 수 불일치")
	}
	for i, arg := range args {
		v := cbytes(arg)
		// SQLITE_TRANSIENT(-1) guarantees SQLite copies before Call returns.
		rc, _, _ := pBind.Call(st, uintptr(i+1), uintptr(unsafe.Pointer(&v[0])), uintptr(len(v)-1), ^uintptr(0))
		runtime.KeepAlive(v)
		if rc != 0 {
			pFinalize.Call(st)
			return 0, fmt.Errorf("SQLite 바인딩 실패 (code=%d): %s", rc, errmsg(d.handle))
		}
	}
	return st, nil
}
func (d *DB) Exec(sql string, args ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.prepare(sql, args)
	if err != nil {
		return err
	}
	defer pFinalize.Call(st)
	rc, _, _ := pStep.Call(st)
	if rc != 101 {
		return fmt.Errorf("SQLite 실행 실패 (code=%d): %s", rc, errmsg(d.handle))
	}
	return nil
}
func (d *DB) Query(sql string, args ...string) ([][]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.prepare(sql, args)
	if err != nil {
		return nil, err
	}
	defer pFinalize.Call(st)
	out := [][]string{}
	for {
		rc, _, _ := pStep.Call(st)
		if rc == 101 {
			return out, nil
		}
		if rc != 100 {
			return nil, fmt.Errorf("SQLite 쿼리 실패 (code=%d): %s", rc, errmsg(d.handle))
		}
		n, _, _ := pColCount.Call(st)
		row := make([]string, int(n))
		for i := range row {
			typ, _, _ := pColType.Call(st, uintptr(i))
			if typ == 5 {
				continue
			}
			ptr, _, _ := pColText.Call(st, uintptr(i))
			size, _, _ := pColBytes.Call(st, uintptr(i))
			if ptr == 0 && size > 0 {
				return nil, errors.New("SQLite 값 포인터 오류")
			}
			if size > 0 {
				value, copyErr := copyNativeBytes(ptr, size)
				if copyErr != nil {
					return nil, copyErr
				}
				row[i] = string(value)
			}
		}
		out = append(out, row)
		if len(out) > 100001 {
			return nil, errors.New("SQLite 조회 결과 상한 초과")
		}
	}
}
