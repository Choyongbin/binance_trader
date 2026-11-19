//go:build linux && cgo

// Package sqlitebridge provides a minimal, parameterized SQLite bridge.
// On Linux tests it uses the installed libsqlite3; on Windows it uses
// winsqlite3.dll already shipped with supported Windows 10/11 systems.
// It is deliberately not a general-purpose driver or an order state store.
package sqlitebridge

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>
static int bind_copy(sqlite3_stmt *s, int idx, const char *p, int n) {
    return sqlite3_bind_text(s, idx, p, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

func Available() bool { return true }

type DB struct {
	mu sync.Mutex
	db *C.sqlite3
}

func Open(path string, readonly bool) (*DB, error) {
	s := C.CString(path)
	defer C.free(unsafe.Pointer(s))
	flags := C.int(C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE)
	if readonly {
		flags = C.SQLITE_OPEN_READONLY
	}
	var h *C.sqlite3
	rc := C.sqlite3_open_v2(s, &h, flags, nil)
	if rc != C.SQLITE_OK {
		var msg string
		if h != nil {
			msg = C.GoString(C.sqlite3_errmsg(h))
			C.sqlite3_close(h)
		}
		return nil, fmt.Errorf("SQLite 열기 실패 (code=%d): %s", int(rc), msg)
	}
	C.sqlite3_busy_timeout(h, 2000)
	return &DB{db: h}, nil
}

func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return nil
	}
	rc := C.sqlite3_close(d.db)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("SQLite 종료 실패 (code=%d)", int(rc))
	}
	d.db = nil
	return nil
}

func (d *DB) prepare(sql string, args []string) (*C.sqlite3_stmt, error) {
	if d.db == nil {
		return nil, errors.New("SQLite 연결이 종료되었습니다")
	}
	p := C.CString(sql)
	defer C.free(unsafe.Pointer(p))
	var stmt *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(d.db, p, -1, &stmt, nil)
	if rc != C.SQLITE_OK {
		return nil, fmt.Errorf("SQLite 준비 실패 (code=%d): %s", int(rc), C.GoString(C.sqlite3_errmsg(d.db)))
	}
	if stmt == nil {
		return nil, errors.New("SQLite 빈 SQL이 허용되지 않습니다")
	}
	if int(C.sqlite3_bind_parameter_count(stmt)) != len(args) {
		C.sqlite3_finalize(stmt)
		return nil, errors.New("SQLite 파라미터 수 불일치")
	}
	for idx, arg := range args {
		b := []byte(arg)
		var ptr *C.char
		if len(b) > 0 {
			ptr = (*C.char)(unsafe.Pointer(&b[0]))
		} else {
			ptr = C.CString("")
			defer C.free(unsafe.Pointer(ptr))
		}
		rc = C.bind_copy(stmt, C.int(idx+1), ptr, C.int(len(b)))
		if rc != C.SQLITE_OK {
			C.sqlite3_finalize(stmt)
			return nil, fmt.Errorf("SQLite 바인딩 오류: %s", C.GoString(C.sqlite3_errmsg(d.db)))
		}
	}
	return stmt, nil
}

func (d *DB) Exec(sql string, args ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.prepare(sql, args)
	if err != nil {
		return err
	}
	defer C.sqlite3_finalize(st)
	if rc := C.sqlite3_step(st); rc != C.SQLITE_DONE {
		return fmt.Errorf("SQLite 실행 실패 (code=%d): %s", int(rc), C.GoString(C.sqlite3_errmsg(d.db)))
	}
	return nil
}

// Query returns SQLite TEXT values; SQL NULL is represented by an empty string.
// Only our strict, private projection schema uses this interface.
func (d *DB) Query(sql string, args ...string) ([][]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.prepare(sql, args)
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(st)
	res := [][]string{}
	for {
		rc := C.sqlite3_step(st)
		if rc == C.SQLITE_DONE {
			return res, nil
		}
		if rc != C.SQLITE_ROW {
			return nil, fmt.Errorf("SQLite 쿼리 실패 (code=%d): %s", int(rc), C.GoString(C.sqlite3_errmsg(d.db)))
		}
		count := int(C.sqlite3_column_count(st))
		row := make([]string, count)
		for i := 0; i < count; i++ {
			if C.sqlite3_column_type(st, C.int(i)) == C.SQLITE_NULL {
				continue
			}
			p := C.sqlite3_column_text(st, C.int(i))
			n := C.sqlite3_column_bytes(st, C.int(i))
			if p == nil && n > 0 {
				return nil, errors.New("SQLite 값 포인터 오류")
			}
			row[i] = C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
		}
		res = append(res, row)
		if len(res) > 100001 {
			return nil, errors.New("SQLite 조회 결과 상한 초과")
		}
	}
}
