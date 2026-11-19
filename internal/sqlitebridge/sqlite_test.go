package sqlitebridge

import (
	"path/filepath"
	"testing"
)

func TestNativeSQLitePreparedStatementsAndReadOnly(t *testing.T) {
	if !Available() {
		t.Skip("SQLite 네이티브 드라이버를 사용할 수 없는 빌드입니다")
	}
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	db, e := Open(path, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, sql := range []string{"PRAGMA journal_mode=DELETE", "PRAGMA synchronous=FULL"} {
		if sql == "PRAGMA journal_mode=DELETE" {
			r, e := db.Query(sql)
			if e != nil || len(r) != 1 || r[0][0] != "delete" {
				t.Fatalf("journal mode %#v %v", r, e)
			}
		} else if e := db.Exec(sql); e != nil {
			t.Fatal(e)
		}
	}
	if e = db.Exec("CREATE TABLE demo (id TEXT PRIMARY KEY, amount TEXT NOT NULL)"); e != nil {
		t.Fatal(e)
	}
	if e = db.Exec("INSERT INTO demo (id,amount) VALUES (?,?)", "모델'; DELETE FROM demo;--", "0.00000001"); e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	ro, e := Open(path, true)
	if e != nil {
		t.Fatal(e)
	}
	defer ro.Close()
	rows, e := ro.Query("SELECT id,amount FROM demo WHERE id=?", "모델'; DELETE FROM demo;--")
	if e != nil || len(rows) != 1 || rows[0][0] != "모델'; DELETE FROM demo;--" || rows[0][1] != "0.00000001" {
		t.Fatalf("prepared UTF8/decimal broken: %#v %v", rows, e)
	}
	if e = ro.Exec("INSERT INTO demo (id,amount) VALUES (?,?)", "bad", "1"); e == nil {
		t.Fatal("readonly database was written")
	}
}
