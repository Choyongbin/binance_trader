//go:build windows

package sqlitebridge

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsForeignSQLiteTextAndErrorBuffer(t *testing.T) {
	if !Available() {
		t.Fatal("built-in WinSQLite DLL unavailable")
	}
	db, err := Open(filepath.Join(t.TempDir(), "한글.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// NULL is represented as an empty string by the existing adapter contract.
	if err := db.Exec("CREATE TABLE data (a TEXT, b TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := strings.Repeat("한글✓", 1024)
	if err := db.Exec("INSERT INTO data(a,b) VALUES (?,?)", data, "A"); err != nil {
		t.Fatal(err)
	}
	got, err := db.Query("SELECT a,b, NULL FROM data")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0]) != 3 || got[0][0] != data || got[0][1] != "A" || got[0][2] != "" {
		t.Fatalf("unexpected native SQLite text copy: rows=%d err=%v", len(got), err)
	}
	// Exercise native sqlite3_errmsg buffer as well as column_text buffer.
	err = db.Exec("INSERT INTO missing_table (a) VALUES (?)", "x")
	if err == nil || !strings.Contains(err.Error(), "missing_table") {
		t.Fatalf("native SQLite error text not copied: %v", err)
	}
}
