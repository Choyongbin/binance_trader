// Package pccheck tests the actual platform SQLite adapter using exclusively
// temporary data. This check never opens an existing user database.
package pccheck

import (
	"errors"
	"os"
	"path/filepath"

	"binance_trader/internal/sqlitebridge"
)

func SQLiteTempRoundTrip() error {
	if !sqlitebridge.Available() {
		return errors.New("이 PC에서 네이티브 SQLite DLL/드라이버를 사용할 수 없습니다")
	}
	dir, err := os.MkdirTemp("", "binance-sqlite-smoke-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	p := filepath.Join(dir, "한글-path-check.sqlite")
	db, err := sqlitebridge.Open(p, false)
	if err != nil {
		return err
	}
	if err := db.Exec("CREATE TABLE smoke(id TEXT PRIMARY KEY, txt TEXT NOT NULL)"); err != nil {
		db.Close()
		return err
	}
	if err := db.Exec("INSERT INTO smoke(id, txt) VALUES (?, ?)", "one", "한글✓'\"; DROP TABLE smoke; --"); err != nil {
		db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	readonly, err := sqlitebridge.Open(p, true)
	if err != nil {
		return err
	}
	defer readonly.Close()
	rows, err := readonly.Query("SELECT txt FROM smoke WHERE id=?", "one")
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "한글✓'\"; DROP TABLE smoke; --" {
		return errors.New("SQLite bind/UTF-8/readonly 조회 결과가 일치하지 않습니다")
	}
	return nil
}
