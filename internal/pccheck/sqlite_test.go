package pccheck

import (
	"binance_trader/internal/sqlitebridge"
	"testing"
)

func TestSQLiteTemporaryReadWriteAndReadonly(t *testing.T) {
	if !sqlitebridge.Available() {
		t.Skip("native SQLite unavailable on this CI platform")
	}
	if err := SQLiteTempRoundTrip(); err != nil {
		t.Fatal(err)
	}
}
