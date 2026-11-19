package wallet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAllocationAndRestart(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	if _, err := s.Change("testreq00001", "ALLOCATE", "50.25", "100", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Change("testreq00002", "RETURN", "5.1", "100", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	again, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := again.Snapshot()
	if err != nil || got.AssignedUSDT != "45.15" || got.Revision != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	for _, v := range []struct{ id, op, amt, avail string }{{"testreq00001", "ALLOCATE", "50.25", "100"}, {"testreq00003", "ALLOCATE", "56", "100"}, {"testreq00004", "RETURN", "100", "100"}} {
		if _, err = again.Change(v.id, v.op, v.amt, v.avail, strings.Repeat("a", 64)); err == nil {
			t.Fatalf("unsafe %+v", v)
		}
	}
	got, _ = again.Snapshot()
	if got.AssignedUSDT != "45.15" {
		t.Fatal(got)
	}
}
func TestCorruptAndInterruptedLock(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	if err := os.Mkdir(filepath.Join(dir, "wallet_demo", "mutation.active"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Change("testreq00001", "ALLOCATE", "1", "100", strings.Repeat("a", 64)); err == nil {
		t.Fatal("allowed interrupted state")
	}
	os.Remove(filepath.Join(dir, "wallet_demo", "mutation.active"))
	if _, err := s.Change("testreq00001", "ALLOCATE", "1", "100", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "wallet_demo", "events", "00000000000000000001.json")
	if err := os.WriteFile(file, []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); err == nil {
		t.Fatal("tampered ledger accepted")
	}
}
func TestDecimal(t *testing.T) {
	for _, bad := range []string{"-1", "01", "1.123456789", "0", "1e4", "NaN"} {
		if _, err := Decimal(bad, false); err == nil {
			t.Fatal(bad)
		}
	}
}

func TestDrawdownAllowsSufficientReturnAndRejectsInsufficientReturn(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fp := strings.Repeat("a", 64)
	if _, err = s.Change("drawdown0001", "ALLOCATE", "100", "100", fp); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Change("drawdown0002", "RETURN", "40", "50", fp); err == nil {
		t.Fatal("returned too little to get under real account available")
	}
	if _, err = s.Change("drawdown0003", "RETURN", "60", "50", fp); err != nil {
		t.Fatal(err)
	}
	state, err := s.SnapshotFor(fp)
	if err != nil || !state.Verified || state.AssignedUSDT != "40" {
		t.Fatalf("bad downscaled wallet %+v %v", state, err)
	}
	wrong, err := s.SnapshotFor(strings.Repeat("b", 64))
	if err != nil || wrong.Verified {
		t.Fatal("unrelated Demo API key gained wallet ownership")
	}
}
