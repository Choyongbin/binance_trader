package protectionfence

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testIntent(op Operation) Intent {
	h := strings.Repeat("a", 64)
	return Intent{Environment: "demo", GroupID: "g-demo-42", Operation: op, Symbol: "BTCUSDT",
		PositionSide: "LONG", QuantityBTC: "0.01000000",
		TargetClientAlgoID: "tp-001", PlanSHA256: h, AccountFingerprintSHA256: strings.Repeat("b", 64),
		EvidenceSHA256: strings.Repeat("c", 64)}
}
func TestSealNoReplayAcrossRestartAndAckLoss(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	in := testIntent(SubmitTP)
	if _, err := s.SealBeforePossibleSend(in, time.Now()); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.SealBeforePossibleSend(in, time.Now()); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("duplicate result %v", err)
	}
	got, err := s2.Inspect(in)
	if err != nil || got.State != state || got.Intent != in {
		t.Fatalf("recovery %+v %v", got, err)
	}
	fs, err := s2.InspectAll()
	if err != nil || len(fs) != 1 {
		t.Fatalf("scan %+v %v", fs, err)
	}
}
func TestConcurrentWorkersOnlyOneFirstFence(t *testing.T) {
	dir := t.TempDir()
	in := testIntent(SubmitSL)
	var first int32
	var replay int32
	var other int32
	var wg sync.WaitGroup
	for j := 0; j < 32; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := Open(dir)
			if err != nil {
				atomic.AddInt32(&other, 1)
				return
			}
			_, err = s.SealBeforePossibleSend(in, time.Now())
			switch {
			case err == nil:
				atomic.AddInt32(&first, 1)
			case errors.Is(err, ErrOutcomeUnknown):
				atomic.AddInt32(&replay, 1)
			case errors.Is(err, ErrCorruptFence):
				atomic.AddInt32(&other, 1) // transient incomplete writer fails closed
			default:
				atomic.AddInt32(&other, 1)
			}
		}()
	}
	wg.Wait()
	if first != 1 || replay+other != 31 {
		t.Fatalf("first=%d replay=%d failClosed=%d", first, replay, other)
	}
	s, _ := Open(dir)
	if _, err := s.Inspect(in); err != nil {
		t.Fatal(err)
	}
}
func TestTamperedOrOrphanedFenceFailClosed(t *testing.T) {
	for _, mode := range []string{"checksum", "truncated", "unexpected", "symlink", "orphan"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := Open(t.TempDir())
			in := testIntent(SubmitTP)
			if _, err := s.SealBeforePossibleSend(in, time.Now()); err != nil {
				t.Fatal(err)
			}
			path := s.name(in)
			switch mode {
			case "checksum":
				b, _ := os.ReadFile(path)
				b[len(b)-5] = 'X'
				_ = os.WriteFile(path, b, 0600)
			case "truncated":
				_ = os.WriteFile(path, []byte("{"), 0600)
			case "unexpected":
				b, _ := os.ReadFile(path)
				b = append(b, []byte("{}")...)
				_ = os.WriteFile(path, b, 0600)
			case "symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(s.dir, "elsewhere"), path); err != nil {
					t.Skip(err)
				}
			case "orphan":
				_ = os.WriteFile(filepath.Join(s.dir, "foreign.json"), []byte("{}"), 0600)
			}
			if _, err := s.InspectAll(); !errors.Is(err, ErrCorruptFence) {
				t.Fatalf("scan=%v", err)
			}
			next := testIntent(SubmitSL)
			if _, err := s.SealBeforePossibleSend(next, time.Now()); !errors.Is(err, ErrCorruptFence) {
				t.Fatalf("new write=%v", err)
			}
		})
	}
}
func TestConflictingIntentDoesNotOverwrite(t *testing.T) {
	s, _ := Open(t.TempDir())
	in := testIntent(SubmitTP)
	_, _ = s.SealBeforePossibleSend(in, time.Now())
	mismatch := in
	mismatch.QuantityBTC = "0.02"
	if _, err := s.SealBeforePossibleSend(mismatch, time.Now()); !errors.Is(err, ErrCorruptFence) {
		t.Fatal(err)
	}
	if got, err := s.Inspect(in); err != nil || got.Intent != in {
		t.Fatalf("changed %+v %v", got, err)
	}
}
func TestRejectUnsupportedOrUntrustedRequests(t *testing.T) {
	cases := []struct {
		name   string
		update func(*Intent)
	}{
		{"real account", func(i *Intent) { i.Environment = "real" }},
		{"path traversal", func(i *Intent) { i.GroupID = "../x" }},
		{"unknown operation", func(i *Intent) { i.Operation = "DELETE_ALL" }},
		{"no signed evidence digest", func(i *Intent) { i.EvidenceSHA256 = "" }},
		{"zero qty", func(i *Intent) { i.QuantityBTC = "0.00000000" }},
		{"exponent qty", func(i *Intent) { i.QuantityBTC = "1e-8" }},
		{"float precision", func(i *Intent) { i.QuantityBTC = "0.000000001" }},
		{"negative", func(i *Intent) { i.QuantityBTC = "-0.01" }},
		{"invalid account", func(i *Intent) { i.AccountFingerprintSHA256 = "SECRET" }},
		{"wrong side", func(i *Intent) { i.PositionSide = "BOTH" }},
		{"submit with exchange ID", func(i *Intent) { i.TargetAlgoID = 14 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := Open(t.TempDir())
			in := testIntent(SubmitTP)
			tc.update(&in)
			if _, err := s.SealBeforePossibleSend(in, time.Now()); err == nil {
				t.Fatal("invalid intent accepted")
			}
			fs, err := s.InspectAll()
			if err != nil || len(fs) != 0 {
				t.Fatalf("wrote invalid %+v %v", fs, err)
			}
		})
	}
	s, _ := Open(t.TempDir())
	in := testIntent(CancelSibling)
	if _, err := s.SealBeforePossibleSend(in, time.Now()); err == nil {
		t.Fatal("cancel missing target accepted")
	}
	in.TargetAlgoID = 3344
	if _, err := s.SealBeforePossibleSend(in, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SealBeforePossibleSend(in, time.Now()); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
}
func TestNeverContainsSecretOrAuthorization(t *testing.T) {
	s, _ := Open(t.TempDir())
	in := testIntent(SubmitTP)
	f, err := s.SealBeforePossibleSend(in, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if f.State != state {
		t.Fatalf("unexpected state %s", f.State)
	}
	b, err := os.ReadFile(s.name(in))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"api_key", "secret", "authorized", "approved", "TRADING_ENABLED"} {
		if strings.Contains(strings.ToLower(string(b)), forbidden) {
			t.Fatalf("unsafe persisted field %s", forbidden)
		}
	}
}

func TestAccountWideSlotBlocksDifferentOperationsAndOwners(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Intent)
	}{
		{"other leg same group", func(i *Intent) { i.Operation = SubmitSL; i.TargetClientAlgoID = "sl-001" }},
		{"different group", func(i *Intent) { i.GroupID = "another-demo-group" }},
		{"sibling cancel", func(i *Intent) { i.Operation = CancelSibling; i.TargetAlgoID = 3388 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := Open(t.TempDir())
			one := testIntent(SubmitTP)
			if _, err := s.SealBeforePossibleSend(one, time.Now()); err != nil {
				t.Fatal(err)
			}
			two := one
			tc.mutate(&two)
			if _, err := s.SealBeforePossibleSend(two, time.Now()); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("second action = %v", err)
			}
			fs, err := s.InspectAll()
			if err != nil || len(fs) != 1 || fs[0].Intent != one {
				t.Fatalf("scan %+v %v", fs, err)
			}
		})
	}
}
func TestConcurrentDifferentMutationsAccountWideSingleSlot(t *testing.T) {
	root := t.TempDir()
	var accepted int32
	var wg sync.WaitGroup
	for n := 0; n < 40; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s, err := Open(root)
			if err != nil {
				return
			}
			i := testIntent(SubmitTP)
			if n%2 != 0 {
				i.Operation = SubmitSL
				i.TargetClientAlgoID = "sl-001"
			}
			if n%3 == 0 {
				i.GroupID = "other-demo-group"
			}
			if _, err = s.SealBeforePossibleSend(i, time.Now()); err == nil {
				atomic.AddInt32(&accepted, 1)
			}
		}(n)
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("different mutations recorded as first: %d", accepted)
	}
	s, _ := Open(root)
	fs, err := s.InspectAll()
	if err != nil || len(fs) != 1 {
		t.Fatalf("after race %+v %v", fs, err)
	}
}
