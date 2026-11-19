package main

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"binance_trader/internal/exchange"
	"binance_trader/internal/trading"
)

func TestDemoBudgetCapFailClosed(t *testing.T) {
	for _, s := range []string{"", "0", "60.00000001", "100", "-1", "oops", "1e3"} {
		if enforceBudgetCap(s) == nil {
			t.Fatalf("unsafe budget %q accepted", s)
		}
	}
	for _, s := range []string{"0.00000001", "25", "60.00000000"} {
		if err := enforceBudgetCap(s); err != nil {
			t.Fatalf("valid budget %q rejected %v", s, err)
		}
	}
}

func TestEncParserStrictNoFallback(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "demo.enc")
	if err := os.WriteFile(file, []byte("FAKE_API_KEY_1234\r\nFAKE_SECRET_KEY_5678\r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readTwoLineCredentials(file)
	if err != nil || got.APIKey != "FAKE_API_KEY_1234" || got.Secret != "FAKE_SECRET_KEY_5678" {
		t.Fatalf("valid two-line fixture rejected: %v", err)
	}
	resolved, err := findCredentialFile(root)
	if err != nil || resolved != file {
		t.Fatalf("file discovery failed %v", err)
	}
	for _, bad := range []string{"key\n", "apikey12345\nsecret12345\nextra", " apikey12345\nsecret12345", "apikey12345\nsecret12345\x00"} {
		if err := os.WriteFile(file, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := readTwoLineCredentials(file)
		if err == nil {
			t.Fatalf("unsafe enc contents were accepted %q", bad)
		}
		if strings.Contains(err.Error(), "apikey12345") {
			t.Fatal("credential leaked in error")
		}
	}
	if err := os.WriteFile(file, []byte("FAKE_API_KEY_1234\nFAKE_SECRET_KEY_5678"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "second.enc"), []byte("other"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := findCredentialFile(root); err == nil {
		t.Fatal("ambiguous credential file silently selected")
	}
}

func TestFlatSignedDemoPreflightRequired(t *testing.T) {
	s := exchange.ExecutionPreflightSnapshot{
		Environment: "demo", FinishedAt: time.Now().UTC(),
		Portfolio:  exchange.AccountPortfolio{CanTrade: true, Assets: []exchange.PortfolioAsset{}, Positions: []exchange.PortfolioPosition{}},
		OpenOrders: []exchange.Order{}, OpenAlgos: []exchange.AlgoOrder{},
		Configuration: exchange.TradingConfiguration{Environment: "demo", Symbol: "BTCUSDT", SettingsMatchV1: true, HedgeMode: true, MarginType: "ISOLATED", Leverage: 15},
	}
	if err := checkFlatDemo(s); err != nil {
		t.Fatalf("valid signed test fixture rejected: %v", err)
	}
	s.OpenOrders = append(s.OpenOrders, exchange.Order{ClientOrderID: "other-owner"})
	if err := checkFlatDemo(s); err == nil {
		t.Fatal("external open order accepted")
	}
	s.OpenOrders = []exchange.Order{}
	s.Portfolio.Positions = nil
	if err := checkFlatDemo(s); err == nil {
		t.Fatal("missing positions collection accepted")
	}
}

type fakeOneShot struct{ calls int }

func (r *fakeOneShot) RunOnce(context.Context) (trading.AutoExecutionResult, error) {
	r.calls++
	return trading.AutoExecutionResult{ClientOrderID: "demo-only-fixture", Dispatched: true}, nil
}
func (r *fakeOneShot) Close() error { return nil }
func TestSingleDispatchBlocksFurtherOrders(t *testing.T) {
	mock := &fakeOneShot{}
	recoveryCalls := 0
	run := &singleDispatch{inner: mock, signedRecovery: func(_ context.Context, id string) (trading.ExecutionRecoveryResult, error) {
		recoveryCalls++
		if id != "demo-only-fixture" {
			t.Fatalf("wrong ID %s", id)
		}
		return trading.ExecutionRecoveryResult{FinalStage: trading.ExecutionSettled}, nil
	}}
	first, err := run.RunOnce(context.Background())
	if !errors.Is(err, trading.ErrSupervisedSingleOrderComplete) || !first.Dispatched || !run.seen || first.Lifecycle != trading.ExecutionSettled {
		t.Fatalf("one order must halt: %+v %v", first, err)
	}
	_, err = run.RunOnce(context.Background())
	if err == nil || mock.calls != 1 || recoveryCalls != 1 {
		t.Fatal("duplicate order callback/recovery called")
	}
}

func TestEncParserWindowsPowerShellEncodings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "demo.enc")
	content := "FAKE_API_KEY_1234\r\nFAKE_SECRET_KEY_5678\r\n"
	utf16Data := []byte{0xff, 0xfe}
	for _, unit := range utf16.Encode([]rune(content)) {
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, unit)
		utf16Data = append(utf16Data, b...)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{"utf16-le-bom", utf16Data},
		{"utf8-bom", append([]byte{0xef, 0xbb, 0xbf}, []byte(content)...)},
		{"invalid-utf16", []byte{0xff, 0xfe, 0x41}},
		{"utf16-with-extra-line", append(append([]byte{}, utf16Data...), []byte{0x78, 0, 0x0a, 0}...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.data, 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readTwoLineCredentials(path)
			if strings.HasPrefix(tc.name, "invalid") || tc.name == "utf16-with-extra-line" {
				if err == nil {
					t.Fatal("invalid credentials accepted")
				}
				return
			}
			if err != nil || got.APIKey != "FAKE_API_KEY_1234" || got.Secret != "FAKE_SECRET_KEY_5678" {
				t.Fatalf("encoded fixture rejected: %v", err)
			}
		})
	}
}

func TestInfeasibleDemoNotionalDetectedBeforeOrder(t *testing.T) {
	if err := checkNotionalFeasibility("60", "100"); err == nil {
		t.Fatal("100 USDT min must block <=90 optimistic max")
	}
	if err := checkNotionalFeasibility("60", "80"); err != nil {
		t.Fatal(err)
	}
	if err := checkNotionalFeasibility("wrong", "100"); err == nil {
		t.Fatal("bad wallet accepted")
	}
	if err := checkNotionalFeasibility("60", "wrong"); err == nil {
		t.Fatal("bad exchange rule accepted")
	}
}

func TestPCDemoExecutionAuditEmptyAndSettledAllowedOnly(t *testing.T) {
	// EMPTY was wrongly rejected in dev.3.54 despite a clean signed account
	// and the shared readiness engine already accepting EMPTY.
	for _, tc := range []struct {
		name string
		a    trading.ExecutionLifecycleAudit
		want bool
	}{
		{"empty", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditEmpty}, true},
		{"settled", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditClear, Total: 1, Settled: 1, Items: []trading.ExecutionLifecycleAuditItem{{Stage: trading.ExecutionSettled}}}, true},
		{"pending", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditPending, Total: 1, Pending: 1, Items: []trading.ExecutionLifecycleAuditItem{{Stage: trading.ExecutionMayHaveSent}}}, false},
		{"blocked", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditBlocked}, false},
		{"unknown", trading.ExecutionLifecycleAudit{State: "UNEXPECTED"}, false},
		{"empty-with-rows", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditEmpty, Total: 1, Items: []trading.ExecutionLifecycleAuditItem{{Stage: trading.ExecutionSettled}}}, false},
		{"settled-missing-rows", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditClear, Total: 1, Settled: 1}, false},
		{"settled-with-pending-row", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditClear, Total: 1, Settled: 1, Items: []trading.ExecutionLifecycleAuditItem{{Stage: trading.ExecutionMayHaveSent}}}, false},
		{"false-trading-authorized", trading.ExecutionLifecycleAudit{State: trading.ExecutionAuditEmpty, TradingAuthorized: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := requireCleanExecutionAudit(tc.a)
			if (err == nil) != tc.want {
				t.Fatalf("allow=%v want=%v err=%v", err == nil, tc.want, err)
			}
		})
	}
}

func TestPCDemoEmptyAuditFromRealEmptyDisk(t *testing.T) {
	root := t.TempDir()
	for _, createDir := range []bool{false, true} {
		if createDir {
			if err := os.MkdirAll(filepath.Join(root, "trading", "execution_lifecycle"), 0700); err != nil {
				t.Fatal(err)
			}
		}
		audit, err := trading.ReviewExecutionLifecycles(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := requireCleanExecutionAudit(audit); err != nil {
			t.Fatalf("empty on disk rejected: %v", err)
		}
	}
}

func TestSingleDispatchSignedRecoveryFailureDoesNotReplay(t *testing.T) {
	mock := &fakeOneShot{}
	run := &singleDispatch{inner: mock, signedRecovery: func(_ context.Context, _ string) (trading.ExecutionRecoveryResult, error) {
		return trading.ExecutionRecoveryResult{FinalStage: trading.ExecutionMayHaveSent}, errors.New("injected signed GET outage")
	}}
	result, err := run.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "signed lifecycle recovery blocked") || !result.Dispatched || mock.calls != 1 {
		t.Fatalf("missing signed failure fence: result=%+v err=%v calls=%d", result, err, mock.calls)
	}
	if _, err = run.RunOnce(context.Background()); err == nil || mock.calls != 1 {
		t.Fatal("unsafe replay on signed outage")
	}
}
func TestSingleDispatchSignedPendingDoesNotReplay(t *testing.T) {
	mock := &fakeOneShot{}
	run := &singleDispatch{inner: mock, signedRecovery: func(_ context.Context, _ string) (trading.ExecutionRecoveryResult, error) {
		return trading.ExecutionRecoveryResult{FinalStage: trading.ExecutionSignedObserved, PendingReason: "not terminal"}, nil
	}}
	result, err := run.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "still pending") || !result.Dispatched || mock.calls != 1 {
		t.Fatalf("pending incorrectly settled: %+v %v", result, err)
	}
	if _, err = run.RunOnce(context.Background()); err == nil || mock.calls != 1 {
		t.Fatal("unsafe replay on pending order")
	}
}
