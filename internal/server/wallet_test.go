package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binance_trader/internal/exchange"
)

type currentWalletMock struct{}

func (currentWalletMock) Fetch(_ context.Context, key, secret string) (exchange.Snapshot, error) {
	if secret != "demo-secret" || (key != "key-one" && key != "key-two") {
		return exchange.Snapshot{}, context.Canceled
	}
	return exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: time.Now().UTC().Format(time.RFC3339Nano), Balances: []exchange.Balance{{Asset: "USDT", Wallet: "200", Available: "150"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}, nil
}
func TestDemoAutoWalletCSRFAccountBindingAndRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	token := bootstrapToken(t, s.Handler())
	if got := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", token, "http://127.0.0.1:8080", `{"request_id":"req0000001","operation":"ALLOCATE","amount_usdt":"100","passphrase":"wallet-secure-password"}`); got.Code != 423 {
		t.Fatalf("allowed unauthenticated budget: %d", got.Code)
	}
	if err = s.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	const pass = "wallet-secure-password"
	if err := s.vault.SaveDemo("key-one", "demo-secret", pass); err != nil {
		t.Fatal(err)
	}
	s.accountingReader = fakeAccountingWindow(func(context.Context, string, string, int64) (exchange.RecoveryBaseline, error) {
		return walletSignedBaseline(t), nil
	})
	if got := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", "invalid", "http://127.0.0.1:8080", `{"request_id":"req0000001","operation":"ALLOCATE","amount_usdt":"100","passphrase":"wallet-secure-password"}`); got.Code != 403 {
		t.Fatal("bad csrf accepted")
	}
	if got := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", token, "http://127.0.0.1:8080", `{"request_id":"req0000001","operation":"ALLOCATE","amount_usdt":"100","passphrase":"wallet-secure-password"}`); got.Code != 200 || !strings.Contains(got.Body.String(), `"assigned_usdt":"100"`) {
		t.Fatalf("initial allocation: %d %s", got.Code, got.Body)
	}
	if got := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", token, "http://127.0.0.1:8080", `{"request_id":"req0000001","operation":"ALLOCATE","amount_usdt":"100","passphrase":"wallet-secure-password"}`); got.Code != 409 {
		t.Fatal("duplicate mutation executed")
	}
	if got := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", token, "http://127.0.0.1:8080", `{"request_id":"req0000002","operation":"ALLOCATE","amount_usdt":"51","passphrase":"wallet-secure-password"}`); got.Code != 409 {
		t.Fatal("excess budget accepted")
	}
	if got := request(s.Handler(), "POST", "/api/v1/wallet/auto/change", token, "http://127.0.0.1:8080", `{"request_id":"req0000003","operation":"RETURN","amount_usdt":"20","passphrase":"wallet-secure-password"}`); got.Code != 200 || !strings.Contains(got.Body.String(), `"assigned_usdt":"80"`) {
		t.Fatalf("return failed %d %s", got.Code, got.Body)
	}
	r := request(s.Handler(), "GET", "/api/v1/wallet/auto", "", "", "")
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body)
	}
	var obj struct {
		Wallet struct {
			AssignedUSDT string `json:"assigned_usdt"`
			Verified     bool   `json:"verified"`
		} `json:"wallet"`
		CanAdjust bool `json:"can_adjust"`
	}
	if err = json.Unmarshal(r.Body.Bytes(), &obj); err != nil || obj.Wallet.AssignedUSDT != "80" || !obj.Wallet.Verified || !obj.CanAdjust {
		t.Fatalf("wallet state %+v %v", obj, err)
	}
	s.Close()
	again, err := newWithFetcher(dir, currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if err = again.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	r = request(again.Handler(), "GET", "/api/v1/wallet/auto", "", "", "")
	if !strings.Contains(r.Body.String(), `"assigned_usdt":"80"`) {
		t.Fatalf("budget not persisted %s", r.Body)
	}
	again.demo.Clear()
	if err = again.demo.Connect(context.Background(), "key-two", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	r = request(again.Handler(), "GET", "/api/v1/wallet/auto", "", "", "")
	if !strings.Contains(r.Body.String(), `"verified":false`) {
		t.Fatalf("wallet cross-account binding failed %s", r.Body)
	}
	token = bootstrapTokenForConnected(t, again.Handler())
	r = request(again.Handler(), "POST", "/api/v1/wallet/auto/change", token, "http://127.0.0.1:8080", `{"request_id":"req0000004","operation":"ALLOCATE","amount_usdt":"1","passphrase":"wallet-secure-password"}`)
	if r.Code != 423 {
		t.Fatalf("cross-account budget mutation allowed: %d %s", r.Code, r.Body)
	}
}
func bootstrapTokenForConnected(t *testing.T, h http.Handler) string {
	t.Helper()
	r := request(h, "GET", "/api/v1/bootstrap", "", "", "")
	var b struct {
		Token string `json:"csrf_token"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &b); err != nil || b.Token == "" {
		t.Fatalf("token %v %s", err, r.Body)
	}
	return b.Token
}

// The GUI must not allow an allocation edit while there are local orders,
// partial fills or pending protection/cancel intents. The signed displayed
// BTC balance alone is not full account reconciliation.
func TestAutoWalletChangeBlocksWhenLocalTradingEvidenceExists(t *testing.T) {
	dir := t.TempDir()
	srv, err := newWithFetcher(dir, currentWalletMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err = srv.demo.Connect(context.Background(), "key-one", "demo-secret"); err != nil {
		t.Fatal(err)
	}
	csrf := bootstrapTokenForConnected(t, srv.Handler())
	loc := filepath.Join(dir, "trading", "manual_cancels")
	if err = os.MkdirAll(loc, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(loc, "unfinished.attempt.json"), []byte(`{"uncertain":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	r := request(srv.Handler(), "GET", "/api/v1/wallet/auto", "", "", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"can_adjust":false`) {
		t.Fatalf("unsafe wallet action buttons: %d %s", r.Code, r.Body)
	}
	r = request(srv.Handler(), "POST", "/api/v1/wallet/auto/change", csrf, "http://127.0.0.1:8080",
		`{"request_id":"test-inflight-01","operation":"ALLOCATE","amount_usdt":"10"}`)
	if r.Code != 423 {
		t.Fatalf("unsafe wallet mutation accepted %d %s", r.Code, r.Body)
	}
	status, err := srv.wallet.SnapshotFor(srv.demo.KeyFingerprint())
	if err != nil || status.AssignedUSDT != "0" {
		t.Fatalf("wallet changed despite local exposure %+v %v", status, err)
	}
}
