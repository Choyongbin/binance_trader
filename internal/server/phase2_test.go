package server

import (
	"binance_trader/internal/exchange"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type readDemoMock struct{}

func (readDemoMock) Fetch(_ context.Context, key, secret string) (exchange.Snapshot, error) {
	if key != "demo-only-key" || secret != "demo-only-secret" {
		return exchange.Snapshot{}, context.Canceled
	}
	return exchange.Snapshot{Status: "connected", Environment: "demo", AsOf: "2026-09-27T00:00:00Z", Balances: []exchange.Balance{{Asset: "USDT", Wallet: "200", Available: "150"}}, Positions: []exchange.Position{}, Orders: []exchange.Order{}, AlgoOrders: []exchange.AlgoOrder{}}, nil
}
func TestDemoCredentialsAndRealNeverFetch(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	origin := "http://127.0.0.1:8080"
	content := `{"api_key":"demo-only-key","secret_key":"demo-only-secret","passphrase":"strong-demo-passphrase"}`
	r := request(h, "POST", "/api/v1/credentials/demo/save", token, origin, content)
	if r.Code != 200 {
		t.Fatalf("save/connect failed %d %s", r.Code, r.Body)
	}
	if strings.Contains(r.Body.String(), "demo-only-key") || strings.Contains(r.Body.String(), "demo-only-secret") {
		t.Fatal("secrets exposed in API")
	}
	activePath := filepath.Join(dir, "credentials", "demo_active.json")
	activeBefore, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatalf("atomic manifest not stored: %v", err)
	}
	var generation struct {
		Active string `json:"active"`
	}
	if err := json.Unmarshal(activeBefore, &generation); err != nil || len(generation.Active) != 32 {
		t.Fatalf("invalid atomic manifest: %v", err)
	}
	file, err := os.ReadFile(filepath.Join(dir, "credentials", "demo_generations", generation.Active, "demo_api_key.enc"))
	if err != nil || strings.Contains(string(file), "demo-only-key") {
		t.Fatal("secrets in active encrypted file")
	}
	bad := request(h, "POST", "/api/v1/credentials/demo/save", token, origin, `{"api_key":"wrong-key","secret_key":"wrong-secret","passphrase":"new-strong-passphrase"}`)
	if bad.Code != 502 {
		t.Fatalf("invalid replacement accepted: %d %s", bad.Code, bad.Body)
	}
	current, err := os.ReadFile(filepath.Join(dir, "credentials", "demo_generations", generation.Active, "demo_api_key.enc"))
	if err != nil || string(current) != string(file) {
		t.Fatal("failed credential validation replaced encrypted active key")
	}
	after, err := os.ReadFile(activePath)
	if err != nil || string(after) != string(activeBefore) {
		t.Fatal("failed credential validation committed new active manifest")
	}
	key, secret, err := s.vault.ReadDemo("strong-demo-passphrase")
	if err != nil || key != "demo-only-key" || secret != "demo-only-secret" {
		t.Fatalf("previous verified pair was lost: %v", err)
	}
	b := request(h, "GET", "/api/v1/account/balances", "", "", "")
	if b.Code != 200 || !strings.Contains(b.Body.String(), "150") {
		t.Fatal("missing authenticated balances")
	}
	r = request(h, "POST", "/api/v1/settings/environment", token, origin, `{"environment":"real"}`)
	if r.Code != 200 {
		t.Fatalf("switch failed %d", r.Code)
	}
	r = request(h, "GET", "/api/v1/account/snapshot", "", "", "")
	var snapshot exchange.Snapshot
	if err := json.Unmarshal(r.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "real_locked" || snapshot.Balances != nil {
		t.Fatal("demo balance leaked to real")
	}
	if r := request(h, "POST", "/api/v1/credentials/demo/unlock", token, origin, `{"passphrase":"strong-demo-passphrase"}`); r.Code != 423 {
		t.Fatal("real environment accepted demo unlock")
	}
	for _, path := range []string{"/api/v1/trading/manual/orders", "/api/v1/trading/auto/start"} {
		if r := request(h, "POST", path, token, origin, `{}`); r.Code != 423 {
			t.Fatal("unsafe trade path", path)
		}
	}
}
