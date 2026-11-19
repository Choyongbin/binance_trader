package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func request(h http.Handler, method, path, token, origin, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, strings.NewReader(body))
	req.Host = "127.0.0.1:8080"
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-CSRF-Token", token)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}
func bootstrapToken(t *testing.T, handler http.Handler) string {
	t.Helper()
	reply := request(handler, "GET", "/api/v1/bootstrap", "", "", "")
	if reply.Code != 200 {
		t.Fatalf("bootstrap failed: %d", reply.Code)
	}
	var boot struct {
		CSRFToken      string         `json:"csrf_token"`
		TradingEnabled bool           `json:"trading_enabled"`
		Connection     string         `json:"connection"`
		Account        map[string]any `json:"account"`
	}
	if err := json.Unmarshal(reply.Body.Bytes(), &boot); err != nil {
		t.Fatal(err)
	}
	if boot.CSRFToken == "" || boot.TradingEnabled || boot.Connection != "locked" {
		t.Fatalf("unsafe boot state: %+v", boot)
	}
	if boot.Account["balances"] != nil || boot.Account["positions"] != nil {
		t.Fatal("fabricated account data")
	}
	return boot.CSRFToken
}
func TestSafetyAndPersistence(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	token := bootstrapToken(t, h)
	origin := "http://127.0.0.1:8080"
	if r := request(h, "POST", "/api/v1/settings/environment", "wrong", origin, `{"environment":"real"}`); r.Code != 403 {
		t.Fatalf("invalid csrf accepted: %d", r.Code)
	}
	if r := request(h, "POST", "/api/v1/settings/environment", token, "https://evil.example", `{"environment":"real"}`); r.Code != 403 {
		t.Fatalf("invalid origin accepted: %d", r.Code)
	}
	if r := request(h, "POST", "/api/v1/settings/environment", token, origin, `{"environment":"real"}`); r.Code != 200 {
		t.Fatalf("environment update failed %d: %s", r.Code, r.Body)
	}
	if r := request(h, "PUT", "/api/v1/models/selection", token, origin, `{"ids":["ema_rsi_macd_v1"]}`); r.Code != 423 {
		t.Fatalf("Real environment model mutation not locked: %d", r.Code)
	}
	for _, path := range []string{"/api/v1/trading/auto/start", "/api/v1/trading/auto/stop", "/api/v1/trading/manual/orders"} {
		if r := request(h, "POST", path, token, origin, `{}`); r.Code != 423 {
			t.Fatalf("unsafe order endpoint %s: %d", path, r.Code)
		}
	}
	for _, path := range []string{"/api/v1/account/balances", "/api/v1/account/positions", "/api/v1/account/orders"} {
		if r := request(h, "GET", path, "", "", ""); r.Code != 503 || !bytes.Contains(r.Body.Bytes(), []byte(`"status":"real_locked"`)) {
			t.Fatalf("unsafe account endpoint %s: %d", path, r.Code)
		}
	}
	s2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	b := request(s2.Handler(), "GET", "/api/v1/bootstrap", "", "", "")
	if !bytes.Contains(b.Body.Bytes(), []byte(`"environment":"real"`)) || !bytes.Contains(b.Body.Bytes(), []byte(`"selected_model_ids":[]`)) {
		t.Fatalf("settings not preserved: %s", b.Body)
	}
	if b.Header().Get("Content-Security-Policy") == "" || b.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("security response headers missing")
	}
}
func TestStaticWebAndHostRestriction(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	index := request(h, "GET", "/", "", "", "")
	if index.Code != 200 || !strings.Contains(index.Body.String(), "수동 거래") {
		t.Fatal("Korean UI not served")
	}
	r := httptest.NewRequest("GET", "http://malicious.example/api/v1/bootstrap", nil)
	result := httptest.NewRecorder()
	h.ServeHTTP(result, r)
	if result.Code != 403 {
		t.Fatal("DNS rebinding host not blocked")
	}
}
