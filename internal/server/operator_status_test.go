package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperatorUnifiedStatusReadOnlyAndNeverAuthorizesTrading(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := request(s.Handler(), "GET", "/api/v1/trading/operator-status", "", "", "")
	if r.Code != 200 {
		t.Fatalf("status=%d body=%s", r.Code, r.Body.String())
	}
	body := r.Body.String()
	for _, want := range []string{`"state":"EMPTY_HISTORICAL_ONLY"`, `"trading_authorized":false`, `"stream_continuity_proven":false`, `"manual_review_required":true`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
	if w := request(s.Handler(), "POST", "/api/v1/trading/manual/orders", "", "", `{}`); w.Code != 403 && w.Code != 423 {
		t.Fatalf("manual unexpectedly unlocked %d", w.Code)
	}
}

func TestOperatorUnifiedStatusRejectsQuery(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if r := request(s.Handler(), "GET", "/api/v1/trading/operator-status?x=1", "", "", ""); r.Code != 400 {
		t.Fatalf("query status=%d", r.Code)
	}
}

func TestOperatorUnifiedStatusFailsClosedOnCorruptEvidence(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := filepath.Join(dir, "trading", "integrated_reviews")
	if err := os.MkdirAll(p, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, "bad.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	r := request(s.Handler(), "GET", "/api/v1/trading/operator-status", "", "", "")
	if r.Code != 503 {
		t.Fatalf("expected 503, got %d %s", r.Code, r.Body.String())
	}
}
