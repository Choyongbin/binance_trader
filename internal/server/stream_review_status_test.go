package server

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStreamReviewStatusReadOnlyLockedAndDamaged(t *testing.T) {
	dir := t.TempDir()
	s, err := newWithFetcher(dir, readDemoMock{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8080/api/v1/trading/stream-review/status", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"report_count":0`) || !strings.Contains(w.Body.String(), `"trading_authorized":false`) {
		t.Fatalf("unsafe status %d %s", w.Code, w.Body)
	}
	p := filepath.Join(dir, "trading", "stream_reviews")
	if err = os.MkdirAll(p, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(p, "rogue"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "잠금") {
		t.Fatalf("damaged archive went green %d %s", w.Code, w.Body)
	}
}
