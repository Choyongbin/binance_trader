package server

import (
	"strings"
	"testing"
)

func TestWebOperationsOverviewRemainsDisplayOnlyAndLocked(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	html := request(h, "GET", "/", "", "", "")
	if html.Code != 200 {
		t.Fatalf("html=%d %s", html.Code, html.Body.String())
	}
	for _, want := range []string{"operationsOverviewRefresh", "overviewAuthorization", "overviewContinuity", "거래 허가를 생성하지 않습니다"} {
		if !strings.Contains(html.Body.String(), want) {
			t.Fatalf("index missing %q", want)
		}
	}
	js := request(h, "GET", "/app.js", "", "", "")
	if js.Code != 200 {
		t.Fatalf("js=%d", js.Code)
	}
	for _, want := range []string{
		"inspectOperationsOverview",
		"ready.trading_authorized!==false",
		"operator.trading_authorized!==false",
		"monitor.trading_authorized!==false",
		"auto.trading_enabled!==false",
		"auto.start_route_locked!==true",
		"auto.stop_route_locked!==true",
		"거래 허가: false",
	} {
		if !strings.Contains(js.Body.String(), want) {
			t.Fatalf("app.js missing safety marker %q", want)
		}
	}
}
