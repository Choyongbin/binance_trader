package exchange

import (
	"binance_trader/internal/wsclient"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDocumentedPrivatePathModeURL(t *testing.T) {
	key := strings.Repeat("K", 64)
	got, err := demoWSURL(key)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got)
	if err != nil || u.Scheme != "wss" || u.Host != "demo-fstream.binance.com" || u.Path != "/private/ws/"+key || u.RawQuery != "" {
		t.Fatalf("unexpected private path URL: %q", got)
	}
	for _, bad := range []string{"", "https://example.org", "abc/123", "listenKey=secret"} {
		if _, err := demoWSURL(bad); err == nil {
			t.Fatal("invalid key accepted")
		}
	}
}

// Local regression for the current Binance private-route contract. The server
// upgrades /private/ws/<listenKey> and rejects the dev.3.49 query form.
func TestLocalPrivateRoutePathUpgradeQuery404(t *testing.T) {
	key := strings.Repeat("Q", 64)
	var rejected, accepted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/private/ws" && r.URL.Query().Get("listenKey") == key {
			rejected = true
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path != "/private/ws/"+key || r.URL.RawQuery != "" {
			t.Errorf("unexpected mock private path: %s", r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		accepted = true
		h, ok := w.(http.Hijacker)
		if !ok {
			t.Error("mock not hijackable")
			return
		}
		c, bw, e := h.Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		defer c.Close()
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		if _, e = fmt.Fprintf(bw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:])); e != nil {
			t.Error(e)
			return
		}
		if e = bw.Flush(); e != nil {
			t.Error(e)
		}
	}))
	defer srv.Close()
	prefix := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	q := url.Values{}
	q.Set("listenKey", key)
	q.Set("events", "ORDER_TRADE_UPDATE/ACCOUNT_UPDATE")
	old, err := wsclient.Dial(ctx, prefix+"/private/ws?"+q.Encode())
	if old != nil || err == nil || wsclient.DiagnoseDial(err).HTTPStatus != http.StatusNotFound {
		if old != nil {
			_ = old.Close()
		}
		t.Fatalf("expected dev.3.49 query route HTTP 404, got %v", err)
	}
	current, err := wsclient.Dial(ctx, prefix+"/private/ws/"+key)
	if err != nil {
		t.Fatalf("expected current path upgrade: %s", wsclient.DiagnoseDial(err).Phase)
	}
	_ = current.Close()
	if !rejected || !accepted {
		t.Fatal("mock did not verify both route modes")
	}
}
