package wsclient

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestPrivatePathUpgrade(t *testing.T) {
	key := strings.Repeat("A", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/private/ws/"+key || r.URL.RawQuery != "" {
			t.Errorf("handshake path mismatch")
			http.Error(w, "bad", 400)
			return
		}
		h, _ := w.(http.Hijacker)
		c, b, e := h.Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		defer c.Close()
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + wsGUID))
		_, _ = fmt.Fprintf(b, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
		_ = b.Flush()
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	u.Scheme = "ws"
	u.Path = "/private/ws/" + key
	c, e := Dial(context.Background(), u.String())
	if e != nil {
		t.Fatal(e)
	}
	_ = c.Close()
}

func TestDemoPrivateQueryFormRejectedBeforeDial(t *testing.T) {
	key := strings.Repeat("L", 64)
	raw := "wss://demo-fstream.binance.com/private/ws?listenKey=" + key + "&events=ORDER_TRADE_UPDATE"
	c, err := Dial(context.Background(), raw)
	if c != nil || err == nil || DiagnoseDial(err).Phase != "URL_VALIDATION" {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("private query form was not rejected locally: %v", err)
	}
	if strings.Contains(err.Error(), key) {
		t.Fatal("listenKey leaked")
	}
}

func TestDemoPrivatePathValidationRejectsMalformedKeyBeforeDial(t *testing.T) {
	for _, raw := range []string{
		"wss://demo-fstream.binance.com/private/ws/short",
		"wss://demo-fstream.binance.com/private/ws/" + strings.Repeat("A", 20) + "/extra",
		"wss://demo-fstream.binance.com/private/ws/" + strings.Repeat("A", 20) + "?x=1",
	} {
		c, err := Dial(context.Background(), raw)
		if c != nil || err == nil || DiagnoseDial(err).Phase != "URL_VALIDATION" {
			if c != nil {
				_ = c.Close()
			}
			t.Fatalf("malformed private path accepted: %s", raw)
		}
	}
}
