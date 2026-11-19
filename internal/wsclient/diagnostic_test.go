package wsclient

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func localPrivateURL(server, key string) string {
	return "ws" + strings.TrimPrefix(server, "http") + "/private/ws/" + key
}

func TestSafePrivateDialFailureStagesAndSecretRedaction(t *testing.T) {
	secret := strings.Repeat("SECRET_PRIVATE_LISTEN_KEY_", 2)
	tests := []struct {
		name    string
		handler http.HandlerFunc
		phase   string
		code    int
	}{
		{"forbidden", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Token", r.URL.Query().Get("listenKey"))
			w.WriteHeader(403)
			_, _ = fmt.Fprintf(w, "private error includes credential=%s", r.URL.Query().Get("listenKey"))
		}, "HTTP_STATUS", 403},
		{"not_found", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }, "HTTP_STATUS", 404},
		{"rate_limited", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }, "HTTP_STATUS", 429},
		{"bad_headers", func(w http.ResponseWriter, r *http.Request) {
			h, _ := w.(http.Hijacker)
			c, b, err := h.Hijack()
			if err != nil {
				return
			}
			defer c.Close()
			// Valid accept, deliberately wrong Connection header.
			sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + wsGUID))
			_, _ = fmt.Fprintf(b, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: keep-alive\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
			_ = b.Flush()
		}, "UPGRADE_HEADERS", 0},
		{"bad_accept", func(w http.ResponseWriter, r *http.Request) {
			h, _ := w.(http.Hijacker)
			c, b, err := h.Hijack()
			if err != nil {
				return
			}
			defer c.Close()
			_, _ = fmt.Fprintf(b, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: INVALID\r\n\r\n")
			_ = b.Flush()
		}, "UPGRADE_ACCEPT", 0},
		{"response_eof", func(w http.ResponseWriter, r *http.Request) {
			h, _ := w.(http.Hijacker)
			c, _, err := h.Hijack()
			if err != nil {
				return
			}
			_ = c.Close()
		}, "UPGRADE_RESPONSE", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			_, err := Dial(context.Background(), localPrivateURL(srv.URL, secret))
			if err == nil {
				t.Fatal("invalid upgrade was accepted")
			}
			d := DiagnoseDial(err)
			if d.Phase != tt.phase || d.HTTPStatus != tt.code {
				t.Fatalf("unexpected classification: %+v", d)
			}
			if !ValidDialDiagnostic(d) {
				t.Fatal("invalid allowlisted result")
			}
			for _, message := range []string{err.Error(), RedactDial(err).Error(), fmt.Sprintf("%+v", d)} {
				if strings.Contains(message, secret) || strings.Contains(message, "private error") || strings.Contains(message, "X-Token") {
					t.Fatalf("exposed confidential data: %s", message)
				}
			}
		})
	}
	t.Run("tls", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
		defer srv.Close()
		srv.Config.ErrorLog = log.New(io.Discard, "", 0)
		_, err := Dial(context.Background(), "wss"+strings.TrimPrefix(srv.URL, "https")+"/market/ws/btcusdt@kline_1m")
		if err == nil || DiagnoseDial(err).Phase != "TLS_HANDSHAKE" {
			t.Fatalf("expected TLS phase: %v", err)
		}
	})
	t.Run("tcp", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		_, err = Dial(context.Background(), "ws://"+addr+"/market/ws/btcusdt@kline_1m")
		if err == nil || DiagnoseDial(err).Phase != "TCP_DIAL" {
			t.Fatalf("expected TCP phase: %v", err)
		}
	})
	t.Run("untrusted_mock_error", func(t *testing.T) {
		secretError := errors.New("wss://invalid/private/ws/" + secret)
		d := DiagnoseDial(secretError)
		if d.Phase != "UNCLASSIFIED" || d.HTTPStatus != 0 || strings.Contains(RedactDial(secretError).Error(), secret) {
			t.Fatal("unsafe untrusted error")
		}
	})
}

func TestNoArbitraryDialDiagnosticInjected(t *testing.T) {
	for _, d := range []SafeDialDiagnostic{{Phase: "HTTP_STATUS", HTTPStatus: 99}, {Phase: "HTTP_STATUS", HTTPStatus: 101}, {Phase: "HTTP_STATUS", HTTPStatus: 600}, {Phase: "TLS_HANDSHAKE", HTTPStatus: 403}, {Phase: "wss://server?listenKey=sensitive"}, {Phase: "UNCLASSIFIED"}} {
		valid := ValidDialDiagnostic(d)
		if d.Phase == "UNCLASSIFIED" {
			if !valid {
				t.Fatalf("rejected whitelisted stage")
			}
		} else if valid {
			t.Fatalf("accepted wrong classification: %+v", d)
		}
	}
}
