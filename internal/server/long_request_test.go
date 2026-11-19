package server

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// Regression for dev.3.13: bounded 80/90/110s diagnostics were reachable
// through a server whose global WriteTimeout was only 45s. ResponseController
// must be able to extend the connection deadline for those explicit handlers.
func TestExtendWriteDeadlineOutlivesGlobalWriteTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		extendWriteDeadline(w, time.Second)
		time.Sleep(150 * time.Millisecond)
		respondJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	srv := &http.Server{Handler: h, WriteTimeout: 40 * time.Millisecond, ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	defer func() {
		_ = srv.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("test HTTP server did not stop")
		}
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	res, err := client.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("extended bounded handler was cut off by global WriteTimeout: %v", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK || string(body) == "" {
		t.Fatalf("unexpected response: status=%d body=%q", res.StatusCode, body)
	}
}
