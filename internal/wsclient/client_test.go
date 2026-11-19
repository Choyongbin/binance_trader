package wsclient

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func writeServerFrame(w io.Writer, op byte, fin bool, payload []byte) error {
	first := op
	if fin {
		first |= 0x80
	}
	if len(payload) > 125 {
		return fmt.Errorf("test frame too big")
	}
	_, err := w.Write(append([]byte{first, byte(len(payload))}, payload...))
	return err
}

func TestDialFragmentAndPingPong(t *testing.T) {
	errors := make(chan error, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/market/ws/btcusdt@kline_1m" || !headerHasToken(r.Header.Get("Upgrade"), "websocket") {
			errors <- fmt.Errorf("wrong upgrade request: %s", r.URL)
			return
		}
		hijack, ok := w.(http.Hijacker)
		if !ok {
			errors <- fmt.Errorf("no hijacker")
			return
		}
		wire, b, err := hijack.Hijack()
		if err != nil {
			errors <- err
			return
		}
		defer wire.Close()
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + wsGUID))
		if _, err = fmt.Fprintf(b, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:])); err != nil {
			errors <- err
			return
		}
		if err = b.Flush(); err != nil {
			errors <- err
			return
		}
		if err = writeServerFrame(b, 1, false, []byte(`{"e":"`)); err != nil {
			errors <- err
			return
		}
		if err = writeServerFrame(b, 9, true, []byte("test-ping")); err != nil {
			errors <- err
			return
		}
		if err = writeServerFrame(b, 0, true, []byte(`kline"}`)); err != nil {
			errors <- err
			return
		}
		if err = b.Flush(); err != nil {
			errors <- err
			return
		}
		// Client control frame must be masked; the pong payload must match.
		_ = wire.SetReadDeadline(time.Now().Add(3 * time.Second))
		hdr := make([]byte, 2)
		if _, err = io.ReadFull(b, hdr); err != nil {
			errors <- err
			return
		}
		if hdr[0] != 0x8a || hdr[1]&0x80 == 0 {
			errors <- fmt.Errorf("wrong masked pong header: %x", hdr)
			return
		}
		n := int(hdr[1] & 127)
		masked := make([]byte, 4+n)
		if _, err = io.ReadFull(b, masked); err != nil {
			errors <- err
			return
		}
		out := make([]byte, n)
		for i := range out {
			out[i] = masked[4+i] ^ masked[i%4]
		}
		if !bytes.Equal(out, []byte("test-ping")) {
			errors <- fmt.Errorf("wrong pong body %q", out)
			return
		}
		errors <- nil
	}))
	defer remote.Close()
	conn, err := Dial(context.Background(), "ws"+strings.TrimPrefix(remote.URL, "http")+"/market/ws/btcusdt@kline_1m")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	text, err := conn.ReadText()
	if err != nil || string(text) != `{"e":"kline"}` {
		t.Fatalf("fragment assembly: %q: %v", text, err)
	}
	select {
	case err := <-errors:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pong test timeout")
	}
}

func TestRejectBadUpgradeAndRemotePlaintext(t *testing.T) {
	if _, err := Dial(context.Background(), "ws://example.com/path"); err == nil {
		t.Fatal("remote plaintext must be refused")
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusSwitchingProtocols)
	}))
	defer remote.Close()
	// Invalid accept header must be rejected.
	_, err := Dial(context.Background(), "ws"+strings.TrimPrefix(remote.URL, "http"))
	if err == nil {
		t.Fatal("bad handshake accepted")
	}
}

func TestHeaderTokens(t *testing.T) {
	if !headerHasToken("keep-alive, Upgrade", "upgrade") || headerHasToken("superupgrade", "upgrade") {
		t.Fatal("header token parser")
	}
}

func TestServerFrameReadRejectsOversized(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	conn := &Conn{wire: a, in: bufio.NewReader(a)}
	go func() { _, _ = b.Write([]byte{0x81, 127, 0, 0, 0, 0, 0, 16, 0, 1}) }()
	if _, err := conn.ReadText(); err == nil {
		t.Fatal("oversized frame accepted")
	}
}

func TestClientPingMasked(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &Conn{wire: a, in: bufio.NewReader(a)}
	done := make(chan error, 1)
	go func() { done <- c.Ping() }()
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(b, hdr); err != nil {
		t.Fatal(err)
	}
	if hdr[0] != 0x89 || hdr[1]&0x80 == 0 {
		t.Fatalf("unmasked client ping %x", hdr)
	}
	body := make([]byte, 4+int(hdr[1]&127))
	if _, err := io.ReadFull(b, body); err != nil {
		t.Fatal(err)
	}
	out := make([]byte, len(body)-4)
	for i := range out {
		out[i] = body[i+4] ^ body[i%4]
	}
	if string(out) != "health" {
		t.Fatalf("unexpected ping payload %q", out)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
