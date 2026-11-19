// Package wsclient implements the RFC 6455 subset needed for Binance
// public market and private event streams. It uses the Go standard library only, so this project
// can build offline without fetching external dependencies. Client control
// frames (pong/close) are masked as required by RFC 6455.
package wsclient

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxMessage = 1 << 20 // 1 MiB; market JSON is much smaller.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Conn is used by a single reader goroutine. The only outgoing frames are
// protocol control frames (PING, PONG and CLOSE); trading payloads cannot be written.
type Conn struct {
	wire    net.Conn
	in      *bufio.Reader
	writeMu sync.Mutex
}

func Dial(ctx context.Context, rawURL string) (*Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.User != nil || u.Fragment != "" {
		return nil, failedDial("URL_VALIDATION", 0)
	}
	if err := allowedQuery(u); err != nil {
		return nil, failedDial("URL_VALIDATION", 0)
	}
	if u.Scheme != "wss" && u.Scheme != "ws" {
		return nil, failedDial("URL_VALIDATION", 0)
	}
	host := u.Hostname()
	if host == "" {
		return nil, failedDial("URL_VALIDATION", 0)
	}
	if u.Scheme == "ws" && host != "localhost" && host != "127.0.0.1" && host != "::1" {
		return nil, failedDial("URL_VALIDATION", 0)
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}
	addr := net.JoinHostPort(host, port)
	d := &net.Dialer{Timeout: 8 * time.Second, KeepAlive: 30 * time.Second}
	wire, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, failedDial("TCP_DIAL", 0)
	}
	ok := false
	defer func() {
		if !ok {
			_ = wire.Close()
		}
	}()
	if u.Scheme == "wss" {
		tlsWire := tls.Client(wire, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err := tlsWire.HandshakeContext(ctx); err != nil {
			return nil, failedDial("TLS_HANDSHAKE", 0)
		}
		wire = tlsWire
	}
	// Context cancellation must interrupt the HTTP upgrade read, not just TCP
	// dialing and TLS. The dial deadline is still bounded independently.
	upgradeDone := make(chan struct{})
	upgradeWire := wire
	go func() {
		select {
		case <-ctx.Done():
			_ = upgradeWire.Close()
		case <-upgradeDone:
		}
	}()
	defer close(upgradeDone)
	deadline := time.Now().Add(10 * time.Second)
	if dl, has := ctx.Deadline(); has && dl.Before(deadline) {
		deadline = dl
	}
	_ = wire.SetDeadline(deadline)
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, failedDial("UPGRADE_WRITE", 0)
	}
	key := base64.StdEncoding.EncodeToString(nonce)
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}
	// Never include path/query (which may include listenKey) in errors/logs.
	handshake := "GET " + path + " HTTP/1.1\r\nHost: " + u.Host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(wire, handshake); err != nil {
		return nil, failedDial("UPGRADE_WRITE", 0)
	}
	reader := bufio.NewReader(wire)
	request := &http.Request{Method: http.MethodGet}
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		return nil, failedDial("UPGRADE_RESPONSE", 0)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		return nil, failedDial("HTTP_STATUS", response.StatusCode)
	}
	if !headerHasToken(response.Header.Get("Connection"), "upgrade") ||
		!headerHasToken(response.Header.Get("Upgrade"), "websocket") {
		return nil, failedDial("UPGRADE_HEADERS", 0)
	}
	expected := sha1.Sum([]byte(key + wsGUID))
	if response.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(expected[:]) {
		return nil, failedDial("UPGRADE_ACCEPT", 0)
	}
	_ = wire.SetDeadline(time.Time{})
	ok = true
	return &Conn{wire: wire, in: reader}, nil
}

func headerHasToken(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func (c *Conn) Close() error { return c.wire.Close() }

// ReadText returns a whole text message, handles fragmented messages and
// replies to server ping frames. A quiet stream causes a timeout and reconnection.
func (c *Conn) ReadText() ([]byte, error) {
	var message []byte
	fragmented := false
	for {
		if err := c.wire.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
			return nil, err
		}
		header := [2]byte{}
		if _, err := io.ReadFull(c.in, header[:]); err != nil {
			return nil, err
		}
		if header[0]&0x70 != 0 {
			return nil, errors.New("unsupported WebSocket reserved flags")
		}
		fin := header[0]&0x80 != 0
		opcode := header[0] & 0xf
		if header[1]&0x80 != 0 {
			return nil, errors.New("server frame must not be masked")
		}
		length := uint64(header[1] & 127)
		if length == 126 {
			var v uint16
			if err := binary.Read(c.in, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			length = uint64(v)
		}
		if length == 127 {
			if err := binary.Read(c.in, binary.BigEndian, &length); err != nil {
				return nil, err
			}
			if length>>63 != 0 {
				return nil, errors.New("bad WebSocket frame size")
			}
		}
		if length > maxMessage || uint64(len(message))+length > maxMessage {
			return nil, errors.New("WebSocket market message too large")
		}
		if opcode >= 8 && (!fin || length > 125) {
			return nil, errors.New("invalid WebSocket control frame")
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(c.in, payload); err != nil {
			return nil, err
		}
		switch opcode {
		case 8:
			_ = c.writeControl(8, payload)
			return nil, errors.New("WebSocket peer closed the connection")
		case 9:
			if err := c.writeControl(10, payload); err != nil {
				return nil, err
			}
			continue
		case 10:
			continue
		case 1:
			if fragmented {
				return nil, errors.New("interleaved WebSocket messages")
			}
			if fin {
				return payload, nil
			}
			fragmented = true
			message = append(message, payload...)
		case 0:
			if !fragmented {
				return nil, errors.New("unexpected WebSocket continuation")
			}
			message = append(message, payload...)
			if fin {
				return message, nil
			}
		default:
			return nil, errors.New("unexpected binary or unknown WebSocket frame")
		}
	}
}

// Ping keeps low-activity authenticated streams alive without inventing
// user account events. Only control frames can be transmitted.
func (c *Conn) Ping() error { return c.writeControl(9, []byte("health")) }

func (c *Conn) writeControl(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if len(payload) > 125 {
		return errors.New("control payload too large")
	}
	mask := [4]byte{}
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	frame := make([]byte, 6+len(payload))
	frame[0] = 0x80 | opcode
	frame[1] = 0x80 | byte(len(payload))
	copy(frame[2:6], mask[:])
	for i, b := range payload {
		frame[6+i] = b ^ mask[i%4]
	}
	_ = c.wire.SetWriteDeadline(time.Now().Add(3 * time.Second))
	for written := 0; written < len(frame); {
		n, err := c.wire.Write(frame[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		written += n
	}
	return nil
}

// Query URLs are restricted to loopback mocks. Binance Demo private user-data
// streams use the documented path form /private/ws/<listenKey> and MUST NOT
// carry listenKey/events query parameters. This deliberately does NOT make
// Dial an open URL proxy.
func allowedQuery(u *url.URL) error {
	if u.Scheme == "wss" && u.Hostname() == "demo-fstream.binance.com" && strings.HasPrefix(u.Path, "/private/") {
		if u.RawQuery != "" || !strings.HasPrefix(u.Path, "/private/ws/") {
			return errors.New("unsupported private WebSocket destination")
		}
		key := strings.TrimPrefix(u.Path, "/private/ws/")
		if key == "" || strings.Contains(key, "/") || len(key) < 16 || len(key) > 256 {
			return errors.New("invalid private key format")
		}
		for _, c := range key {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
				return errors.New("invalid private key format")
			}
		}
		return nil
	}
	if u.RawQuery == "" {
		return nil
	}
	local := u.Scheme == "ws" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1")
	if !local {
		return errors.New("unsupported WebSocket query destination")
	}
	return nil
}
