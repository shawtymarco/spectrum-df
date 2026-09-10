package spectrum

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/df-mc/dragonfly/server/session"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type handshakeTestTransport struct {
	streams chan io.ReadWriteCloser
	closed  chan struct{}
	once    sync.Once
}

func newHandshakeTestTransport(streams ...io.ReadWriteCloser) *handshakeTestTransport {
	t := &handshakeTestTransport{streams: make(chan io.ReadWriteCloser, 100), closed: make(chan struct{})}
	for _, stream := range streams {
		t.streams <- stream
	}
	return t
}

func (*handshakeTestTransport) Listen(string) error { return nil }
func (t *handshakeTestTransport) Accept() (io.ReadWriteCloser, error) {
	select {
	case stream := <-t.streams:
		return stream, nil
	case <-t.closed:
		return nil, net.ErrClosed
	}
}
func (t *handshakeTestTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func TestSilentHandshakeDoesNotBlockAnotherLogin(t *testing.T) {
	silent, silentPeer := net.Pipe()
	valid, validPeer := net.Pipe()
	defer silentPeer.Close()
	defer validPeer.Close()
	l := &Listener{transport: newHandshakeTestTransport(silent, valid), resolver: NewProtocolResolver(nil), handshakeTimeout: 2 * time.Second}
	defer l.Close()
	id := uuid.New()
	writeResult := make(chan error, 1)
	go func() { writeResult <- sendTestConnectionRequest(validPeer, id) }()
	accepted := make(chan session.Conn, 1)
	errs := make(chan error, 1)
	go func() { c, err := l.Accept(); accepted <- c; errs <- err }()
	select {
	case c := <-accepted:
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if c.IdentityData().Identity != id.String() {
			t.Fatal("listener accepted the wrong connection")
		}
	case <-time.After(time.Second):
		t.Fatal("silent first handshake blocked the next login")
	}
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
}

func TestIncompleteHandshakeTimesOutAndLogsPhase(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "silent", true: "partial_frame"}[partial], func(t *testing.T) {
			logs := &handshakeLogCapture{written: make(chan struct{}, 1)}
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
			defer slog.SetDefault(previous)
			stream, peer := net.Pipe()
			defer peer.Close()
			l := &Listener{transport: newHandshakeTestTransport(stream), handshakeTimeout: 50 * time.Millisecond}
			defer l.Close()
			l.start()
			if partial {
				// A complete length header followed by no payload must be bounded too.
				if err := binary.Write(peer, binary.LittleEndian, uint32(128)); err != nil {
					t.Fatal(err)
				}
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := peer.Read(make([]byte, 1)); err == nil {
				t.Fatal("incomplete handshake remained open")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("listener did not close the incomplete handshake")
			}
			select {
			case <-logs.written:
			case <-time.After(time.Second):
				t.Fatal("handshake timeout did not emit a diagnostic")
			}
			_ = l.Close()
			if !strings.Contains(logs.String(), "SpectrumDF connection handshake failed") || !strings.Contains(logs.String(), "phase=connection_request") || !strings.Contains(logs.String(), "deadline exceeded") {
				t.Fatalf("timeout lost its error/phase diagnostic: %s", logs.String())
			}
		})
	}
}

type handshakeLogCapture struct {
	mu sync.Mutex
	bytes.Buffer
	written chan struct{}
}

func (l *handshakeLogCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	n, err := l.Buffer.Write(p)
	l.mu.Unlock()
	select {
	case l.written <- struct{}{}:
	default:
	}
	return n, err
}

func (l *handshakeLogCapture) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Buffer.String()
}

func TestListenerCloseInterruptsPendingHandshake(t *testing.T) {
	stream, peer := net.Pipe()
	defer peer.Close()
	l := &Listener{transport: newHandshakeTestTransport(stream)}
	accepted := make(chan error, 1)
	go func() { _, err := l.Accept(); accepted <- err }()
	// A partial header proves the handshake has entered its blocking read.
	_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := peer.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener close left a handshake worker blocked")
	}
	if err := <-accepted; err == nil {
		t.Fatal("closed listener accepted a connection")
	}
}

func TestHandshakeCapacityIsBoundedAndRecovers(t *testing.T) {
	silent, silentPeer := net.Pipe()
	defer silentPeer.Close()
	tr := newHandshakeTestTransport(silent)
	l := &Listener{transport: tr, handshakeLimit: 1, handshakeTimeout: time.Second}
	defer l.Close()
	l.start()
	_ = silentPeer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := silentPeer.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	overflow, overflowPeer := net.Pipe()
	defer overflowPeer.Close()
	tr.streams <- overflow
	_ = overflowPeer.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := overflowPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess handshake was not rejected")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("handshake capacity was not bounded")
	}
	// Closing the blocker must release its worker, allowing a fresh login.
	_ = silentPeer.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stream, peer := net.Pipe()
		tr.streams <- stream
		id := uuid.New()
		if err := sendTestConnectionRequest(peer, id); err != nil {
			_ = peer.Close()
			continue
		}
		c, err := l.Accept()
		_ = peer.Close()
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		return
	}
	t.Fatal("capacity did not recover after the incomplete stream closed")
}

func TestCompletedHandshakeExpiresWithoutApplicationAccept(t *testing.T) {
	stream, peer := net.Pipe()
	defer peer.Close()
	l := &Listener{transport: newHandshakeTestTransport(stream), handshakeTimeout: 50 * time.Millisecond}
	defer l.Close()
	l.start()
	id := uuid.New()
	if err := sendTestConnectionRequest(peer, id); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("unclaimed connection remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("application delivery was not bounded")
	}
	_ = l.Close()
	if l.HasSession(id) {
		t.Fatal("expired unclaimed connection remained registered")
	}
}

func sendTestConnectionRequest(peer net.Conn, id uuid.UUID) error {
	_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
	identity, _ := json.Marshal(login.IdentityData{Identity: id.String(), XUID: id.String(), DisplayName: "HandshakeTest"})
	data, _ := json.Marshal(login.ClientData{GameVersion: protocol.CurrentVersion})
	pk := &spectrumpacket.ConnectionRequest{Addr: "127.0.0.1:12345", ProtocolID: protocol.CurrentProtocol, IdentityData: identity, ClientData: data}
	var buf bytes.Buffer
	header := packet.Header{PacketID: pk.ID()}
	if err := header.Write(&buf); err != nil {
		return err
	}
	pk.Marshal(protocol.NewWriter(&buf, 0))
	if err := spectrumprotocol.NewWriter(peer).Write(snappy.Encode(nil, buf.Bytes())); err != nil {
		return err
	}
	_, err := spectrumprotocol.NewReader(peer).ReadPacket()
	return err
}
