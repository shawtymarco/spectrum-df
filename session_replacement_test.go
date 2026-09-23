package spectrum

import (
	"bytes"
	"net"
	"testing"
	"time"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Dragonfly can reject a replacement after the private handshake (for example
// "Already logged in"). Its closure must not orphan the original live player.
func TestRejectedReplacementPreservesOriginalTransfer(t *testing.T) {
	transport := newHandshakeTestTransport()
	l := &Listener{transport: transport, resolver: NewProtocolResolver(nil)}
	defer l.Close()
	id := uuid.New()
	original, peer := acceptReplacementTestConn(t, l, transport, id)
	replacement, _ := acceptReplacementTestConn(t, l, transport, id)
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if !l.HasSession(id) {
		t.Fatal("rejected replacement removed the original live session")
	}
	result := make(chan error, 1)
	go func() { result <- l.Transfer(id, "bedwars:19143") }()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	encoded, err := spectrumprotocol.NewReader(peer).ReadPacket()
	if err != nil {
		t.Fatalf("original session did not receive transfer: %v", err)
	}
	payload, err := snappy.Decode(nil, encoded[1:])
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer(payload)
	header := new(packet.Header)
	if err := header.Read(buf); err != nil || header.PacketID != spectrumpacket.IDTransfer {
		t.Fatalf("restored session packet = %d, err = %v", header.PacketID, err)
	}
	pk := new(spectrumpacket.Transfer)
	pk.Marshal(protocol.NewReader(buf, 0, false))
	if pk.Addr != "bedwars:19143" {
		t.Fatalf("transfer target = %q", pk.Addr)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	_ = original.Close()
	if l.HasSession(id) {
		t.Fatal("closed original session remained registered")
	}
}

func TestClosedPredecessorIsNeverRestored(t *testing.T) {
	transport := newHandshakeTestTransport()
	l := &Listener{transport: transport, resolver: NewProtocolResolver(nil)}
	defer l.Close()
	id := uuid.New()
	original, _ := acceptReplacementTestConn(t, l, transport, id)
	middle, _ := acceptReplacementTestConn(t, l, transport, id)
	newest, _ := acceptReplacementTestConn(t, l, transport, id)
	_ = middle.Close()
	_ = original.Close()
	if !l.HasSession(id) {
		t.Fatal("old session closure removed the newest session")
	}
	_ = newest.Close()
	if l.HasSession(id) {
		t.Fatal("closing the newest session restored a closed predecessor")
	}
}

func acceptReplacementTestConn(t *testing.T, l *Listener, transport *handshakeTestTransport, id uuid.UUID) (*conn, net.Conn) {
	t.Helper()
	stream, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	transport.streams <- stream
	result := make(chan error, 1)
	go func() { result <- sendTestConnectionRequest(peer, id) }()
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	c := accepted.(*conn)
	t.Cleanup(func() { _ = c.Close() })
	return c, peer
}
