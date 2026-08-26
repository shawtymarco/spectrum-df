package spectrum

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type queuedTransport struct {
	streams []io.ReadWriteCloser
	index   int
}

func (*queuedTransport) Listen(string) error { return nil }
func (t *queuedTransport) Accept() (io.ReadWriteCloser, error) {
	if t.index >= len(t.streams) {
		return nil, errors.New("transport exhausted")
	}
	stream := t.streams[t.index]
	t.index++
	return stream, nil
}
func (*queuedTransport) Close() error { return nil }

type trackedStream struct {
	closed bool
}

func (*trackedStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (*trackedStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *trackedStream) Close() error {
	s.closed = true
	return nil
}

type protocolWithID struct {
	minecraft.Protocol
	id int32
}

func (p protocolWithID) ID() int32 { return p.id }

func TestProtocolResolver(t *testing.T) {
	historical := protocolWithID{Protocol: minecraft.DefaultProtocol, id: minecraft.DefaultProtocol.ID() - 1}
	resolver := NewProtocolResolver([]minecraft.Protocol{historical})

	for _, proto := range []minecraft.Protocol{minecraft.DefaultProtocol, historical} {
		got, ok := resolver(proto.ID(), login.ClientData{})
		if !ok || got.ID() != proto.ID() {
			t.Fatalf("resolve protocol %d = (%v, %v)", proto.ID(), got, ok)
		}
	}
	if _, ok := resolver(-1, login.ClientData{}); ok {
		t.Fatal("unexpected resolution for unsupported protocol")
	}
}

func TestListenerSkipsFailedConnectionHandshakes(t *testing.T) {
	broken, invalid, valid := &trackedStream{}, &trackedStream{}, &trackedStream{}
	transport := &queuedTransport{streams: []io.ReadWriteCloser{broken, invalid, valid}}
	validIdentity := uuid.New()
	calls := 0
	listener := &Listener{
		transport: transport,
		resolver:  NewProtocolResolver(nil),
		connect: func(stream io.ReadWriteCloser, _ packet.Pool, _ ProtocolResolver) (*conn, error) {
			calls++
			switch calls {
			case 1:
				return nil, errors.New("handshake closed")
			case 2:
				return &conn{conn: stream, identityData: login.IdentityData{Identity: "invalid"}, closed: make(chan struct{})}, nil
			default:
				return &conn{conn: stream, identityData: login.IdentityData{Identity: validIdentity.String()}, closed: make(chan struct{})}, nil
			}
		},
	}

	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if got := accepted.IdentityData().Identity; got != validIdentity.String() {
		t.Fatalf("accepted identity = %q, want %q", got, validIdentity)
	}
	if calls != 3 || transport.index != 3 {
		t.Fatalf("connection attempts = %d/%d, want 3/3", calls, transport.index)
	}
	if !broken.closed || !invalid.closed {
		t.Fatal("failed connection streams were not closed")
	}
	if valid.closed {
		t.Fatal("accepted connection stream was closed")
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShouldDecodePacketForProtocol(t *testing.T) {
	historical := protocolWithID{Protocol: minecraft.DefaultProtocol, id: minecraft.DefaultProtocol.ID() - 1}
	if shouldDecodePacketForProtocol(packet.IDText, minecraft.DefaultProtocol) {
		t.Fatal("native text packet unexpectedly left the raw fast path")
	}
	if !shouldDecodePacketForProtocol(packet.IDText, historical) {
		t.Fatal("historical text packet unexpectedly used the raw fast path")
	}
	if !shouldDecodePacketForProtocol(spectrumpacket.IDFlush, minecraft.DefaultProtocol) {
		t.Fatal("internal flush packet must always be decoded")
	}
}

func TestFlushWritesInternalPacket(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	c := &conn{
		conn:   client,
		writer: spectrumprotocol.NewWriter(client),
		proto:  minecraft.DefaultProtocol,
		closed: make(chan struct{}),
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Flush() }()

	payload, err := spectrumprotocol.NewReader(server).ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != packetDecodeNeeded {
		t.Fatalf("decode marker = %d, want %d", payload[0], packetDecodeNeeded)
	}
	decoded, err := snappy.Decode(nil, payload[1:])
	if err != nil {
		t.Fatal(err)
	}
	header := &packet.Header{}
	if err := header.Read(bytes.NewBuffer(decoded)); err != nil {
		t.Fatal(err)
	}
	if header.PacketID != spectrumpacket.IDFlush {
		t.Fatalf("packet ID = %d, want %d", header.PacketID, spectrumpacket.IDFlush)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestListenerTransferWritesInternalPacket(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	identity := uuid.New()
	c := &conn{
		conn:   client,
		writer: spectrumprotocol.NewWriter(client),
		proto:  minecraft.DefaultProtocol,
		closed: make(chan struct{}),
	}
	l := &Listener{}
	l.sessions.Store(identity, c)
	c.onClose = func() { l.sessions.CompareAndDelete(identity, c) }
	if !l.HasSession(identity) {
		t.Fatal("listener did not report the active session")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- l.Transfer(identity, "bedwars:19143") }()
	payload, err := spectrumprotocol.NewReader(server).ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := snappy.Decode(nil, payload[1:])
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer(decoded)
	header := &packet.Header{}
	if err := header.Read(buf); err != nil {
		t.Fatal(err)
	}
	if header.PacketID != spectrumpacket.IDTransfer {
		t.Fatalf("packet ID = %d, want %d", header.PacketID, spectrumpacket.IDTransfer)
	}
	transfer := &spectrumpacket.Transfer{}
	transfer.Marshal(protocol.NewReader(buf, 0, false))
	if transfer.Addr != "bedwars:19143" {
		t.Fatalf("transfer address = %q", transfer.Addr)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if l.HasSession(identity) {
		t.Fatal("closed session remained registered")
	}
}
