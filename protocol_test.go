package spectrum

import (
	"bytes"
	"net"
	"testing"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/golang/snappy"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

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
