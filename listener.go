package spectrum

import (
	tr "github.com/cooldogedev/spectrum-df/transport"
	"github.com/df-mc/dragonfly/server/session"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type Listener struct {
	transport tr.Transport
	resolver  ProtocolResolver
}

// ProtocolResolver resolves the public client protocol forwarded by Spectrum.
// The backend wire remains native; the result exposes presentation
// capabilities such as Dragonfly's block runtime ID mapper.
type ProtocolResolver func(protocolID int32, clientData login.ClientData) (minecraft.Protocol, bool)

// NewProtocolResolver builds a resolver containing the native protocol and the
// additional historical protocols passed.
func NewProtocolResolver(protocols []minecraft.Protocol) ProtocolResolver {
	byID := make(map[int32]minecraft.Protocol, len(protocols)+1)
	byID[minecraft.DefaultProtocol.ID()] = minecraft.DefaultProtocol
	for _, proto := range protocols {
		if _, exists := byID[proto.ID()]; !exists {
			byID[proto.ID()] = proto
		}
	}
	return func(protocolID int32, _ login.ClientData) (minecraft.Protocol, bool) {
		proto, ok := byID[protocolID]
		return proto, ok
	}
}

func NewListener(addr string, transport tr.Transport) (*Listener, error) {
	return NewListenerWithResolver(addr, transport, NewProtocolResolver(nil))
}

// NewListenerWithProtocols creates a listener that exposes the selected
// public client protocol to Dragonfly while keeping Spectrum's backend wire
// native.
func NewListenerWithProtocols(addr string, transport tr.Transport, protocols []minecraft.Protocol) (*Listener, error) {
	return NewListenerWithResolver(addr, transport, NewProtocolResolver(protocols))
}

// NewListenerWithResolver creates a listener using a custom protocol resolver.
func NewListenerWithResolver(addr string, transport tr.Transport, resolver ProtocolResolver) (*Listener, error) {
	if transport == nil {
		transport = tr.NewSpectral()
	}
	if resolver == nil {
		resolver = NewProtocolResolver(nil)
	}

	if err := transport.Listen(addr); err != nil {
		return nil, err
	}
	return &Listener{transport: transport, resolver: resolver}, nil
}

// Accept ...
func (l *Listener) Accept() (session.Conn, error) {
	c, err := l.transport.Accept()
	if err != nil {
		return nil, err
	}
	return newConn(c, packet.NewClientPool(), l.resolver)
}

// Disconnect ...
func (l *Listener) Disconnect(conn session.Conn, reason string) error {
	_ = conn.WritePacket(&packet.Disconnect{
		HideDisconnectionScreen: reason == "",
		Message:                 reason,
	})
	return conn.Close()
}

// Close ...
func (l *Listener) Close() error {
	return l.transport.Close()
}
