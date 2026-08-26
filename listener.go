package spectrum

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	tr "github.com/cooldogedev/spectrum-df/transport"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/df-mc/dragonfly/server/session"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type Listener struct {
	transport tr.Transport
	resolver  ProtocolResolver
	connect   func(io.ReadWriteCloser, packet.Pool, ProtocolResolver) (*conn, error)
	sessions  sync.Map
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
	return &Listener{transport: transport, resolver: resolver, connect: newConn}, nil
}

// Accept ...
func (l *Listener) Accept() (session.Conn, error) {
	connect := l.connect
	if connect == nil {
		connect = newConn
	}
	for {
		stream, err := l.transport.Accept()
		if err != nil {
			// Transport errors represent the listener lifecycle. Dragonfly may
			// safely treat these as terminal and stop accepting connections.
			return nil, err
		}
		c, err := connect(stream, packet.NewClientPool(), l.resolver)
		if err != nil {
			// A backend crash may race a proxy fallback and close only this new
			// stream during its handshake. It must not terminate the backend's
			// shared Dragonfly listener.
			_ = stream.Close()
			slog.Warn("discarding incomplete SpectrumDF connection", "err", err)
			continue
		}
		identity, err := uuid.Parse(c.IdentityData().Identity)
		if err != nil {
			_ = c.Close()
			slog.Warn("discarding SpectrumDF connection with invalid identity", "err", fmt.Errorf("parse player identity: %w", err))
			continue
		}
		l.sessions.Store(identity, c)
		c.onClose = func() {
			l.sessions.CompareAndDelete(identity, c)
		}
		return c, nil
	}
}

// Transfer requests a seamless backend switch for an active Spectrum player.
// The public RakNet connection remains open while Spectrum connects the player
// to addr through its configured backend transport.
func (l *Listener) Transfer(identity uuid.UUID, addr string) error {
	if addr == "" {
		return errors.New("transfer backend address is empty")
	}
	value, ok := l.sessions.Load(identity)
	if !ok {
		return fmt.Errorf("player %s has no active SpectrumDF session", identity)
	}
	return value.(*conn).WritePacket(&spectrumpacket.Transfer{Addr: addr})
}

// HasSession reports whether identity is currently connected through this
// SpectrumDF listener.
func (l *Listener) HasSession(identity uuid.UUID) bool {
	_, ok := l.sessions.Load(identity)
	return ok
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
