package spectrum

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

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
	startOnce sync.Once
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelCauseFunc
	incoming  chan *conn
	done      chan struct{}
	closeErr  error
	// Set only before the first Accept or Close. Zero uses the safe defaults.
	handshakeTimeout time.Duration
	handshakeLimit   int
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
	l.start()
	select {
	case <-l.ctx.Done():
		return nil, context.Cause(l.ctx)
	case c := <-l.incoming:
		if err := context.Cause(l.ctx); err != nil {
			_ = c.Close()
			return nil, err
		}
		return c, nil
	}
}

// Transfer requests a seamless backend switch for an active Spectrum player.
// The public RakNet connection remains open while Spectrum connects the player
// to addr through its configured backend transport.
func (l *Listener) Transfer(identity uuid.UUID, addr string) error {
	return l.transfer(identity, addr, false)
}

// TransferReady requests a backend switch whose target must publish
// BackendReady before Spectrum exposes its final state to the client.
func (l *Listener) TransferReady(identity uuid.UUID, addr string) error {
	return l.transfer(identity, addr, true)
}

func (l *Listener) transfer(identity uuid.UUID, addr string, waitForReady bool) error {
	if addr == "" {
		return errors.New("transfer backend address is empty")
	}
	value, ok := l.sessions.Load(identity)
	if !ok {
		return fmt.Errorf("player %s has no active SpectrumDF session", identity)
	}
	return value.(*conn).WritePacket(&spectrumpacket.Transfer{Addr: addr, WaitForReady: waitForReady})
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
	l.start()
	l.closeOnce.Do(func() {
		l.cancel(errors.New("SpectrumDF listener closed"))
		l.closeErr = l.transport.Close()
		<-l.done
	})
	return l.closeErr
}
