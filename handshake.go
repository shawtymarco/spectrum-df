package spectrum

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultHandshakeLimit   = 64
)

func (l *Listener) start() {
	l.startOnce.Do(func() {
		if l.handshakeTimeout <= 0 {
			l.handshakeTimeout = defaultHandshakeTimeout
		}
		if l.handshakeLimit <= 0 {
			l.handshakeLimit = defaultHandshakeLimit
		}
		if l.connect == nil {
			l.connect = newConn
		}
		l.ctx, l.cancel = context.WithCancelCause(context.Background())
		l.incoming = make(chan *conn)
		l.done = make(chan struct{})
		go l.acceptStreams()
	})
}

// A silent stream must never own the shared accept loop. Each handshake has a
// deadline and one bounded worker, including the wait for the application to
// accept the completed connection. Listener shutdown joins every worker.
func (l *Listener) acceptStreams() {
	defer close(l.done)
	var workers sync.WaitGroup
	defer workers.Wait()
	slots := make(chan struct{}, l.handshakeLimit)
	var lastCapacityLog time.Time
	for {
		stream, err := l.transport.Accept()
		if err != nil {
			if l.ctx.Err() == nil {
				slog.Error("SpectrumDF transport accept failed", "err", err)
			}
			l.cancel(err)
			return
		}
		if l.ctx.Err() != nil {
			_ = stream.Close()
			return
		}
		select {
		case slots <- struct{}{}:
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-slots }()
				l.handshake(&handshakeStream{ReadWriteCloser: stream})
			}()
		default:
			_ = stream.Close()
			if now := time.Now(); now.Sub(lastCapacityLog) >= time.Second {
				lastCapacityLog = now
				slog.Error("SpectrumDF handshake capacity exceeded", "limit", l.handshakeLimit)
			}
		}
	}
}

// Close must interrupt reads as well as writes. Serialise competing timeout,
// handshake-error and listener-close paths even for a non-idempotent transport.
type handshakeStream struct {
	io.ReadWriteCloser
	once sync.Once
	err  error
}

func (s *handshakeStream) Close() error {
	s.once.Do(func() { s.err = s.ReadWriteCloser.Close() })
	return s.err
}

func (l *Listener) handshake(stream *handshakeStream) {
	started := time.Now()
	phase := "connection_request"
	ctx, cancel := context.WithTimeout(l.ctx, l.handshakeTimeout)
	defer cancel()
	closed := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() {
		_ = stream.Close()
		close(closed)
	})
	c, err := l.connect(stream, packet.NewClientPool(), l.resolver)
	if !stopClose() {
		<-closed
	}
	if cause := context.Cause(ctx); cause != nil {
		err = cause
	}
	if err == nil {
		var identity uuid.UUID
		identity, err = uuid.Parse(c.IdentityData().Identity)
		if err == nil {
			c.onClose = func() { l.sessions.CompareAndDelete(identity, c) }
			l.sessions.Store(identity, c)
			phase = "accept_delivery"
			select {
			case l.incoming <- c:
				return
			case <-ctx.Done():
				err = context.Cause(ctx)
			}
		}
	}
	if c != nil {
		_ = c.Close()
	} else {
		_ = stream.Close()
	}
	if l.ctx.Err() == nil {
		level := slog.LevelWarn
		if errors.Is(err, context.DeadlineExceeded) {
			level = slog.LevelError
		}
		slog.Log(context.Background(), level, "SpectrumDF connection handshake failed",
			"phase", phase, "duration_ms", time.Since(started).Milliseconds(),
			"timeout_ms", l.handshakeTimeout.Milliseconds(), "err", err)
	}
}
