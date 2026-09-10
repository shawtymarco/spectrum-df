package transport

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/cooldogedev/spectral"
)

type Spectral struct {
	listener *spectral.Listener
	incoming chan io.ReadWriteCloser
	closed   chan struct{}
	once     sync.Once
	closeErr error
}

func NewSpectral() *Spectral {
	return &Spectral{
		incoming: make(chan io.ReadWriteCloser, 100),
		closed:   make(chan struct{}),
	}
}

// Listen ...
func (s *Spectral) Listen(addr string) (err error) {
	listener, err := spectral.Listen(addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			connection, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			go s.handle(connection)
		}
	}()
	s.listener = listener
	return
}

// Accept ...
func (s *Spectral) Accept() (io.ReadWriteCloser, error) {
	select {
	case <-s.closed:
		return nil, errors.New("closed listener")
	case c := <-s.incoming:
		return c, nil
	}
}

// Close ...
func (s *Spectral) Close() (err error) {
	s.once.Do(func() {
		close(s.closed)
		s.closeErr = s.listener.Close()
	})
	return s.closeErr
}

func (s *Spectral) handle(connection spectral.Connection) {
	defer connection.CloseWithError(0, "failed to accept stream")
	for {
		stream, err := connection.AcceptStream(connection.Context())
		if err != nil {
			return
		}
		select {
		case s.incoming <- stream:
		case <-s.closed:
			_ = stream.Close()
			return
		case <-connection.Context().Done():
			_ = stream.Close()
			return
		}
	}
}
