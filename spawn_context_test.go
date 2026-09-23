package spectrum

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	spectrumprotocol "github.com/cooldogedev/spectrum/protocol"
	"github.com/golang/snappy"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// The edge completes the public client's StartGame before acknowledging spawn
// to SpectrumDF. A mobile client may legitimately take longer than the private
// connection-request deadline to initialise its world.
func TestSpawnHandshakeWaitsForSlowPublicClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, peer, result := pendingSpawnHandshake(t)
		time.Sleep(11 * time.Second)
		select {
		case err := <-result:
			t.Fatalf("backend abandoned public client before its spawn budget: %v", err)
		default:
		}
		writeSpawnResponse(t, peer, &packet.SetLocalPlayerAsInitialised{})
		if err := <-result; err != nil {
			t.Fatalf("slow public client could not complete spawn: %v", err)
		}
		select {
		case <-c.closed:
			t.Fatal("successful slow spawn closed the backend")
		default:
		}
	})
}

func TestSpawnHandshakeStillBoundsUnresponsivePublicClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		_, peer, result := pendingSpawnHandshake(t)
		err := <-result
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "player_initialised") {
			t.Fatalf("unresponsive spawn lost timeout or phase: %v", err)
		}
		if elapsed := time.Since(started); elapsed != time.Minute {
			t.Fatalf("public spawn budget = %v, want one minute", elapsed)
		}
		if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("timed-out stream remained open: %v", err)
		}
	})
}

func pendingSpawnHandshake(t *testing.T) (*conn, net.Conn, <-chan error) {
	t.Helper()
	stream, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	c := &conn{conn: stream, reader: spectrumprotocol.NewReader(stream), writer: spectrumprotocol.NewWriter(stream),
		pool: packet.NewClientPool(), proto: minecraft.DefaultProtocol, closed: make(chan struct{})}
	t.Cleanup(func() { _ = c.Close() })
	result := make(chan error, 1)
	go func() { result <- c.StartGameContext(context.Background(), minecraft.GameData{}) }()
	reader := spectrumprotocol.NewReader(peer)
	read := func() {
		t.Helper()
		if _, err := reader.ReadPacket(); err != nil {
			t.Fatal(err)
		}
	}
	read() // StartGame
	read() // ItemRegistry
	writeSpawnResponse(t, peer, &packet.RequestChunkRadius{ChunkRadius: 16})
	read() // ChunkRadiusUpdated
	read() // PlayStatus
	return c, peer, result
}

func writeSpawnResponse(t *testing.T, peer io.Writer, pk packet.Packet) {
	t.Helper()
	var encoded bytes.Buffer
	if err := (&packet.Header{PacketID: pk.ID()}).Write(&encoded); err != nil {
		t.Fatal(err)
	}
	pk.Marshal(protocol.NewWriter(&encoded, 0))
	if err := spectrumprotocol.NewWriter(peer).Write(snappy.Encode(nil, encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
}

func TestSpawnHandshakeCancellationClosesBlockedIO(t *testing.T) {
	for _, readBootstrap := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocked_write", true: "waiting_chunk_radius"}[readBootstrap], func(t *testing.T) {
			stream, peer := net.Pipe()
			defer peer.Close()
			c := &conn{conn: stream, reader: spectrumprotocol.NewReader(stream), writer: spectrumprotocol.NewWriter(stream),
				pool: packet.NewClientPool(), proto: minecraft.DefaultProtocol, closed: make(chan struct{})}
			defer c.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- c.StartGameContext(ctx, minecraft.GameData{}) }()
			if readBootstrap {
				reader := spectrumprotocol.NewReader(peer)
				_ = peer.SetReadDeadline(time.Now().Add(time.Second))
				for i := 0; i < 2; i++ {
					if _, err := reader.ReadPacket(); err != nil {
						t.Fatal(err)
					}
				}
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation cause: %v", err)
				}
				if readBootstrap && !strings.Contains(err.Error(), "request_chunk_radius") {
					t.Fatalf("missing stalled phase: %v", err)
				}
			case <-time.After(time.Second):
				_ = c.Close()
				<-result
				t.Fatal("spawn handshake ignored cancellation")
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("cancelled stream remained open: %v", err)
			}
		})
	}
}

func TestCompletedSpawnReleasesCancellationAndClosesOnce(t *testing.T) {
	stream, peer := net.Pipe()
	defer peer.Close()
	c := &conn{conn: stream, reader: spectrumprotocol.NewReader(stream), writer: spectrumprotocol.NewWriter(stream),
		pool: packet.NewClientPool(), proto: minecraft.DefaultProtocol, closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.StartGameContext(ctx, minecraft.GameData{}) }()
	reader := spectrumprotocol.NewReader(peer)
	_ = peer.SetDeadline(time.Now().Add(time.Second))
	read := func() {
		t.Helper()
		if _, err := reader.ReadPacket(); err != nil {
			t.Fatal(err)
		}
	}
	write := func(pk packet.Packet) {
		t.Helper()
		var encoded bytes.Buffer
		if err := (&packet.Header{PacketID: pk.ID()}).Write(&encoded); err != nil {
			t.Fatal(err)
		}
		pk.Marshal(protocol.NewWriter(&encoded, 0))
		if err := spectrumprotocol.NewWriter(peer).Write(snappy.Encode(nil, encoded.Bytes())); err != nil {
			t.Fatal(err)
		}
	}
	read() // StartGame
	read() // ItemRegistry
	write(&packet.RequestChunkRadius{ChunkRadius: 16})
	read() // ChunkRadiusUpdated
	read() // PlayStatus
	write(&packet.SetLocalPlayerAsInitialised{})
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	cancel()
	go func() {
		result <- c.WritePacket(&packet.Text{TextType: packet.TextTypeRaw, Message: "still connected"})
	}()
	read()
	if err := <-result; err != nil {
		t.Fatalf("completed handshake retained cancellation hook: %v", err)
	}
	var closed int
	c.onClose = func() { closed++ }
	var closers sync.WaitGroup
	for i := 0; i < 20; i++ {
		closers.Add(1)
		go func() { defer closers.Done(); _ = c.Close() }()
	}
	closers.Wait()
	if closed != 1 {
		t.Fatalf("close callback called %d times", closed)
	}
}
