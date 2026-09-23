package spectrum_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cooldogedev/spectrum"
	spectrumdf "github.com/cooldogedev/spectrum-df"
	"github.com/cooldogedev/spectrum/server"
	"github.com/cooldogedev/spectrum/util"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Exercise the actual public RakNet -> Spectrum -> SpectrumDF handshake. The
// client withholds its final public acknowledgement beyond the old ten-second
// backend deadline, as a slow mobile world initialisation does.
func TestSlowPublicSpawnThroughSpectrum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	port, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := port.LocalAddr().String()
	_ = port.Close()
	backend, err := spectrumdf.NewListener(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendReady := make(chan error, 1)
	go func() {
		conn, err := backend.Accept()
		if err == nil {
			defer conn.Close()
			err = conn.StartGameContext(ctx, minecraft.GameData{WorldName: "slow-spawn", BaseGameVersion: protocol.CurrentVersion})
		}
		backendReady <- err
		<-ctx.Done()
	}()
	opts := util.DefaultOpts()
	opts.Addr = "127.0.0.1:0"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edge := spectrum.NewSpectrum(server.NewStaticDiscovery(address, address), logger, opts, nil)
	if err := edge.Listen(minecraft.ListenConfig{AuthenticationDisabled: true, ErrorLog: logger}); err != nil {
		t.Fatal(err)
	}
	defer edge.Close()
	edgeReady := make(chan error, 1)
	go func() { _, err := edge.Accept(); edgeReady <- err }()
	clientProtocol := &slowSpawnProtocol{Protocol: minecraft.DefaultProtocol}
	client, err := (minecraft.Dialer{Protocol: clientProtocol, ErrorLog: logger}).DialContext(ctx, "raknet", edge.Listener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.DoSpawnContext(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ready := range []<-chan error{backendReady, edgeReady} {
		select {
		case err := <-ready:
			if err != nil {
				t.Fatalf("slow public client failed backend login: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

type slowSpawnProtocol struct {
	minecraft.Protocol
	once sync.Once
}

func (p *slowSpawnProtocol) ConvertFromLatest(pk packet.Packet, conn *minecraft.Conn) []packet.Packet {
	if _, ok := pk.(*packet.SetLocalPlayerAsInitialised); ok {
		p.once.Do(func() { time.Sleep(11 * time.Second) })
	}
	return p.Protocol.ConvertFromLatest(pk, conn)
}
