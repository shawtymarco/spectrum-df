package spectrum

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLatencyRetainsBothSegments(t *testing.T) {
	for _, tc := range []struct{ client, sent, received, wantClient, wantBackend int64 }{
		{40, 1000, 1015, 40, 30},
		{100, 1000, 1080, 100, 160},
		{-1, 1000, 999, 0, 0},
	} {
		sample := latencyComponents(tc.client, tc.sent, tc.received)
		if sample.Client.Milliseconds() != tc.wantClient || sample.Backend.Milliseconds() != tc.wantBackend {
			t.Fatal(sample)
		}
		c := new(conn)
		c.latency.Store(&sample)
		if c.Latency()*2 != sample.RoundTrip() {
			t.Fatal("Dragonfly half-RTT changed")
		}
	}
}

func TestListenerLatencyIsSessionScopedAndCoherent(t *testing.T) {
	l := new(Listener)
	id := uuid.New()
	c := &conn{closed: make(chan struct{})}
	if _, ok := l.Latency(id); ok {
		t.Fatal("missing session has latency")
	}
	l.sessions.Store(id, c)
	if _, ok := l.Latency(id); ok {
		t.Fatal("unmeasured session has latency")
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			x := time.Duration(i) * time.Millisecond
			c.latency.Store(&Latency{Client: x, Backend: 2 * x})
		}
	})
	for range 1000 {
		if sample, ok := l.Latency(id); ok && sample.Backend != sample.Client*2 {
			t.Fatal("mixed samples", sample)
		}
	}
	wg.Wait()
	close(c.closed)
	if _, ok := l.Latency(id); ok {
		t.Fatal("closed session has latency")
	}
}
