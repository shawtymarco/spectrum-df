package spectrum

import (
	"time"

	"github.com/google/uuid"
)

// Latency retains both segments of the proxy's most recent latency probe.
// Client is the measured public client/proxy RTT. Backend estimates the
// proxy/backend RTT as twice the one-way timestamp delta, so it includes
// delivery/processing delay and depends on synchronized host clocks.
type Latency struct {
	Client  time.Duration
	Backend time.Duration
}

func (l Latency) RoundTrip() time.Duration { return l.Client + l.Backend }

// Latency returns one coherent sample for the current session. The boolean is
// false until the first probe arrives, or after the session has closed.
func (l *Listener) Latency(identity uuid.UUID) (Latency, bool) {
	value, ok := l.sessions.Load(identity)
	if !ok {
		return Latency{}, false
	}
	c := value.(*conn)
	select {
	case <-c.closed:
		return Latency{}, false
	default:
	}
	sample := c.latency.Load()
	if sample == nil {
		return Latency{}, false
	}
	return *sample, true
}

// LatencyComponents exposes the measurement without requiring wrappers to
// depend on the Latency type. Both durations are round trips from one sample.
func (l *Listener) LatencyComponents(identity uuid.UUID) (client, backend time.Duration, measured bool) {
	sample, ok := l.Latency(identity)
	return sample.Client, sample.Backend, ok
}
