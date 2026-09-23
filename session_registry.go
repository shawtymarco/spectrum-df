package spectrum

import (
	"sync"

	"github.com/google/uuid"
)

// connectionRegistry retains live predecessors while a replacement connects.
// Dragonfly may reject that replacement after the private handshake. Removing
// only the rejected connection restores the predecessor's transfer/latency
// route; closing a predecessor never removes or later resurrects it.
type connectionRegistry struct {
	mu       sync.RWMutex
	sessions map[uuid.UUID][]*conn
}

func (r *connectionRegistry) Store(identity uuid.UUID, c *conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions == nil {
		r.sessions = make(map[uuid.UUID][]*conn)
	}
	r.sessions[identity] = append(r.sessions[identity], c)
}

func (r *connectionRegistry) Load(identity uuid.UUID) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sessions := r.sessions[identity]
	if len(sessions) == 0 {
		return nil, false
	}
	return sessions[len(sessions)-1], true
}

func (r *connectionRegistry) Remove(identity uuid.UUID, c *conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sessions := r.sessions[identity]
	for i, candidate := range sessions {
		if candidate != c {
			continue
		}
		copy(sessions[i:], sessions[i+1:])
		sessions[len(sessions)-1] = nil
		sessions = sessions[:len(sessions)-1]
		if len(sessions) == 0 {
			delete(r.sessions, identity)
		} else {
			r.sessions[identity] = sessions
		}
		return
	}
}
