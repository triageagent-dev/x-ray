// Package hub carries memory events to connected clients: a search hit, a
// change to an item, a metrics snapshot. Each Hub is an instance, so a host
// owns the lifetime of its hub and nothing is shared between processes' hubs
// or tests. What carries an event to the other replicas of a host is a
// Transport the host passes in; with none, the hub is local.
package hub

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

// FrameType is the envelope's explicit discriminator.
type FrameType string

const (
	// FrameHit marks a Frame whose Data is a HitEvent.
	FrameHit FrameType = "hit"
	// FrameChange marks a Frame whose Data is a ChangeEvent.
	FrameChange FrameType = "change"
	// FrameMetrics marks a Frame whose Data is whatever PublishMetrics was given.
	FrameMetrics FrameType = "metrics"
)

// Frame is the one envelope every subscriber receives:
// {"type": "hit"|"change"|"metrics", "data": <payload>}. Data is marshalled once
// per event, so every subscriber decodes it independently.
type Frame struct {
	Type FrameType       `json:"type"`
	Data json.RawMessage `json:"data"`
}

// HitEvent reports that a search surfaced items a downstream consumer used.
type HitEvent struct {
	ItemIDs []string `json:"itemIds"`
	Query   string   `json:"query"`
	// Kind is the search's kind filter, usually empty: not a per-item kind.
	Kind string `json:"kind,omitempty"`
	// Kinds is the kind of each item, parallel to ItemIDs, so a client can place
	// a hit by category without having the item itself. A client that finds it
	// absent or shorter than ItemIDs must fall back to behaviour that needs no
	// kind.
	Kinds []string `json:"kinds,omitempty"`
	// Tags is the tags of each item, parallel to ItemIDs and empty for a private
	// item, so a client can place a hit in a category a deployment split off its
	// kind by tag (ADR-035). A client that finds it absent or shorter than ItemIDs
	// places hits by kind alone.
	Tags [][]string `json:"tags,omitempty"`
}

// ChangeEvent reports that an item was created, updated or deleted.
type ChangeEvent struct {
	Op string `json:"op"` // "upsert" | "delete"
	// Item is set for an upsert. It is typed any so a host can send the shape its
	// own API already uses for an item without this package knowing it.
	Item any `json:"item,omitempty"`
	// ID is set for a delete, and Kind is the deleted item's kind: a delete has
	// nothing else on the wire, and a client cannot tell which category region a
	// deleted item belonged to without it.
	ID   string `json:"id,omitempty"`
	Kind string `json:"kind,omitempty"`
}

// Transport carries a locally originated event to the other replicas of a host.
// Hub calls it after the local effects of PublishHit and PublishChange, and
// never for an event applied from a peer (ApplyRemoteHit, ApplyRemoteChange) or
// for a change scoped to one user (PublishChangeTo), so a mesh built on top of it
// cannot loop an event back and forth. Implementations must not block.
type Transport interface {
	PublishHit(HitEvent)
	PublishChange(ChangeEvent)
}

// Hub fans events out to subscribers. Each subscriber channel is buffered and
// written to without blocking: a slow client drops frames rather than stalling a
// publisher.
type Hub struct {
	transport Transport

	mu      sync.RWMutex
	clients map[chan Frame]string // subscriber → owner userHash ("" = anonymous or no auth)

	hitMu     sync.RWMutex
	hitCounts map[string]*atomic.Int64
}

// New builds a hub. transport may be nil.
func New(transport Transport) *Hub {
	return &Hub{
		transport: transport,
		clients:   make(map[chan Frame]string),
		hitCounts: make(map[string]*atomic.Int64),
	}
}

// Subscribe registers a subscriber and returns its channel and a function that
// removes it and closes the channel; call that function exactly once. userHash
// scopes delivery of PublishChangeTo to that owner; "" is an anonymous
// subscription that receives only broadcasts.
func (h *Hub) Subscribe(userHash string) (<-chan Frame, func()) {
	ch := make(chan Frame, 8)
	h.mu.Lock()
	h.clients[ch] = userHash
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
		close(ch)
	}
}

// ClientCount is the number of subscribers on this hub. A sampler that only
// matters when someone listens can gate on it.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// PublishHit broadcasts a locally originated hit, counts it per item, then hands
// it to the transport.
func (h *Hub) PublishHit(evt HitEvent) {
	h.applyHit(evt)
	if h.transport != nil {
		h.transport.PublishHit(evt)
	}
}

// ApplyRemoteHit applies a hit received from a peer: the same local effects as
// PublishHit, without handing it to the transport.
func (h *Hub) ApplyRemoteHit(evt HitEvent) { h.applyHit(evt) }

func (h *Hub) applyHit(evt HitEvent) {
	h.hitMu.Lock()
	for _, id := range evt.ItemIDs {
		c, ok := h.hitCounts[id]
		if !ok {
			c = &atomic.Int64{}
			h.hitCounts[id] = c
		}
		c.Add(1)
	}
	h.hitMu.Unlock()
	h.broadcast(FrameHit, evt)
}

// HitCount is how many times the item was surfaced by a search.
func (h *Hub) HitCount(id string) int64 {
	h.hitMu.RLock()
	c := h.hitCounts[id]
	h.hitMu.RUnlock()
	if c == nil {
		return 0
	}
	return c.Load()
}

// PublishChange broadcasts a locally originated change to every subscriber, then
// hands it to the transport. Use it for shared items only.
func (h *Hub) PublishChange(evt ChangeEvent) {
	h.broadcast(FrameChange, evt)
	if h.transport != nil {
		h.transport.PublishChange(evt)
	}
}

// ApplyRemoteChange applies a change received from a peer: the same broadcast as
// PublishChange, without handing it to the transport.
func (h *Hub) ApplyRemoteChange(evt ChangeEvent) { h.broadcast(FrameChange, evt) }

// PublishChangeTo sends a change only to the subscribers owned by userHash: the
// channel for private items, whose content must not reach other users. With no
// userHash (no authentication) it broadcasts, so the single local user still sees
// live updates. It is never handed to the transport: the event carries no owner,
// so a peer applying it could only broadcast it.
func (h *Hub) PublishChangeTo(userHash string, evt ChangeEvent) {
	if userHash == "" {
		h.broadcast(FrameChange, evt)
		return
	}
	b, _ := json.Marshal(evt)
	f := Frame{Type: FrameChange, Data: b}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch, owner := range h.clients {
		if owner != userHash {
			continue
		}
		select {
		case ch <- f:
		default:
		}
	}
}

// PublishMetrics broadcasts a metrics snapshot to this hub's subscribers. It is
// local by design: the numbers are one process's view, and forwarding them would
// mix one replica's counters into another's stream.
func (h *Hub) PublishMetrics(v any) { h.broadcast(FrameMetrics, v) }

func (h *Hub) broadcast(kind FrameType, v any) {
	b, _ := json.Marshal(v)
	f := Frame{Type: kind, Data: b}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- f:
		default:
		}
	}
}
