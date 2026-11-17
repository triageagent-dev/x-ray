// Package mesh lets the replicas of a service talk to one another: each finds
// its peers through a headless Service, dials every one over mutual TLS, and
// sends them small typed frames. It carries state that must be the same on every
// replica and does not belong in a shared store: counters that converge by
// exchanging running totals (pkg/meshsync), a login made on one pod and
// presented to another, a one-time code issued by one pod and redeemed on
// another.
//
// It is best effort. A frame sent while a peer is unreachable is lost, so what
// travels must be level state that is resent or can be read back from a shared
// store, not events that must arrive. A frame can be sent with a wait for each
// peer's acknowledgement (BroadcastWait) when the next request may land on a
// peer that has to know.
//
// Each replica sends on the connections it dialed and receives on every
// connection, so a frame reaches a peer once however the pair connected.
package mesh

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

// ProtocolVersion is stamped on every frame; a frame of another version is
// dropped, so a rolling update between incompatible builds degrades to each
// replica counting its own work instead of misreading a peer.
const ProtocolVersion = 1

// TLSServerName is the DNS name every mesh certificate carries and every dial
// verifies. Peers are dialed by pod IP, which changes on every restart, so the
// certificate names the mesh, not a pod.
const TLSServerName = "xray-mesh"

const (
	path        = "/mesh"
	kindHello   = "hello"
	kindAck     = "ack"
	maxFrame    = 4 << 20
	sendBuffer  = 256
	writeWait   = 5 * time.Second
	dialTimeout = 10 * time.Second
	pongWait    = 60 * time.Second
	pingEvery   = 20 * time.Second
)

// Frame is one message on the mesh.
type Frame struct {
	V      int             `json:"v"`
	Kind   string          `json:"k"`
	Origin string          `json:"o"`
	ID     uint64          `json:"i,omitempty"` // set when the sender waits for an ack
	Data   json.RawMessage `json:"d,omitempty"`
}

// TLS is the mesh's mutual-TLS setup: both sides present a certificate and
// accept only a peer whose certificate the mesh CA signed.
type TLS struct {
	Server *tls.Config
	Client *tls.Config
}

// LoadTLS reads ca.crt, tls.crt and tls.key from dir.
func LoadTLS(dir string) (*TLS, error) {
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("mesh tls: read CA: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		return nil, fmt.Errorf("mesh tls: load key pair: %w", err)
	}
	return NewTLS(caPEM, cert)
}

// NewTLS builds the server and client configs from the CA (PEM) and this pod's
// certificate.
func NewTLS(caPEM []byte, cert tls.Certificate) (*TLS, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("mesh tls: no CA certificate in ca.crt")
	}
	return &TLS{
		Server: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		},
		Client: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			ServerName:   TLSServerName,
		},
	}, nil
}

// Config configures a Mesh.
type Config struct {
	// Self names this replica (its pod name); a frame carries it as Origin.
	Self string
	// Address is the listen address for peers, such as ":8085".
	Address string
	// Port is the port peers listen on, for dialing a discovered address.
	Port int
	// ServiceDNS is the headless Service name that resolves to every ready pod.
	ServiceDNS string
	// StaticPeers are host:port addresses dialed in addition to the Service's.
	StaticPeers []string
	// LookupHost resolves ServiceDNS; net.LookupHost when nil.
	LookupHost func(string) ([]string, error)
	// TLS is required: the mesh does not run in the clear.
	TLS *TLS
	// DialInterval is how often missing peers are dialed again; 5s when zero.
	DialInterval time.Duration
	Logger       zerolog.Logger
}

// Handler receives a frame's payload and the replica that sent it.
type Handler func(origin string, data json.RawMessage)

// Mesh is one replica's side of the mesh. Safe for concurrent use.
type Mesh struct {
	cfg Config

	hmu       sync.RWMutex
	handlers  map[string]Handler
	onConnect func(send func(kind string, v any))

	pmu   sync.Mutex
	peers map[string]*peerConn // dialed addr → connection, nil while dialing

	acks   sync.Map // id → chan struct{}
	nextID atomic.Uint64

	// Dropped counts frames not sent because a peer's buffer was full.
	Dropped atomic.Int64
}

// New builds a Mesh. It does nothing until Start.
func New(cfg Config) *Mesh {
	if cfg.DialInterval <= 0 {
		cfg.DialInterval = 5 * time.Second
	}
	if cfg.LookupHost == nil {
		cfg.LookupHost = net.LookupHost
	}
	return &Mesh{cfg: cfg, handlers: make(map[string]Handler), peers: make(map[string]*peerConn)}
}

// Handle registers fn for frames of kind. Call before Start.
func (m *Mesh) Handle(kind string, fn Handler) {
	m.hmu.Lock()
	m.handlers[kind] = fn
	m.hmu.Unlock()
}

// OnConnect registers fn, called each time this replica connects to a peer, with
// a send that goes to that peer alone: the place to replay level state a peer
// that was away has missed. Call before Start.
func (m *Mesh) OnConnect(fn func(send func(kind string, v any))) {
	m.hmu.Lock()
	m.onConnect = fn
	m.hmu.Unlock()
}

// Connected is how many peers this replica sends to now.
func (m *Mesh) Connected() int {
	m.pmu.Lock()
	defer m.pmu.Unlock()
	n := 0
	for _, pc := range m.peers {
		if pc != nil {
			n++
		}
	}
	return n
}

// NeedLeaderElection is false: every replica runs the mesh.
func (m *Mesh) NeedLeaderElection() bool { return false }

// Broadcast sends v, as JSON, to every connected peer without waiting.
func (m *Mesh) Broadcast(kind string, v any) {
	f, ok := m.frame(kind, v, 0)
	if !ok {
		return
	}
	for _, pc := range m.connected() {
		if !pc.enqueue(f) {
			m.Dropped.Add(1)
		}
	}
}

// BroadcastWait sends v to every connected peer and waits until each has handled
// it, or ctx ends. It returns how many peers acknowledged and how many were
// connected: a caller that needs every peer to know before it answers checks the
// two.
func (m *Mesh) BroadcastWait(ctx context.Context, kind string, v any) (acked, sent int) {
	peers := m.connected()
	if len(peers) == 0 {
		return 0, 0
	}
	type waiter struct {
		id uint64
		ch chan struct{}
	}
	var waits []waiter
	for _, pc := range peers {
		id := m.nextID.Add(1)
		f, ok := m.frame(kind, v, id)
		if !ok {
			return 0, len(peers)
		}
		ch := make(chan struct{}, 1)
		m.acks.Store(id, ch)
		if !pc.enqueue(f) {
			m.acks.Delete(id)
			m.Dropped.Add(1)
			continue
		}
		waits = append(waits, waiter{id, ch})
	}
	for _, w := range waits {
		select {
		case <-w.ch:
			acked++
		case <-ctx.Done():
		}
		m.acks.Delete(w.id)
	}
	return acked, len(peers)
}

func (m *Mesh) frame(kind string, v any, id uint64) (Frame, bool) {
	raw, err := json.Marshal(v)
	if err != nil {
		m.cfg.Logger.Warn().Err(err).Str("kind", kind).Msg("mesh: frame does not marshal")
		return Frame{}, false
	}
	return Frame{V: ProtocolVersion, Kind: kind, Origin: m.cfg.Self, ID: id, Data: raw}, true
}

func (m *Mesh) connected() []*peerConn {
	m.pmu.Lock()
	defer m.pmu.Unlock()
	out := make([]*peerConn, 0, len(m.peers))
	for _, pc := range m.peers {
		if pc != nil {
			out = append(out, pc)
		}
	}
	return out
}

// Start listens for peers and dials the ones it finds until ctx ends.
func (m *Mesh) Start(ctx context.Context) error {
	if m.cfg.TLS == nil {
		return errors.New("mesh: TLS is required")
	}
	if m.cfg.Self == "" {
		return errors.New("mesh: no identity for this replica")
	}
	mux := http.NewServeMux()
	mux.HandleFunc(path, m.accept)
	srv := &http.Server{Addr: m.cfg.Address, Handler: mux, TLSConfig: m.cfg.TLS.Server, ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() {
		ln, err := net.Listen("tcp", m.cfg.Address)
		if err != nil {
			errc <- fmt.Errorf("mesh: listen: %w", err)
			return
		}
		errc <- srv.Serve(tls.NewListener(ln, m.cfg.TLS.Server))
	}()
	go m.discoverLoop(ctx)
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		m.closeAll()
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (m *Mesh) closeAll() {
	m.pmu.Lock()
	defer m.pmu.Unlock()
	for _, pc := range m.peers {
		if pc != nil {
			pc.close()
		}
	}
}

// ── peers ─────────────────────────────────────────────────────────────────────

type peerConn struct {
	conn   *websocket.Conn
	out    chan Frame
	done   chan struct{}
	closed sync.Once
}

func newPeerConn(c *websocket.Conn) *peerConn {
	return &peerConn{conn: c, out: make(chan Frame, sendBuffer), done: make(chan struct{})}
}

func (p *peerConn) enqueue(f Frame) bool {
	select {
	case <-p.done:
		return false
	default:
	}
	select {
	case p.out <- f:
		return true
	default:
		return false
	}
}

func (p *peerConn) close() {
	p.closed.Do(func() {
		close(p.done)
		_ = p.conn.Close()
	})
}

// writeLoop is the connection's only writer.
func (p *peerConn) writeLoop() {
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-p.done:
			return
		case f := <-p.out:
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := p.conn.WriteJSON(f); err != nil {
				p.close()
				return
			}
		case <-ping.C:
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				p.close()
				return
			}
		}
	}
}

func (m *Mesh) discoverLoop(ctx context.Context) {
	m.dialAll(ctx)
	t := time.NewTicker(m.cfg.DialInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.dialAll(ctx)
		}
	}
}

func (m *Mesh) dialAll(ctx context.Context) {
	for _, addr := range m.discover() {
		m.pmu.Lock()
		_, taken := m.peers[addr]
		if !taken {
			m.peers[addr] = nil // claimed: no second dial of the same address
		}
		m.pmu.Unlock()
		if !taken {
			go m.dial(ctx, addr)
		}
	}
}

// discover returns the host:port of every peer to dial: the static ones and one
// per address the Service resolves to, less this pod's own.
func (m *Mesh) discover() []string {
	out := append([]string(nil), m.cfg.StaticPeers...)
	if m.cfg.ServiceDNS == "" {
		return out
	}
	ips, err := m.cfg.LookupHost(m.cfg.ServiceDNS)
	if err != nil {
		m.cfg.Logger.Debug().Err(err).Str("service", m.cfg.ServiceDNS).Msg("mesh: peer lookup failed")
		return out
	}
	own := ownAddresses()
	for _, ip := range ips {
		if _, self := own[ip]; self {
			continue
		}
		out = append(out, net.JoinHostPort(ip, fmt.Sprint(m.cfg.Port)))
	}
	return out
}

func ownAddresses() map[string]struct{} {
	set := make(map[string]struct{})
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return set
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok {
			set[n.IP.String()] = struct{}{}
		}
	}
	return set
}

func (m *Mesh) release(addr string) {
	m.pmu.Lock()
	delete(m.peers, addr)
	m.pmu.Unlock()
}

func (m *Mesh) dial(ctx context.Context, addr string) {
	defer m.release(addr)
	d := websocket.Dialer{HandshakeTimeout: dialTimeout, TLSClientConfig: m.cfg.TLS.Client}
	conn, _, err := d.DialContext(ctx, "wss://"+addr+path, nil)
	if err != nil {
		m.cfg.Logger.Debug().Err(err).Str("addr", addr).Msg("mesh: dial failed")
		return
	}
	pc := newPeerConn(conn)
	go pc.writeLoop()
	if !pc.enqueue(Frame{V: ProtocolVersion, Kind: kindHello, Origin: m.cfg.Self}) {
		pc.close()
		return
	}
	m.pmu.Lock()
	m.peers[addr] = pc
	m.pmu.Unlock()
	m.cfg.Logger.Info().Str("addr", addr).Msg("mesh: peer connected")

	m.hmu.RLock()
	oc := m.onConnect
	m.hmu.RUnlock()
	if oc != nil {
		oc(func(kind string, v any) {
			if f, ok := m.frame(kind, v, 0); ok && !pc.enqueue(f) {
				m.Dropped.Add(1)
			}
		})
	}
	m.read(pc)
	pc.close()
}

var upgrader = websocket.Upgrader{ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}

// accept serves a peer that dialed this replica. The TLS handshake has already
// required a certificate the mesh CA signed.
func (m *Mesh) accept(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	pc := newPeerConn(conn)
	go pc.writeLoop()
	m.read(pc)
	pc.close()
}

// read dispatches the connection's frames until it drops.
func (m *Mesh) read(pc *peerConn) {
	pc.conn.SetReadLimit(maxFrame)
	_ = pc.conn.SetReadDeadline(time.Now().Add(pongWait))
	pc.conn.SetPongHandler(func(string) error {
		return pc.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		var f Frame
		if err := pc.conn.ReadJSON(&f); err != nil {
			return
		}
		_ = pc.conn.SetReadDeadline(time.Now().Add(pongWait))
		if f.Origin == m.cfg.Self {
			return // dialed ourselves
		}
		if f.V != ProtocolVersion {
			continue
		}
		switch f.Kind {
		case kindHello:
		case kindAck:
			if ch, ok := m.acks.Load(f.ID); ok {
				select {
				case ch.(chan struct{}) <- struct{}{}:
				default:
				}
			}
		default:
			m.hmu.RLock()
			h := m.handlers[f.Kind]
			m.hmu.RUnlock()
			if h != nil {
				h(f.Origin, f.Data)
			}
			if f.ID != 0 {
				pc.enqueue(Frame{V: ProtocolVersion, Kind: kindAck, Origin: m.cfg.Self, ID: f.ID})
			}
		}
	}
}
