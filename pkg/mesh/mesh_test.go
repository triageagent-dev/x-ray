package mesh

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCA is a CA and what it signs, for a mesh of replicas in one test.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "mesh-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue signs a certificate for the mesh name, as the chart's does.
func (ca *testCA) issue(t *testing.T, serial int64) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: TLSServerName},
		DNSNames:  []string{TLSServerName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

// pair starts two replicas that dial each other, and waits until both send.
func pair(t *testing.T, ca *testCA) (a, b *Mesh) {
	t.Helper()
	addrA, addrB := freeAddr(t), freeAddr(t)
	mk := func(self, addr string, peer string, serial int64) *Mesh {
		tc, err := NewTLS(ca.pem, ca.issue(t, serial))
		require.NoError(t, err)
		return New(Config{Self: self, Address: addr, StaticPeers: []string{peer}, TLS: tc, DialInterval: 50 * time.Millisecond, Logger: zerolog.Nop()})
	}
	a, b = mk("a", addrA, addrB, 2), mk("b", addrB, addrA, 3)
	return a, b
}

func run(t *testing.T, ms ...*Mesh) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, m := range ms {
		go func() { _ = m.Start(ctx) }()
	}
	require.Eventually(t, func() bool {
		for _, m := range ms {
			if m.Connected() == 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond, "the replicas connect")
}

type got struct {
	origin string
	data   string
}

func collect(m *Mesh, kind string) *safeList {
	l := &safeList{}
	m.Handle(kind, func(origin string, data json.RawMessage) {
		l.add(got{origin, string(data)})
	})
	return l
}

type safeList struct {
	mu sync.Mutex
	v  []got
}

func (l *safeList) add(g got) { l.mu.Lock(); l.v = append(l.v, g); l.mu.Unlock() }
func (l *safeList) all() []got {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]got(nil), l.v...)
}

func TestMesh_BroadcastReachesThePeerOnce(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	atB := collect(b, "x")
	atA := collect(a, "x")
	run(t, a, b)

	a.Broadcast("x", map[string]int{"n": 1})

	require.Eventually(t, func() bool { return len(atB.all()) == 1 }, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, got{"a", `{"n":1}`}, atB.all()[0])
	time.Sleep(100 * time.Millisecond)
	assert.Len(t, atB.all(), 1, "once, however the pair connected")
	assert.Empty(t, atA.all(), "a replica does not hear itself")
}

func TestMesh_BothDirections(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	atA, atB := collect(a, "x"), collect(b, "x")
	run(t, a, b)

	a.Broadcast("x", 1)
	b.Broadcast("x", 2)

	require.Eventually(t, func() bool { return len(atA.all()) == 1 && len(atB.all()) == 1 }, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, "b", atA.all()[0].origin)
	assert.Equal(t, "a", atB.all()[0].origin)
}

func TestMesh_BroadcastWaitReturnsAfterThePeerHandledIt(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	var handled sync.WaitGroup
	handled.Add(1)
	var seen bool
	var mu sync.Mutex
	b.Handle("code", func(string, json.RawMessage) {
		time.Sleep(50 * time.Millisecond) // slow enough that a send-and-forget return would beat it
		mu.Lock()
		seen = true
		mu.Unlock()
		handled.Done()
	})
	run(t, a, b)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	acked, sent := a.BroadcastWait(ctx, "code", "c1")

	assert.Equal(t, 1, sent)
	assert.Equal(t, 1, acked)
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, seen, "the ack comes after the handler ran")
}

func TestMesh_BroadcastWaitWithNoPeersIsImmediate(t *testing.T) {
	a, _ := pair(t, newTestCA(t))
	acked, sent := a.BroadcastWait(context.Background(), "x", 1)
	assert.Zero(t, acked)
	assert.Zero(t, sent)
}

func TestMesh_OnConnectReplaysToTheNewPeer(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	atB := collect(b, "state")
	a.OnConnect(func(send func(string, any)) { send("state", "level") })
	run(t, a, b)

	require.Eventually(t, func() bool { return len(atB.all()) >= 1 }, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, `"level"`, atB.all()[0].data)
}

func TestMesh_APeerWithACertificateOfAnotherCAIsRefused(t *testing.T) {
	a, _ := pair(t, newTestCA(t))
	// c trusts and presents certificates of a different CA.
	other := newTestCA(t)
	tc, err := NewTLS(other.pem, other.issue(t, 9))
	require.NoError(t, err)
	c := New(Config{Self: "c", Address: freeAddr(t), StaticPeers: []string{a.cfg.Address}, TLS: tc, DialInterval: 50 * time.Millisecond, Logger: zerolog.Nop()})
	atA := collect(a, "x")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Start(ctx) }()
	go func() { _ = c.Start(ctx) }()

	time.Sleep(400 * time.Millisecond)
	assert.Zero(t, c.Connected(), "no connection to a mesh whose CA did not sign this replica's certificate")
	c.Broadcast("x", 1)
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, atA.all())
}

func TestMesh_RequiresTLSAndAnIdentity(t *testing.T) {
	assert.Error(t, New(Config{Self: "a"}).Start(context.Background()), "no TLS")
	ca := newTestCA(t)
	tc, err := NewTLS(ca.pem, ca.issue(t, 1))
	require.NoError(t, err)
	assert.Error(t, New(Config{TLS: tc}).Start(context.Background()), "no identity")
}

func TestMesh_DiscoverSkipsItsOwnAddress(t *testing.T) {
	m := New(Config{
		Self: "a", Port: 8085, ServiceDNS: "mesh.ns.svc",
		LookupHost:  func(string) ([]string, error) { return []string{"127.0.0.1", "10.1.2.3"}, nil },
		StaticPeers: []string{"static:1"},
	})
	own := ownAddresses()
	_, haveLoopback := own["127.0.0.1"]
	want := []string{"static:1", "10.1.2.3:8085"}
	if !haveLoopback {
		want = []string{"static:1", "127.0.0.1:8085", "10.1.2.3:8085"}
	}
	assert.Equal(t, want, m.discover())
}

func TestMesh_RequestIsAnsweredByTheReplicaAsked(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	b.HandleRequest("claim", func(origin string, data json.RawMessage) any {
		return map[string]string{"from": origin, "got": string(data)}
	})
	run(t, a, b)
	// The dialer learns the name of the replica it dialed from its hello.
	require.Eventually(t, func() bool {
		var out map[string]string
		return a.Request(context.Background(), "b", "claim", "x", &out) == nil
	}, 3*time.Second, 20*time.Millisecond)

	var out map[string]string
	require.NoError(t, a.Request(context.Background(), "b", "claim", "x", &out))
	assert.Equal(t, map[string]string{"from": "a", "got": `"x"`}, out)
}

func TestMesh_RequestToAReplicaNotConnectedFails(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	run(t, a, b)
	assert.ErrorIs(t, a.Request(context.Background(), "nobody", "claim", 1, nil), ErrNoPeer)
}

func TestMesh_RequestStopsWaitingWhenTheContextEnds(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b.HandleRequest("slow", func(string, json.RawMessage) any { <-release; return 1 })
	run(t, a, b)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := a.Request(ctx, "b", "slow", 1, nil)
		return err != nil && err != ErrNoPeer
	}, 3*time.Second, 20*time.Millisecond)
}

// With a headless Service configured, holding the mesh certificate is not enough:
// the caller must be a pod the Service resolves to.
func TestMesh_RefusesACallerTheServiceDoesNotResolveTo(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	a.cfg.ServiceDNS = "mesh.ns.svc"
	a.cfg.LookupHost = func(string) ([]string, error) { return []string{"10.9.9.9"}, nil } // not b's address
	run2 := func() {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() { _ = a.Start(ctx) }()
		go func() { _ = b.Start(ctx) }()
	}
	run2()
	time.Sleep(500 * time.Millisecond)
	assert.Zero(t, b.Connected(), "b holds a valid certificate, but a does not know its address")
}

func TestMesh_AcceptsACallerTheServiceResolvesTo(t *testing.T) {
	a, b := pair(t, newTestCA(t))
	a.cfg.ServiceDNS = "mesh.ns.svc"
	a.cfg.LookupHost = func(string) ([]string, error) { return []string{"127.0.0.1"}, nil }
	run(t, a, b)
	assert.NotZero(t, b.Connected())
}
