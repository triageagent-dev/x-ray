// Package meshtest builds replicas that talk to each other over a real mutual-TLS
// mesh, for the tests of whatever uses pkg/mesh.
package meshtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/pkg/mesh"
)

// Pair returns two replicas, "a" and "b", that dial each other, not yet started.
func Pair(t *testing.T) (a, b *mesh.Mesh) {
	t.Helper()
	ca, caPEM := newCA(t)
	addrA, addrB := freeAddr(t), freeAddr(t)
	mk := func(self, addr, peer string, serial int64) *mesh.Mesh {
		tc, err := mesh.NewTLS(caPEM, issue(t, ca, serial))
		if err != nil {
			t.Fatal(err)
		}
		return mesh.New(mesh.Config{Self: self, Address: addr, StaticPeers: []string{peer}, TLS: tc, DialInterval: 50 * time.Millisecond, Logger: zerolog.Nop()})
	}
	return mk("a", addrA, addrB, 2), mk("b", addrB, addrA, 3)
}

// Run starts the replicas and returns once each is connected to a peer.
func Run(t *testing.T, ms ...*mesh.Mesh) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, m := range ms {
		go func() { _ = m.Start(ctx) }()
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, m := range ms {
		for m.Connected() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("the replicas did not connect")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) (*authority, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "mesh-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &authority{cert, key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issue(t *testing.T, ca *authority, serial int64) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: mesh.TLSServerName},
		DNSNames:  []string{mesh.TLSServerName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
