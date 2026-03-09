package repl

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"
)

// memTLSStore is an in-memory TLSStore for testing.
type memTLSStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemTLSStore() *memTLSStore {
	return &memTLSStore{data: make(map[string][]byte)}
}

func (m *memTLSStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *memTLSStore) PutIfAbsent(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[key]; exists {
		return ErrAlreadyExists
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *memTLSStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *memTLSStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

func TestEnsureTLSGenerates(t *testing.T) {
	t.Parallel()

	store := newMemTLSStore()
	cacheDir := t.TempDir()

	ctx := context.Background()
	cfg := TLSConfig{
		Store:    store,
		Prefix:   "test-prefix",
		CacheDir: cacheDir,
	}

	tlsCfg, err := EnsureTLS(ctx, cfg)
	if err != nil {
		t.Fatal("EnsureTLS:", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}

	// Verify single bundle object uploaded to S3
	if store.count() != 1 {
		t.Fatalf("expected 1 S3 object (bundle), got %d", store.count())
	}

	// Verify certs are cached locally
	_, err = loadFromCache(cacheDir)
	if err != nil {
		t.Fatal("expected certs to be cached locally:", err)
	}
}

func TestEnsureTLSLoadsFromS3(t *testing.T) {
	t.Parallel()

	store := newMemTLSStore()
	cacheDir1 := t.TempDir()

	ctx := context.Background()
	prefix := "test-prefix"

	// Generate certs first
	cfg1 := TLSConfig{
		Store:    store,
		Prefix:   prefix,
		CacheDir: cacheDir1,
	}
	_, err := EnsureTLS(ctx, cfg1)
	if err != nil {
		t.Fatal("first EnsureTLS:", err)
	}

	// Now load from S3 with a fresh cache dir
	cacheDir2 := t.TempDir()
	cfg2 := TLSConfig{
		Store:    store,
		Prefix:   prefix,
		CacheDir: cacheDir2,
	}
	tlsCfg, err := EnsureTLS(ctx, cfg2)
	if err != nil {
		t.Fatal("second EnsureTLS:", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}

	// Verify local cache was populated
	_, err = loadFromCache(cacheDir2)
	if err != nil {
		t.Fatal("expected certs cached in new dir:", err)
	}
}

func TestEnsureTLSHandshake(t *testing.T) {
	t.Parallel()

	store := newMemTLSStore()
	cacheDir := t.TempDir()
	ctx := context.Background()

	cfg := TLSConfig{
		Store:    store,
		Prefix:   "test",
		CacheDir: cacheDir,
	}

	tlsCfg, err := EnsureTLS(ctx, cfg)
	if err != nil {
		t.Fatal("EnsureTLS:", err)
	}

	// Do mTLS handshake over net.Pipe
	serverCfg := tlsCfg.Clone()
	clientCfg := tlsCfg.Clone()
	clientCfg.InsecureSkipVerify = true // skip hostname verification (we verify via shared CA, not hostname)

	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck
	defer client.Close() //nolint:errcheck

	errCh := make(chan error, 2)
	go func() {
		tlsServer := tls.Server(server, serverCfg)
		errCh <- tlsServer.Handshake()
	}()
	go func() {
		tlsClient := tls.Client(client, clientCfg)
		errCh <- tlsClient.Handshake()
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal("mTLS handshake failed:", err)
		}
	}
}

func TestEnsureTLSRejectsRogue(t *testing.T) {
	t.Parallel()

	store := newMemTLSStore()
	cacheDir := t.TempDir()
	ctx := context.Background()

	cfg := TLSConfig{
		Store:    store,
		Prefix:   "test",
		CacheDir: cacheDir,
	}

	tlsCfg, err := EnsureTLS(ctx, cfg)
	if err != nil {
		t.Fatal("EnsureTLS:", err)
	}

	// Generate a rogue cert signed by a different CA
	rogueCA, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rogueCASerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	rogueCACert := &x509.Certificate{
		SerialNumber:          rogueCASerial,
		Subject:               pkix.Name{CommonName: "rogue-ca"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	rogueCACertDER, err := x509.CreateCertificate(rand.Reader, rogueCACert, rogueCACert, &rogueCA.PublicKey, rogueCA)
	if err != nil {
		t.Fatal(err)
	}

	rogueNodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rogueNodeSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	rogueNodeCert := &x509.Certificate{
		SerialNumber: rogueNodeSerial,
		Subject:      pkix.Name{CommonName: "rogue-node"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	parsedRogueCA, err := x509.ParseCertificate(rogueCACertDER)
	if err != nil {
		t.Fatal(err)
	}
	rogueNodeCertDER, err := x509.CreateCertificate(rand.Reader, rogueNodeCert, parsedRogueCA, &rogueNodeKey.PublicKey, rogueCA)
	if err != nil {
		t.Fatal(err)
	}

	rogueNodeCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rogueNodeCertDER})
	rogueNodeKeyDER, err := x509.MarshalECPrivateKey(rogueNodeKey)
	if err != nil {
		t.Fatal(err)
	}
	rogueNodeKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: rogueNodeKeyDER})
	rogueCACertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rogueCACertDER})

	roguePool := x509.NewCertPool()
	roguePool.AppendCertsFromPEM(rogueCACertPEM)
	rogueTLSCert, err := tls.X509KeyPair(rogueNodeCertPEM, rogueNodeKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	rogueCfg := &tls.Config{
		Certificates: []tls.Certificate{rogueTLSCert},
		RootCAs:      roguePool,
		ClientCAs:    roguePool,
		MinVersion:   tls.VersionTLS13,
	}

	// Attempt handshake — should fail
	serverPipe, clientPipe := net.Pipe()

	serverCfg := tlsCfg.Clone()

	// Set deadlines to prevent indefinite blocking
	deadline := time.Now().Add(5 * time.Second)
	_ = serverPipe.SetDeadline(deadline)
	_ = clientPipe.SetDeadline(deadline)

	errCh := make(chan error, 2)
	go func() {
		tlsServer := tls.Server(serverPipe, serverCfg)
		errCh <- tlsServer.Handshake()
		_ = serverPipe.Close()
	}()
	go func() {
		// Client uses InsecureSkipVerify so it doesn't reject on hostname, only on CA
		rogueCfg.InsecureSkipVerify = true
		tlsClient := tls.Client(clientPipe, rogueCfg)
		errCh <- tlsClient.Handshake()
		_ = clientPipe.Close()
	}()

	// At least one side should fail
	var handshakeFailed bool
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			handshakeFailed = true
		}
	}

	if !handshakeFailed {
		t.Fatal("expected rogue handshake to fail")
	}
}

func TestEnsureTLSConcurrentRace(t *testing.T) {
	t.Parallel()

	store := newMemTLSStore()
	ctx := context.Background()
	prefix := "race-test"

	// Two "nodes" call EnsureTLS concurrently on the same store.
	// Both should get a valid TLS config using the same CA.
	const n = 2
	configs := make([]*tls.Config, n)
	errs := make([]error, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cfg := TLSConfig{
				Store:    store,
				Prefix:   prefix,
				CacheDir: t.TempDir(),
			}
			configs[idx], errs[idx] = EnsureTLS(ctx, cfg)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("node %d: EnsureTLS failed: %v", i, errs[i])
		}
		if configs[i] == nil {
			t.Fatalf("node %d: nil tls.Config", i)
		}
	}

	// Both configs must share the same CA — verify mTLS handshake succeeds.
	serverCfg := configs[0].Clone()
	clientCfg := configs[1].Clone()
	clientCfg.InsecureSkipVerify = true

	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck
	defer client.Close() //nolint:errcheck

	errCh := make(chan error, 2)
	go func() {
		tlsServer := tls.Server(server, serverCfg)
		errCh <- tlsServer.Handshake()
	}()
	go func() {
		tlsClient := tls.Client(client, clientCfg)
		errCh <- tlsClient.Handshake()
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("mTLS handshake between concurrently-initialized nodes failed: %v", err)
		}
	}
}
