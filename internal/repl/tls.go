package repl

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// ErrAlreadyExists is returned by TLSStore.PutIfAbsent when the key already exists.
var ErrAlreadyExists = errors.New("key already exists")

// TLSStore is the interface for storing and retrieving TLS certificates.
type TLSStore interface {
	Put(ctx context.Context, key string, data []byte) error
	// PutIfAbsent writes data only if the key does not exist.
	// Returns ErrAlreadyExists if the key is already present.
	PutIfAbsent(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// TLSConfig configures EnsureTLS.
type TLSConfig struct {
	Store     TLSStore     // S3 storage for certs
	Prefix    string       // S3 key prefix (e.g. "klite-<clusterID>")
	CacheDir  string       // Local cache directory (e.g. "<data-dir>/repl-tls/")
	ExtraSANs []string     // Additional DNS SANs for the node certificate
	Logger    *slog.Logger
	Clock     clock.Clock  // If nil, uses clock.RealClock{}
}

// EnsureTLS ensures TLS certificates exist for replication mTLS. It checks
// the local cache first, then S3, and generates new certs if neither has them.
// Returns a tls.Config suitable for mTLS (both server and client use the same
// config with RequireAndVerifyClientCert).
func EnsureTLS(ctx context.Context, cfg TLSConfig) (*tls.Config, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}

	// Try local cache first
	tlsConf, err := loadFromCache(cfg.CacheDir)
	if err == nil {
		cfg.Logger.Info("repl TLS: loaded certificates from local cache")
		return tlsConf, nil
	}

	// Try S3 (single bundled object)
	tlsConf, err = loadFromStore(ctx, cfg)
	if err == nil {
		// Cache locally for next restart
		if cacheErr := saveBundleToCache(ctx, cfg); cacheErr != nil {
			cfg.Logger.Warn("repl TLS: failed to cache certs locally", "err", cacheErr)
		}
		cfg.Logger.Info("repl TLS: loaded certificates from S3")
		return tlsConf, nil
	}

	// Generate new certs and upload as a single atomic bundle.
	cfg.Logger.Info("repl TLS: generating new CA and node certificates")
	if genErr := generateAndUpload(ctx, cfg); genErr != nil {
		if !errors.Is(genErr, ErrAlreadyExists) {
			return nil, fmt.Errorf("repl TLS: generate certs: %w", genErr)
		}
		// Another node won the race — load its certs from S3.
		cfg.Logger.Info("repl TLS: another node generated certs first, loading from S3")
	}

	// Load the certs from S3 (either ours or the winner's).
	tlsConf, err = loadFromStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("repl TLS: load generated certs: %w", err)
	}

	// Cache locally
	if cacheErr := saveBundleToCache(ctx, cfg); cacheErr != nil {
		cfg.Logger.Warn("repl TLS: failed to cache certs locally", "err", cacheErr)
	}

	return tlsConf, nil
}

// bundleKey returns the S3 key for the single TLS certificate bundle.
func bundleKey(prefix string) string {
	return prefix + "/repl/certs.bundle"
}

func cacheFiles(cacheDir string) (caCrt, caKey, nodeCrt, nodeKey string) {
	return filepath.Join(cacheDir, "ca.crt"),
		filepath.Join(cacheDir, "ca.key"),
		filepath.Join(cacheDir, "node.crt"),
		filepath.Join(cacheDir, "node.key")
}

// PEM block type markers used to distinguish cert components in the bundle.
const (
	pemCACert  = "CERTIFICATE"         // CA cert: first CERTIFICATE block
	pemCAKey   = "EC PRIVATE KEY"      // CA key: first EC PRIVATE KEY block
	pemNodeCrt = "KLITE NODE CERT"     // custom marker
	pemNodeKey = "KLITE NODE PRIV KEY" // custom marker
)

// marshalBundle concatenates all four PEM components into a single blob.
// The node cert and key use custom PEM type markers so they can be
// distinguished from the CA cert and key during parsing.
func marshalBundle(caCrt, caKey, nodeCrt, nodeKey []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(caCrt)
	buf.Write(caKey)
	// Re-encode node cert with a distinct PEM type so unmarshal can
	// tell it apart from the CA certificate block.
	blk, _ := pem.Decode(nodeCrt)
	if blk == nil {
		return nil, fmt.Errorf("node certificate is not valid PEM")
	}
	buf.Write(pem.EncodeToMemory(&pem.Block{Type: pemNodeCrt, Bytes: blk.Bytes}))
	blk, _ = pem.Decode(nodeKey)
	if blk == nil {
		return nil, fmt.Errorf("node key is not valid PEM")
	}
	buf.Write(pem.EncodeToMemory(&pem.Block{Type: pemNodeKey, Bytes: blk.Bytes}))
	return buf.Bytes(), nil
}

// unmarshalBundle splits the concatenated PEM blob back into its four
// components, returning standard PEM types for the node cert/key so
// crypto/tls can parse them.
func unmarshalBundle(data []byte) (caCrt, caKey, nodeCrt, nodeKey []byte, err error) {
	rest := data
	for {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}
		switch blk.Type {
		case pemCACert:
			caCrt = pem.EncodeToMemory(blk)
		case pemCAKey:
			caKey = pem.EncodeToMemory(blk)
		case pemNodeCrt:
			// Re-encode as standard CERTIFICATE for crypto/tls
			nodeCrt = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: blk.Bytes})
		case pemNodeKey:
			// Re-encode as standard EC PRIVATE KEY for crypto/tls
			nodeKey = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: blk.Bytes})
		}
	}
	if caCrt == nil || caKey == nil || nodeCrt == nil || nodeKey == nil {
		return nil, nil, nil, nil, fmt.Errorf("incomplete TLS bundle: missing one or more PEM blocks")
	}
	return caCrt, caKey, nodeCrt, nodeKey, nil
}

func loadFromCache(cacheDir string) (*tls.Config, error) {
	caCrtPath, _, nodeCrtPath, nodeKeyPath := cacheFiles(cacheDir)

	caCrtPEM, err := os.ReadFile(caCrtPath)
	if err != nil {
		return nil, err
	}
	nodeCrtPEM, err := os.ReadFile(nodeCrtPath)
	if err != nil {
		return nil, err
	}
	nodeKeyPEM, err := os.ReadFile(nodeKeyPath)
	if err != nil {
		return nil, err
	}

	return buildTLSConfig(caCrtPEM, nodeCrtPEM, nodeKeyPEM)
}

func loadFromStore(ctx context.Context, cfg TLSConfig) (*tls.Config, error) {
	key := bundleKey(cfg.Prefix)
	data, err := cfg.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	caCrt, _, nodeCrt, nodeKey, err := unmarshalBundle(data)
	if err != nil {
		return nil, err
	}
	return buildTLSConfig(caCrt, nodeCrt, nodeKey)
}

// saveBundleToCache downloads the bundle from S3, splits it, and writes
// individual files to the local cache directory.
func saveBundleToCache(ctx context.Context, cfg TLSConfig) error {
	if err := os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
		return err
	}

	key := bundleKey(cfg.Prefix)
	data, err := cfg.Store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}

	caCrt, caKey, nodeCrt, nodeKey, err := unmarshalBundle(data)
	if err != nil {
		return err
	}

	caCrtPath, caKeyPath, nodeCrtPath, nodeKeyPath := cacheFiles(cfg.CacheDir)
	files := []struct {
		path string
		data []byte
	}{
		{caCrtPath, caCrt},
		{caKeyPath, caKey},
		{nodeCrtPath, nodeCrt},
		{nodeKeyPath, nodeKey},
	}
	for _, f := range files {
		if err := os.WriteFile(f.path, f.data, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", f.path, err)
		}
	}
	return nil
}

func generateAndUpload(ctx context.Context, cfg TLSConfig) error {
	// Generate CA key pair
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}

	caSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate CA serial: %w", err)
	}

	now := cfg.Clock.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "klite-repl-ca"},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caPriv.PublicKey, caPriv)
	if err != nil {
		return fmt.Errorf("create CA cert: %w", err)
	}

	// Generate node key pair
	nodePriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate node key: %w", err)
	}

	nodeSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate node serial: %w", err)
	}

	nodeTemplate := &x509.Certificate{
		SerialNumber: nodeSerial,
		Subject:      pkix.Name{CommonName: "klite-repl-node"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     tlsDNSNames(cfg.ExtraSANs),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return fmt.Errorf("parse CA cert: %w", err)
	}

	nodeCertDER, err := x509.CreateCertificate(rand.Reader, nodeTemplate, caCert, &nodePriv.PublicKey, caPriv)
	if err != nil {
		return fmt.Errorf("create node cert: %w", err)
	}

	// Encode PEM
	caCrtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	caKeyDER, err := x509.MarshalECPrivateKey(caPriv)
	if err != nil {
		return fmt.Errorf("marshal CA key: %w", err)
	}
	caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER})

	nodeCrtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeCertDER})
	nodeKeyDER, err := x509.MarshalECPrivateKey(nodePriv)
	if err != nil {
		return fmt.Errorf("marshal node key: %w", err)
	}
	nodeKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: nodeKeyDER})

	// Upload as a single atomic bundle via PutIfAbsent. Either the whole
	// bundle is written or nothing is — no partial-upload risk.
	bundle, err := marshalBundle(caCrtPEM, caKeyPEM, nodeCrtPEM, nodeKeyPEM)
	if err != nil {
		return fmt.Errorf("marshal TLS bundle: %w", err)
	}
	key := bundleKey(cfg.Prefix)
	if err := cfg.Store.PutIfAbsent(ctx, key, bundle); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("upload %s: %w", key, err)
	}

	// Also cache locally
	if err := os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	caCrtPath, caKeyPath, nodeCrtPath, nodeKeyPath := cacheFiles(cfg.CacheDir)
	localFiles := []struct {
		path string
		data []byte
	}{
		{caCrtPath, caCrtPEM},
		{caKeyPath, caKeyPEM},
		{nodeCrtPath, nodeCrtPEM},
		{nodeKeyPath, nodeKeyPEM},
	}

	for _, f := range localFiles {
		if err := os.WriteFile(f.path, f.data, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", f.path, err)
		}
	}

	return nil
}

// tlsDNSNames returns the SAN DNS names for the replication TLS certificate.
// Always includes "localhost"; extra entries are appended as-is.
func tlsDNSNames(extra []string) []string {
	names := []string{"localhost"}
	return append(names, extra...)
}

func buildTLSConfig(caCrtPEM, nodeCrtPEM, nodeKeyPEM []byte) (*tls.Config, error) {
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCrtPEM) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	cert, err := tls.X509KeyPair(nodeCrtPEM, nodeKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load node cert+key: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		RootCAs:      caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
