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
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// TLSStore is the interface for storing and retrieving TLS certificates.
type TLSStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// TLSConfig configures EnsureTLS.
type TLSConfig struct {
	Store    TLSStore // S3 storage for certs
	Prefix   string   // S3 key prefix (e.g. "klite-<clusterID>")
	CacheDir string   // Local cache directory (e.g. "<data-dir>/repl-tls/")
	Logger   *slog.Logger
}

// EnsureTLS ensures TLS certificates exist for replication mTLS. It checks
// the local cache first, then S3, and generates new certs if neither has them.
// Returns a tls.Config suitable for mTLS (both server and client use the same
// config with RequireAndVerifyClientCert).
func EnsureTLS(ctx context.Context, cfg TLSConfig) (*tls.Config, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Try local cache first
	tlsConf, err := loadFromCache(cfg.CacheDir)
	if err == nil {
		cfg.Logger.Info("repl TLS: loaded certificates from local cache")
		return tlsConf, nil
	}

	// Try S3
	tlsConf, err = loadFromStore(ctx, cfg)
	if err == nil {
		// Cache locally for next restart
		if cacheErr := saveToCache(ctx, cfg); cacheErr != nil {
			cfg.Logger.Warn("repl TLS: failed to cache certs locally", "err", cacheErr)
		}
		cfg.Logger.Info("repl TLS: loaded certificates from S3")
		return tlsConf, nil
	}

	// Generate new certs
	cfg.Logger.Info("repl TLS: generating new CA and node certificates")
	if genErr := generateAndUpload(ctx, cfg); genErr != nil {
		return nil, fmt.Errorf("repl TLS: generate certs: %w", genErr)
	}

	// Load the newly generated certs from S3
	tlsConf, err = loadFromStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("repl TLS: load generated certs: %w", err)
	}

	// Cache locally
	if cacheErr := saveToCache(ctx, cfg); cacheErr != nil {
		cfg.Logger.Warn("repl TLS: failed to cache certs locally", "err", cacheErr)
	}

	return tlsConf, nil
}

func s3Keys(prefix string) (caCrt, caKey, nodeCrt, nodeKey string) {
	base := prefix + "/repl"
	return base + "/ca.crt", base + "/ca.key", base + "/node.crt", base + "/node.key"
}

func cacheFiles(cacheDir string) (caCrt, caKey, nodeCrt, nodeKey string) {
	return filepath.Join(cacheDir, "ca.crt"),
		filepath.Join(cacheDir, "ca.key"),
		filepath.Join(cacheDir, "node.crt"),
		filepath.Join(cacheDir, "node.key")
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
	caCrtKey, _, nodeCrtKey, nodeKeyKey := s3Keys(cfg.Prefix)

	caCrtPEM, err := cfg.Store.Get(ctx, caCrtKey)
	if err != nil {
		return nil, err
	}
	nodeCrtPEM, err := cfg.Store.Get(ctx, nodeCrtKey)
	if err != nil {
		return nil, err
	}
	nodeKeyPEM, err := cfg.Store.Get(ctx, nodeKeyKey)
	if err != nil {
		return nil, err
	}

	return buildTLSConfig(caCrtPEM, nodeCrtPEM, nodeKeyPEM)
}

func saveToCache(ctx context.Context, cfg TLSConfig) error {
	if err := os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
		return err
	}

	caCrtKey, caKeyKey, nodeCrtKey, nodeKeyKey := s3Keys(cfg.Prefix)
	caCrtPath, caKeyPath, nodeCrtPath, nodeKeyPath := cacheFiles(cfg.CacheDir)

	files := []struct {
		s3Key    string
		diskPath string
	}{
		{caCrtKey, caCrtPath},
		{caKeyKey, caKeyPath},
		{nodeCrtKey, nodeCrtPath},
		{nodeKeyKey, nodeKeyPath},
	}

	for _, f := range files {
		data, err := cfg.Store.Get(ctx, f.s3Key)
		if err != nil {
			return fmt.Errorf("get %s: %w", f.s3Key, err)
		}
		if err := os.WriteFile(f.diskPath, data, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", f.diskPath, err)
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

	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "klite-repl-ca"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
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
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
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

	// Upload to S3
	caCrtKey, caKeyKey, nodeCrtKey, nodeKeyKey := s3Keys(cfg.Prefix)

	uploads := []struct {
		key  string
		data []byte
	}{
		{caCrtKey, caCrtPEM},
		{caKeyKey, caKeyPEM},
		{nodeCrtKey, nodeCrtPEM},
		{nodeKeyKey, nodeKeyPEM},
	}

	for _, u := range uploads {
		if err := cfg.Store.Put(ctx, u.key, u.data); err != nil {
			return fmt.Errorf("upload %s: %w", u.key, err)
		}
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
