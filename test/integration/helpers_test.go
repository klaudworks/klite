package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/broker"
	"github.com/klaudworks/klite/internal/sasl"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	saslplain "github.com/twmb/franz-go/pkg/sasl/plain"
	saslscram "github.com/twmb/franz-go/pkg/sasl/scram"
)

// TestBroker wraps a running broker for tests.
type TestBroker struct {
	Addr   string
	Broker *broker.Broker
	cancel context.CancelFunc
}

// BrokerOpt is a functional option for configuring a test broker.
type BrokerOpt func(*broker.Config)

// WithAutoCreateTopics sets the auto-create-topics config.
func WithAutoCreateTopics(v bool) BrokerOpt {
	return func(c *broker.Config) { c.AutoCreateTopics = v }
}

// WithDefaultPartitions sets the default partition count.
func WithDefaultPartitions(n int) BrokerOpt {
	return func(c *broker.Config) { c.DefaultPartitions = n }
}

// WithWALEnabled enables or disables WAL persistence.
func WithWALEnabled(v bool) BrokerOpt {
	return func(c *broker.Config) { c.WALEnabled = v }
}

// WithDataDir sets the data directory (for restart tests using the same dir).
func WithDataDir(dir string) BrokerOpt {
	return func(c *broker.Config) { c.DataDir = dir }
}

// WithRingBufferMaxMem sets the global ring buffer memory budget.
func WithRingBufferMaxMem(n int64) BrokerOpt {
	return func(c *broker.Config) { c.RingBufferMaxMem = n }
}

// WithWALSegmentMaxBytes sets the max segment size before rotation.
func WithWALSegmentMaxBytes(n int64) BrokerOpt {
	return func(c *broker.Config) { c.WALSegmentMaxBytes = n }
}

// WithS3 configures S3 storage with an in-memory S3 backend.
func WithS3(s3api s3store.S3API, bucket, prefix string) BrokerOpt {
	return func(c *broker.Config) {
		c.S3Bucket = bucket
		c.S3Prefix = prefix
		c.S3API = s3api
	}
}

// WithS3FlushInterval sets the S3 flush interval.
func WithS3FlushInterval(d time.Duration) BrokerOpt {
	return func(c *broker.Config) { c.S3FlushInterval = d }
}

// WithRetentionCheckInterval sets the retention check interval.
func WithRetentionCheckInterval(d time.Duration) BrokerOpt {
	return func(c *broker.Config) { c.RetentionCheckInterval = d }
}

// WithSASL enables SASL authentication with a pre-configured store.
func WithSASL(store *sasl.Store) BrokerOpt {
	return func(c *broker.Config) {
		c.SASLEnabled = true
		c.SASLStore = store
	}
}

// WithSASLCLI enables SASL with CLI-flag user configuration.
func WithSASLCLI(mechanism, user, password string) BrokerOpt {
	return func(c *broker.Config) {
		c.SASLEnabled = true
		c.SASLMechanism = mechanism
		c.SASLUser = user
		c.SASLPassword = password
	}
}

// PlainSASLOpt returns a kgo.Opt for SASL PLAIN authentication.
func PlainSASLOpt(user, pass string) kgo.Opt {
	return kgo.SASL(saslplain.Auth{User: user, Pass: pass}.AsMechanism())
}

// Scram256SASLOpt returns a kgo.Opt for SASL SCRAM-SHA-256 authentication.
func Scram256SASLOpt(user, pass string) kgo.Opt {
	return kgo.SASL(saslscram.Auth{User: user, Pass: pass}.AsSha256Mechanism())
}

// Scram512SASLOpt returns a kgo.Opt for SASL SCRAM-SHA-512 authentication.
func Scram512SASLOpt(user, pass string) kgo.Opt {
	return kgo.SASL(saslscram.Auth{User: user, Pass: pass}.AsSha512Mechanism())
}

// StartBroker starts a klite broker in-process on a random port.
// Registers cleanup with t.Cleanup().
func StartBroker(t *testing.T, opts ...BrokerOpt) *TestBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cfg := broker.DefaultConfig()
	cfg.Listener = ln
	cfg.DataDir = t.TempDir()
	cfg.LogLevel = "debug"
	for _, o := range opts {
		o(&cfg)
	}

	b := broker.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		b.Wait()
	})
	go b.Run(ctx)

	// Wait for broker to be ready (initialization complete)
	select {
	case <-b.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready within 5s")
	}

	return &TestBroker{Addr: ln.Addr().String(), Broker: b, cancel: cancel}
}

// Stop gracefully stops the broker, triggering shutdown flush (including S3).
// Blocks until the broker has fully stopped.
func (tb *TestBroker) Stop() {
	tb.cancel()
	tb.Broker.Wait()
}

// NewClient creates a franz-go client connected to the given broker address.
func NewClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	allOpts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.RequestRetries(0), // Fail fast in tests
	}
	allOpts = append(allOpts, opts...)
	cl, err := kgo.NewClient(allOpts...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

// NewAdminClient creates a kadm admin client connected to the given broker address.
func NewAdminClient(t *testing.T, addr string) *kadm.Client {
	t.Helper()
	cl := NewClient(t, addr)
	return kadm.NewClient(cl)
}

// ProduceSync produces records synchronously and asserts no errors.
func ProduceSync(t *testing.T, cl *kgo.Client, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, records...)
	for _, r := range results {
		require.NoError(t, r.Err)
	}
}

// ProduceN produces n records to the given topic.
func ProduceN(t *testing.T, cl *kgo.Client, topic string, n int) []*kgo.Record {
	t.Helper()
	records := make([]*kgo.Record, n)
	for i := range records {
		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}
	ProduceSync(t, cl, records...)
	return records
}

// ConsumeN consumes n records from the client with a timeout.
func ConsumeN(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "timed out waiting for %d records, got %d", n, len(records))
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	return records[:n]
}
