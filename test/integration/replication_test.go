package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/broker"
	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/lease/memlease"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReplicaPair holds two brokers sharing the same InMemoryS3 and MemCluster.
type ReplicaPair struct {
	Primary  *TestBroker
	Standby  *TestBroker
	Cluster  *memlease.MemCluster
	ElectorA *memlease.MemElector
	ElectorB *memlease.MemElector
}

// WithLeaseElector sets the lease elector for a test broker.
func WithLeaseElector(e lease.Elector) BrokerOpt {
	return func(c *broker.Config) {
		c.LeaseElector = e
	}
}

// WithReplicationAddr sets the replication listen address.
func WithReplicationAddr(addr string) BrokerOpt {
	return func(c *broker.Config) {
		c.ReplicationAddr = addr
	}
}

// StartReplicaPair starts a primary and standby broker sharing the same InMemoryS3 and MemCluster.
func StartReplicaPair(t *testing.T, extraOpts ...BrokerOpt) *ReplicaPair {
	t.Helper()

	memS3 := s3store.NewInMemoryS3()
	cluster := memlease.NewCluster()
	electorA := cluster.NewElector("node-a")
	electorB := cluster.NewElector("node-b")

	lnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	replLnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	lnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Build broker A config
	cfgA := broker.DefaultConfig()
	cfgA.Listener = lnA
	cfgA.HealthAddr = healthLnA.Addr().String()
	cfgA.HealthListener = healthLnA
	cfgA.DataDir = t.TempDir()
	cfgA.LogLevel = "debug"
	cfgA.ReplicationAddr = replLnA.Addr().String()
	cfgA.LeaseElector = electorA
	cfgA.S3Bucket = "test-bucket"
	cfgA.S3API = memS3
	cfgA.ChunkPoolMemory = 32 * 1024 * 1024
	for _, o := range extraOpts {
		o(&cfgA)
	}

	brokerA := broker.New(cfgA)
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelA()
		brokerA.Wait()
	})
	go func() { _ = brokerA.Run(ctxA) }()

	// Build broker B config
	cfgB := broker.DefaultConfig()
	cfgB.Listener = lnB
	cfgB.HealthAddr = healthLnB.Addr().String()
	cfgB.HealthListener = healthLnB
	cfgB.DataDir = t.TempDir()
	cfgB.LogLevel = "debug"
	cfgB.ReplicationAddr = "127.0.0.1:0" // will listen on random port
	cfgB.LeaseElector = electorB
	cfgB.S3Bucket = "test-bucket"
	cfgB.S3API = memS3
	cfgB.ChunkPoolMemory = 32 * 1024 * 1024
	for _, o := range extraOpts {
		o(&cfgB)
	}

	brokerB := broker.New(cfgB)
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelB()
		brokerB.Wait()
	})
	go func() { _ = brokerB.Run(ctxB) }()

	// Wait for both brokers to be ready
	select {
	case <-brokerA.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker A did not become ready")
	}
	select {
	case <-brokerB.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker B did not become ready")
	}

	// Wait for electors to be running (callbacks registered) so Elect() works
	select {
	case <-electorA.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("elector A did not become ready")
	}
	select {
	case <-electorB.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("elector B did not become ready")
	}

	return &ReplicaPair{
		Primary:  &TestBroker{Addr: lnA.Addr().String(), Broker: brokerA, cancel: cancelA},
		Standby:  &TestBroker{Addr: lnB.Addr().String(), Broker: brokerB, cancel: cancelB},
		Cluster:  cluster,
		ElectorA: electorA,
		ElectorB: electorB,
	}
}

func TestReplicationSingleNodeMode(t *testing.T) {
	t.Parallel()

	// Start a broker WITHOUT --replication-addr
	tb := StartBroker(t)

	// Produce and consume normally
	cl := NewClient(t, tb.Addr, kgo.AllowAutoTopicCreation())
	topic := "test-single-node"
	ProduceN(t, cl, topic, 100)

	consumer := NewClient(t, tb.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 100, 10*time.Second)
	require.Len(t, records, 100)
}

func TestReplicationBasic(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)

	// Elect A as primary
	pair.ElectorA.Elect()
	time.Sleep(500 * time.Millisecond) // let primary start listener

	// Produce records to primary
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)

	topic := "test-repl-basic"
	records := make([]*kgo.Record, 50)
	for i := range records {
		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, records...)
	for _, r := range results {
		require.NoError(t, r.Err, "produce failed")
	}

	// Verify records can be consumed from primary
	consumer := NewClient(t, pair.Primary.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := ConsumeN(t, consumer, 50, 10*time.Second)
	require.Len(t, consumed, 50)
}

func TestReplicationFailover(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)

	// Elect A as primary
	pair.ElectorA.Elect()
	time.Sleep(500 * time.Millisecond)

	// Produce records
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	topic := "test-failover"
	ProduceN(t, cl, topic, 50)
	cl.Close()

	// Demote A, promote B
	pair.ElectorA.Demote()
	time.Sleep(200 * time.Millisecond)
	pair.ElectorB.Elect()
	time.Sleep(500 * time.Millisecond) // let B start listener

	// Connect to new primary (B) and verify we can produce
	cl2 := NewClient(t, pair.Standby.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl2, topic, 50)
	cl2.Close()
}

func TestReplicationDemotionStopsWrites(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)

	// Elect A as primary
	pair.ElectorA.Elect()
	time.Sleep(500 * time.Millisecond)

	// Verify we can produce to A
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl, "test-demotion", 10)
	cl.Close()

	// Demote A
	pair.ElectorA.Demote()
	time.Sleep(200 * time.Millisecond)

	// Attempting to connect to demoted A should fail (connection refused)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := net.DialTimeout("tcp", pair.Primary.Addr, 2*time.Second)
	if err == nil {
		// If dial succeeds, produce should fail
		cl2, err := kgo.NewClient(
			kgo.SeedBrokers(pair.Primary.Addr),
			kgo.RequestRetries(0),
		)
		if err == nil {
			rec := &kgo.Record{Topic: "test-demotion", Value: []byte("should-fail")}
			results := cl2.ProduceSync(ctx, rec)
			if len(results) > 0 && results[0].Err == nil {
				t.Fatal("expected produce to fail after demotion")
			}
			cl2.Close()
		}
	}
	// If dial fails, that's the expected behavior — connection refused
}
