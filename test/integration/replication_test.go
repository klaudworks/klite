package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/broker"
	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/lease/memlease"
	"github.com/klaudworks/klite/internal/lease/s3lease"
	s3store "github.com/klaudworks/klite/internal/s3"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReplicaPair holds two brokers sharing the same InMemoryS3 and MemCluster.
type ReplicaPair struct {
	Primary     *TestBroker
	Standby     *TestBroker
	Cluster     *memlease.MemCluster
	ElectorA    *memlease.MemElector
	ElectorB    *memlease.MemElector
	HealthAddrA string
	HealthAddrB string
	S3          *s3store.InMemoryS3
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

// allocPort binds a random port, extracts the address, and immediately
// closes the listener so the port is free for the broker to use.
// Note: there is a small TOCTOU window where the OS could reassign the
// port between Close and the broker's bind. This is a pragmatic trade-off;
// flaky failures from this are extremely rare on loopback.
func allocPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// waitPrimary polls the /primaryz endpoint until it returns 200 or the
// timeout expires. This replaces fixed time.Sleep calls after Elect().
func waitPrimary(t *testing.T, healthAddr string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.After(timeout)
	for {
		resp, err := client.Get("http://" + healthAddr + "/primaryz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("node at %s did not become primary within %s", healthAddr, timeout)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitStandby polls the /primaryz endpoint until it returns 503 "standby"
// or the timeout expires.
func waitStandby(t *testing.T, healthAddr string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.After(timeout)
	for {
		resp, err := client.Get("http://" + healthAddr + "/primaryz")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable && string(body) == "standby" {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("node at %s did not become standby within %s", healthAddr, timeout)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// StartReplicaPair starts a primary and standby broker sharing the same InMemoryS3 and MemCluster.
func StartReplicaPair(t *testing.T, extraOpts ...BrokerOpt) *ReplicaPair {
	t.Helper()

	memS3 := s3store.NewInMemoryS3()
	cluster := memlease.NewCluster()

	// Allocate replication ports up front so MemElectors can advertise them.
	replAddrA := allocPort(t)
	replAddrB := allocPort(t)

	electorA := cluster.NewElector("node-a", replAddrA)
	electorB := cluster.NewElector("node-b", replAddrB)

	lnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	lnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	healthAddrA := healthLnA.Addr().String()
	healthAddrB := healthLnB.Addr().String()

	// Both brokers must share a cluster ID so they use the same S3 prefix
	// and therefore the same TLS certificates for mTLS replication.
	sharedClusterID := "test-repl-cluster"

	// Build broker A config
	cfgA := broker.DefaultConfig()
	cfgA.Listener = lnA
	cfgA.HealthAddr = healthAddrA
	cfgA.HealthListener = healthLnA
	cfgA.DataDir = t.TempDir()
	cfgA.LogLevel = "debug"
	cfgA.ClusterID = sharedClusterID
	cfgA.ReplicationAddr = replAddrA
	cfgA.ReplicationAdvertisedAddr = replAddrA
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
	cfgB.HealthAddr = healthAddrB
	cfgB.HealthListener = healthLnB
	cfgB.DataDir = t.TempDir()
	cfgB.LogLevel = "debug"
	cfgB.ClusterID = sharedClusterID
	cfgB.ReplicationAddr = replAddrB
	cfgB.ReplicationAdvertisedAddr = replAddrB
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
		Primary:     &TestBroker{Addr: lnA.Addr().String(), Broker: brokerA, cancel: cancelA},
		Standby:     &TestBroker{Addr: lnB.Addr().String(), Broker: brokerB, cancel: cancelB},
		Cluster:     cluster,
		ElectorA:    electorA,
		ElectorB:    electorB,
		HealthAddrA: healthAddrA,
		HealthAddrB: healthAddrB,
		S3:          memS3,
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
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

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
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

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
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

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
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

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
	waitStandby(t, pair.HealthAddrA, 5*time.Second)

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

func TestReplicationDataContinuity(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)

	// Elect A as primary
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

	// Wait for B to connect as standby receiver
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond)

	// Produce records to A
	topic := "test-data-continuity"
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl, topic, 100)
	cl.Close()

	// Allow replication to catch up
	time.Sleep(300 * time.Millisecond)

	// Failover: demote A, promote B
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// Consume from B — all 100 records produced to A must be available
	consumer := NewClient(t, pair.Standby.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	records := ConsumeN(t, consumer, 100, 10*time.Second)
	require.Len(t, records, 100)

	// Verify record contents
	for i, r := range records {
		require.Equal(t, fmt.Sprintf("key-%d", i), string(r.Key))
		require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value))
	}
}

func TestReplicationDoubleFailover(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)
	topic := "test-double-failover"

	// Phase 1: A is primary, produce 50 records
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl, topic, 50)
	cl.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Phase 2: Failover to B, produce 50 more
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	cl2 := NewClient(t, pair.Standby.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	for i := 50; i < 100; i++ {
		ProduceSync(t, cl2, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}
	cl2.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Phase 3: Failover back to A, produce 50 more
	pair.ElectorB.Demote()
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

	cl3 := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	for i := 100; i < 150; i++ {
		ProduceSync(t, cl3, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}

	// Consume all 150 from A (now primary again)
	consumer := NewClient(t, pair.Primary.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 150, 15*time.Second)
	require.Len(t, records, 150)
}

func TestReplicationRoleChangeHookSequence(t *testing.T) {
	t.Parallel()

	memS3 := s3store.NewInMemoryS3()
	cluster := memlease.NewCluster()

	replAddrA := allocPort(t)
	electorA := cluster.NewElector("node-a", replAddrA)

	lnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	var events []string

	hook := &mockRoleHook{
		onSetPrimary: func() {
			mu.Lock()
			events = append(events, "set-primary")
			mu.Unlock()
		},
		onClearPrimary: func() {
			mu.Lock()
			events = append(events, "clear-primary")
			mu.Unlock()
		},
	}

	cfgA := broker.DefaultConfig()
	cfgA.Listener = lnA
	cfgA.HealthAddr = healthLnA.Addr().String()
	cfgA.HealthListener = healthLnA
	cfgA.DataDir = t.TempDir()
	cfgA.LogLevel = "debug"
	cfgA.ReplicationAddr = replAddrA
	cfgA.ReplicationAdvertisedAddr = replAddrA
	cfgA.LeaseElector = electorA
	cfgA.S3Bucket = "test-bucket"
	cfgA.S3API = memS3
	cfgA.ChunkPoolMemory = 32 * 1024 * 1024
	cfgA.RoleChangeHook = hook

	brokerA := broker.New(cfgA)
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelA()
		brokerA.Wait()
	})
	go func() { _ = brokerA.Run(ctxA) }()

	select {
	case <-brokerA.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready")
	}

	select {
	case <-electorA.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("elector did not become ready")
	}

	healthAddr := healthLnA.Addr().String()

	// Promote
	electorA.Elect()
	waitPrimary(t, healthAddr, 5*time.Second)

	// Demote
	electorA.Demote()
	waitStandby(t, healthAddr, 5*time.Second)

	// Re-promote
	electorA.Elect()
	waitPrimary(t, healthAddr, 5*time.Second)

	// Graceful shutdown (should call ClearPrimary)
	cancelA()
	brokerA.Wait()

	mu.Lock()
	defer mu.Unlock()

	expected := []string{"set-primary", "clear-primary", "set-primary", "clear-primary"}
	require.Equal(t, expected, events, "hook call sequence")
}

type mockRoleHook struct {
	onSetPrimary   func()
	onClearPrimary func()
}

func (m *mockRoleHook) SetPrimary()   { m.onSetPrimary() }
func (m *mockRoleHook) ClearPrimary() { m.onClearPrimary() }

func TestReplicationPrimaryzFailover(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)
	client := &http.Client{Timeout: 2 * time.Second}

	// Before election: both should return 503 from /primaryz
	respA, err := client.Get("http://" + pair.HealthAddrA + "/primaryz")
	require.NoError(t, err)
	bodyA, _ := io.ReadAll(respA.Body)
	_ = respA.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, respA.StatusCode,
		"/primaryz A before election: got %d body=%q", respA.StatusCode, bodyA)

	// Elect A
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

	// A should be 200, B should be 503
	respA, err = client.Get("http://" + pair.HealthAddrA + "/primaryz")
	require.NoError(t, err)
	bodyA, _ = io.ReadAll(respA.Body)
	_ = respA.Body.Close()
	require.Equal(t, http.StatusOK, respA.StatusCode,
		"/primaryz A after election: got %d body=%q", respA.StatusCode, bodyA)
	require.Equal(t, "primary", string(bodyA))

	respB, err := client.Get("http://" + pair.HealthAddrB + "/primaryz")
	require.NoError(t, err)
	bodyB, _ := io.ReadAll(respB.Body)
	_ = respB.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, respB.StatusCode,
		"/primaryz B while A is primary: got %d body=%q", respB.StatusCode, bodyB)
	require.Equal(t, "standby", string(bodyB))

	// Failover to B
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// A should be 503, B should be 200
	respA, err = client.Get("http://" + pair.HealthAddrA + "/primaryz")
	require.NoError(t, err)
	bodyA, _ = io.ReadAll(respA.Body)
	_ = respA.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, respA.StatusCode,
		"/primaryz A after demotion: got %d body=%q", respA.StatusCode, bodyA)

	respB, err = client.Get("http://" + pair.HealthAddrB + "/primaryz")
	require.NoError(t, err)
	bodyB, _ = io.ReadAll(respB.Body)
	_ = respB.Body.Close()
	require.Equal(t, http.StatusOK, respB.StatusCode,
		"/primaryz B after promotion: got %d body=%q", respB.StatusCode, bodyB)
	require.Equal(t, "primary", string(bodyB))
}

func TestReplicationGracefulShutdownFastFailover(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t)

	// Elect A as primary
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	// Produce records
	topic := "test-graceful-failover"
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl, topic, 50)
	cl.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Gracefully stop A (calls Release(), standby should promote quickly)
	pair.Primary.Stop()

	// Promote B (in production, the standby detects the released lease)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// B should be able to serve the data
	consumer := NewClient(t, pair.Standby.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	records := ConsumeN(t, consumer, 50, 10*time.Second)
	require.Len(t, records, 50)
}

// TestReplicationPromotionWithS3Data verifies that after failover, the newly
// promoted primary serves data from both S3 (cold tier) and WAL/chunks (hot
// tier). This is the gap between the existing replication tests (which don't
// flush to S3) and the S3 tests (which don't use replication).
func TestReplicationPromotionWithS3Data(t *testing.T) {
	t.Parallel()

	pair := StartReplicaPair(t,
		WithS3FlushInterval(1*time.Second),
		WithS3FlushCheckInterval(200*time.Millisecond),
	)

	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	topic := "test-repl-s3-promotion"
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl, topic, 50)

	// Wait for S3 flush to complete on the primary
	require.Eventually(t, func() bool {
		for _, k := range pair.S3.Keys() {
			if strings.Contains(k, topic) && strings.HasSuffix(k, ".obj") {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "S3 flush did not occur")

	// Produce more records that stay in WAL/chunks (not yet flushed to S3)
	for i := 50; i < 100; i++ {
		ProduceSync(t, cl, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}
	cl.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Failover: demote A, promote B
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// Verify HW is correct on the new primary
	admin := NewAdminClient(t, pair.Standby.Addr)
	offsets, err := admin.ListEndOffsets(context.Background(), topic)
	require.NoError(t, err)
	lo, ok := offsets.Lookup(topic, 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(100), lo.Offset, "HW should be 100 after promotion")

	// Consume all 100 records: 0-49 from S3 cold path, 50-99 from WAL/chunks
	consumer := NewClient(t, pair.Standby.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, 100, 15*time.Second)
	require.Len(t, records, 100)
	for i, r := range records {
		require.Equal(t, fmt.Sprintf("key-%d", i), string(r.Key))
		require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value))
	}
}

// TestReplicationPromotionWithWALPressure reproduces the data loss bug observed
// in the e2e test: when a standby's WAL is under disk pressure, old segments
// are pruned (PruneSegment), removing WAL index entries. If those entries cover
// offsets that haven't been flushed to S3 by the primary, a gap appears after
// failover where neither S3 nor WAL can serve the data.
//
// This test configures small WAL limits (matching the e2e Helm values) to force
// disk pressure on the standby. After failover, it asserts that ALL acked records
// are consumable from offset 0 — the invariant that failed in the e2e test.
func TestReplicationPromotionWithWALPressure(t *testing.T) {
	t.Parallel()

	const (
		walSegmentSize = 32 * 1024 // 32 KiB segments
		walMaxDisk     = 64 * 1024 // 64 KiB total WAL — only 2 segments
		s3ObjSize      = 16 * 1024 // 16 KiB S3 objects — flush often
		totalRecords   = 200       // enough to overflow WAL multiple times
		valuePadding   = 256       // ~256 bytes per record to fill WAL faster
	)

	pair := StartReplicaPair(t,
		WithWALSegmentMaxBytes(walSegmentSize),
		WithWALMaxDiskSize(walMaxDisk),
		WithS3FlushInterval(1*time.Second),
		WithS3FlushCheckInterval(200*time.Millisecond),
		WithS3TargetObjectSize(s3ObjSize),
	)

	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	topic := "test-wal-pressure-promotion"
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)

	// Produce records one at a time so each gets its own batch (maximizes
	// WAL entries and segment rotation). The padded value ensures WAL fills
	// fast enough to trigger disk-pressure pruning on the standby.
	pad := strings.Repeat("x", valuePadding)
	for i := 0; i < totalRecords; i++ {
		ProduceSync(t, cl, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d-%s", i, pad)),
		})
	}

	// Wait for S3 flush — at least some data must be in S3
	require.Eventually(t, func() bool {
		for _, k := range pair.S3.Keys() {
			if strings.Contains(k, topic) && strings.HasSuffix(k, ".obj") {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "S3 flush did not occur")

	cl.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Failover: demote A (simulates killed primary whose final flush is
	// blocked by NetworkPolicy in e2e), promote B.
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// Verify HW is correct on the new primary
	admin := NewAdminClient(t, pair.Standby.Addr)
	offsets, err := admin.ListEndOffsets(context.Background(), topic)
	require.NoError(t, err)
	lo, ok := offsets.Lookup(topic, 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(totalRecords), lo.Offset,
		"HW should equal total produced records after promotion")

	// The critical assertion: consume ALL records from offset 0.
	// This fails if there is a gap between S3 data and WAL data.
	consumer := NewClient(t, pair.Standby.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := ConsumeN(t, consumer, totalRecords, 30*time.Second)
	require.Len(t, records, totalRecords)
	for i, r := range records {
		require.Equal(t, fmt.Sprintf("key-%d", i), string(r.Key),
			"record %d key mismatch", i)
	}
}

// TestStandbyWALPrunePreservesS3Gap verifies the invariant that after
// failover under WAL disk pressure, the promoted standby's high watermark
// and data coverage have no gaps. Specifically: the newly promoted primary
// must be able to serve the full contiguous range [logStart, HW) from the
// combination of S3 cold storage and WAL.
//
// Unlike TestReplicationPromotionWithWALPressure which does a single failover,
// this test does TWO failover rounds (matching the e2e test structure) to
// exercise the case where the second promotion exposes the gap.
func TestStandbyWALPrunePreservesS3Gap(t *testing.T) {
	t.Parallel()

	const (
		walSegmentSize  = 32 * 1024
		walMaxDisk      = 64 * 1024
		s3ObjSize       = 16 * 1024
		recordsPerRound = 150
		valuePadding    = 256
	)

	pair := StartReplicaPair(t,
		WithWALSegmentMaxBytes(walSegmentSize),
		WithWALMaxDiskSize(walMaxDisk),
		WithS3FlushInterval(1*time.Second),
		WithS3FlushCheckInterval(200*time.Millisecond),
		WithS3TargetObjectSize(s3ObjSize),
	)

	topic := "test-wal-prune-s3-gap"
	pad := strings.Repeat("x", valuePadding)

	// ---- Round 1: A=primary, B=standby ----
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	for i := 0; i < recordsPerRound; i++ {
		ProduceSync(t, cl, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d-%s", i, pad)),
		})
	}

	// Wait for S3 flush
	require.Eventually(t, func() bool {
		for _, k := range pair.S3.Keys() {
			if strings.Contains(k, topic) && strings.HasSuffix(k, ".obj") {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "round 1: S3 flush did not occur")
	cl.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Failover: A → B
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// Checkpoint: verify all round-1 records are consumable
	consumer1 := NewClient(t, pair.Standby.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	recs1 := ConsumeN(t, consumer1, recordsPerRound, 30*time.Second)
	require.Len(t, recs1, recordsPerRound, "round 1 checkpoint failed")

	// ---- Round 2: B=primary, A=standby ----
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	cl2 := NewClient(t, pair.Standby.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	for i := recordsPerRound; i < 2*recordsPerRound; i++ {
		ProduceSync(t, cl2, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d-%s", i, pad)),
		})
	}

	// Wait for S3 flush of round-2 data
	s3KeysBefore := len(pair.S3.Keys())
	require.Eventually(t, func() bool {
		return len(pair.S3.Keys()) > s3KeysBefore
	}, 10*time.Second, 200*time.Millisecond, "round 2: S3 flush did not occur")
	cl2.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Failover: B → A (this is the critical promotion — A was standby under
	// WAL pressure for two full rounds of production)
	pair.ElectorB.Demote()
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)

	totalRecords := 2 * recordsPerRound

	// Verify HW
	admin := NewAdminClient(t, pair.Primary.Addr)
	offsets, err := admin.ListEndOffsets(context.Background(), topic)
	require.NoError(t, err)
	lo, ok := offsets.Lookup(topic, 0)
	require.True(t, ok)
	require.NoError(t, lo.Err)
	require.Equal(t, int64(totalRecords), lo.Offset,
		"HW should equal total produced records after second promotion")

	// The critical assertion: consume ALL records from offset 0 after two
	// failover rounds. This is the exact scenario that failed in the e2e test.
	consumer2 := NewClient(t, pair.Primary.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	recs2 := ConsumeN(t, consumer2, totalRecords, 30*time.Second)
	require.Len(t, recs2, totalRecords)
	for i, r := range recs2 {
		require.Equal(t, fmt.Sprintf("key-%d", i), string(r.Key),
			"record %d key mismatch after second failover", i)
	}
}

// TestFetchReturnsDataForAllOffsetsAfterPromotion verifies that after failover
// under WAL disk pressure, every individual offset in [0, HW) is fetchable.
// This catches the specific symptom where Fetch returns empty responses (no
// error, no data) for offsets in the gap between S3 flush watermark and WAL
// index start, causing consumers to spin indefinitely.
//
// The test produces records, triggers WAL pressure + S3 flush, does a failover,
// then walks through offsets one at a time using individual Fetch calls via
// franz-go's DirectFetch, asserting that each returns at least one batch.
func TestFetchReturnsDataForAllOffsetsAfterPromotion(t *testing.T) {
	t.Parallel()

	const (
		walSegmentSize = 32 * 1024
		walMaxDisk     = 64 * 1024
		s3ObjSize      = 16 * 1024
		totalRecords   = 200
		valuePadding   = 256
	)

	pair := StartReplicaPair(t,
		WithWALSegmentMaxBytes(walSegmentSize),
		WithWALMaxDiskSize(walMaxDisk),
		WithS3FlushInterval(1*time.Second),
		WithS3FlushCheckInterval(200*time.Millisecond),
		WithS3TargetObjectSize(s3ObjSize),
	)

	pair.ElectorA.Elect()
	waitPrimary(t, pair.HealthAddrA, 5*time.Second)
	waitStandby(t, pair.HealthAddrB, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	topic := "test-fetch-all-offsets"
	cl := NewClient(t, pair.Primary.Addr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	pad := strings.Repeat("x", valuePadding)
	for i := 0; i < totalRecords; i++ {
		ProduceSync(t, cl, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d-%s", i, pad)),
		})
	}

	require.Eventually(t, func() bool {
		for _, k := range pair.S3.Keys() {
			if strings.Contains(k, topic) && strings.HasSuffix(k, ".obj") {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "S3 flush did not occur")
	cl.Close()
	time.Sleep(300 * time.Millisecond) // replication catch-up

	// Failover
	pair.ElectorA.Demote()
	waitStandby(t, pair.HealthAddrA, 5*time.Second)
	pair.ElectorB.Elect()
	waitPrimary(t, pair.HealthAddrB, 5*time.Second)

	// Walk through every offset individually. Use a consuming client that
	// reads one record at a time and tracks the offset. If any fetch stalls
	// (empty response for valid offset), the 5s per-offset timeout fires.
	consumer := NewClient(t, pair.Standby.Addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumed := 0
	deadline := time.After(60 * time.Second)
	for consumed < totalRecords {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fetches := consumer.PollFetches(ctx)
		cancel()

		var fetchErr error
		fetches.EachError(func(_ string, _ int32, err error) {
			fetchErr = err
		})

		var batchRecords int
		fetches.EachRecord(func(r *kgo.Record) {
			batchRecords++
		})

		if batchRecords == 0 {
			// No records returned — this is the symptom of the gap bug.
			// The consumer is stuck at an offset where neither S3 nor WAL
			// can serve data. Fail immediately with a diagnostic message.
			require.Failf(t, "fetch returned no records",
				"consumer stalled at offset %d/%d (fetch err: %v)",
				consumed, totalRecords, fetchErr)
		}

		consumed += batchRecords

		select {
		case <-deadline:
			require.Failf(t, "overall timeout",
				"consumed only %d/%d records before 60s deadline",
				consumed, totalRecords)
		default:
		}
	}

	require.Equal(t, totalRecords, consumed,
		"should consume exactly %d records", totalRecords)
}

// TestReplicationS3LeaseFailover uses real s3lease electors (backed by
// LocalStack) instead of memlease. Failover happens through actual lease
// expiry timing: the primary is killed, the standby detects the expired
// lease after ~leaseDuration, and promotes itself. This tests the full
// integration path: S3 conditional writes → lease expiry → role transition
// → Kafka listener restart → data continuity.
func TestReplicationS3LeaseFailover(t *testing.T) {
	t.Parallel()

	s3Client := startLocalStack(t)

	sharedClusterID := "test-s3lease-failover"
	leaseKey := sharedClusterID + "/lease"
	leaseDur := 3 * time.Second
	renewInt := 500 * time.Millisecond
	retryInt := 300 * time.Millisecond

	// Allocate ports for Kafka + replication + health for both nodes
	replAddrA := allocPort(t)
	replAddrB := allocPort(t)

	lnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	lnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthLnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	electorA := s3lease.New(s3lease.Config{
		S3:            s3Client,
		Bucket:        s3LeaseBucket,
		Key:           leaseKey,
		Holder:        "node-a",
		ReplAddr:      replAddrA,
		LeaseDuration: leaseDur,
		RenewInterval: renewInt,
		RetryInterval: retryInt,
	})
	electorB := s3lease.New(s3lease.Config{
		S3:            s3Client,
		Bucket:        s3LeaseBucket,
		Key:           leaseKey,
		Holder:        "node-b",
		ReplAddr:      replAddrB,
		LeaseDuration: leaseDur,
		RenewInterval: renewInt,
		RetryInterval: retryInt,
	})

	memS3 := s3store.NewInMemoryS3()

	buildCfg := func(ln, healthLn net.Listener, replAddr string, elector interface{}) broker.Config {
		cfg := broker.DefaultConfig()
		cfg.Listener = ln
		cfg.HealthAddr = healthLn.Addr().String()
		cfg.HealthListener = healthLn
		cfg.DataDir = t.TempDir()
		cfg.LogLevel = "debug"
		cfg.ClusterID = sharedClusterID
		cfg.ReplicationAddr = replAddr
		cfg.ReplicationAdvertisedAddr = replAddr
		cfg.LeaseElector = elector
		cfg.S3Bucket = "test-bucket"
		cfg.S3API = memS3
		cfg.ChunkPoolMemory = 32 * 1024 * 1024
		return cfg
	}

	// Start both brokers
	cfgA := buildCfg(lnA, healthLnA, replAddrA, electorA)
	brokerA := broker.New(cfgA)
	ctxA, cancelA := context.WithCancel(context.Background())
	go func() { _ = brokerA.Run(ctxA) }()

	cfgB := buildCfg(lnB, healthLnB, replAddrB, electorB)
	brokerB := broker.New(cfgB)
	ctxB, cancelB := context.WithCancel(context.Background())
	go func() { _ = brokerB.Run(ctxB) }()

	// Ensure both get cleaned up
	t.Cleanup(func() {
		cancelA()
		cancelB()
		brokerA.Wait()
		brokerB.Wait()
	})

	// Wait for both brokers to be ready
	for _, br := range []*broker.Broker{brokerA, brokerB} {
		select {
		case <-br.Ready():
		case <-time.After(10 * time.Second):
			t.Fatal("broker did not become ready")
		}
	}

	// Track which node wins the election.
	healthAddrA := healthLnA.Addr().String()
	healthAddrB := healthLnB.Addr().String()

	type node struct {
		kafkaAddr  string
		healthAddr string
		elector    *s3lease.Elector
		cancel     context.CancelFunc
		broker     *broker.Broker
	}
	nodes := [2]node{
		{lnA.Addr().String(), healthAddrA, electorA, cancelA, brokerA},
		{lnB.Addr().String(), healthAddrB, electorB, cancelB, brokerB},
	}

	var primary, standby *node
	deadline := time.After(15 * time.Second)
	for primary == nil {
		for i := range nodes {
			if nodes[i].elector.Role() == lease.RolePrimary {
				primary = &nodes[i]
				standby = &nodes[1-i]
				break
			}
		}
		if primary != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no broker became primary within 15s")
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("primary=%s standby=%s", primary.kafkaAddr, standby.kafkaAddr)

	// Wait for primary to start its Kafka listener
	waitPrimary(t, primary.healthAddr, 5*time.Second)
	waitStandby(t, standby.healthAddr, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let replication receiver connect

	// Produce records to the primary
	topic := "test-s3lease-failover"
	cl := NewClient(t, primary.kafkaAddr,
		kgo.AllowAutoTopicCreation(),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	ProduceN(t, cl, topic, 50)
	cl.Close()

	// Allow replication to catch up
	time.Sleep(300 * time.Millisecond)

	// Kill the primary — standby detects expired lease and promotes
	primary.cancel()
	primary.broker.Wait()

	// Wait for the standby to become primary (lease expiry + claim + listener start)
	waitPrimary(t, standby.healthAddr, leaseDur+10*time.Second)

	// Consume from the new primary — all 50 records should be available
	consumer := NewClient(t, standby.kafkaAddr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(5*time.Second),
	)
	records := ConsumeN(t, consumer, 50, 15*time.Second)
	require.Len(t, records, 50)

	for i, r := range records {
		require.Equal(t, fmt.Sprintf("key-%d", i), string(r.Key))
		require.Equal(t, fmt.Sprintf("value-%d", i), string(r.Value))
	}
}
