//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/bench"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func init() {
	tclog.SetDefault(log.New(os.Stderr, "", log.Ltime))
}

// logf writes a timestamped log line to stderr.
// Using stderr directly instead of t.Log avoids the file:line prefix
// and gives us second-accurate timestamps for correlating events.
func logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s  %s\n", time.Now().Format("15:04:05"), msg)
}

const (
	k3sImage        = "rancher/k3s:v1.31.4-k3s1"
	kliteImageName  = "klite-e2e:test"
	helmRelease     = "klite-e2e"
	testNamespace   = "default"
	s3FlushInterval = 3 * time.Second
)

// clusterStateName identifies the stable states of the klite cluster.
//
// The FSM has four stable states, parameterized by which pod is primary:
//
//	Healthy(primary=A, standby=B)   — both pods running, replication connected
//	Healthy(primary=B, standby=A)   — same, roles swapped
//	Degraded(primary=B, isolated=A) — B serving, A isolated by NetworkPolicy
//	Degraded(primary=A, isolated=B) — A serving, B isolated by NetworkPolicy
//
// Transitions:
//
//	disrupt():  Healthy(primary=A, standby=B)  → Degraded(primary=B, isolated=A)
//	heal():     Degraded(primary=B, isolated=A) → Healthy(primary=B, standby=A)
type clusterStateName int

const (
	stateHealthy  clusterStateName = iota // both pods running, replication connected
	stateDegraded                         // one pod serving, other isolated
)

func (s clusterStateName) String() string {
	switch s {
	case stateHealthy:
		return "Healthy"
	case stateDegraded:
		return "Degraded"
	default:
		return "Unknown"
	}
}

// clusterState describes the observable cluster condition with named parameters.
type clusterState struct {
	name      clusterStateName
	primary   string // pod serving Kafka traffic
	secondary string // standby (Healthy) or isolated pod (Degraded)
}

func (s clusterState) String() string {
	switch s.name {
	case stateHealthy:
		return fmt.Sprintf("Healthy(primary=%s, standby=%s)", s.primary, s.secondary)
	case stateDegraded:
		return fmt.Sprintf("Degraded(primary=%s, isolated=%s)", s.primary, s.secondary)
	default:
		return fmt.Sprintf("Unknown(primary=%s, secondary=%s)", s.primary, s.secondary)
	}
}

// transition describes an in-progress FSM transition for diagnostics.
// Every convergence check receives this so that timeout errors include
// the source state, action, and target state.
type transition struct {
	round  int
	from   clusterState
	action string
	to     clusterState
}

func (tr transition) String() string {
	return fmt.Sprintf("[round %d] %s → %s() → %s", tr.round, tr.from, tr.action, tr.to)
}

// check is a named convergence condition within a transition.
// On timeout it reports: transition context + which specific check failed.
func (tr transition) check(t *testing.T, name string, timeout, poll time.Duration, fn func() bool) {
	t.Helper()
	start := time.Now()
	deadline := time.After(timeout)
	for {
		if fn() {
			logf("%s: %s (%s)", tr, name, time.Since(start).Round(time.Millisecond))
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s: convergence check %q timed out after %v", tr, name, timeout)
		case <-time.After(poll):
		}
	}
}

// cluster represents a klite deployment in k3s. It tracks the cluster state
// and provides FSM transitions for failover testing.
type cluster struct {
	t              *testing.T
	ctx            context.Context
	k8s            *kubernetes.Clientset
	kubeconfigPath string
	topic          string

	state clusterState
	round int

	// Background producer state — runs for the entire test.
	producerCancel context.CancelFunc
	producerDone   chan struct{}
	producerAcked  atomic.Int64
	producerErrors atomic.Int64

	// Port-forward to socat proxy.
	proxyAddr string
	pfCancel  context.CancelFunc
}

// setupCluster creates a k3s cluster, deploys LocalStack + socat proxy + klite,
// and waits for the cluster to reach the initial Healthy state.
func setupCluster(t *testing.T) *cluster {
	t.Helper()
	ctx := t.Context()

	// --- k3s ---
	logf("starting k3s container...")
	k3sC, err := k3s.Run(ctx, k3sImage)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(k3sC) })

	kubeYAML, err := k3sC.GetKubeConfig(ctx)
	require.NoError(t, err)
	kubeconfigPath := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfigPath, kubeYAML, 0o600))

	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeYAML)
	require.NoError(t, err)
	k8sClient, err := kubernetes.NewForConfig(restCfg)
	require.NoError(t, err)

	waitForK3sReady(t, k8sClient)

	// --- Parallel setup: docker build runs alongside LocalStack + socat ---
	// Docker build + image load is the slowest step (~15s). Running it in
	// parallel with LocalStack and socat deployment saves significant time.
	root := projectRoot()
	imageReady := make(chan error, 1)
	go func() {
		logf("building klite Docker image...")
		dockerBuild := exec.CommandContext(ctx, "docker", "build", "-t", kliteImageName, "-f", "Dockerfile", ".")
		dockerBuild.Dir = root
		dockerBuild.Stdout, dockerBuild.Stderr = os.Stderr, os.Stderr
		if err := dockerBuild.Run(); err != nil {
			imageReady <- fmt.Errorf("docker build: %w", err)
			return
		}
		logf("loading image into k3s...")
		if err := k3sC.LoadImages(ctx, kliteImageName); err != nil {
			imageReady <- fmt.Errorf("image load: %w", err)
			return
		}
		logf("klite image loaded into k3s")
		imageReady <- nil
	}()

	// LocalStack and socat deploy sequentially (they call t.Fatal internally)
	// but run concurrently with docker build above.
	logf("deploying LocalStack into k3s...")
	deployLocalStack(t, k8sClient, kubeconfigPath)
	logf("deploying socat proxy...")
	deploySocatProxy(t, k8sClient)

	// Wait for docker build + image load to complete.
	if err := <-imageReady; err != nil {
		t.Fatalf("parallel setup failed: %v", err)
	}

	// --- Helm install ---
	logf("installing klite Helm chart...")
	chartPath := filepath.Join(root, "charts", "klite")
	helmInstall := exec.CommandContext(ctx, "helm", "install", helmRelease, chartPath,
		"--kubeconfig", kubeconfigPath,
		"--namespace", testNamespace,
		"--set", "replication.enabled=true",
		"--set", "s3.bucket=klite-e2e",
		"--set", "s3.region=us-east-1",
		"--set", "s3.endpoint=http://localstack.default.svc.cluster.local:4566",
		"--set", fmt.Sprintf("image.repository=%s", strings.Split(kliteImageName, ":")[0]),
		"--set", fmt.Sprintf("image.tag=%s", strings.Split(kliteImageName, ":")[1]),
		"--set", "image.pullPolicy=Never",
		"--set", "replication.leaseDuration=5s",
		"--set", "replication.leaseRenewInterval=1s",
		"--set", "replication.leaseRetryInterval=1s",
		"--set", "podSecurityContext.fsGroup=0",
		"--set", "containerSecurityContext.runAsNonRoot=false",
		"--set", "containerSecurityContext.runAsUser=0",
		"--set", "containerSecurityContext.readOnlyRootFilesystem=false",
		"--set", "extraEnvVars[0].name=AWS_ACCESS_KEY_ID",
		"--set", "extraEnvVars[0].value=test",
		"--set", "extraEnvVars[1].name=AWS_SECRET_ACCESS_KEY",
		"--set", "extraEnvVars[1].value=test",
		"--set", "broker.logLevel=debug",
		"--set-string", "broker.extraArgs[0]=--wal-segment-max-bytes=4194304",
		"--set-string", "broker.extraArgs[1]=--wal-max-disk-size=8388608",
		"--set-string", fmt.Sprintf("broker.extraArgs[2]=--s3-flush-interval=%s", s3FlushInterval),
		"--set-string", "broker.extraArgs[3]=--s3-flush-check-interval=2s",
		"--set-string", "broker.extraArgs[4]=--s3-target-object-size=4194304",
	)
	helmInstall.Stdout, helmInstall.Stderr = os.Stderr, os.Stderr
	require.NoError(t, helmInstall.Run(), "helm install failed")
	t.Cleanup(func() {
		if t.Failed() {
			dumpPodInfo(t, k8sClient)
		}
		helmDel := exec.Command("helm", "uninstall", helmRelease,
			"--kubeconfig", kubeconfigPath, "--namespace", testNamespace)
		helmDel.Stdout, helmDel.Stderr = os.Stderr, os.Stderr
		_ = helmDel.Run()
	})

	// --- Wait for Healthy state ---
	logf("waiting for pods to be running...")
	waitForPodsRunning(t, k8sClient, 2, 90*time.Second)

	logf("waiting for a pod to become primary...")
	primary := waitForPrimaryLabel(t, k8sClient, 90*time.Second)
	logf("primary: %s", primary)

	standby := otherPod(primary)
	logf("standby: %s", standby)

	logf("waiting for standby to connect to primary...")
	waitForStandbyConnected(t, k8sClient, primary, 60*time.Second)

	// --- Port-forward to socat proxy (stable across failovers) ---
	pfCtx, pfCancel := context.WithCancel(ctx)
	localPort := portForward(t, pfCtx, kubeconfigPath, "pod/kafka-proxy", 9092)
	proxyAddr := fmt.Sprintf("127.0.0.1:%d", localPort)
	waitForPortReady(t, ctx, proxyAddr, 10*time.Second)
	logf("proxy port-forward: %s -> pod/kafka-proxy:9092", proxyAddr)

	c := &cluster{
		t:              t,
		ctx:            ctx,
		k8s:            k8sClient,
		kubeconfigPath: kubeconfigPath,
		topic:          "e2e-k3s-failover",
		state: clusterState{
			name:      stateHealthy,
			primary:   primary,
			secondary: standby,
		},
		proxyAddr: proxyAddr,
	}
	t.Cleanup(func() { pfCancel() })

	logf("initial state: %s", c.state)
	return c
}

// startProducer launches a background goroutine that produces 1KB records
// continuously through the socat proxy until stopped. The producer survives
// failovers because the proxy pod is stable and franz-go reconnects
// automatically when connections drop.
func (c *cluster) startProducer() {
	c.t.Helper()
	pCtx, pCancel := context.WithCancel(c.ctx)
	c.producerCancel = pCancel
	c.producerDone = make(chan struct{})

	go func() {
		defer close(c.producerDone)
		c.runProducer(pCtx)
	}()

	// Periodic diagnostic logging of ack/error counts.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pCtx.Done():
				return
			case <-ticker.C:
				logf("producer heartbeat: acked=%d errors=%d",
					c.producerAcked.Load(), c.producerErrors.Load())
			}
		}
	}()

	logf("background producer started")
}

func (c *cluster) runProducer(ctx context.Context) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.proxyAddr),
		kgo.DefaultProduceTopic(c.topic),
		kgo.ProducerBatchMaxBytes(1_048_576),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(4096),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RetryBackoffFn(func(int) time.Duration { return 200 * time.Millisecond }),
		kgo.RequestRetries(50),
		kgo.AllowAutoTopicCreation(),
		kgo.Dialer(func(dCtx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(dCtx, "tcp", c.proxyAddr)
		}),
	)
	if err != nil {
		c.t.Errorf("producer client creation failed: %v", err)
		return
	}
	defer client.Close()

	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Throttle to ~1000 rec/s (1 MB/s). Enough to always have in-flight
	// records during failover without overwhelming the WAL.
	const targetRecsPerSec = 1000
	interval := time.Second / targetRecsPerSec

	for ctx.Err() == nil {
		rec := &kgo.Record{Value: append([]byte(nil), payload...)}
		client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				c.producerErrors.Add(1)
				return
			}
			c.producerAcked.Add(1)
		})
		time.Sleep(interval)
	}

	// Flush any buffered records with a grace period.
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		logf("producer flush error (expected during shutdown): %v", err)
	}
}

// waitForProducerAcked blocks until the producer has acked at least n records,
// ensuring there is meaningful in-flight traffic before triggering a disruption.
func (c *cluster) waitForProducerAcked(n int64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.After(timeout)
	for c.producerAcked.Load() < n {
		select {
		case <-deadline:
			c.t.Fatalf("producer did not ack %d records within %v (got %d)",
				n, timeout, c.producerAcked.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
	logf("producer reached %d acked records", c.producerAcked.Load())
}

// stopProducer cancels the background producer and returns the number of
// acked records.
func (c *cluster) stopProducer() int64 {
	c.t.Helper()
	c.producerCancel()
	<-c.producerDone
	acked := c.producerAcked.Load()
	errors := c.producerErrors.Load()
	logf("producer stopped: %d acked, %d errors", acked, errors)
	return acked
}

// consumeAll reads all records from the topic from the beginning and returns
// the count. Uses bench.RunConsumer with the socat proxy.
func (c *cluster) consumeAll(expected int64) int64 {
	c.t.Helper()

	// Query the broker's high watermark before consuming for diagnostics.
	c.logPartitionHW()

	cfg := bench.DefaultConsumerConfig()
	cfg.Brokers = []string{c.proxyAddr}
	cfg.Topic = c.topic
	cfg.NumRecords = expected
	cfg.Timeout = 15 * time.Second
	cfg.Out = io.Discard
	cfg.KgoOpts = []kgo.Opt{
		kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", c.proxyAddr)
		}),
	}
	result, err := bench.RunConsumer(c.ctx, cfg)
	if err != nil {
		logf("consume warning: consumed %d/%d records: %v", result.Records, expected, err)
	}
	return result.Records
}

// logPartitionHW queries the broker for the current high watermark via ListOffsets.
func (c *cluster) logPartitionHW() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.proxyAddr),
		kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", c.proxyAddr)
		}),
	)
	if err != nil {
		logf("logPartitionHW: client error: %v", err)
		return
	}
	defer client.Close()

	// Use ListEndOffsets (latest) to get the HW.
	reqCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	req := kmsg.NewListOffsetsRequest()
	req.ReplicaID = -1
	req.IsolationLevel = 0 // READ_UNCOMMITTED to see true HW
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = c.topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = 0
	rp.Timestamp = -1 // latest
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(reqCtx, client)
	if err != nil {
		logf("logPartitionHW: ListOffsets error: %v", err)
		return
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			logf("logPartitionHW: topic=%s partition=%d offset=%d errorCode=%d",
				t.Topic, p.Partition, p.Offset, p.ErrorCode)
		}
	}
}

// disrupt transitions:
//
//	Healthy(primary=A, standby=B) → Degraded(primary=B, isolated=A)
//
// Action: apply NetworkPolicy to isolate A, then delete A's pod.
// Convergence: B has klite.io/role=primary, A's pod has no Kafka listener.
func (c *cluster) disrupt() {
	c.t.Helper()
	require.Equal(c.t, stateHealthy, c.state.name,
		"disrupt() requires Healthy state, got %s", c.state)

	c.round++
	oldPrimary := c.state.primary
	expectedNewPrimary := c.state.secondary
	netpolName := fmt.Sprintf("isolate-%s-round%d", oldPrimary, c.round)

	tr := transition{
		round:  c.round,
		from:   c.state,
		action: "disrupt",
		to: clusterState{
			name:      stateDegraded,
			primary:   expectedNewPrimary,
			secondary: oldPrimary,
		},
	}
	logf("%s: starting (producer: acked=%d errors=%d)", tr,
		c.producerAcked.Load(), c.producerErrors.Load())

	// Action: isolate + kill the primary.
	applyIsolationPolicy(c.t, c.k8s, netpolName, oldPrimary)
	c.t.Cleanup(func() { removeNetworkPolicy(c.t, c.k8s, netpolName) })

	logf("%s: deleting pod %s", tr, oldPrimary)
	err := c.k8s.CoreV1().Pods(testNamespace).Delete(c.ctx, oldPrimary, metav1.DeleteOptions{})
	require.NoError(c.t, err, "%s: failed to delete pod %s", tr, oldPrimary)

	// Convergence: wait for the other pod to become primary.
	start := time.Now()
	var newPrimary string
	tr.check(c.t, "new primary elected", 30*time.Second, 500*time.Millisecond, func() bool {
		pods, err := c.k8s.CoreV1().Pods(testNamespace).List(c.ctx, metav1.ListOptions{
			LabelSelector: "klite.io/role=primary",
		})
		if err != nil {
			return false
		}
		for _, p := range pods.Items {
			if p.DeletionTimestamp == nil && p.Name != oldPrimary {
				newPrimary = p.Name
				return true
			}
		}
		return false
	})
	logf("%s: failover took %s, new primary: %s (producer: acked=%d errors=%d)", tr,
		time.Since(start).Round(time.Millisecond), newPrimary,
		c.producerAcked.Load(), c.producerErrors.Load())

	// Query HW from new primary right after failover for diagnostics.
	c.logPartitionHW()

	// Convergence: verify labels are correct on all pods.
	tr.check(c.t, "labels correct", 10*time.Second, 1*time.Second, func() bool {
		selector := fmt.Sprintf("app.kubernetes.io/instance=%s", helmRelease)
		pods, err := c.k8s.CoreV1().Pods(testNamespace).List(c.ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil {
			return false
		}
		for _, p := range pods.Items {
			role := p.Labels["klite.io/role"]
			if p.Name == newPrimary && role != "primary" {
				return false
			}
			if p.Name != newPrimary && role == "primary" {
				return false
			}
		}
		return true
	})

	// Convergence: wait for the isolated pod to be recreated by StatefulSet controller.
	tr.check(c.t, "isolated pod recreated", 60*time.Second, 1*time.Second, func() bool {
		p, err := c.k8s.CoreV1().Pods(testNamespace).Get(c.ctx, oldPrimary, metav1.GetOptions{})
		return err == nil && p.DeletionTimestamp == nil && p.Status.PodIP != ""
	})

	// Convergence: verify the recreated pod is not primary (it should be
	// a standby or not yet ready, confirming the lease is held by the new primary).
	tr.check(c.t, "recreated pod not primary", 15*time.Second, 500*time.Millisecond, func() bool {
		result := c.k8s.CoreV1().RESTClient().Get().
			Namespace(testNamespace).
			Resource("pods").
			SubResource("proxy").
			Name(oldPrimary + ":8080").
			Suffix("/replz").
			Do(c.ctx)
		raw, err := result.Raw()
		if err != nil {
			return false // pod not ready yet
		}
		var status struct {
			Role string `json:"role"`
		}
		if json.Unmarshal(raw, &status) != nil {
			return false
		}
		return status.Role == "standby" || status.Role == "not_ready"
	})

	c.state = clusterState{
		name:      stateDegraded,
		primary:   newPrimary,
		secondary: oldPrimary,
	}
	logf("%s: completed → %s", tr, c.state)
}

// heal transitions:
//
//	Degraded(primary=B, isolated=A) → Healthy(primary=B, standby=A)
//
// Action: remove the NetworkPolicy isolating A.
// Convergence: 2 pods Running, A connected as standby to B.
func (c *cluster) heal() {
	c.t.Helper()
	require.Equal(c.t, stateDegraded, c.state.name,
		"heal() requires Degraded state, got %s", c.state)

	isolated := c.state.secondary
	netpolName := fmt.Sprintf("isolate-%s-round%d", isolated, c.round)

	tr := transition{
		round:  c.round,
		from:   c.state,
		action: "heal",
		to: clusterState{
			name:      stateHealthy,
			primary:   c.state.primary,
			secondary: isolated,
		},
	}
	logf("%s: starting", tr)

	// Convergence: wait for pods to be running before removing isolation.
	tr.check(c.t, "all pods running", 60*time.Second, 1*time.Second, func() bool {
		selector := fmt.Sprintf("app.kubernetes.io/instance=%s", helmRelease)
		pods, err := c.k8s.CoreV1().Pods(testNamespace).List(c.ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil {
			return false
		}
		running := 0
		for _, p := range pods.Items {
			if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
				running++
			}
		}
		return running >= 2
	})

	// Action: remove network isolation.
	removeNetworkPolicy(c.t, c.k8s, netpolName)

	// Convergence: the healed pod reconnects as standby.
	tr.check(c.t, "standby reconnected", 60*time.Second, 500*time.Millisecond, func() bool {
		result := c.k8s.CoreV1().RESTClient().Get().
			Namespace(testNamespace).
			Resource("pods").
			SubResource("proxy").
			Name(c.state.primary + ":8080").
			Suffix("/replz").
			Do(c.ctx)
		raw, err := result.Raw()
		if err != nil {
			return false
		}
		var status struct {
			StandbyConnected *bool `json:"standby_connected"`
		}
		return json.Unmarshal(raw, &status) == nil && status.StandbyConnected != nil && *status.StandbyConnected
	})

	// Wait for the S3 flusher to persist any WAL data that was written while
	// the standby was disconnected. After this sleep, all pre-reconnection
	// data is in S3 and all post-reconnection data is WAL-synced to the
	// standby — so a subsequent disrupt() cannot lose acked records.
	logf("%s: waiting %s for S3 flush", tr, s3FlushInterval)
	time.Sleep(s3FlushInterval)

	// Log broker HW after heal to verify replication caught up.
	c.logPartitionHW()

	c.state = clusterState{
		name:      stateHealthy,
		primary:   c.state.primary,
		secondary: isolated,
	}
	logf("%s: completed → %s (producer: acked=%d errors=%d)", tr, c.state,
		c.producerAcked.Load(), c.producerErrors.Load())
}

// ---------------------------------------------------------------------------
// TestK3sHelmReplicationFailover
// ---------------------------------------------------------------------------

// TestK3sHelmReplicationFailover deploys klite into a k3s cluster with
// replication and exercises two failover cycles (A→B, then B→A) while a
// background producer writes records.
//
// FSM:
//
//	                  disrupt()                         heal()
//	Healthy(A,B) ──────────────> Degraded(B,A) ──────────────> Healthy(B,A)
//	                  disrupt()                         heal()
//	Healthy(B,A) ──────────────> Degraded(A,B) ──────────────> Healthy(A,B)
//
// After round 1 the producer is stopped and a checkpoint consume verifies
// zero data loss. Then the producer restarts for round 2, and a final
// consume verifies the cumulative total.
//
// A socat TCP proxy pod provides a stable entry point that survives failovers.
func TestK3sHelmReplicationFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping k3s e2e test in short mode")
	}

	c := setupCluster(t)

	// Round 1: single failover + checkpoint.
	c.startProducer()
	c.waitForProducerAcked(10_000, 30*time.Second)
	c.disrupt()
	c.heal()
	checkpoint := c.stopProducer()
	require.Greater(t, checkpoint, int64(0), "expected at least some records produced")

	logf("checkpoint consume: expecting %d records...", checkpoint)
	consumed := c.consumeAll(checkpoint)
	require.Equal(t, checkpoint, consumed,
		"checkpoint: consumed records must equal produced (exactly-once with sync replication): got %d, want %d", consumed, checkpoint)
	logf("checkpoint passed: %d records verified after first failover", consumed)

	// Round 2: failover back to original primary.
	c.startProducer()
	c.waitForProducerAcked(checkpoint+10_000, 30*time.Second)
	c.disrupt()
	c.heal()

	produced := c.stopProducer()
	require.Greater(t, produced, checkpoint, "expected more records produced in round 2")
	total := produced // producerAcked is cumulative across rounds; produced already includes round 1

	logf("final consume: expecting %d records...", total)
	consumed = c.consumeAll(total)
	require.Equal(t, total, consumed,
		"consumed records must equal produced (exactly-once with sync replication): got %d, want %d", consumed, total)
	logf("all %d records verified across %d failovers", consumed, c.round)
}

// ---------------------------------------------------------------------------
// Infrastructure helpers
// ---------------------------------------------------------------------------

func otherPod(pod string) string {
	if pod == helmRelease+"-0" {
		return helmRelease + "-1"
	}
	return helmRelease + "-0"
}

// deploySocatProxy creates a pod that forwards TCP connections to the klite
// Service. This provides a stable port-forward target that survives primary
// failovers (the proxy pod itself never restarts).
func deploySocatProxy(t *testing.T, client *kubernetes.Clientset) {
	t.Helper()
	ctx := t.Context()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kafka-proxy",
			Namespace: testNamespace,
			Labels:    map[string]string{"app": "kafka-proxy"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "socat",
				Image:   "alpine/socat",
				Command: []string{"socat", "TCP-LISTEN:9092,fork,reuseaddr", fmt.Sprintf("TCP:%s.%s.svc.cluster.local:9092", helmRelease, testNamespace)},
				Ports:   []corev1.ContainerPort{{ContainerPort: 9092}},
			}},
		},
	}
	_, err := client.CoreV1().Pods(testNamespace).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	deadline := time.After(120 * time.Second)
	for {
		p, err := client.CoreV1().Pods(testNamespace).Get(ctx, "kafka-proxy", metav1.GetOptions{})
		if err == nil {
			for _, cond := range p.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					logf("socat proxy pod is ready")
					return
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("socat proxy pod did not become ready within 120s")
		case <-time.After(1 * time.Second):
		}
	}
}

// deployLocalStack creates a LocalStack pod + service inside k3s and waits
// for it to be ready, then creates the S3 bucket.
func deployLocalStack(t *testing.T, client *kubernetes.Clientset, kubeconfigPath string) {
	t.Helper()
	ctx := t.Context()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "localstack",
			Namespace: testNamespace,
			Labels:    map[string]string{"app": "localstack"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "localstack",
				Image: "localstack/localstack:4.4.0",
				Ports: []corev1.ContainerPort{{ContainerPort: 4566}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/_localstack/health",
							Port: intstr.FromInt32(4566),
						},
					},
					PeriodSeconds: 2,
				},
			}},
		},
	}
	_, err := client.CoreV1().Pods(testNamespace).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "localstack", Namespace: testNamespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "localstack"},
			Ports: []corev1.ServicePort{{
				Port: 4566, TargetPort: intstr.FromInt32(4566),
			}},
		},
	}
	_, err = client.CoreV1().Services(testNamespace).Create(ctx, svc, metav1.CreateOptions{})
	require.NoError(t, err)

	deadline := time.After(120 * time.Second)
	for {
		p, err := client.CoreV1().Pods(testNamespace).Get(ctx, "localstack", metav1.GetOptions{})
		if err == nil {
			for _, cond := range p.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					logf("LocalStack pod is ready")
					goto lsReady
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("LocalStack pod did not become ready within 120s")
		case <-time.After(2 * time.Second):
		}
	}

lsReady:
	kubectlExec := exec.CommandContext(ctx, "kubectl",
		"--kubeconfig", kubeconfigPath,
		"exec", "localstack", "--",
		"awslocal", "s3", "mb", "s3://klite-e2e",
	)
	kubectlExec.Stdout, kubectlExec.Stderr = os.Stderr, os.Stderr
	require.NoError(t, kubectlExec.Run(), "failed to create S3 bucket in LocalStack")
	logf("S3 bucket klite-e2e created")
}

// ---------------------------------------------------------------------------
// Convergence helpers — each polls until a condition is met or times out.
// ---------------------------------------------------------------------------

func waitForK3sReady(t *testing.T, client *kubernetes.Clientset) {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for {
		nodes, err := client.CoreV1().Nodes().List(t.Context(), metav1.ListOptions{})
		if err == nil && len(nodes.Items) > 0 {
			for _, cond := range nodes.Items[0].Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
					logf("k3s node is ready")
					return
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("k3s node did not become ready within 60s")
		case <-time.After(1 * time.Second):
		}
	}
}

func waitForPodsRunning(t *testing.T, client *kubernetes.Clientset, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	selector := fmt.Sprintf("app.kubernetes.io/instance=%s", helmRelease)
	for {
		pods, err := client.CoreV1().Pods(testNamespace).List(t.Context(), metav1.ListOptions{
			LabelSelector: selector,
		})
		if err == nil {
			running := 0
			for _, p := range pods.Items {
				if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
					running++
				}
			}
			if running >= count {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("pods did not reach Running within %v", timeout)
		case <-time.After(1 * time.Second):
		}
	}
}

func waitForPrimaryLabel(t *testing.T, client *kubernetes.Clientset, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		pods, err := client.CoreV1().Pods(testNamespace).List(t.Context(), metav1.ListOptions{
			LabelSelector: "klite.io/role=primary",
		})
		if err == nil {
			for _, p := range pods.Items {
				if p.DeletionTimestamp == nil {
					return p.Name
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for primary label")
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForStandbyConnected polls the primary's /replz health endpoint until
// it reports standby_connected=true, or times out.
func waitForStandbyConnected(t *testing.T, client *kubernetes.Clientset, primaryPod string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		result := client.CoreV1().RESTClient().Get().
			Namespace(testNamespace).
			Resource("pods").
			SubResource("proxy").
			Name(primaryPod + ":8080").
			Suffix("/replz").
			Do(t.Context())

		if raw, err := result.Raw(); err == nil {
			var status struct {
				StandbyConnected *bool `json:"standby_connected"`
			}
			if json.Unmarshal(raw, &status) == nil && status.StandbyConnected != nil && *status.StandbyConnected {
				logf("standby connected to primary %s", primaryPod)
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for standby to connect to %s", primaryPod)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func waitForPortReady(t *testing.T, ctx context.Context, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		select {
		case <-deadline:
			t.Fatalf("port %s not ready within %v", addr, timeout)
		case <-ctx.Done():
			t.Fatal("interrupted waiting for port")
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ---------------------------------------------------------------------------
// NetworkPolicy helpers
// ---------------------------------------------------------------------------

func applyIsolationPolicy(t *testing.T, client *kubernetes.Clientset, name, podName string) {
	t.Helper()
	netpol := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"statefulset.kubernetes.io/pod-name": podName,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{},
			Egress:  []networkingv1.NetworkPolicyEgressRule{},
		},
	}
	_, err := client.NetworkingV1().NetworkPolicies(testNamespace).Create(
		t.Context(), netpol, metav1.CreateOptions{})
	require.NoError(t, err, "failed to create NetworkPolicy %s", name)
	logf("NetworkPolicy %s applied: isolating %s", name, podName)
}

func removeNetworkPolicy(t *testing.T, client *kubernetes.Clientset, name string) {
	t.Helper()
	err := client.NetworkingV1().NetworkPolicies(testNamespace).Delete(
		t.Context(), name, metav1.DeleteOptions{})
	if err != nil {
		logf("NetworkPolicy %s delete (may already be gone): %v", name, err)
		return
	}
	logf("NetworkPolicy %s removed", name)
}

// ---------------------------------------------------------------------------
// Port-forward + misc
// ---------------------------------------------------------------------------

func portForward(t *testing.T, ctx context.Context, kubeconfigPath, target string, remotePort int) int {
	t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl",
		"--kubeconfig", kubeconfigPath,
		"port-forward", target,
		fmt.Sprintf(":%d", remotePort),
		"--namespace", testNamespace,
	)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	buf := make([]byte, 256)
	n, err := stdout.Read(buf)
	require.NoError(t, err)
	line := string(buf[:n])

	var localPort int
	_, err = fmt.Sscanf(extractForwardingLine(line), "Forwarding from 127.0.0.1:%d", &localPort)
	require.NoError(t, err, "failed to parse port-forward output: %q", line)
	require.Greater(t, localPort, 0)
	return localPort
}

func extractForwardingLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "Forwarding from") {
			return line
		}
	}
	return s
}

func dumpPodInfo(t *testing.T, client *kubernetes.Clientset) {
	t.Helper()
	ctx := context.Background()
	pods, err := client.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/instance=%s", helmRelease),
	})
	if err != nil {
		logf("failed to list pods: %v", err)
		return
	}
	for _, p := range pods.Items {
		logf("--- pod %s: phase=%s ---", p.Name, p.Status.Phase)
		for _, cs := range p.Status.ContainerStatuses {
			logf("  container %s: ready=%v restarts=%d", cs.Name, cs.Ready, cs.RestartCount)
		}
		logf("  labels: %v", p.Labels)

		logReq := client.CoreV1().Pods(testNamespace).GetLogs(p.Name, &corev1.PodLogOptions{TailLines: ptr(int64(500))})
		logStream, err := logReq.Stream(ctx)
		if err != nil {
			logf("  logs: (error: %v)", err)
			continue
		}
		logBytes, _ := io.ReadAll(logStream)
		logStream.Close()
		logf("  logs (last 500 lines):\n%s", string(logBytes))
	}
}

func ptr[T any](v T) *T {
	return &v
}
