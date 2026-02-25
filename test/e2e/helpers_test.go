//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testBucket = "klite-e2e"
	testRegion = "us-east-1"
)

// shared LocalStack state
var (
	localstackEndpoint string
	localstackReady    bool
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := localstack.Run(ctx, "localstack/localstack:4.4.0")
	if err != nil {
		log.Fatalf("start localstack: %v", err)
	}

	port, err := container.MappedPort(ctx, nat.Port("4566/tcp"))
	if err != nil {
		log.Fatalf("get localstack port: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("get localstack host: %v", err)
	}

	localstackEndpoint = fmt.Sprintf("http://%s:%s", host, port.Port())
	localstackReady = true

	// Create the test bucket
	s3Client, err := newS3Client(ctx)
	if err != nil {
		log.Fatalf("create s3 client: %v", err)
	}
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		log.Fatalf("create bucket: %v", err)
	}

	code := m.Run()

	if err := testcontainers.TerminateContainer(container); err != nil {
		log.Printf("terminate localstack: %v", err)
	}

	os.Exit(code)
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(testRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
	)
	if err != nil {
		return nil, err
	}
	endpoint := localstackEndpoint
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	}), nil
}

// buildBroker compiles the klite binary once per test run.
var (
	brokerBinary     string
	brokerBuildOnce  sync.Once
	brokerBuildError error
)

func buildBroker(t *testing.T) string {
	t.Helper()
	brokerBuildOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "klite-e2e-*")
		if err != nil {
			brokerBuildError = fmt.Errorf("create temp dir: %w", err)
			return
		}
		binPath := tmpDir + "/klite"
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/klite")
		cmd.Dir = projectRoot()
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			brokerBuildError = fmt.Errorf("build klite: %w", err)
			return
		}
		brokerBinary = binPath
	})
	require.NoError(t, brokerBuildError)
	return brokerBinary
}

func projectRoot() string {
	// test/e2e -> project root
	wd, _ := os.Getwd()
	if strings.HasSuffix(wd, "test/e2e") {
		return wd[:len(wd)-len("test/e2e")]
	}
	return wd
}

// brokerProcess represents a running klite subprocess.
type brokerProcess struct {
	cmd     *exec.Cmd
	addr    string
	stopped bool
}

// startBroker launches klite as a subprocess and waits for it to be ready.
// Each call gets the provided s3Prefix so tests don't interfere with each other.
func startBroker(t *testing.T, binary, dataDir, s3Prefix string, extraArgs ...string) *brokerProcess {
	t.Helper()

	args := []string{
		"--listen", "127.0.0.1:0",
		"--data-dir", dataDir,
		"--s3-bucket", testBucket,
		"--s3-region", testRegion,
		"--s3-endpoint", localstackEndpoint,
		"--s3-prefix", s3Prefix,
		"--s3-flush-interval", "24h",
		"--log-level", "debug",
	}
	args = append(args, extraArgs...)

	cmd := exec.Command(binary, args...)

	// Set dummy AWS credentials for LocalStack
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_DEFAULT_REGION="+testRegion,
	)

	// Capture stderr for log parsing (slog writes to stderr)
	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)

	require.NoError(t, cmd.Start())

	// Parse the listen address from the "klite started" log line
	addrCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if addr := parseListenAddr(line); addr != "" {
				addrCh <- addr
			}
		}
	}()

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("broker did not start within 15s")
	}

	bp := &brokerProcess{cmd: cmd, addr: addr}
	t.Cleanup(func() { bp.kill() })
	return bp
}

// kill forcefully kills the broker process. Used as a cleanup fallback.
func (bp *brokerProcess) kill() {
	if bp.stopped || bp.cmd.Process == nil {
		return
	}
	bp.cmd.Process.Kill()
	bp.cmd.Wait()
	bp.stopped = true
}

// stopGraceful sends SIGINT and waits for the process to exit cleanly.
func (bp *brokerProcess) stopGraceful(t *testing.T) {
	t.Helper()
	if bp.stopped || bp.cmd.Process == nil {
		return
	}
	require.NoError(t, bp.cmd.Process.Signal(os.Interrupt))
	done := make(chan error, 1)
	go func() { done <- bp.cmd.Wait() }()
	select {
	case err := <-done:
		bp.stopped = true
		require.NoError(t, err, "broker should exit cleanly on SIGINT")
	case <-time.After(15 * time.Second):
		bp.cmd.Process.Kill()
		bp.cmd.Wait()
		bp.stopped = true
		t.Fatal("broker did not shut down within 15s after SIGINT")
	}
}

// parseListenAddr extracts the listen address from a klite log line.
// Looks for listen= in the structured log output.
func parseListenAddr(line string) string {
	// slog text output: level=INFO msg="klite started" listen=127.0.0.1:54321 ...
	if !strings.Contains(line, "klite started") {
		return ""
	}
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, "listen=") {
			return strings.TrimPrefix(field, "listen=")
		}
	}
	return ""
}

// newKgoClient creates a franz-go client for the given broker address.
func newKgoClient(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	allOpts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.RequestRetries(3),
		kgo.RetryTimeout(10 * time.Second),
	}
	allOpts = append(allOpts, opts...)
	cl, err := kgo.NewClient(allOpts...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

// newAdminClient creates a kadm admin client.
func newAdminClient(t *testing.T, addr string) *kadm.Client {
	t.Helper()
	cl := newKgoClient(t, addr)
	return kadm.NewClient(cl)
}

// produceSync produces records synchronously.
func produceSync(t *testing.T, cl *kgo.Client, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, records...)
	for _, r := range results {
		require.NoError(t, r.Err)
	}
}

// listS3Objects returns all object keys in the test bucket under the given prefix.
func listS3Objects(t *testing.T, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	client, err := newS3Client(ctx)
	require.NoError(t, err)

	pfx := prefix + "/"
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(testBucket),
		Prefix: &pfx,
	})
	require.NoError(t, err)

	var keys []string
	for _, obj := range out.Contents {
		keys = append(keys, aws.ToString(obj.Key))
	}
	return keys
}
