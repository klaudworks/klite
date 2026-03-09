package integration

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/sasl"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/crypto/pbkdf2"
)

// createTopicSASL creates a topic using an authenticated client.
func createTopicSASL(t *testing.T, addr, topic string, opts ...kgo.Opt) {
	t.Helper()
	cl := NewClient(t, addr, opts...)
	admin := kadm.NewClient(cl)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)
}

func TestSASLPlainSuccess(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("alice", "alice-pass")

	tb := StartBroker(t, WithSASL(store))

	saslOpt := PlainSASLOpt("alice", "alice-pass")
	createTopicSASL(t, tb.Addr, "sasl-plain-test", saslOpt)

	cl := NewClient(t, tb.Addr, saslOpt,
		kgo.DefaultProduceTopic("sasl-plain-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{
		Topic:     "sasl-plain-test",
		Partition: 0,
		Value:     []byte("hello"),
	})
}

func TestSASLPlainBadPassword(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("alice", "alice-pass")

	tb := StartBroker(t, WithSASL(store))

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(tb.Addr),
		kgo.RequestRetries(0),
		PlainSASLOpt("alice", "wrong-pass"),
	)
	require.NoError(t, err, "client creation should succeed (connection is lazy)")
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "sasl-test",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err, "expected produce to fail with bad SASL credentials")
}

func TestSASLPlainNoAuth(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("alice", "alice-pass")

	tb := StartBroker(t, WithSASL(store))

	// Client without SASL should fail (Metadata request is blocked by gate)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(tb.Addr),
		kgo.RequestRetries(0),
	)
	require.NoError(t, err, "client creation should succeed (connection is lazy)")
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "sasl-test",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err, "expected produce to fail without SASL auth")
}

func TestSASLScram256Success(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	auth := sasl.NewScramAuth(sasl.MechanismScram256, "bob-pass")
	store.AddScram256("bob", auth)

	tb := StartBroker(t, WithSASL(store))

	saslOpt := Scram256SASLOpt("bob", "bob-pass")
	createTopicSASL(t, tb.Addr, "sasl-scram256-test", saslOpt)

	cl := NewClient(t, tb.Addr, saslOpt,
		kgo.DefaultProduceTopic("sasl-scram256-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{
		Topic:     "sasl-scram256-test",
		Partition: 0,
		Value:     []byte("hello scram256"),
	})
}

func TestSASLScram512Success(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	auth := sasl.NewScramAuth(sasl.MechanismScram512, "carol-pass")
	store.AddScram512("carol", auth)

	tb := StartBroker(t, WithSASL(store))

	saslOpt := Scram512SASLOpt("carol", "carol-pass")
	createTopicSASL(t, tb.Addr, "sasl-scram512-test", saslOpt)

	cl := NewClient(t, tb.Addr, saslOpt,
		kgo.DefaultProduceTopic("sasl-scram512-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{
		Topic:     "sasl-scram512-test",
		Partition: 0,
		Value:     []byte("hello scram512"),
	})
}

func TestSASLScramBadPassword(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	auth := sasl.NewScramAuth(sasl.MechanismScram256, "correct-pass")
	store.AddScram256("user1", auth)

	tb := StartBroker(t, WithSASL(store))

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(tb.Addr),
		kgo.RequestRetries(0),
		Scram256SASLOpt("user1", "wrong-pass"),
	)
	require.NoError(t, err, "client creation should succeed (connection is lazy)")
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "test",
		Value: []byte("fail"),
	})
	require.Error(t, results[0].Err, "expected produce to fail with wrong SCRAM password")
}

func TestSASLUnsupportedMechanism(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("user", "pass")

	tb := StartBroker(t, WithSASL(store))

	// Attempt SCRAM-256 auth against a broker that only has PLAIN users.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(tb.Addr),
		kgo.RequestRetries(0),
		Scram256SASLOpt("user", "pass"),
	)
	require.NoError(t, err, "client creation should succeed (connection is lazy)")
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "test",
		Value: []byte("should-fail"),
	})
	require.Error(t, results[0].Err, "expected produce to fail with unsupported SASL mechanism")
}

func TestSASLDisabled(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t)

	// Create topic first
	admin := NewAdminClient(t, tb.Addr)
	_, err := admin.CreateTopic(context.Background(), 1, 1, nil, "no-sasl-test")
	require.NoError(t, err)

	cl := NewClient(t, tb.Addr,
		kgo.DefaultProduceTopic("no-sasl-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{
		Topic:     "no-sasl-test",
		Partition: 0,
		Value:     []byte("hello no sasl"),
	})
}

func TestAlterUserScramCredentials(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("admin", "admin-pass")

	tb := StartBroker(t, WithSASL(store))

	adminOpt := PlainSASLOpt("admin", "admin-pass")
	cl := NewClient(t, tb.Addr, adminOpt)

	// Create a SCRAM-SHA-256 user via API
	salt := []byte("test-salt!")
	saltedPass := pbkdf2.Key([]byte("new-user-pass"), salt, 4096, sha256.Size, sha256.New)

	req := kmsg.NewAlterUserSCRAMCredentialsRequest()
	upsertion := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
	upsertion.Name = "new-scram-user"
	upsertion.Mechanism = 1 // SCRAM-SHA-256
	upsertion.Iterations = 4096
	upsertion.Salt = salt
	upsertion.SaltedPassword = saltedPass
	req.Upsertions = append(req.Upsertions, upsertion)

	resp, err := req.RequestWith(context.Background(), cl)
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, int16(0), resp.Results[0].ErrorCode, "expected no error for upsertion")

	// Create a topic and produce with the new user
	newSASLOpt := Scram256SASLOpt("new-scram-user", "new-user-pass")
	createTopicSASL(t, tb.Addr, "alter-scram-test", newSASLOpt)

	newCl := NewClient(t, tb.Addr, newSASLOpt,
		kgo.DefaultProduceTopic("alter-scram-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, newCl, &kgo.Record{
		Topic:     "alter-scram-test",
		Partition: 0,
		Value:     []byte("hello from new user"),
	})
}

func TestAlterUserScramDeleteUser(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("admin", "admin-pass")
	auth := sasl.NewScramAuth(sasl.MechanismScram256, "to-delete-pass")
	store.AddScram256("to-delete", auth)

	tb := StartBroker(t, WithSASL(store))

	// Create topic with admin, verify the to-delete user can produce
	adminOpt := PlainSASLOpt("admin", "admin-pass")
	createTopicSASL(t, tb.Addr, "delete-test", adminOpt)

	scramOpt := Scram256SASLOpt("to-delete", "to-delete-pass")
	cl := NewClient(t, tb.Addr, scramOpt,
		kgo.DefaultProduceTopic("delete-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{
		Topic:     "delete-test",
		Partition: 0,
		Value:     []byte("before delete"),
	})

	// Delete user via admin
	adminCl := NewClient(t, tb.Addr, adminOpt)

	req := kmsg.NewAlterUserSCRAMCredentialsRequest()
	deletion := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
	deletion.Name = "to-delete"
	deletion.Mechanism = 1
	req.Deletions = append(req.Deletions, deletion)

	resp, err := req.RequestWith(context.Background(), adminCl)
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, int16(0), resp.Results[0].ErrorCode, "expected no error for deletion")

	// Verify deleted user can no longer connect
	failCl, err := kgo.NewClient(
		kgo.SeedBrokers(tb.Addr),
		kgo.RequestRetries(0),
		Scram256SASLOpt("to-delete", "to-delete-pass"),
	)
	require.NoError(t, err, "client creation should succeed (connection is lazy)")
	defer failCl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := failCl.ProduceSync(ctx, &kgo.Record{
		Topic: "delete-test",
		Value: []byte("after delete"),
	})
	require.Error(t, results[0].Err, "expected produce to fail after user deletion")
}

func TestDescribeUserScramCredentials(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("admin", "admin-pass")
	auth256 := sasl.NewScramAuth(sasl.MechanismScram256, "user-pass")
	store.AddScram256("scram-user", auth256)

	tb := StartBroker(t, WithSASL(store))
	cl := NewClient(t, tb.Addr, PlainSASLOpt("admin", "admin-pass"))

	req := kmsg.NewDescribeUserSCRAMCredentialsRequest()
	u := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
	u.Name = "scram-user"
	req.Users = append(req.Users, u)

	resp, err := req.RequestWith(context.Background(), cl)
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "scram-user", resp.Results[0].User)
	require.Equal(t, int16(0), resp.Results[0].ErrorCode)
	require.Len(t, resp.Results[0].CredentialInfos, 1)
	require.Equal(t, int8(1), resp.Results[0].CredentialInfos[0].Mechanism)
}

func TestDescribeUserScramCredentialsAll(t *testing.T) {
	t.Parallel()
	store := sasl.NewStore()
	store.AddPlain("admin", "admin-pass")
	auth256 := sasl.NewScramAuth(sasl.MechanismScram256, "user1-pass")
	store.AddScram256("user1", auth256)
	auth512 := sasl.NewScramAuth(sasl.MechanismScram512, "user2-pass")
	store.AddScram512("user2", auth512)

	tb := StartBroker(t, WithSASL(store))
	cl := NewClient(t, tb.Addr, PlainSASLOpt("admin", "admin-pass"))

	req := kmsg.NewDescribeUserSCRAMCredentialsRequest()
	// Users is nil by default = list all

	resp, err := req.RequestWith(context.Background(), cl)
	require.NoError(t, err)
	require.Len(t, resp.Results, 2)

	users := make(map[string]bool)
	for _, r := range resp.Results {
		users[r.User] = true
		require.Equal(t, int16(0), r.ErrorCode)
	}
	require.True(t, users["user1"], "expected user1 in results")
	require.True(t, users["user2"], "expected user2 in results")
}

func TestSASLConfigFileUsers(t *testing.T) {
	t.Parallel()
	tb := StartBroker(t, WithSASLCLI(sasl.MechanismScram256, "cli-user", "cli-pass"))

	saslOpt := Scram256SASLOpt("cli-user", "cli-pass")
	createTopicSASL(t, tb.Addr, "cli-user-test", saslOpt)

	cl := NewClient(t, tb.Addr, saslOpt,
		kgo.DefaultProduceTopic("cli-user-test"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	ProduceSync(t, cl, &kgo.Record{
		Topic:     "cli-user-test",
		Partition: 0,
		Value:     []byte("hello from CLI user"),
	})
}
