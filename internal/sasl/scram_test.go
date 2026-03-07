package sasl

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"strings"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

func TestParsePlainValid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		wantUser string
		wantPass string
	}{
		{"\x00alice\x00secret", "alice", "secret"},
		{"alice\x00alice\x00secret", "alice", "secret"},
		{"\x00bob\x00p@ss w0rd!", "bob", "p@ss w0rd!"},
	}
	for _, tc := range tests {
		user, pass, err := ParsePlain([]byte(tc.input))
		if err != nil {
			t.Fatalf("ParsePlain(%q): unexpected error: %v", tc.input, err)
		}
		if user != tc.wantUser || pass != tc.wantPass {
			t.Fatalf("ParsePlain(%q) = (%q, %q), want (%q, %q)",
				tc.input, user, pass, tc.wantUser, tc.wantPass)
		}
	}
}

func TestParsePlainInvalid(t *testing.T) {
	t.Parallel()
	tests := []string{
		"nozero",
		"only\x00one",
		"wrong\x00user\x00pass", // authzid != username
	}
	for _, input := range tests {
		_, _, err := ParsePlain([]byte(input))
		if err == nil {
			t.Fatalf("ParsePlain(%q): expected error, got nil", input)
		}
	}
}

func TestScramRoundTrip256(t *testing.T) {
	t.Parallel()
	testScramRoundTrip(t, MechanismScram256)
}

func TestScramRoundTrip512(t *testing.T) {
	t.Parallel()
	testScramRoundTrip(t, MechanismScram512)
}

func testScramRoundTrip(t *testing.T, mechanism string) {
	t.Helper()
	password := "test-password"
	auth := NewScramAuth(mechanism, password)

	clientNonce := base64.RawStdEncoding.EncodeToString([]byte("client-nonce-123"))
	username := "testuser"
	client0Msg := []byte(fmt.Sprintf("n,,n=%s,r=%s", username, clientNonce))

	c0, err := ParseScramClient0(client0Msg)
	if err != nil {
		t.Fatalf("ParseScramClient0: %v", err)
	}
	if c0.User != username {
		t.Fatalf("user = %q, want %q", c0.User, username)
	}

	s0, serverFirst := NewScramServerFirst(c0, auth)

	clientFinal := buildClientFinalForTest(t, mechanism, password, c0, serverFirst)

	serverFinalMsg, err := s0.ServerFinal(clientFinal)
	if err != nil {
		t.Fatalf("ServerFinal: %v", err)
	}
	if !strings.HasPrefix(string(serverFinalMsg), "v=") {
		t.Fatalf("server-final should start with 'v=', got %q", serverFinalMsg)
	}
}

func TestScramBadPassword(t *testing.T) {
	t.Parallel()
	password := "correct-password"
	auth := NewScramAuth(MechanismScram256, password)

	clientNonce := base64.RawStdEncoding.EncodeToString([]byte("nonce-abc"))
	client0Msg := []byte(fmt.Sprintf("n,,n=user1,r=%s", clientNonce))

	c0, err := ParseScramClient0(client0Msg)
	if err != nil {
		t.Fatalf("ParseScramClient0: %v", err)
	}

	s0, serverFirst := NewScramServerFirst(c0, auth)

	clientFinal := buildClientFinalForTest(t, MechanismScram256, "wrong-password", c0, serverFirst)

	_, err = s0.ServerFinal(clientFinal)
	if err == nil {
		t.Fatal("expected error with wrong password, got nil")
	}
}

func TestScramClient0Parsing(t *testing.T) {
	t.Parallel()

	valid := []struct {
		input    string
		wantUser string
	}{
		{"n,,n=user,r=nonce123", "user"},
		{"n,a=user,n=user,r=nonce123", "user"},
		{"n,,n=user=3Dname,r=nonce123", "user=name"},
	}
	for _, tc := range valid {
		c0, err := ParseScramClient0([]byte(tc.input))
		if err != nil {
			t.Fatalf("ParseScramClient0(%q): %v", tc.input, err)
		}
		if c0.User != tc.wantUser {
			t.Fatalf("user = %q, want %q", c0.User, tc.wantUser)
		}
	}

	invalid := []string{
		"",
		"garbage",
		"n,a=wrong,n=user,r=nonce",
	}
	for _, input := range invalid {
		_, err := ParseScramClient0([]byte(input))
		if err == nil {
			t.Fatalf("ParseScramClient0(%q): expected error, got nil", input)
		}
	}
}

func TestStoreOperations(t *testing.T) {
	t.Parallel()
	s := NewStore()

	if !s.Empty() {
		t.Fatal("new store should be empty")
	}

	s.AddPlain("alice", "pass1")
	if s.Empty() {
		t.Fatal("store should not be empty after AddPlain")
	}
	if !s.LookupPlain("alice", "pass1") {
		t.Fatal("expected LookupPlain to succeed")
	}
	if s.LookupPlain("alice", "wrong") {
		t.Fatal("expected LookupPlain to fail with wrong password")
	}
	if s.LookupPlain("nobody", "pass1") {
		t.Fatal("expected LookupPlain to fail with unknown user")
	}

	auth := NewScramAuth(MechanismScram256, "s256pass")
	s.AddScram256("bob", auth)
	got, ok := s.LookupScram256("bob")
	if !ok {
		t.Fatal("expected LookupScram256 to find bob")
	}
	if got.Mechanism != MechanismScram256 {
		t.Fatalf("mechanism = %q, want %q", got.Mechanism, MechanismScram256)
	}

	auth512 := NewScramAuth(MechanismScram512, "s512pass")
	s.AddScram512("carol", auth512)
	got512, ok := s.LookupScram512("carol")
	if !ok {
		t.Fatal("expected LookupScram512 to find carol")
	}
	if got512.Mechanism != MechanismScram512 {
		t.Fatalf("mechanism = %q, want %q", got512.Mechanism, MechanismScram512)
	}

	if !s.DeleteScram256("bob") {
		t.Fatal("expected DeleteScram256 to succeed")
	}
	if s.DeleteScram256("bob") {
		t.Fatal("expected DeleteScram256 to fail on second call")
	}

	all := s.ListScramUsers()
	if _, ok := all["carol"]; !ok {
		t.Fatal("expected carol in ListScramUsers")
	}

	infos := s.ScramUserInfoFor("carol")
	if len(infos) != 1 || infos[0].Mechanism != 2 {
		t.Fatalf("ScramUserInfoFor(carol) = %v, want mechanism=2", infos)
	}
}

func TestScramPreHashed(t *testing.T) {
	t.Parallel()
	s := NewStore()

	salt := []byte("test-salt!")
	saltedPass := pbkdf2.Key([]byte("mypass"), salt, 4096, sha256.Size, sha256.New)

	auth := ScramAuthFromPreHashed(MechanismScram256, 4096, saltedPass, salt)
	s.AddScram256("prehashed-user", auth)

	got, ok := s.LookupScram256("prehashed-user")
	if !ok {
		t.Fatal("expected to find prehashed-user")
	}
	if got.Iterations != 4096 {
		t.Fatalf("iterations = %d, want 4096", got.Iterations)
	}
}

// buildClientFinalForTest simulates the client-side SCRAM computation.
func buildClientFinalForTest(t *testing.T, mechanism, password string, c0 ScramClient0, serverFirst []byte) []byte {
	t.Helper()

	// Parse server-first: r=<nonce>,s=<salt>,i=<iterations>
	var combinedNonce, salt64 string
	var iterations int
	parts := strings.Split(string(serverFirst), ",")
	for _, p := range parts {
		if strings.HasPrefix(p, "r=") {
			combinedNonce = p[2:]
		} else if strings.HasPrefix(p, "s=") {
			salt64 = p[2:]
		} else if strings.HasPrefix(p, "i=") {
			_, _ = fmt.Sscanf(p[2:], "%d", &iterations)
		}
	}

	saltBytes, err := base64.StdEncoding.DecodeString(salt64)
	if err != nil {
		t.Fatalf("decode salt: %v", err)
	}

	var h func() hash.Hash
	var saltedPass []byte
	switch mechanism {
	case MechanismScram256:
		h = sha256.New
		saltedPass = pbkdf2.Key([]byte(password), saltBytes, iterations, sha256.Size, sha256.New)
	case MechanismScram512:
		h = sha512.New
		saltedPass = pbkdf2.Key([]byte(password), saltBytes, iterations, sha512.Size, sha512.New)
	}

	clientKey := computeHMAC(h, saltedPass, []byte("Client Key"))
	storedKey := computeHash(h, clientKey)

	channelBinding := "biws"
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, combinedNonce)

	authMsg := buildAuthMessage(c0.Bare, serverFirst, []byte(clientFinalWithoutProof))

	clientSignature := computeHMAC(h, storedKey, authMsg)

	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	return []byte(fmt.Sprintf("%s,p=%s", clientFinalWithoutProof,
		base64.StdEncoding.EncodeToString(clientProof)))
}
