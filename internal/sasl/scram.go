package sasl

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"regexp"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

const (
	MechanismPlain    = "PLAIN"
	MechanismScram256 = "SCRAM-SHA-256"
	MechanismScram512 = "SCRAM-SHA-512"

	DefaultIterations = 4096
	MinIterations     = 4096
	MaxIterations     = 16384
)

type ScramAuth struct {
	Mechanism  string // "SCRAM-SHA-256" or "SCRAM-SHA-512"
	Iterations int
	SaltedPass []byte
	Salt       []byte
}

func NewScramAuth(mechanism, password string) ScramAuth {
	salt := make([]byte, 10)
	if _, err := rand.Read(salt); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return NewScramAuthWithSalt(mechanism, password, salt, DefaultIterations)
}

func NewScramAuthWithSalt(mechanism, password string, salt []byte, iterations int) ScramAuth {
	var saltedPass []byte
	switch mechanism {
	case MechanismScram256:
		saltedPass = pbkdf2.Key([]byte(password), salt, iterations, sha256.Size, sha256.New)
	case MechanismScram512:
		saltedPass = pbkdf2.Key([]byte(password), salt, iterations, sha512.Size, sha512.New)
	default:
		panic("unsupported SCRAM mechanism: " + mechanism)
	}
	return ScramAuth{
		Mechanism:  mechanism,
		Iterations: iterations,
		SaltedPass: saltedPass,
		Salt:       salt,
	}
}

// ScramAuthFromPreHashed creates a ScramAuth from pre-hashed credentials
// (as received from AlterUserScramCredentials API).
func ScramAuthFromPreHashed(mechanism string, iterations int, saltedPass, salt []byte) ScramAuth {
	return ScramAuth{
		Mechanism:  mechanism,
		Iterations: iterations,
		SaltedPass: append([]byte(nil), saltedPass...),
		Salt:       append([]byte(nil), salt...),
	}
}

// ParsePlain parses SASL PLAIN auth bytes: \0<username>\0<password>
// or <authzid>\0<username>\0<password>.
func ParsePlain(auth []byte) (user, pass string, err error) {
	parts := strings.SplitN(string(auth), "\x00", 3)
	if len(parts) != 3 {
		return "", "", errors.New("invalid PLAIN auth format")
	}
	if len(parts[0]) != 0 && parts[0] != parts[1] {
		return "", "", errors.New("authzid is not equal to username")
	}
	return parts[1], parts[2], nil
}

type ScramClient0 struct {
	User  string
	Bare  []byte // client-first-message-bare
	Nonce []byte // nonce in client0
}

var scramUnescaper = strings.NewReplacer("=3D", "=", "=2C", ",")

func ParseScramClient0(client0 []byte) (ScramClient0, error) {
	m := reClient0.FindSubmatch(client0)
	if len(m) == 0 {
		return ScramClient0{}, errors.New("invalid client-first-message")
	}
	zid := string(m[1])
	bare := bytes.Clone(m[2])
	user := string(m[3])
	nonce := bytes.Clone(m[4])
	ext := string(m[5])

	if len(ext) != 0 {
		return ScramClient0{}, errors.New("extensions not supported")
	}
	if zid != "" && zid != user {
		return ScramClient0{}, errors.New("authzid is not equal to username")
	}
	return ScramClient0{
		User:  scramUnescaper.Replace(user),
		Bare:  bare,
		Nonce: nonce,
	}, nil
}

type ScramServer0 struct {
	Auth        ScramAuth
	Client0Bare []byte
	ServerFirst []byte
}

func NewScramServerFirst(client0 ScramClient0, auth ScramAuth) (ScramServer0, []byte) {
	serverNonceBytes := make([]byte, 16)
	if _, err := rand.Read(serverNonceBytes); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	nonce := make([]byte, 0, len(client0.Nonce)+24)
	nonce = append(nonce, client0.Nonce...)
	nonce = append(nonce, base64.RawStdEncoding.EncodeToString(serverNonceBytes)...)
	serverFirst := []byte(fmt.Sprintf("r=%s,s=%s,i=%d",
		nonce,
		base64.StdEncoding.EncodeToString(auth.Salt),
		auth.Iterations,
	))
	return ScramServer0{
		Auth:        auth,
		Client0Bare: client0.Bare,
		ServerFirst: serverFirst,
	}, serverFirst
}

func (s *ScramServer0) ServerFinal(clientFinal []byte) ([]byte, error) {
	m := reClientFinal.FindSubmatch(clientFinal)
	if len(m) == 0 {
		return nil, errors.New("invalid client-final-message")
	}
	finalWithoutProof := m[1]
	channel := m[2]
	clientProof64 := m[3]

	h := sha256.New
	if s.Auth.Mechanism == MechanismScram512 {
		h = sha512.New
	}

	if !bytes.Equal(channel, []byte("biws")) { // "biws" == base64("n,,")
		return nil, errors.New("invalid channel binding")
	}
	clientProof, err := base64.StdEncoding.DecodeString(string(clientProof64))
	if err != nil {
		return nil, errors.New("client proof is not valid base64")
	}
	if len(clientProof) != h().Size() {
		return nil, fmt.Errorf("client proof length %d != expected %d", len(clientProof), h().Size())
	}

	clientKey := computeHMAC(h, s.Auth.SaltedPass, []byte("Client Key"))
	storedKey := computeHash(h, clientKey)
	authMessage := buildAuthMessage(s.Client0Bare, s.ServerFirst, finalWithoutProof)
	clientSignature := computeHMAC(h, storedKey, authMessage)

	// Verify: XOR(ClientProof, ClientSignature) should give ClientKey,
	// then H(result) should equal StoredKey
	usedKey := make([]byte, len(clientProof))
	copy(usedKey, clientProof)
	for i, b := range clientSignature {
		usedKey[i] ^= b
	}
	hashedUsedKey := computeHash(h, usedKey)
	if !bytes.Equal(hashedUsedKey, storedKey) {
		return nil, errors.New("invalid password")
	}

	serverKey := computeHMAC(h, s.Auth.SaltedPass, []byte("Server Key"))
	serverSignature := computeHMAC(h, serverKey, authMessage)

	serverFinal := []byte(fmt.Sprintf("v=%s", base64.StdEncoding.EncodeToString(serverSignature)))
	return serverFinal, nil
}

func computeHMAC(h func() hash.Hash, key, data []byte) []byte {
	mac := hmac.New(h, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func computeHash(h func() hash.Hash, data []byte) []byte {
	hasher := h()
	hasher.Write(data)
	return hasher.Sum(nil)
}

func buildAuthMessage(c0bare, s0, finalWithoutProof []byte) []byte {
	msg := make([]byte, 0, len(c0bare)+1+len(s0)+1+len(finalWithoutProof))
	msg = append(msg, c0bare...)
	msg = append(msg, ',')
	msg = append(msg, s0...)
	msg = append(msg, ',')
	msg = append(msg, finalWithoutProof...)
	return msg
}

var reClient0, reClientFinal *regexp.Regexp

func init() {
	// https://datatracker.ietf.org/doc/html/rfc5802#section-7
	const (
		value     = `[\x01-\x2b\x2d-\x7f]+`
		printable = `[\x21-\x2b\x2d-\x7e]+`
		saslName  = `(?:[\x01-\x2b\x2d-\x3c\x3e-\x7f]|=2C|=3D)+`
		b64       = `[a-zA-Z0-9/+]+={0,3}`
		ext       = `(?:,[a-zA-Z]+=[\x01-\x2b\x2d-\x7f]+)*`
	)

	// client-first-message
	client0 := fmt.Sprintf(`^n,(?:a=(%s))?,((?:m=%s,)?n=(%s),r=(%s)(%s))$`, saslName, value, saslName, printable, ext)

	// client-final-message
	clientFinal := fmt.Sprintf(`^(c=(%s),r=%s),p=(%s)$`, b64, printable, b64)

	reClient0 = regexp.MustCompile(client0)
	reClientFinal = regexp.MustCompile(clientFinal)
}
