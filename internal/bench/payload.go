package bench

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand/v2"
	"strconv"
	"time"
)

const hexChars = "0123456789abcdef"

var (
	services = []string{
		"svc-payment", "svc-orders", "svc-users", "svc-inventory",
		"svc-shipping", "svc-notify", "svc-gateway", "svc-search",
	}
	envs    = []string{"prod", "staging", "dev"}
	levels  = []string{"info", "warn", "error", "debug"}
	methods = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths   = []string{
		"/api/v1/orders/checkout", "/api/v1/users/profile",
		"/api/v1/inventory/check", "/api/v1/payments/charge",
		"/api/v1/shipping/track", "/api/v1/search/products",
		"/api/v1/orders/history", "/api/v1/notify/send",
	}
	regions    = []string{"eu-west-1", "us-east-1", "ap-southeast-1"}
	azSuffixes = []string{"a", "b", "c"}
	currencies = []string{"EUR", "USD", "GBP", "JPY"}
	payMethods = []string{"card", "paypal", "bank", "crypto"}
	tagSets    = []string{
		`["checkout","payment","critical"]`,
		`["search","query","normal"]`,
		`["shipping","tracking","normal"]`,
		`["auth","login","security"]`,
		`["inventory","stock","warning"]`,
		`["notify","email","normal"]`,
	}
)

// Not safe for concurrent use — each goroutine needs its own instance.
type PayloadGenerator struct {
	targetSize int
	buf        []byte
	rng        *mrand.Rand
}

func NewPayloadGenerator(targetSize int) *PayloadGenerator {
	return &PayloadGenerator{
		targetSize: targetSize,
		buf:        make([]byte, 0, targetSize+256),
		rng:        mrand.New(mrand.NewPCG(randSeed(), randSeed())),
	}
}

func (g *PayloadGenerator) appendHex(b []byte, n int) []byte {
	for range n {
		b = append(b, hexChars[g.rng.IntN(16)])
	}
	return b
}

func (g *PayloadGenerator) appendUUID(b []byte) []byte {
	b = g.appendHex(b, 8)
	b = append(b, '-')
	b = g.appendHex(b, 4)
	b = append(b, '-')
	b = g.appendHex(b, 4)
	b = append(b, '-')
	b = g.appendHex(b, 4)
	b = append(b, '-')
	b = g.appendHex(b, 12)
	return b
}

// Generate builds a JSON payload of exactly g.targetSize bytes.
// The returned slice is only valid until the next call to Generate.
func (g *PayloadGenerator) Generate() []byte {
	b := g.buf[:0]

	region := regions[g.rng.IntN(len(regions))]
	az := region + azSuffixes[g.rng.IntN(len(azSuffixes))]

	b = append(b, `{"ts":"`...)
	b = time.Now().UTC().AppendFormat(b, time.RFC3339)
	b = append(b, `","id":"`...)
	b = g.appendHex(b, 16)
	b = append(b, `","src":"`...)
	b = append(b, services[g.rng.IntN(len(services))]...)
	b = append(b, `","env":"`...)
	b = append(b, envs[g.rng.IntN(len(envs))]...)
	b = append(b, `","host":"ip-172-31-`...)
	b = strconv.AppendInt(b, int64(g.rng.IntN(255)), 10)
	b = append(b, '-')
	b = strconv.AppendInt(b, int64(g.rng.IntN(255)), 10)
	b = append(b, `","level":"`...)
	b = append(b, levels[g.rng.IntN(len(levels))]...)
	b = append(b, `","method":"`...)
	b = append(b, methods[g.rng.IntN(len(methods))]...)
	b = append(b, `","path":"`...)
	b = append(b, paths[g.rng.IntN(len(paths))]...)
	b = append(b, `","status":`...)
	statuses := []int{200, 200, 200, 200, 201, 204, 400, 404, 500}
	b = strconv.AppendInt(b, int64(statuses[g.rng.IntN(len(statuses))]), 10)
	b = append(b, `,"duration_ms":`...)
	b = strconv.AppendInt(b, int64(1+g.rng.IntN(500)), 10)
	b = append(b, `,"user_id":"usr_`...)
	b = g.appendHex(b, 16)
	b = append(b, `","session":"ses_`...)
	b = g.appendHex(b, 32)
	b = append(b, `","trace_id":"`...)
	b = g.appendUUID(b)
	b = append(b, `","request_id":"req_`...)
	b = g.appendHex(b, 16)
	b = append(b, `","region":"`...)
	b = append(b, region...)
	b = append(b, `","az":"`...)
	b = append(b, az...)
	b = append(b, `","payload_bytes":`...)
	b = strconv.AppendInt(b, int64(100+g.rng.IntN(9900)), 10)
	b = append(b, `,"tags":`...)
	b = append(b, tagSets[g.rng.IntN(len(tagSets))]...)
	b = append(b, `,"meta":{"cart_items":`...)
	b = strconv.AppendInt(b, int64(1+g.rng.IntN(20)), 10)
	b = append(b, `,"total_cents":`...)
	b = strconv.AppendInt(b, int64(100+g.rng.IntN(99900)), 10)
	b = append(b, `,"currency":"`...)
	b = append(b, currencies[g.rng.IntN(len(currencies))]...)
	b = append(b, `","payment_method":"`...)
	b = append(b, payMethods[g.rng.IntN(len(payMethods))]...)
	b = append(b, `","retry":`...)
	b = strconv.AppendInt(b, int64(g.rng.IntN(3)), 10)
	b = append(b, `},"msg":"`...)

	// Pad msg with random hex to reach targetSize exactly.
	// We need room for closing `"}` (2 bytes).
	needed := g.targetSize - len(b) - 2
	if needed > 0 {
		b = g.appendHex(b, needed)
	}
	b = append(b, `"}`...)

	g.buf = b
	return b
}

// GenerateCopy returns a freshly allocated payload. Safe to pass to
// async producers that hold a reference after the call returns.
func (g *PayloadGenerator) GenerateCopy() []byte {
	g.Generate()
	out := make([]byte, len(g.buf))
	copy(out, g.buf)
	return out
}

func randSeed() uint64 {
	n, _ := rand.Int(rand.Reader, new(big.Int).SetUint64(^uint64(0)))
	return n.Uint64()
}
