package bench

import (
	"encoding/json"
	"testing"
)

func TestPayloadGeneratorSize(t *testing.T) {
	g := NewPayloadGenerator(1024)
	for i := range 1000 {
		b := g.Generate()
		if len(b) != 1024 {
			t.Fatalf("iteration %d: got %d bytes, want 1024", i, len(b))
		}
	}
}

func TestPayloadGeneratorValidJSON(t *testing.T) {
	g := NewPayloadGenerator(1024)
	for i := range 100 {
		b := g.Generate()
		if !json.Valid(b) {
			t.Fatalf("iteration %d: invalid JSON: %s", i, string(b[:min(200, len(b))]))
		}
	}
}

func TestPayloadGeneratorUniqueness(t *testing.T) {
	g := NewPayloadGenerator(1024)
	seen := make(map[string]struct{}, 1000)
	for i := range 1000 {
		b := g.Generate()
		s := string(b)
		if _, ok := seen[s]; ok {
			t.Fatalf("iteration %d: duplicate payload", i)
		}
		seen[s] = struct{}{}
	}
}

func TestPayloadGeneratorMinSize(t *testing.T) {
	// The fixed JSON structure is ~590 bytes. Anything at or above 700
	// should produce an exact-size payload.
	for _, size := range []int{700, 1024, 2048, 4096} {
		g := NewPayloadGenerator(size)
		b := g.Generate()
		if len(b) != size {
			t.Fatalf("size %d: got %d bytes", size, len(b))
		}
		if !json.Valid(b) {
			t.Fatalf("size %d: invalid JSON", size)
		}
	}
}

func BenchmarkPayloadGenerate(b *testing.B) {
	g := NewPayloadGenerator(1024)
	b.SetBytes(1024)
	for range b.N {
		g.Generate()
	}
}
