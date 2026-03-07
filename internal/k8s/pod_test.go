package k8s

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// writeTokenFile creates a temporary token file and returns its path.
func writeTokenFile(t *testing.T, token string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPodLabeler_SetPrimary(t *testing.T) {
	var mu sync.Mutex
	var patchPod string
	var gotBody map[string]any

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("expected Bearer test-token, got %s", r.Header.Get("Authorization"))
		}

		switch r.Method {
		case http.MethodGet:
			// List pods — return empty list (no stale primaries)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[]}`))
		case http.MethodPatch:
			mu.Lock()
			patchPod = r.URL.Path
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &gotBody)
			mu.Unlock()

			if r.Header.Get("Content-Type") != "application/merge-patch+json" {
				t.Errorf("content-type = %q, want application/merge-patch+json", r.Header.Get("Content-Type"))
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	p := &PodLabeler{
		client:    srv.Client(),
		apiHost:   srv.URL,
		tokenPath: writeTokenFile(t, "test-token"),
		namespace: "default",
		podName:   "klite-0",
		logger:    slog.Default(),
	}

	p.SetPrimary()

	mu.Lock()
	defer mu.Unlock()

	if patchPod != "/api/v1/namespaces/default/pods/klite-0" {
		t.Errorf("patch path = %q, want /api/v1/namespaces/default/pods/klite-0", patchPod)
	}

	metadata := gotBody["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if labels["klite.io/role"] != "primary" {
		t.Errorf("label = %v, want primary", labels["klite.io/role"])
	}
}

func TestPodLabeler_SetPrimary_ClearsSiblings(t *testing.T) {
	var mu sync.Mutex
	patched := map[string]any{} // pod path -> label value (nil = removed)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			// Return klite-1 as having the stale primary label
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"klite-1"}}]}`))
		case http.MethodPatch:
			body, _ := io.ReadAll(r.Body)
			var b map[string]any
			_ = json.Unmarshal(body, &b)
			meta := b["metadata"].(map[string]any)
			labels := meta["labels"].(map[string]any)

			mu.Lock()
			patched[r.URL.Path] = labels["klite.io/role"]
			mu.Unlock()

			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	p := &PodLabeler{
		client:        srv.Client(),
		apiHost:       srv.URL,
		tokenPath:     writeTokenFile(t, "test-token"),
		namespace:     "default",
		podName:       "klite-0",
		labelSelector: "app.kubernetes.io/name=klite",
		logger:        slog.Default(),
	}

	p.SetPrimary()

	mu.Lock()
	defer mu.Unlock()

	// klite-1 should have its label cleared (nil)
	siblingPath := "/api/v1/namespaces/default/pods/klite-1"
	if v, ok := patched[siblingPath]; !ok {
		t.Error("sibling klite-1 was not patched")
	} else if v != nil {
		t.Errorf("sibling label = %v, want nil (removal)", v)
	}

	// klite-0 should have its label set to primary
	selfPath := "/api/v1/namespaces/default/pods/klite-0"
	if v, ok := patched[selfPath]; !ok {
		t.Error("self klite-0 was not patched")
	} else if v != "primary" {
		t.Errorf("self label = %v, want primary", v)
	}
}

func TestPodLabeler_ClearPrimary(t *testing.T) {
	var gotBody map[string]any

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := &PodLabeler{
		client:    srv.Client(),
		apiHost:   srv.URL,
		tokenPath: writeTokenFile(t, "test-token"),
		namespace: "default",
		podName:   "klite-0",
		logger:    slog.Default(),
	}

	p.ClearPrimary()

	metadata := gotBody["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if labels["klite.io/role"] != nil {
		t.Errorf("label = %v, want nil (removal)", labels["klite.io/role"])
	}
}

func TestPodLabeler_NilSafe(t *testing.T) {
	var p *PodLabeler
	// Must not panic.
	p.SetPrimary()
	p.ClearPrimary()
}

func TestPodLabeler_LogsOnError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"forbidden"}`))
	}))
	defer srv.Close()

	p := &PodLabeler{
		client:    srv.Client(),
		apiHost:   srv.URL,
		tokenPath: writeTokenFile(t, "test-token"),
		namespace: "default",
		podName:   "klite-0",
		logger:    slog.Default(),
	}

	// Should not panic, just log.
	p.SetPrimary()
}

func TestPodLabeler_TokenRereadFromDisk(t *testing.T) {
	tokenFile := writeTokenFile(t, "token-v1")
	var mu sync.Mutex
	var tokens []string

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tokens = append(tokens, r.Header.Get("Authorization"))
		mu.Unlock()

		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[]}`))
		case http.MethodPatch:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	p := &PodLabeler{
		client:    srv.Client(),
		apiHost:   srv.URL,
		tokenPath: tokenFile,
		namespace: "default",
		podName:   "klite-0",
		logger:    slog.Default(),
	}

	p.SetPrimary()

	// Rotate the token on disk
	if err := os.WriteFile(tokenFile, []byte("token-v2"), 0o600); err != nil {
		t.Fatal(err)
	}

	p.ClearPrimary()

	mu.Lock()
	defer mu.Unlock()

	// First two calls (list + patch from SetPrimary) should use v1,
	// the ClearPrimary call should use v2.
	foundV1 := false
	foundV2 := false
	for _, tok := range tokens {
		if tok == "Bearer token-v1" {
			foundV1 = true
		}
		if tok == "Bearer token-v2" {
			foundV2 = true
		}
	}
	if !foundV1 {
		t.Error("expected token-v1 to be used in SetPrimary calls")
	}
	if !foundV2 {
		t.Error("expected token-v2 to be used in ClearPrimary call after rotation")
	}
}
