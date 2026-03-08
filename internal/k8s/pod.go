// Package k8s provides lightweight Kubernetes API helpers using raw HTTP.
// No dependency on client-go — uses the in-cluster service account token directly.
package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

const (
	tokenPath  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath     = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	nsPath     = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	apiHost    = "https://kubernetes.default.svc"
	labelKey   = "klite.io/role"
	labelValue = "primary"

	// Static JSON Merge Patch bodies — avoids marshal/alloc on every call.
	patchSet    = `{"metadata":{"labels":{"klite.io/role":"primary"}}}`
	patchRemove = `{"metadata":{"labels":{"klite.io/role":null}}}`

	maxRetries   = 3
	retryBackoff = 500 * time.Millisecond
)

// PodLabeler patches the klite.io/role label on the current pod.
// It auto-detects whether it's running inside Kubernetes by checking
// for the service account token. Outside k8s, all operations are no-ops.
type PodLabeler struct {
	client        *http.Client
	apiHost       string
	tokenPath     string // re-read on every call to handle projected token rotation
	namespace     string
	podName       string
	labelSelector string // scopes sibling pod lookups (e.g. "app.kubernetes.io/name=klite")
	logger        *slog.Logger
	clk           clock.Clock
}

// NewPodLabeler creates a labeler that will patch the pod identified by
// podName in the given namespace. labelSelector scopes the sibling lookup
// used to clear stale primary labels on promotion. If namespace is empty,
// it reads from the mounted service account. If not running in k8s, returns nil.
func NewPodLabeler(podName, namespace, labelSelector string, logger *slog.Logger) *PodLabeler {
	if podName == "" {
		return nil
	}

	if _, err := os.Stat(tokenPath); err != nil {
		// Not running in Kubernetes.
		return nil
	}

	if namespace == "" {
		ns, err := os.ReadFile(nsPath)
		if err != nil {
			logger.Warn("k8s: cannot read namespace, pod labeling disabled", "err", err)
			return nil
		}
		namespace = strings.TrimSpace(string(ns))
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		logger.Warn("k8s: cannot read CA cert, pod labeling disabled", "err", err)
		return nil
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: pool,
			},
		},
	}

	return &PodLabeler{
		client:        client,
		apiHost:       apiHost,
		tokenPath:     tokenPath,
		namespace:     namespace,
		podName:       podName,
		labelSelector: labelSelector,
		logger:        logger,
		clk:           clock.RealClock{},
	}
}

func (p *PodLabeler) SetClock(c clock.Clock) {
	p.clk = c
}

func (p *PodLabeler) getClock() clock.Clock {
	if p.clk != nil {
		return p.clk
	}
	return clock.RealClock{}
}

// readToken re-reads the service account token from disk on every call.
// Projected SA tokens rotate (typically every hour); caching the token
// at startup would cause 401s after expiry.
func (p *PodLabeler) readToken() string {
	data, err := os.ReadFile(p.tokenPath)
	if err != nil {
		p.logger.Error("k8s: failed to read service account token", "err", err)
		return ""
	}
	return strings.TrimSpace(string(data))
}

// SetPrimary clears the klite.io/role label from any sibling pods that
// still have it (fencing the old primary), then adds it to this pod.
// Retries transient failures with backoff.
func (p *PodLabeler) SetPrimary() {
	if p == nil {
		return
	}
	p.clearSiblingLabels()
	p.patchLabelWithRetry(p.podName, patchSet, "set")
}

// ClearPrimary removes the klite.io/role label from this pod.
// Retries transient failures with backoff.
func (p *PodLabeler) ClearPrimary() {
	if p == nil {
		return
	}
	p.patchLabelWithRetry(p.podName, patchRemove, "removed")
}

// clearSiblingLabels lists pods matching the label selector that have
// klite.io/role=primary and removes the label from any that aren't this pod.
func (p *PodLabeler) clearSiblingLabels() {
	selector := labelKey + "=" + labelValue
	if p.labelSelector != "" {
		selector = p.labelSelector + "," + selector
	}

	token := p.readToken()
	if token == "" {
		return
	}

	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?labelSelector=%s",
		p.apiHost, p.namespace, selector)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		p.logger.Error("k8s: failed to create list request", "err", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Error("k8s: pod list failed", "err", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		p.logger.Error("k8s: pod list rejected",
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		return
	}

	var result struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		p.logger.Error("k8s: failed to decode pod list", "err", err)
		return
	}

	for _, pod := range result.Items {
		if pod.Metadata.Name == p.podName {
			continue
		}
		p.logger.Info("k8s: clearing stale primary label", "pod", pod.Metadata.Name)
		p.patchLabel(pod.Metadata.Name, patchRemove, "removed")
	}
}

// patchLabelWithRetry attempts the patch up to maxRetries times with backoff.
func (p *PodLabeler) patchLabelWithRetry(podName, patch, action string) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			p.getClock().Sleep(retryBackoff * time.Duration(attempt))
		}
		if p.patchLabel(podName, patch, action) {
			return
		}
	}
	p.logger.Error("k8s: pod label patch failed after retries",
		"pod", podName, "action", action, "attempts", maxRetries)
}

// patchLabel sends a single patch request. Returns true on success.
func (p *PodLabeler) patchLabel(podName, patch, action string) bool {
	token := p.readToken()
	if token == "" {
		return false
	}

	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", p.apiHost, p.namespace, podName)
	req, err := http.NewRequest(http.MethodPatch, url, strings.NewReader(patch))
	if err != nil {
		p.logger.Error("k8s: failed to create patch request", "err", err)
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/merge-patch+json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Error("k8s: pod label patch failed", "pod", podName, "err", err)
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		p.logger.Error("k8s: pod label patch rejected",
			"pod", podName,
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		return false
	}

	p.logger.Info("k8s: pod label updated", "pod", podName, "action", action, "label", labelKey)
	return true
}
