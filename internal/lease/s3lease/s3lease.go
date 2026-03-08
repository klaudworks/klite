package s3lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/lease"
)

// S3API is the subset of the S3 client needed for lease operations.
type S3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// LeaseBody is the JSON body stored in the S3 lease object.
type LeaseBody struct {
	Version   int       `json:"version"`
	Holder    string    `json:"holder"`
	Epoch     int64     `json:"epoch"`
	RenewedAt time.Time `json:"renewedAt"`
	ReplAddr  string    `json:"replAddr"`
}

// Config configures the S3 lease elector.
type Config struct {
	S3            S3API
	Bucket        string
	Key           string // Full S3 key for the lease object, e.g. "<prefix>/lease"
	Holder        string // Identity of this node
	ReplAddr      string // Replication listen address (host:port)
	LeaseDuration time.Duration
	RenewInterval time.Duration
	RetryInterval time.Duration
	Logger        *slog.Logger
	Clock         clock.Clock // If nil, uses real clock
}

// Elector implements lease.Elector using S3 conditional writes.
type Elector struct {
	cfg Config

	mu                  sync.Mutex
	role                lease.Role
	cachedETag          string
	cachedEpoch         int64
	cachedPrimaryAddr   string // replAddr from the last-read lease body
	lastSuccessfulRenew time.Time
	leaseCancel         context.CancelFunc
}

func New(cfg Config) *Elector {
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = 5 * time.Second
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	return &Elector{
		cfg:  cfg,
		role: lease.RoleStandby,
	}
}

func (e *Elector) Role() lease.Role {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.role
}

func (e *Elector) Epoch() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cachedEpoch < 0 {
		return 0
	}
	return uint64(e.cachedEpoch)
}

func (e *Elector) PrimaryAddr() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cachedPrimaryAddr
}

func (e *Elector) Run(ctx context.Context, cb lease.Callbacks) error {
	if err := e.probeConditionalWrites(ctx); err != nil {
		return fmt.Errorf("s3 lease: conditional write probe failed: %w", err)
	}

	for {
		e.mu.Lock()
		role := e.role
		e.mu.Unlock()

		var interval time.Duration
		switch role {
		case lease.RoleStandby:
			interval = e.cfg.RetryInterval
		case lease.RolePrimary:
			interval = e.cfg.RenewInterval
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.cfg.Clock.After(interval):
		}

		e.mu.Lock()
		role = e.role
		e.mu.Unlock()

		switch role {
		case lease.RoleStandby:
			e.tryAcquire(ctx, cb)
		case lease.RolePrimary:
			e.tryRenew(ctx, cb)
		}
	}
}

func (e *Elector) Release() error {
	e.mu.Lock()
	if e.role != lease.RolePrimary {
		e.mu.Unlock()
		return nil
	}
	cachedETag := e.cachedETag
	epoch := e.cachedEpoch
	e.mu.Unlock()

	body := LeaseBody{
		Version:   1,
		Holder:    e.cfg.Holder,
		Epoch:     epoch,
		RenewedAt: time.Time{}, // zero time — effectively expired
		ReplAddr:  e.cfg.ReplAddr,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal release body: %w", err)
	}

	_, err = e.cfg.S3.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:  &e.cfg.Bucket,
		Key:     &e.cfg.Key,
		Body:    bytes.NewReader(data),
		IfMatch: &cachedETag,
	})
	if err != nil {
		if isPreconditionFailed(err) {
			e.cfg.Logger.Warn("s3 lease: release CAS failed, another node claimed")
			return nil
		}
		e.cfg.Logger.Warn("s3 lease: release failed, lease will expire naturally", "err", err)
		return err
	}

	e.cfg.Logger.Info("s3 lease: released successfully")
	return nil
}

func (e *Elector) probeConditionalWrites(ctx context.Context) error {
	probeKey := e.cfg.Key + ".probe"
	star := "*"

	// Write a probe object
	_, err := e.cfg.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &e.cfg.Bucket,
		Key:         &probeKey,
		Body:        bytes.NewReader([]byte("probe")),
		IfNoneMatch: &star,
	})
	if err != nil && !isPreconditionFailed(err) {
		return fmt.Errorf("probe PutObject failed (S3 backend may not support conditional writes): %w", err)
	}

	// Try conditional overwrite — if probe existed already (412 above), use
	// a fresh unconditional PUT then test If-Match
	if isPreconditionFailed(err) {
		// Object exists from a prior probe — that's fine, conditional writes work
		return nil
	}

	// Object was created. Try If-None-Match again — should get 412
	_, err = e.cfg.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &e.cfg.Bucket,
		Key:         &probeKey,
		Body:        bytes.NewReader([]byte("probe2")),
		IfNoneMatch: &star,
	})
	if err == nil {
		return fmt.Errorf("S3 backend does not support conditional writes: If-None-Match did not return 412")
	}
	if !isPreconditionFailed(err) {
		return fmt.Errorf("S3 backend returned unexpected error for conditional write: %w", err)
	}

	return nil
}

// s3CallTimeout returns a context with a deadline for a single S3 call.
// The timeout is the renew interval (or retry interval for standbys) to
// ensure the lease loop never gets stuck on a hanging S3 call (e.g. when
// packets are silently dropped by a network partition).
func (e *Elector) s3CallTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := e.cfg.RenewInterval
	if timeout > e.cfg.RetryInterval {
		timeout = e.cfg.RetryInterval
	}
	return context.WithTimeout(parent, timeout)
}

func (e *Elector) tryAcquire(ctx context.Context, cb lease.Callbacks) {
	callCtx, cancel := e.s3CallTimeout(ctx)
	defer cancel()

	body, etag, err := e.getLease(callCtx)
	if err != nil {
		if isNoSuchKey(err) {
			e.claimEmpty(callCtx, cb)
			return
		}
		e.cfg.Logger.Warn("s3 lease: get failed", "err", err)
		return
	}

	if body.Version != 1 {
		e.cfg.Logger.Error("s3 lease: unknown version, refusing to claim", "version", body.Version)
		return
	}

	// Cache the current primary's replication address so the standby
	// can discover it via PrimaryAddr().
	e.mu.Lock()
	e.cachedPrimaryAddr = body.ReplAddr
	e.mu.Unlock()

	elapsed := e.cfg.Clock.Since(body.RenewedAt)
	if elapsed < e.cfg.LeaseDuration {
		return
	}

	// Lease expired — try to claim
	newBody := LeaseBody{
		Version:   1,
		Holder:    e.cfg.Holder,
		Epoch:     body.Epoch + 1,
		RenewedAt: e.cfg.Clock.Now(),
		ReplAddr:  e.cfg.ReplAddr,
	}
	data, err := json.Marshal(newBody)
	if err != nil {
		e.cfg.Logger.Error("s3 lease: marshal failed", "err", err)
		return
	}

	out, err := e.cfg.S3.PutObject(callCtx, &s3.PutObjectInput{
		Bucket:  &e.cfg.Bucket,
		Key:     &e.cfg.Key,
		Body:    bytes.NewReader(data),
		IfMatch: &etag,
	})
	if err != nil {
		if isPreconditionFailed(err) {
			e.cfg.Logger.Info("s3 lease: lost acquisition race")
			return
		}
		e.cfg.Logger.Warn("s3 lease: acquisition PutObject failed", "err", err)
		return
	}

	e.becomeLeader(aws.ToString(out.ETag), newBody.Epoch, cb)
}

func (e *Elector) claimEmpty(ctx context.Context, cb lease.Callbacks) {
	newBody := LeaseBody{
		Version:   1,
		Holder:    e.cfg.Holder,
		Epoch:     1,
		RenewedAt: e.cfg.Clock.Now(),
		ReplAddr:  e.cfg.ReplAddr,
	}
	data, err := json.Marshal(newBody)
	if err != nil {
		e.cfg.Logger.Error("s3 lease: marshal failed", "err", err)
		return
	}

	star := "*"
	out, err := e.cfg.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &e.cfg.Bucket,
		Key:         &e.cfg.Key,
		Body:        bytes.NewReader(data),
		IfNoneMatch: &star,
	})
	if err != nil {
		if isPreconditionFailed(err) {
			e.cfg.Logger.Info("s3 lease: lost acquisition race (create)")
			return
		}
		e.cfg.Logger.Warn("s3 lease: create PutObject failed", "err", err)
		return
	}

	e.becomeLeader(aws.ToString(out.ETag), 1, cb)
}

func (e *Elector) becomeLeader(etag string, epoch int64, cb lease.Callbacks) {
	e.mu.Lock()
	e.role = lease.RolePrimary
	e.cachedETag = etag
	e.cachedEpoch = epoch
	e.lastSuccessfulRenew = e.cfg.Clock.Now()

	var leaseCtx context.Context
	leaseCtx, e.leaseCancel = context.WithCancel(context.Background())
	e.mu.Unlock()

	e.cfg.Logger.Info("s3 lease: elected as primary", "epoch", epoch)

	if cb.OnElected != nil {
		cb.OnElected(leaseCtx)
	}
}

func (e *Elector) tryRenew(ctx context.Context, cb lease.Callbacks) {
	e.mu.Lock()
	cachedETag := e.cachedETag
	epoch := e.cachedEpoch
	lastRenew := e.lastSuccessfulRenew
	e.mu.Unlock()

	body := LeaseBody{
		Version:   1,
		Holder:    e.cfg.Holder,
		Epoch:     epoch,
		RenewedAt: e.cfg.Clock.Now(),
		ReplAddr:  e.cfg.ReplAddr,
	}
	data, err := json.Marshal(body)
	if err != nil {
		e.cfg.Logger.Error("s3 lease: marshal renewal failed", "err", err)
		return
	}

	callCtx, cancel := e.s3CallTimeout(ctx)
	defer cancel()

	out, err := e.cfg.S3.PutObject(callCtx, &s3.PutObjectInput{
		Bucket:  &e.cfg.Bucket,
		Key:     &e.cfg.Key,
		Body:    bytes.NewReader(data),
		IfMatch: &cachedETag,
	})
	if err != nil {
		if isPreconditionFailed(err) {
			e.cfg.Logger.Warn("s3 lease: lost lease (CAS failed)")
			e.becomeDemoted(cb)
			return
		}

		// Transient error — check cumulative failure window
		elapsed := e.cfg.Clock.Since(lastRenew)
		if elapsed >= e.cfg.LeaseDuration {
			e.cfg.Logger.Warn("s3 lease: transient errors exceeded lease duration, demoting",
				"elapsed", elapsed, "leaseDuration", e.cfg.LeaseDuration)
			e.becomeDemoted(cb)
			return
		}

		e.cfg.Logger.Warn("s3 lease: transient renewal error, will retry",
			"err", err, "sinceLastRenew", elapsed)
		return
	}

	e.mu.Lock()
	e.cachedETag = aws.ToString(out.ETag)
	e.lastSuccessfulRenew = e.cfg.Clock.Now()
	e.mu.Unlock()
}

func (e *Elector) becomeDemoted(cb lease.Callbacks) {
	e.mu.Lock()
	e.role = lease.RoleStandby
	cancel := e.leaseCancel
	e.leaseCancel = nil
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if cb.OnDemoted != nil {
		cb.OnDemoted()
	}
}

func (e *Elector) getLease(ctx context.Context) (*LeaseBody, string, error) {
	out, err := e.cfg.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &e.cfg.Bucket,
		Key:    &e.cfg.Key,
	})
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close() //nolint:errcheck

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read lease body: %w", err)
	}

	var body LeaseBody
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, "", fmt.Errorf("unmarshal lease: %w", err)
	}

	return &body, aws.ToString(out.ETag), nil
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "PreconditionFailed" || code == "ConditionalRequestConflict"
	}
	return false
}

func isNoSuchKey(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NoSuchKey")
}

var _ lease.Elector = (*Elector)(nil)
