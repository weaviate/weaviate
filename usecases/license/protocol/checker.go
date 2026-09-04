//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State is what a Weaviate node knows about its license right now.
type State string

const (
	// StateUnlicensed: no key configured; community mode, no checks run.
	StateUnlicensed State = "unlicensed"
	// StateValid: the last signed answer said valid and has not expired.
	StateValid State = "valid"
	// StateExpired / StateRevoked / StateUnknownLicense: the last signed
	// answer said so.
	StateExpired        State = "expired"
	StateRevoked        State = "revoked"
	StateUnknownLicense State = "unknown"
	// StateUnreachable: no trustworthy answer yet, or the last attempt
	// failed; the previous state (if any) is kept in Snapshot.LastStatus.
	StateUnreachable State = "unreachable"
	// StateDegraded: enforcement is on and no valid answer has been obtained
	// within the grace period. Enterprise features should be disabled.
	StateDegraded State = "degraded"
)

// Defaults for the check loop.
const (
	DefaultGracePeriod  = 7 * 24 * time.Hour
	MinCheckInterval    = time.Hour
	MaxCheckInterval    = 7 * 24 * time.Hour
	InitialRetryBackoff = time.Minute
	MaxRetryBackoff     = time.Hour
)

// Snapshot is a point-in-time view of the checker.
type Snapshot struct {
	State           State     `json:"state"`
	LicenseID       string    `json:"license_id,omitempty"`
	LastStatus      Status    `json:"last_status,omitempty"` // from the last signed answer
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
	LastCheckedAt   time.Time `json:"last_checked_at,omitempty"` // last signed answer of any status
	LastValidAt     time.Time `json:"last_valid_at,omitempty"`   // last signed "valid"
	NextCheckAt     time.Time `json:"next_check_at,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	ClusterMismatch bool      `json:"cluster_mismatch,omitempty"`
	Enforcing       bool      `json:"enforcing"`
	GraceEndsAt     time.Time `json:"grace_ends_at,omitempty"` // when degradation would start, if enforcing
}

// Allowed reports whether enterprise features may run.
func (s Snapshot) Allowed() bool { return s.State != StateDegraded }

// Checker runs the client side of the protocol for one node.
type Checker struct {
	Client *Client
	// ClusterID is reported on every check. ClusterIDFunc, when set, is
	// consulted instead on each check, for hosts whose cluster identity is
	// only known some time after startup.
	ClusterID       string
	ClusterIDFunc   func() string
	InstanceID      string
	WeaviateVersion string

	// CachePath, when set, persists the last signed response so a restart
	// during an outage does not lose license state. The signature is
	// re-verified on load, so a tampered cache is ignored.
	CachePath string
	// GracePeriod is how long without a signed "valid" before the node
	// degrades. Zero means DefaultGracePeriod.
	GracePeriod time.Duration
	// Enforce turns the degraded state on. When false the checker only logs
	// (RFC phase 1).
	Enforce bool
	// OnChange is called whenever the State changes.
	OnChange func(old, new Snapshot)
	Log      *slog.Logger
	Now      func() time.Time

	mu        sync.Mutex
	snap      Snapshot
	lastResp  *VerifyResponse
	backoff   time.Duration
	startedAt time.Time
}

type cacheFile struct {
	Response    VerifyResponse `json:"response"`
	LastValidAt time.Time      `json:"last_valid_at"`
	GraceAnchor time.Time      `json:"grace_anchor"` // start of the current no-valid-answer run
}

func (c *Checker) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Checker) log() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

func (c *Checker) grace() time.Duration {
	if c.GracePeriod > 0 {
		return c.GracePeriod
	}
	return DefaultGracePeriod
}

// Snapshot returns the current view, recomputing time-dependent state.
func (c *Checker) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recompute()
	return c.snap
}

// Allowed is shorthand for Snapshot().Allowed().
func (c *Checker) Allowed() bool { return c.Snapshot().Allowed() }

// Start loads the cache and returns; call Run to begin checking.
func (c *Checker) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Client == nil {
		c.snap = Snapshot{State: StateUnlicensed}
		return
	}
	c.snap = Snapshot{State: StateUnreachable, LicenseID: c.Client.LicenseID, Enforcing: c.Enforce}
	c.startedAt = c.now()
	c.loadCache()
	c.recompute()
}

// Run checks immediately, then keeps checking until ctx ends. It returns
// at once for an unlicensed checker.
func (c *Checker) Run(ctx context.Context) {
	if c.Client == nil {
		return
	}
	for {
		c.CheckNow(ctx)
		next := c.Snapshot().NextCheckAt
		d := next.Sub(c.now())
		if d < 0 {
			d = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

// CheckNow performs one verify call and updates the snapshot.
func (c *Checker) CheckNow(ctx context.Context) Snapshot {
	if c.Client == nil {
		return Snapshot{State: StateUnlicensed}
	}
	clusterID := c.ClusterID
	if c.ClusterIDFunc != nil {
		if v := c.ClusterIDFunc(); v != "" {
			clusterID = v
		}
	}
	resp, err := c.Client.Verify(ctx, clusterID, c.InstanceID, c.WeaviateVersion)
	now := c.now()

	c.mu.Lock()
	old := c.snap
	if err != nil {
		c.snap.LastError = err.Error()
		if c.backoff == 0 {
			c.backoff = InitialRetryBackoff
		} else {
			c.backoff *= 2
			if c.backoff > MaxRetryBackoff {
				c.backoff = MaxRetryBackoff
			}
		}
		c.snap.NextCheckAt = now.Add(c.backoff)
		c.log().Warn("license check failed", "license_id", c.Client.LicenseID, "err", err, "retry_in", c.backoff)
	} else {
		c.backoff = 0
		c.snap.LastError = ""
		c.lastResp = &resp
		c.snap.LastStatus = resp.Status
		c.snap.LastCheckedAt = resp.CheckedAt
		c.snap.ExpiresAt = resp.ExpiresAt
		c.snap.ClusterMismatch = resp.ClusterMismatch
		if resp.Status == StatusValid {
			c.snap.LastValidAt = resp.CheckedAt
		}
		interval := resp.NextCheckAfter.Sub(now)
		if interval < MinCheckInterval {
			interval = MinCheckInterval
		}
		if interval > MaxCheckInterval {
			interval = MaxCheckInterval
		}
		c.snap.NextCheckAt = now.Add(interval)
		c.saveCache()
	}
	c.recompute()
	newSnap := c.snap
	c.mu.Unlock()

	c.logState(old, newSnap)
	if old.State != newSnap.State && c.OnChange != nil {
		c.OnChange(old, newSnap)
	}
	return newSnap
}

// recompute derives State from the last answer, the clock and the grace
// period. Caller holds mu.
func (c *Checker) recompute() {
	if c.Client == nil {
		return
	}
	now := c.now()
	s := &c.snap
	s.Enforcing = c.Enforce
	s.GraceEndsAt = time.Time{}

	var base State
	switch {
	case c.lastResp == nil:
		base = StateUnreachable
	case c.lastResp.Status == StatusValid && now.Before(c.lastResp.ExpiresAt):
		base = StateValid
	case c.lastResp.Status == StatusValid: // a cached valid answer whose expiry has since passed
		base = StateExpired
	case c.lastResp.Status == StatusExpired:
		base = StateExpired
	case c.lastResp.Status == StatusRevoked:
		base = StateRevoked
	default:
		base = StateUnknownLicense
	}

	// Grace runs from the last signed "valid" answer (RFC 6.4: "no valid
	// response for 7 days"). A node that never had one is anchored on its own
	// start time, persisted through the cache, so it still degrades
	// eventually. A stale "valid" answer counts as unreachable, not valid:
	// a week-long license-server outage must not be indistinguishable from
	// a healthy license.
	anchor := s.LastValidAt
	if anchor.IsZero() {
		anchor = c.startedAt
	}
	if anchor.IsZero() {
		anchor = now
	}
	s.GraceEndsAt = anchor.Add(c.grace())
	inGrace := now.Before(s.GraceEndsAt)

	switch {
	case base == StateValid && inGrace:
		// fine
	case base == StateValid && c.Enforce:
		base = StateDegraded
	case base == StateValid:
		base = StateUnreachable
	case !inGrace && c.Enforce:
		base = StateDegraded
	}
	s.State = base
}

func (c *Checker) logState(old, new Snapshot) {
	l := c.log().With("license_id", new.LicenseID, "state", new.State)
	switch {
	case old.State != new.State:
		l.Info("license state changed", "from", old.State, "expires_at", new.ExpiresAt, "enforcing", new.Enforcing)
	case new.State == StateValid:
		l.Debug("license ok", "expires_at", new.ExpiresAt, "next_check_at", new.NextCheckAt)
	}
	if new.ClusterMismatch && !old.ClusterMismatch {
		l.Warn("license was issued for a different cluster; contact Weaviate support")
	}
	if new.State != StateValid && new.State != StateUnlicensed {
		if new.State == StateDegraded {
			l.Error("license degraded: enterprise features are disabled; contact Weaviate support")
		} else if new.Enforcing {
			l.Warn("license not confirmed; enterprise features will be disabled at grace end", "grace_ends_at", new.GraceEndsAt)
		}
	}
}

// ---- cache ----------------------------------------------------------------

func (c *Checker) loadCache() {
	if c.CachePath == "" {
		return
	}
	raw, err := os.ReadFile(c.CachePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			c.log().Warn("license cache unreadable", "path", c.CachePath, "err", err)
		}
		return
	}
	var f cacheFile
	if err := json.Unmarshal(raw, &f); err != nil {
		c.log().Warn("license cache corrupt; ignoring", "path", c.CachePath, "err", err)
		return
	}
	if f.Response.LicenseID != c.Client.LicenseID {
		return // cache from a different key
	}
	if err := c.Client.TrustedKeys.Verify(f.Response); err != nil {
		c.log().Warn("license cache signature invalid; ignoring", "path", c.CachePath, "err", err)
		return
	}
	resp := f.Response
	c.lastResp = &resp
	c.snap.LastStatus = resp.Status
	c.snap.LastCheckedAt = resp.CheckedAt
	c.snap.ExpiresAt = resp.ExpiresAt
	c.snap.ClusterMismatch = resp.ClusterMismatch
	c.snap.LastValidAt = f.LastValidAt
	if f.LastValidAt.IsZero() && !f.GraceAnchor.IsZero() && f.GraceAnchor.Before(c.startedAt) {
		c.startedAt = f.GraceAnchor
	}
	c.snap.NextCheckAt = c.now() // check straight away
	c.log().Info("license state restored from cache", "status", resp.Status, "checked_at", resp.CheckedAt)
}

func (c *Checker) saveCache() {
	if c.CachePath == "" || c.lastResp == nil {
		return
	}
	anchor := c.snap.LastValidAt
	if anchor.IsZero() {
		anchor = c.startedAt
	}
	raw, err := json.Marshal(cacheFile{Response: *c.lastResp, LastValidAt: c.snap.LastValidAt, GraceAnchor: anchor})
	if err != nil {
		return
	}
	tmp := c.CachePath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(c.CachePath), 0o700); err == nil {
		if err := os.WriteFile(tmp, raw, 0o600); err == nil {
			err = os.Rename(tmp, c.CachePath)
		}
		if err != nil {
			c.log().Warn("license cache write failed", "path", c.CachePath, "err", err)
		}
	}
}
