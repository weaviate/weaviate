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

// Package checker runs the client side of the license protocol for one
// node: periodic signed verify calls against the license service, a
// signed-response disk cache, and the license state machine that decides
// whether enterprise features may run.
package checker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	licenseclient "github.com/weaviate/weaviate/adapters/clients/license"
	"github.com/weaviate/weaviate/entities/license"
	licensestate "github.com/weaviate/weaviate/usecases/license"
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
	State           licensestate.Status `json:"state"`
	LicenseID       string              `json:"license_id,omitempty"`
	LastStatus      license.Status      `json:"last_status,omitempty"` // from the last signed answer
	ExpiresAt       time.Time           `json:"expires_at,omitempty"`
	LastCheckedAt   time.Time           `json:"last_checked_at,omitempty"` // last signed answer of any status
	LastValidAt     time.Time           `json:"last_valid_at,omitempty"`   // last signed "valid"
	NextCheckAt     time.Time           `json:"next_check_at,omitempty"`
	LastError       string              `json:"last_error,omitempty"`
	ClusterMismatch bool                `json:"cluster_mismatch,omitempty"`
	GraceEndsAt     time.Time           `json:"grace_ends_at,omitempty"` // when degradation starts
}

// Allowed reports whether enterprise features may run.
func (s Snapshot) Allowed() bool { return s.State != licensestate.StatusDegraded }

// Checker runs the client side of the protocol for one node.
type Checker struct {
	Client *licenseclient.Client
	// ClusterID is reported on every check. ClusterIDFunc, when set, is
	// consulted instead on each check, for hosts whose cluster identity is
	// only known some time after startup.
	ClusterID       string
	ClusterIDFunc   func() string
	InstanceID      string
	WeaviateVersion string

	// CachePath, when set, persists the last signed response so a restart
	// during an outage does not lose license state. Signatures and license
	// IDs are re-verified on load, so a tampered or foreign cache is
	// ignored.
	CachePath string
	// GracePeriod is how long without a signed "valid" before the node
	// degrades. Zero means DefaultGracePeriod.
	GracePeriod time.Duration
	// OnChange is called whenever the State changes.
	OnChange func(old, new Snapshot)
	Log      *slog.Logger
	Now      func() time.Time

	mu        sync.Mutex
	snap      Snapshot
	lastResp  *license.VerifyResponse
	lastValid *license.VerifyResponse // last signed "valid" answer; anchors the grace period
	backoff   time.Duration
}

type cacheFile struct {
	Response license.VerifyResponse `json:"response"` // last signed answer, any status
	// LastValid is the last signed "valid" answer. The grace period is
	// derived exclusively from its signed CheckedAt; keeping it signed
	// means a hand-edited cache cannot move the grace anchor into the
	// future to postpone degradation.
	LastValid *license.VerifyResponse `json:"last_valid,omitempty"`
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

// Snapshot returns the current view, recomputing time-dependent state. When
// the recompute crosses a state boundary (expiry or the grace period passing
// on the local clock), OnChange is invoked, after the lock is released.
func (c *Checker) Snapshot() Snapshot {
	c.mu.Lock()
	old := c.snap
	c.recompute()
	newSnap := c.snap
	c.mu.Unlock()
	if old.State != newSnap.State && c.OnChange != nil {
		c.OnChange(old, newSnap)
	}
	return newSnap
}

// Allowed is shorthand for Snapshot().Allowed().
func (c *Checker) Allowed() bool { return c.Snapshot().Allowed() }

// Start loads the cache and returns; call Run to begin checking.
func (c *Checker) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Client == nil {
		c.snap = Snapshot{State: licensestate.StatusUnlicensed}
		return
	}
	c.snap = Snapshot{LicenseID: c.Client.LicenseID}
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
		return Snapshot{State: licensestate.StatusUnlicensed}
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
		if resp.Status == license.StatusValid {
			c.lastValid = &resp
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
	s.GraceEndsAt = time.Time{}

	var base licensestate.Status
	switch {
	case c.lastResp == nil:
		base = licensestate.StatusUnreachable
	case c.lastResp.Status == license.StatusValid && now.Before(c.lastResp.ExpiresAt):
		base = licensestate.StatusValid
	case c.lastResp.Status == license.StatusValid: // a cached valid answer whose expiry has since passed
		base = licensestate.StatusExpired
	case c.lastResp.Status == license.StatusExpired:
		base = licensestate.StatusExpired
	case c.lastResp.Status == license.StatusRevoked:
		base = licensestate.StatusRevoked
	default:
		base = licensestate.StatusUnknown
	}

	// Grace runs only from a signed "valid" answer for this license ("no
	// valid response for 7 days"). A node that never had one — the server
	// unreachable or every answer non-valid — degrades at once; otherwise
	// every restart would start a fresh grace period. A stale "valid"
	// answer counts as expired, not valid: a week-long license-server
	// outage must not be indistinguishable from a healthy license.
	anchor := s.LastValidAt
	if anchor.IsZero() {
		s.State = licensestate.StatusDegraded
		return
	}
	s.GraceEndsAt = anchor.Add(c.grace())
	if !now.Before(s.GraceEndsAt) {
		base = licensestate.StatusDegraded
	}
	s.State = base
}

func (c *Checker) logState(old, new Snapshot) {
	l := c.log().With("license_id", new.LicenseID, "state", new.State)
	switch {
	case old.State != new.State:
		l.Info("license state changed", "from", old.State, "expires_at", new.ExpiresAt)
	case new.State == licensestate.StatusValid:
		l.Debug("license ok", "expires_at", new.ExpiresAt, "next_check_at", new.NextCheckAt)
	}
	if new.ClusterMismatch && !old.ClusterMismatch {
		l.Warn("license was issued for a different cluster; contact Weaviate support")
	}
	if new.State != licensestate.StatusValid && new.State != licensestate.StatusUnlicensed {
		if new.State == licensestate.StatusDegraded {
			l.Error("license degraded: enterprise features are disabled; contact Weaviate support")
		} else {
			l.Warn("license not confirmed; enterprise features will be disabled at grace end", "grace_ends_at", new.GraceEndsAt)
		}
	}
}

// ---- cache ----------------------------------------------------------------

// cachedAnswerOK reports whether resp was signed by a trusted server key for
// this license.
func (c *Checker) cachedAnswerOK(resp license.VerifyResponse) error {
	if resp.LicenseID != c.Client.LicenseID {
		return errors.New("license: cached answer is for a different license")
	}
	return c.Client.TrustedKeys.Verify(resp)
}

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
	if err := c.cachedAnswerOK(f.Response); err != nil {
		c.log().Warn("license cache invalid; ignoring", "path", c.CachePath, "err", err)
		return
	}
	// The grace anchor is derived only from signed answers, so a
	// hand-edited cache cannot postpone degradation. If a last-valid answer
	// is present, it must be signed, for this license, and say "valid",
	// otherwise the whole cache is ignored.
	if f.LastValid != nil {
		if err := c.cachedAnswerOK(*f.LastValid); err != nil || f.LastValid.Status != license.StatusValid {
			c.log().Warn("license cache invalid; ignoring", "path", c.CachePath)
			return
		}
	}
	resp := f.Response
	c.lastResp = &resp
	c.snap.LastStatus = resp.Status
	c.snap.LastCheckedAt = resp.CheckedAt
	c.snap.ExpiresAt = resp.ExpiresAt
	c.snap.ClusterMismatch = resp.ClusterMismatch
	switch {
	case f.LastValid != nil:
		valid := *f.LastValid
		c.lastValid = &valid
		c.snap.LastValidAt = valid.CheckedAt
	case resp.Status == license.StatusValid:
		c.lastValid = &resp
		c.snap.LastValidAt = resp.CheckedAt
	}
	c.snap.NextCheckAt = c.now() // check straight away
	c.log().Info("license state restored from cache", "status", resp.Status, "checked_at", resp.CheckedAt)
}

func (c *Checker) saveCache() {
	if c.CachePath == "" || c.lastResp == nil {
		return
	}
	raw, err := json.Marshal(cacheFile{Response: *c.lastResp, LastValid: c.lastValid})
	if err != nil {
		return
	}
	tmp := c.CachePath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(c.CachePath), 0o700); err != nil {
		c.log().Warn("license cache write failed", "path", c.CachePath, "err", err)
		return
	}
	if err = os.WriteFile(tmp, raw, 0o600); err == nil {
		err = os.Rename(tmp, c.CachePath)
	}
	if err != nil {
		c.log().Warn("license cache write failed", "path", c.CachePath, "err", err)
	}
}
