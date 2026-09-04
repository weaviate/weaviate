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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// fakeServer answers verify calls with a configurable status, signing every
// response with its key. It uses nothing outside this package.
type fakeServer struct {
	srv     *httptest.Server
	key     ServerKey
	trusted ServerKeySet
	status  atomic.Value // Status
	down    atomic.Bool
	calls   atomic.Int32
	clock   func() time.Time
	expires time.Time
}

func newFakeServer(t *testing.T, clock func() time.Time) *fakeServer {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := &fakeServer{key: ServerKey{ID: "k", PrivateKey: priv}, trusted: ServerKeySet{"k": pub}, clock: clock}
	f.status.Store(StatusValid)
	f.expires = clock().Add(365 * 24 * time.Hour)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if f.down.Load() {
			http.Error(w, "down", 503)
			return
		}
		var req VerifyRequest
		json.NewDecoder(r.Body).Decode(&req)
		now := f.clock()
		resp := VerifyResponse{LicenseID: req.LicenseID, Status: f.status.Load().(Status), ExpiresAt: f.expires,
			CheckedAt: now, NextCheckAfter: now.Add(24 * time.Hour), Nonce: req.Nonce}
		f.key.Sign(&resp)
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newChecker(t *testing.T, f *fakeServer, clk *clock, enforce bool, cache string) *Checker {
	t.Helper()
	lic, _ := Generate()
	client, err := NewClient(lic.Key(), f.trusted)
	if err != nil {
		t.Fatal(err)
	}
	client.ServerURL = f.srv.URL
	c := &Checker{Client: client, ClusterID: "c-1", InstanceID: "n-1", WeaviateVersion: "1.34.2",
		CachePath: cache, Enforce: enforce, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c.Start()
	return c
}

func TestUnlicensed(t *testing.T) {
	c := &Checker{}
	c.Start()
	if s := c.Snapshot(); s.State != StateUnlicensed || !s.Allowed() {
		t.Fatalf("%+v", s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Run(ctx) // returns immediately
}

func TestHappyPathAndScheduling(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	var changes []State
	c := newChecker(t, f, clk, true, "")
	c.OnChange = func(_, n Snapshot) { changes = append(changes, n.State) }
	if s := c.Snapshot(); s.State != StateUnreachable {
		t.Fatalf("before first check: %v", s.State)
	}
	s := c.CheckNow(context.Background())
	if s.State != StateValid || !s.Allowed() || s.LastValidAt != clk.t || s.ExpiresAt != f.expires {
		t.Fatalf("after check: %+v", s)
	}
	if s.NextCheckAt.Sub(clk.t) != 24*time.Hour {
		t.Fatalf("next check should follow the server's interval, got %v", s.NextCheckAt.Sub(clk.t))
	}
	if len(changes) != 1 || changes[0] != StateValid {
		t.Fatalf("OnChange: %v", changes)
	}
	// Expiry passing on the client clock flips the state without a call.
	// The server kept saying valid right up to expiry, so grace runs from
	// that last answer: expired-but-allowed for 7 days, then degraded.
	clk.t = f.expires.Add(-time.Hour)
	c.CheckNow(context.Background())
	clk.t = f.expires.Add(time.Second)
	if s := c.Snapshot(); s.State != StateExpired || !s.Allowed() {
		t.Fatalf("expired by clock: %+v", s)
	}
	clk.advance(DefaultGracePeriod)
	if s := c.Snapshot(); s.State != StateDegraded || s.Allowed() {
		t.Fatalf("expired past grace: %+v", s)
	}
}

func TestOutageGraceAndDegrade(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	c := newChecker(t, f, clk, true, "")
	c.CheckNow(context.Background())

	f.down.Store(true)
	s := c.CheckNow(context.Background())
	if s.State != StateValid || s.LastError == "" {
		t.Fatalf("outage should keep the last valid state: %+v", s)
	}
	if s.NextCheckAt.Sub(clk.t) != InitialRetryBackoff {
		t.Fatalf("first retry backoff: %v", s.NextCheckAt.Sub(clk.t))
	}
	c.CheckNow(context.Background())
	if got := c.Snapshot().NextCheckAt.Sub(clk.t); got != 2*InitialRetryBackoff {
		t.Fatalf("backoff should double: %v", got)
	}
	for i := 0; i < 10; i++ {
		c.CheckNow(context.Background())
	}
	if got := c.Snapshot().NextCheckAt.Sub(clk.t); got != MaxRetryBackoff {
		t.Fatalf("backoff should cap: %v", got)
	}

	// Still valid up to the grace boundary, degraded after it, and only
	// because Enforce is on.
	clk.advance(DefaultGracePeriod - time.Minute)
	if s := c.Snapshot(); s.State != StateValid {
		t.Fatalf("inside grace: %v", s.State)
	}
	clk.advance(2 * time.Minute)
	if s := c.Snapshot(); s.State != StateDegraded || s.Allowed() {
		t.Fatalf("outage past grace: %+v", s)
	}
	// Server returns and says valid: recovered, grace anchor refreshed.
	f.down.Store(false)
	if s := c.CheckNow(context.Background()); s.State != StateValid {
		t.Fatalf("recovery from outage: %+v", s)
	}
	// Now the license is revoked: allowed through grace, degraded after.
	f.status.Store(StatusRevoked)
	s = c.CheckNow(context.Background())
	if s.State != StateRevoked || s.GraceEndsAt.IsZero() {
		t.Fatalf("revoked answer: %+v", s)
	}
	if !s.Allowed() {
		t.Fatal("must stay allowed inside grace after revocation")
	}
	clk.advance(DefaultGracePeriod + time.Second)
	if s := c.Snapshot(); s.State != StateDegraded || s.Allowed() {
		t.Fatalf("after grace: %+v", s)
	}
	// A fresh valid answer recovers immediately.
	f.status.Store(StatusValid)
	f.expires = clk.t.Add(time.Hour)
	if s := c.CheckNow(context.Background()); s.State != StateValid || !s.Allowed() {
		t.Fatalf("recovery: %+v", s)
	}
	// Backoff resets after success.
	if got := c.Snapshot().NextCheckAt.Sub(clk.t); got != 24*time.Hour {
		t.Fatalf("interval after recovery: %v", got)
	}
}

func TestLogOnlyNeverDegrades(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	f.status.Store(StatusRevoked)
	c := newChecker(t, f, clk, false, "")
	c.CheckNow(context.Background())
	clk.advance(30 * 24 * time.Hour)
	if s := c.Snapshot(); s.State != StateRevoked || !s.Allowed() || s.Enforcing {
		t.Fatalf("log-only: %+v", s)
	}
}

func TestNeverReachableDegradesFromStart(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	f.down.Store(true)
	c := newChecker(t, f, clk, true, "")
	c.CheckNow(context.Background())
	if s := c.Snapshot(); s.State != StateUnreachable || !s.Allowed() {
		t.Fatalf("fresh node during outage must be allowed: %+v", s)
	}
	clk.advance(DefaultGracePeriod + time.Second)
	if s := c.Snapshot(); s.State != StateDegraded {
		t.Fatalf("fresh node after grace: %+v", s)
	}
}

func TestCacheRoundTripAndTamper(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	dir := t.TempDir()
	cache := filepath.Join(dir, "sub", "license.json")

	c1 := newChecker(t, f, clk, true, cache)
	c1.CheckNow(context.Background())
	if _, err := os.Stat(cache); err != nil {
		t.Fatal("cache not written")
	}

	// Restart during an outage: state restored from cache, no call needed.
	f.down.Store(true)
	c2 := &Checker{Client: c1.Client, CachePath: cache, Enforce: true, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c2.Start()
	s := c2.Snapshot()
	if s.State != StateValid || s.LastValidAt != clk.t || s.NextCheckAt != clk.t {
		t.Fatalf("restored: %+v", s)
	}
	if !s.Allowed() {
		t.Fatal("restored valid state must be allowed")
	}

	// Tampered cache (status flipped) is ignored: signature no longer matches.
	raw, _ := os.ReadFile(cache)
	var cf cacheFile
	json.Unmarshal(raw, &cf)
	cf.Response.Status = StatusValid
	cf.Response.ExpiresAt = cf.Response.ExpiresAt.Add(10 * 365 * 24 * time.Hour)
	tampered, _ := json.Marshal(cf)
	os.WriteFile(cache, tampered, 0o600)
	c3 := &Checker{Client: c1.Client, CachePath: cache, Enforce: true, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c3.Start()
	if s := c3.Snapshot(); s.State != StateUnreachable {
		t.Fatalf("tampered cache accepted: %+v", s)
	}

	// Cache for another license key is ignored.
	other, _ := Generate()
	oc, _ := NewClient(other.Key(), f.trusted)
	oc.ServerURL = f.srv.URL
	c4 := &Checker{Client: oc, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c4.Start()
	if s := c4.Snapshot(); s.State != StateUnreachable {
		t.Fatalf("foreign cache accepted: %+v", s)
	}
	// Corrupt file is ignored, not fatal.
	os.WriteFile(cache, []byte("{nope"), 0o600)
	c5 := &Checker{Client: c1.Client, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c5.Start()
	if s := c5.Snapshot(); s.State != StateUnreachable {
		t.Fatalf("corrupt cache: %+v", s)
	}
}

func TestRunLoopStopsOnContext(t *testing.T) {
	clk := &clock{time.Now()}
	f := newFakeServer(t, clk.now)
	c := newChecker(t, f, clk, false, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for f.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	if f.calls.Load() != 1 || c.Snapshot().State != StateValid {
		t.Fatalf("calls=%d state=%v", f.calls.Load(), c.Snapshot().State)
	}
}
