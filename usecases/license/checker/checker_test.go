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

package checker

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

	licenseclient "github.com/weaviate/weaviate/adapters/clients/license"
	"github.com/weaviate/weaviate/entities/license"
	licensestate "github.com/weaviate/weaviate/usecases/license"
)

// fakeServer answers verify calls with a configurable status, signing every
// response with its key.
type fakeServer struct {
	srv     *httptest.Server
	key     license.ServerKey
	trusted license.ServerKeySet
	status  atomic.Value // license.Status
	down    atomic.Bool
	calls   atomic.Int32
	clock   func() time.Time
	expires time.Time
}

func newFakeServer(t *testing.T, clock func() time.Time) *fakeServer {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := &fakeServer{key: license.ServerKey{ID: "k", PrivateKey: priv}, trusted: license.ServerKeySet{"k": pub}, clock: clock}
	f.status.Store(license.StatusValid)
	f.expires = clock().Add(365 * 24 * time.Hour)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if f.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		var req license.VerifyRequest
		json.NewDecoder(r.Body).Decode(&req)
		now := f.clock()
		resp := license.VerifyResponse{
			LicenseID: req.LicenseID, Status: f.status.Load().(license.Status), ExpiresAt: f.expires,
			CheckedAt: now, NextCheckAfter: now.Add(24 * time.Hour), Nonce: req.Nonce,
		}
		f.key.Sign(&resp)
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newChecker(t *testing.T, f *fakeServer, clk *clock, cache string) *Checker {
	t.Helper()
	lic, _ := license.Generate()
	client, err := licenseclient.NewClient(lic.Key(), f.trusted)
	if err != nil {
		t.Fatal(err)
	}
	client.ServerURL = f.srv.URL
	c := &Checker{
		Client: client, ClusterID: "c-1", InstanceID: "n-1", WeaviateVersion: "1.34.2",
		CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now,
	}
	c.Start()
	return c
}

func TestUnlicensed(t *testing.T) {
	c := &Checker{}
	c.Start()
	if s := c.Snapshot(); s.State != licensestate.StatusUnlicensed || !s.Allowed() {
		t.Fatalf("%+v", s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Run(ctx) // returns immediately
}

func TestHappyPathAndScheduling(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	var changes []licensestate.Status
	c := newChecker(t, f, clk, "")
	c.OnChange = func(_, n Snapshot) { changes = append(changes, n.State) }
	// Before the first signed "valid" answer the node is already degraded:
	// there is no grace without one.
	if s := c.Snapshot(); s.State != licensestate.StatusDegraded || s.Allowed() {
		t.Fatalf("before first check: %+v", s)
	}
	s := c.CheckNow(context.Background())
	if s.State != licensestate.StatusValid || !s.Allowed() || s.LastValidAt != clk.t || s.ExpiresAt != f.expires {
		t.Fatalf("after check: %+v", s)
	}
	if s.NextCheckAt.Sub(clk.t) != 24*time.Hour {
		t.Fatalf("next check should follow the server's interval, got %v", s.NextCheckAt.Sub(clk.t))
	}
	if len(changes) != 1 || changes[0] != licensestate.StatusValid {
		t.Fatalf("OnChange: %v", changes)
	}
	// Expiry passing on the client clock flips the state without a call.
	// The server kept saying valid right up to expiry, so grace runs from
	// that last answer: expired-but-allowed for 7 days, then degraded.
	clk.t = f.expires.Add(-time.Hour)
	c.CheckNow(context.Background())
	clk.t = f.expires.Add(time.Second)
	if s := c.Snapshot(); s.State != licensestate.StatusExpired || !s.Allowed() {
		t.Fatalf("expired by clock: %+v", s)
	}
	clk.advance(DefaultGracePeriod)
	if s := c.Snapshot(); s.State != licensestate.StatusDegraded || s.Allowed() {
		t.Fatalf("expired past grace: %+v", s)
	}
}

func TestOutageGraceAndDegrade(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	c := newChecker(t, f, clk, "")
	c.CheckNow(context.Background())

	f.down.Store(true)
	s := c.CheckNow(context.Background())
	if s.State != licensestate.StatusValid || s.LastError == "" {
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

	// Still valid up to the grace boundary, degraded after it.
	clk.advance(DefaultGracePeriod - time.Minute)
	if s := c.Snapshot(); s.State != licensestate.StatusValid {
		t.Fatalf("inside grace: %v", s.State)
	}
	clk.advance(2 * time.Minute)
	if s := c.Snapshot(); s.State != licensestate.StatusDegraded || s.Allowed() {
		t.Fatalf("outage past grace: %+v", s)
	}
	// Server returns and says valid: recovered, grace anchor refreshed.
	f.down.Store(false)
	if s := c.CheckNow(context.Background()); s.State != licensestate.StatusValid {
		t.Fatalf("recovery from outage: %+v", s)
	}
	// Now the license is revoked: allowed through grace, degraded after.
	f.status.Store(license.StatusRevoked)
	s = c.CheckNow(context.Background())
	if s.State != licensestate.StatusRevoked || s.GraceEndsAt.IsZero() {
		t.Fatalf("revoked answer: %+v", s)
	}
	if !s.Allowed() {
		t.Fatal("must stay allowed inside grace after revocation")
	}
	clk.advance(DefaultGracePeriod + time.Second)
	if s := c.Snapshot(); s.State != licensestate.StatusDegraded || s.Allowed() {
		t.Fatalf("after grace: %+v", s)
	}
	// A fresh valid answer recovers immediately.
	f.status.Store(license.StatusValid)
	f.expires = clk.t.Add(time.Hour)
	if s := c.CheckNow(context.Background()); s.State != licensestate.StatusValid || !s.Allowed() {
		t.Fatalf("recovery: %+v", s)
	}
	// Backoff resets after success.
	if got := c.Snapshot().NextCheckAt.Sub(clk.t); got != 24*time.Hour {
		t.Fatalf("interval after recovery: %v", got)
	}
}

// TestNoGraceWithoutValidAnswer pins the fail-closed rule: without a signed
// "valid" answer for this license, the node degrades at once — otherwise
// every restart would start a fresh grace period.
func TestNoGraceWithoutValidAnswer(t *testing.T) {
	newClock := func() *clock { return &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)} }

	for _, st := range []license.Status{license.StatusExpired, license.StatusRevoked, license.StatusUnknown} {
		t.Run("server signs "+string(st), func(t *testing.T) {
			clk := newClock()
			f := newFakeServer(t, clk.now)
			f.status.Store(st)
			c := newChecker(t, f, clk, "")
			if s := c.CheckNow(context.Background()); s.State != licensestate.StatusDegraded || s.Allowed() {
				t.Fatalf("first check with a non-valid answer must degrade at once: %+v", s)
			}
			// A restart with no cache is degraded immediately, too.
			c2 := &Checker{Client: c.Client, Log: slog.New(slog.DiscardHandler), Now: clk.now}
			c2.Start()
			if s := c2.Snapshot(); s.State != licensestate.StatusDegraded || s.Allowed() {
				t.Fatalf("restart without any valid answer: %+v", s)
			}
		})
	}

	t.Run("server unreachable and no cache", func(t *testing.T) {
		clk := newClock()
		f := newFakeServer(t, clk.now)
		f.down.Store(true)
		c := newChecker(t, f, clk, "")
		if s := c.CheckNow(context.Background()); s.State != licensestate.StatusDegraded || s.Allowed() {
			t.Fatalf("unreachable with no cache must degrade at once: %+v", s)
		}
	})
}

func TestCacheRoundTripAndTamper(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	dir := t.TempDir()
	cache := filepath.Join(dir, "sub", "license.json")

	c1 := newChecker(t, f, clk, cache)
	c1.CheckNow(context.Background())
	if _, err := os.Stat(cache); err != nil {
		t.Fatal("cache not written")
	}

	// Restart during an outage: state restored from cache, no call needed.
	f.down.Store(true)
	c2 := &Checker{Client: c1.Client, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c2.Start()
	s := c2.Snapshot()
	if s.State != licensestate.StatusValid || s.LastValidAt != clk.t || s.NextCheckAt != clk.t {
		t.Fatalf("restored: %+v", s)
	}
	if !s.Allowed() {
		t.Fatal("restored valid state must be allowed")
	}

	// Tampered cache (status flipped) is ignored: signature no longer
	// matches. With no signed "valid" answer the node degrades at once.
	raw, _ := os.ReadFile(cache)
	var cf cacheFile
	json.Unmarshal(raw, &cf)
	cf.Response.Status = license.StatusValid
	cf.Response.ExpiresAt = cf.Response.ExpiresAt.Add(10 * 365 * 24 * time.Hour)
	tampered, _ := json.Marshal(cf)
	os.WriteFile(cache, tampered, 0o600)
	c3 := &Checker{Client: c1.Client, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c3.Start()
	if s := c3.Snapshot(); s.State != licensestate.StatusDegraded {
		t.Fatalf("tampered cache accepted: %+v", s)
	}

	// Cache for another license key is ignored.
	other, _ := license.Generate()
	oc, _ := licenseclient.NewClient(other.Key(), f.trusted)
	oc.ServerURL = f.srv.URL
	c4 := &Checker{Client: oc, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c4.Start()
	if s := c4.Snapshot(); s.State != licensestate.StatusDegraded {
		t.Fatalf("foreign cache accepted: %+v", s)
	}
	// Corrupt file is ignored, not fatal.
	os.WriteFile(cache, []byte("{nope"), 0o600)
	c5 := &Checker{Client: c1.Client, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c5.Start()
	if s := c5.Snapshot(); s.State != licensestate.StatusDegraded {
		t.Fatalf("corrupt cache: %+v", s)
	}
}

// TestCacheLastValidForOtherLicense: the last-valid answer in the cache must
// be signed for this license, not merely by a trusted server key — otherwise
// a copied cache entry could set the grace anchor for the wrong license.
func TestCacheLastValidForOtherLicense(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	dir := t.TempDir()
	cacheA := filepath.Join(dir, "a.json")
	cacheB := filepath.Join(dir, "b.json")

	ca := newChecker(t, f, clk, cacheA) // fresh license A
	ca.CheckNow(context.Background())
	cb := newChecker(t, f, clk, cacheB) // fresh license B
	cb.CheckNow(context.Background())

	var cfA, cfB cacheFile
	rawA, _ := os.ReadFile(cacheA)
	json.Unmarshal(rawA, &cfA)
	rawB, _ := os.ReadFile(cacheB)
	json.Unmarshal(rawB, &cfB)
	if cfA.LastValid == nil || cfB.LastValid == nil {
		t.Fatal("both caches should carry a last-valid answer")
	}

	// A's latest answer is genuine, but the last-valid answer was signed
	// for B: the whole cache must be ignored.
	cfA.LastValid = cfB.LastValid
	forged, _ := json.Marshal(cfA)
	os.WriteFile(cacheA, forged, 0o600)

	c2 := &Checker{Client: ca.Client, CachePath: cacheA, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c2.Start()
	if s := c2.Snapshot(); s.State != licensestate.StatusDegraded {
		t.Fatalf("cache with a last-valid answer for another license accepted: %+v", s)
	}
}

func TestRunLoopStopsOnContext(t *testing.T) {
	clk := &clock{time.Now()}
	f := newFakeServer(t, clk.now)
	c := newChecker(t, f, clk, "")
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
	if f.calls.Load() != 1 || c.Snapshot().State != licensestate.StatusValid {
		t.Fatalf("calls=%d state=%v", f.calls.Load(), c.Snapshot().State)
	}
}

func TestCacheGraceMetadataTamper(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	dir := t.TempDir()
	cache := filepath.Join(dir, "license.json")

	c1 := newChecker(t, f, clk, cache)
	c1.CheckNow(context.Background()) // valid answer, cache written

	// Hand-edit the last-valid answer's timestamp into the future. The
	// signature no longer matches, so the whole cache must be ignored —
	// and with no signed "valid" answer left, the node degrades at once
	// rather than gaining extended grace.
	raw, _ := os.ReadFile(cache)
	var cf cacheFile
	json.Unmarshal(raw, &cf)
	if cf.LastValid == nil {
		t.Fatal("cache should carry the last valid answer")
	}
	cf.LastValid.CheckedAt = clk.t.Add(100 * 24 * time.Hour)
	tampered, _ := json.Marshal(cf)
	os.WriteFile(cache, tampered, 0o600)

	f.down.Store(true) // license service down during the restart
	c2 := &Checker{Client: c1.Client, CachePath: cache, Log: slog.New(slog.DiscardHandler), Now: clk.now}
	c2.Start()
	if s := c2.Snapshot(); s.State != licensestate.StatusDegraded || s.Allowed() {
		t.Fatalf("tampered grace metadata accepted: %+v", s)
	}
}

func TestOnChangeFiresOnClockDrivenTransition(t *testing.T) {
	clk := &clock{time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	f := newFakeServer(t, clk.now)
	c := newChecker(t, f, clk, "")
	var changes []licensestate.Status
	c.OnChange = func(_, n Snapshot) { changes = append(changes, n.State) }
	c.CheckNow(context.Background()) // degraded -> valid
	if len(changes) != 1 || changes[0] != licensestate.StatusValid {
		t.Fatalf("OnChange after check: %v", changes)
	}

	// The grace period passes on the local clock without any server call.
	// Snapshot recomputes the state and must notify OnChange.
	clk.advance(DefaultGracePeriod + time.Second)
	if s := c.Snapshot(); s.State != licensestate.StatusDegraded {
		t.Fatalf("state after grace: %+v", s)
	}
	if len(changes) != 2 || changes[1] != licensestate.StatusDegraded {
		t.Fatalf("OnChange not fired for clock-driven transition: %v", changes)
	}
}
