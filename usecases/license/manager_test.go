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

package license

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/license/protocol"
)

func b64pub(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(pub)
}

// fakeServer signs whatever status it is told to return.
type fakeServer struct {
	srv    *httptest.Server
	key    protocol.ServerKey
	pubB64 string
	status protocol.Status
	seen   []protocol.VerifyRequest
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f := &fakeServer{key: protocol.ServerKey{ID: "k", PrivateKey: priv}, pubB64: base64.RawURLEncoding.EncodeToString(pub), status: protocol.StatusValid}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req protocol.VerifyRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		f.seen = append(f.seen, req)
		now := time.Now()
		resp := protocol.VerifyResponse{LicenseID: req.LicenseID, Status: f.status, ExpiresAt: now.Add(30 * 24 * time.Hour),
			CheckedAt: now, NextCheckAfter: now.Add(24 * time.Hour), Nonce: req.Nonce}
		require.NoError(t, f.key.Sign(&resp))
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestCommunityMode(t *testing.T) {
	logger, _ := test.NewNullLogger()
	reg := prometheus.NewRegistry()
	m, err := New(Config{}, Deps{NodeName: "n", Version: "1.34.2", Logger: logger, Registerer: reg})
	require.NoError(t, err)
	assert.True(t, m.Allowed())
	assert.NoError(t, m.Require())
	assert.Equal(t, map[string]interface{}{"status": "unlicensed"}, m.MetaInfo())
	assert.Equal(t, 1.0, testutil.ToFloat64(m.status.WithLabelValues("unlicensed")))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m.Run(ctx) // returns immediately
}

func TestLicensedNode(t *testing.T) {
	f := newFakeServer(t)
	lic, err := protocol.Generate()
	require.NoError(t, err)
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	reg := prometheus.NewRegistry()
	cfg := Config{Key: lic.Key(), ServerURL: f.srv.URL, ServerKeys: []string{"k:" + f.pubB64},
		GracePeriod: protocol.DefaultGracePeriod, CachePath: t.TempDir() + "/license.json", Enforce: true}

	clusterID := ""
	m, err := New(cfg, Deps{NodeName: "node-1", ClusterID: func() string { return clusterID }, Version: "1.34.2", Logger: logger, Registerer: reg})
	require.NoError(t, err)
	assert.Equal(t, "unreachable", m.MetaInfo()["status"])
	assert.True(t, m.Allowed(), "unreachable inside grace is still allowed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)
	require.Eventually(t, func() bool { return m.Snapshot().State == protocol.StateValid }, 5*time.Second, 10*time.Millisecond)

	info := m.MetaInfo()
	assert.Equal(t, "valid", info["status"])
	assert.Equal(t, lic.ID, info["licenseId"])
	assert.Equal(t, true, info["enforcing"])
	assert.NotEmpty(t, info["expiresAt"])
	assert.NotContains(t, info, "graceEndsAt")
	assert.NoError(t, m.Require())
	assert.Equal(t, 1.0, testutil.ToFloat64(m.status.WithLabelValues("valid")))
	assert.Equal(t, 0.0, testutil.ToFloat64(m.status.WithLabelValues("unreachable")))
	assert.Greater(t, testutil.ToFloat64(m.expires), float64(time.Now().Unix()))

	// The request carried the node name; the cluster id was still empty.
	require.Len(t, f.seen, 1)
	assert.Equal(t, "node-1", f.seen[0].InstanceID)
	assert.Equal(t, "", f.seen[0].ClusterID)
	assert.Equal(t, "1.34.2", f.seen[0].WeaviateVersion)

	// Once raft has committed a cluster id it is reported on the next check.
	clusterID = "c-raft"
	m.checker.CheckNow(ctx)
	assert.Equal(t, "c-raft", f.seen[1].ClusterID)

	// Logs went through logrus with the action field.
	var sawConfigured bool
	for _, e := range hook.AllEntries() {
		if e.Message == "license configured" && e.Data["action"] == "license" {
			sawConfigured = true
		}
	}
	assert.True(t, sawConfigured)
}

func TestClusterIDOverride(t *testing.T) {
	f := newFakeServer(t)
	lic, _ := protocol.Generate()
	logger, _ := test.NewNullLogger()
	cfg := Config{Key: lic.Key(), ServerURL: f.srv.URL, ServerKeys: []string{"k:" + f.pubB64}, ClusterID: "c-fixed"}
	m, err := New(cfg, Deps{NodeName: "n", ClusterID: func() string { return "c-raft" }, Version: "x", Logger: logger})
	require.NoError(t, err)
	m.checker.CheckNow(context.Background())
	require.Len(t, f.seen, 1)
	assert.Equal(t, "c-fixed", f.seen[0].ClusterID)
}

func TestNoTrustedKeysFailsClosed(t *testing.T) {
	f := newFakeServer(t)
	lic, _ := protocol.Generate()
	logger, hook := test.NewNullLogger()
	m, err := New(Config{Key: lic.Key(), ServerURL: f.srv.URL}, Deps{NodeName: "n", Version: "x", Logger: logger})
	require.NoError(t, err)
	m.checker.CheckNow(context.Background())
	s := m.Snapshot()
	assert.Equal(t, protocol.StateUnreachable, s.State, "an unverifiable answer must never become valid")
	assert.Contains(t, s.LastError, "unknown server key")
	var sawError bool
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.ErrorLevel {
			sawError = true
		}
	}
	assert.True(t, sawError)
}

func TestRevokedDegradesWhenEnforcing(t *testing.T) {
	f := newFakeServer(t)
	f.status = protocol.StatusRevoked
	lic, _ := protocol.Generate()
	logger, _ := test.NewNullLogger()
	cfg := Config{Key: lic.Key(), ServerURL: f.srv.URL, ServerKeys: []string{"k:" + f.pubB64}, Enforce: true, GracePeriod: time.Millisecond}
	m, err := New(cfg, Deps{NodeName: "n", Version: "x", Logger: logger, Registerer: prometheus.NewRegistry()})
	require.NoError(t, err)
	m.checker.CheckNow(context.Background())
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, protocol.StateDegraded, m.Snapshot().State)
	assert.ErrorIs(t, m.Require(), ErrDegraded)
	assert.Equal(t, "degraded", m.MetaInfo()["status"])
	assert.NotEmpty(t, m.MetaInfo()["graceEndsAt"])

	// Same config without enforcement stays allowed.
	cfg.Enforce = false
	m2, err := New(cfg, Deps{NodeName: "n", Version: "x", Logger: logger})
	require.NoError(t, err)
	m2.checker.CheckNow(context.Background())
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, protocol.StateRevoked, m2.Snapshot().State)
	assert.NoError(t, m2.Require())
}
