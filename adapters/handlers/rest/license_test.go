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

package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	protocol "github.com/weaviate/weaviate/entities/license"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	licensechecker "github.com/weaviate/weaviate/usecases/license/checker"
)

func TestNewLicenseCheckerUsesEmbeddedConfiguration(t *testing.T) {
	customer, err := protocol.Generate()
	require.NoError(t, err)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	appState, _ := newLicenseTestAppState(t, customer)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	t.Setenv("LICENSE_SERVER_URL", server.URL)
	t.Setenv("LICENSE_SERVER_KEYS", "test:"+base64.RawURLEncoding.EncodeToString(pub))
	t.Setenv("LICENSE_CLUSTER_ID", "cluster-test")

	checker, err := newLicenseChecker(appState)
	require.NoError(t, err)
	require.NotNil(t, checker)
	require.Equal(t, customer.ID, checker.Client.LicenseID)
	require.Empty(t, checker.Client.ServerURL, "runtime environment must not replace the compiled server URL")
	require.Len(t, checker.Client.TrustedKeys, 2, "runtime environment must not replace the compiled trust set")
	require.Equal(t, "node-a", checker.InstanceID)
	require.Empty(t, checker.ClusterID)
	require.NotNil(t, checker.ClusterIDFunc)
	require.Empty(t, checker.CachePath)

	// Tests inject the fake only after construction; production has no such
	// runtime configuration path.
	checker.Client.ServerURL = server.URL + "/"
	checker.Client.TrustedKeys = protocol.ServerKeySet{"test": pub}
	checker.ClusterID = "cluster-test"
	checker.ClusterIDFunc = nil
	checker.Start()
	require.Zero(t, calls.Load(), "construction and Start must not perform network calls")
}

func TestNewLicenseCheckerCommunitySkipsConfiguration(t *testing.T) {
	appState := &state.State{ServerConfig: &config.WeaviateConfig{}}
	t.Setenv("LICENSE_SERVER_URL", "http://127.0.0.1:8080")
	t.Setenv("LICENSE_SERVER_KEYS", "not-a-key")
	checker, err := newLicenseChecker(appState)
	require.NoError(t, err)
	require.Nil(t, checker)
}

func TestLicenseCheckerRunImmediateAndShutdown(t *testing.T) {
	customer, err := protocol.Generate()
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	appState, _ := newLicenseTestAppState(t, customer)
	var calls atomic.Int32
	requests := make(chan protocol.VerifyRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req protocol.VerifyRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.NoError(t, req.VerifySignature(customer.PublicKey))
		requests <- req
		now := time.Now().UTC()
		resp := protocol.VerifyResponse{
			LicenseID: req.LicenseID, Status: protocol.StatusValid,
			ExpiresAt: now.Add(24 * time.Hour), CheckedAt: now,
			NextCheckAfter: now.Add(time.Hour), Nonce: req.Nonce,
			Entitlements: []string{protocol.EntitlementEnterprise},
		}
		require.NoError(t, (protocol.ServerKey{ID: "test", PrivateKey: priv}).Sign(&resp))
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer server.Close()
	checker, err := newLicenseChecker(appState)
	require.NoError(t, err)
	checker.Client.ServerURL = server.URL
	checker.Client.TrustedKeys = protocol.ServerKeySet{"test": pub}
	checker.ClusterIDFunc = func() string { return "cluster-test" }
	checker.Start()
	require.Zero(t, calls.Load())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		runLicenseChecker(ctx, checker)
		close(done)
	}, appState.Logger)

	require.Eventually(t, func() bool {
		return calls.Load() == 1 && checker.Snapshot().LastStatus == protocol.StatusValid
	}, 5*time.Second, 10*time.Millisecond)
	req := <-requests
	require.Equal(t, customer.ID, req.LicenseID)
	require.Equal(t, "cluster-test", req.ClusterID)
	require.Equal(t, "node-a", req.InstanceID)
	require.NotEmpty(t, req.Nonce)
	require.Equal(t, protocol.StatusValid, checker.Snapshot().LastStatus)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("checker did not stop after shutdown cancellation")
	}
}

func TestRunLicenseCheckerCancellationWhileWaitingForClusterID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	enterrors.GoWrapper(func() {
		runLicenseChecker(ctx, &licensechecker.Checker{ClusterIDFunc: func() string { return "" }})
		close(done)
	}, logger)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("checker did not stop while waiting for a cluster ID")
	}
}

func TestLicenseCheckerRejectsForgedResponses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*protocol.VerifyResponse)
		signKey bool
	}{
		{name: "edited status", mutate: func(r *protocol.VerifyResponse) { r.Status = protocol.StatusRevoked }, signKey: true},
		{name: "wrong nonce", mutate: func(r *protocol.VerifyResponse) { r.Nonce = "different" }, signKey: true},
		{name: "wrong server key", signKey: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			customer, err := protocol.Generate()
			require.NoError(t, err)
			pub, priv, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			appState, logs := newLicenseTestAppState(t, customer)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req protocol.VerifyRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				require.NoError(t, req.VerifySignature(customer.PublicKey))
				now := time.Now().UTC()
				resp := protocol.VerifyResponse{
					LicenseID: req.LicenseID, Status: protocol.StatusValid,
					ExpiresAt: now.Add(time.Hour), CheckedAt: now,
					NextCheckAfter: now.Add(time.Hour), Nonce: req.Nonce,
					Entitlements: []string{protocol.EntitlementEnterprise},
				}
				signingKey := priv
				if !tc.signKey {
					signingKey = otherPriv
				}
				require.NoError(t, (protocol.ServerKey{ID: "test", PrivateKey: signingKey}).Sign(&resp))
				if tc.mutate != nil {
					tc.mutate(&resp)
				}
				require.NoError(t, json.NewEncoder(w).Encode(resp))
			}))
			defer server.Close()
			checker, err := newLicenseChecker(appState)
			require.NoError(t, err)
			checker.Client.ServerURL = server.URL
			checker.Client.TrustedKeys = protocol.ServerKeySet{"test": pub}
			checker.Start()
			snapshot := checker.CheckNow(context.Background())
			require.NotEmpty(t, snapshot.LastError)
			require.NotEqual(t, protocol.StatusValid, snapshot.LastStatus)
			require.Contains(t, logs.String(), "license check failed")
			require.Contains(t, logs.String(), "action=license")
			require.NotContains(t, logs.String(), customer.Key())
		})
	}
}

func TestLicenseLogHandler(t *testing.T) {
	var logs bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(io.Writer(&logs))
	logger.SetLevel(logrus.DebugLevel)
	handler := licenseLogHandler{logger: logger}
	slog.New(handler.WithGroup("check")).With("license_id", "lic_test").Warn("failed", "retry_in", time.Minute)
	require.Contains(t, logs.String(), "action=license")
	require.Contains(t, logs.String(), "check.license_id=lic_test")
	require.Contains(t, logs.String(), "check.retry_in=1m0s")
	require.Contains(t, logs.String(), "level=warning")
}

func newLicenseTestAppState(t *testing.T, customer protocol.License) (*state.State, *bytes.Buffer) {
	t.Helper()
	t.Setenv("LICENSE_KEY", customer.Key())
	t.Setenv("LICENSE_KEY_FILE", "")
	var logs bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(io.Writer(&logs))
	cfg := &config.WeaviateConfig{}
	cfg.Config.License = license.State{Status: license.StatusUnreachable, LicenseID: customer.ID}
	cfg.Config.Cluster.Hostname = "node-a"
	return &state.State{ServerConfig: cfg, Logger: logger}, &logs
}
