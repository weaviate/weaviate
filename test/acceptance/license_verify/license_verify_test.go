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

package license_verify

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	protocol "github.com/weaviate/weaviate/entities/license"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

const metricsPort = 2112

var acceptanceHTTPClient = &http.Client{Timeout: 10 * time.Second}

type testEnvironment struct {
	customer  protocol.License
	server    *fakeLicenseServer
	expiresAt time.Time
	imageTag  string
}

var (
	sharedTestEnvironmentOnce sync.Once
	sharedTestEnvironment     *testEnvironment
	sharedTestEnvironmentErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedTestEnvironment != nil {
		sharedTestEnvironment.server.close()
		if sharedTestEnvironment.imageTag != "" {
			output, err := exec.Command("docker", "image", "rm", sharedTestEnvironment.imageTag).CombinedOutput()
			if err != nil {
				fmt.Fprintf(os.Stderr, "remove license test image: %v\n%s", err, output)
			}
		}
	}
	os.Exit(code)
}

func getTestEnvironment(t *testing.T) *testEnvironment {
	t.Helper()
	sharedTestEnvironmentOnce.Do(func() {
		sharedTestEnvironment, sharedTestEnvironmentErr = buildTestEnvironment(t)
	})
	require.NoError(t, sharedTestEnvironmentErr)
	return sharedTestEnvironment
}

func buildTestEnvironment(t *testing.T) (*testEnvironment, error) {
	customer, err := protocol.Generate()
	if err != nil {
		return nil, err
	}
	expiresAt := time.Now().Add(48 * time.Hour)
	server := newFakeLicenseServer(t, customer.PublicKey, expiresAt)

	workingDirectory, err := os.Getwd()
	if err != nil {
		server.close()
		return nil, err
	}
	repositoryRoot := filepath.Clean(filepath.Join(workingDirectory, "..", "..", ".."))
	git := exec.Command("git", "rev-parse", "--short", "HEAD")
	git.Dir = repositoryRoot
	revision, err := git.CombinedOutput()
	if err != nil {
		server.close()
		return nil, fmt.Errorf("read git revision: %w: %s", err, revision)
	}

	imageTag := fmt.Sprintf("weaviate/license-m5-acceptance:%d-%d", os.Getpid(), time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	args := []string{
		"build",
		"--file", "Dockerfile",
		"--target", "weaviate",
		"--tag", imageTag,
		"--build-arg", "TARGETARCH=" + runtime.GOARCH,
		"--build-arg", "GIT_REVISION=" + strings.TrimSpace(string(revision)),
		"--build-arg", "EXTRA_BUILD_ARGS=-race",
		"--build-arg", "LICENSE_SERVER_URL=" + server.url(),
		"--build-arg", "LICENSE_SERVER_KEYS=" + server.trustedKeys(),
		".",
	}
	t.Logf("building purpose-specific license test image %s", imageTag)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = repositoryRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		server.close()
		return nil, fmt.Errorf("build license test image: %w\n%s", err, output)
	}
	return &testEnvironment{
		customer:  customer,
		server:    server,
		expiresAt: expiresAt,
		imageTag:  imageTag,
	}, nil
}

func TestLicenseVerifyFullStartup(t *testing.T) {
	environment := getTestEnvironment(t)
	customer := environment.customer
	server := environment.server
	expiresAt := environment.expiresAt
	t.Cleanup(func() { server.requireNoHandlerErrors(t) })

	compose := startCompose(t, environment, customer.Key())
	ctx := context.Background()

	var meta *models.Meta
	var rawMeta string
	require.Eventually(t, func() bool {
		meta, rawMeta = getMeta(t, compose.GetWeaviate().URI())
		return server.callCount() == 1 && server.requestCount() == 1 &&
			meta.License != nil && meta.License.Status == string(protocol.StatusValid)
	}, 15*time.Second, 50*time.Millisecond, "startup must complete one signed verification")
	req, ok := server.firstRequest()
	require.True(t, ok)
	resp, ok := server.firstResponse()
	require.True(t, ok)

	require.NotNil(t, meta.License)
	require.NotEmpty(t, meta.Version)
	require.Equal(t, customer.ID, req.LicenseID)
	require.Equal(t, docker.Weaviate0, req.InstanceID)
	require.Equal(t, meta.Version, req.WeaviateVersion)
	require.NotEmpty(t, req.ClusterID, "licensed startup must use the Raft-generated cluster ID")
	require.NotEmpty(t, req.Nonce)
	nonce, err := base64.RawURLEncoding.DecodeString(req.Nonce)
	require.NoError(t, err)
	require.Len(t, nonce, protocol.NonceSize)
	require.Equal(t, req.Nonce, resp.Nonce)
	require.Equal(t, []string{protocol.EntitlementEnterprise}, resp.Entitlements)

	require.Equal(t, "enterprise", meta.License.Edition)
	require.Equal(t, string(protocol.StatusValid), meta.License.Status)
	require.Equal(t, customer.ID, meta.License.LicenseID)
	require.NotNil(t, meta.License.ExpiresAt)
	require.Equal(t, expiresAt.Unix(), time.Time(*meta.License.ExpiresAt).Unix())
	require.NotNil(t, meta.License.LastCheckedAt)
	require.Equal(t, resp.CheckedAt.Unix(), time.Time(*meta.License.LastCheckedAt).Unix())
	require.NotContains(t, rawMeta, customer.Key(), "meta must never expose the private license key")
	assertValidLicenseMetrics(t, compose.GetWeaviate().Container(), expiresAt)

	for _, tc := range []struct {
		name      string
		mode      responseMode
		wantError string
	}{
		{name: "wrong server key", mode: respondWithWrongServerKey, wantError: "unknown server key"},
		{name: "edited status", mode: respondWithEditedStatus, wantError: "signature verification failed"},
		{name: "wrong nonce", mode: respondWithWrongNonce, wantError: "response does not match request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeRequests := server.requestCount()
			logsBefore, ok := containerLogs(ctx, t, compose.GetWeaviate().Container())
			require.True(t, ok)
			server.setMode(tc.mode)
			restartNode(ctx, t, compose)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Greater(c, server.requestCount(), beforeRequests)
				logs, ok := containerLogs(ctx, c, compose.GetWeaviate().Container())
				if !assert.True(c, ok) || !assert.GreaterOrEqual(c, len(logs), len(logsBefore)) {
					return
				}
				startupLogs := logs[len(logsBefore):]
				assert.Contains(c, startupLogs, "license check failed")
				assert.Contains(c, startupLogs, tc.wantError)
				assert.NotContains(c, startupLogs, customer.Key())
			}, 15*time.Second, 50*time.Millisecond)
		})
	}

	server.setMode(respondValid)
	restartNode(ctx, t, compose)
	require.Eventually(t, func() bool {
		meta, _ = getMeta(t, compose.GetWeaviate().URI())
		return meta.License != nil && meta.License.Status == string(protocol.StatusValid)
	}, 15*time.Second, 50*time.Millisecond)

	server.close()
	meta, rawMeta = getMeta(t, compose.GetWeaviate().URI())
	require.NotNil(t, meta.License)
	require.Equal(t, string(protocol.StatusValid), meta.License.Status)
	require.Equal(t, customer.ID, meta.License.LicenseID)
	require.NotContains(t, rawMeta, customer.Key())

	res, err := acceptanceHTTPClient.Get("http://" + compose.GetWeaviate().URI() + "/v1/schema")
	require.NoError(t, err, "the node must remain responsive after its license server stops")
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	server.reopen(t)
}

func TestLicenseVerifyKeylessStartupMakesNoCalls(t *testing.T) {
	environment := getTestEnvironment(t)
	server := environment.server
	t.Cleanup(func() { server.requireNoHandlerErrors(t) })
	callsBefore := server.callCount()
	requestsBefore := server.requestCount()

	compose := startCompose(t, environment, "")
	meta, _ := getMeta(t, compose.GetWeaviate().URI())
	require.NotNil(t, meta.License)
	require.Equal(t, "community", meta.License.Edition)
	require.Empty(t, meta.License.Status)
	require.Empty(t, meta.License.LicenseID)
	require.Nil(t, meta.License.ExpiresAt)
	require.Nil(t, meta.License.LastCheckedAt)
	require.Equal(t, callsBefore, server.callCount(), "a key-less node must not contact the license server")
	require.Equal(t, requestsBefore, server.requestCount())
}

func startCompose(t *testing.T, environment *testEnvironment, licenseKey string) *docker.DockerCompose {
	t.Helper()
	t.Setenv("TEST_WEAVIATE_IMAGE", environment.imageTag)

	builder := docker.New().
		WithWeaviate().
		WithWeaviateHostGateway().
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true")
	if licenseKey != "" {
		builder.WithWeaviateEnv("LICENSE_KEY", licenseKey)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	compose, err := builder.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate test containers: %v", err)
		}
	})
	return compose
}

func restartNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	timeout := 30 * time.Second
	require.NoError(t, compose.StopNode(ctx, 0, &timeout))
	require.NoError(t, compose.StartNode(ctx, 0))
}

func getMeta(t *testing.T, uri string) (*models.Meta, string) {
	t.Helper()
	res, err := acceptanceHTTPClient.Get("http://" + uri + "/v1/meta")
	require.NoError(t, err)
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, "meta response: %s", raw)
	var meta models.Meta
	require.NoError(t, json.Unmarshal(raw, &meta))
	return &meta, string(raw)
}

func assertValidLicenseMetrics(t *testing.T, container testcontainers.Container, expiresAt time.Time) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		lines, ok := scrapeLicenseMetrics(c, container)
		if !assert.True(c, ok) {
			return
		}
		status, ok := metricValue(c, lines, "weaviate_license_status{", `edition="enterprise"`, `status="valid"`)
		if assert.True(c, ok, "no valid enterprise license status series") {
			assert.Equal(c, 1.0, status)
		}
		expiry, ok := metricValue(c, lines, "weaviate_license_expires_at_seconds ")
		if assert.True(c, ok, "no license expiry series") {
			assert.Equal(c, float64(expiresAt.Unix()), expiry)
		}
	}, 15*time.Second, 100*time.Millisecond)
}

const metricsOutputSentinel = "___LICENSE_METRICS_BEGIN___"

func scrapeLicenseMetrics(t assert.TestingT, container testcontainers.Container) ([]string, bool) {
	cmd := fmt.Sprintf("echo %s; wget -qO- http://localhost:%d/metrics | "+
		"grep -E '^weaviate_license_(status|expires_at_seconds)'", metricsOutputSentinel, metricsPort)
	code, reader, err := container.Exec(context.Background(), []string{"sh", "-c", cmd})
	if !assert.NoError(t, err) {
		return nil, false
	}
	out, err := io.ReadAll(reader)
	if !assert.NoError(t, err) || !assert.Zero(t, code, "metrics probe failed: %s", out) {
		return nil, false
	}
	idx := strings.Index(string(out), metricsOutputSentinel)
	if !assert.GreaterOrEqual(t, idx, 0, "metrics probe returned no sentinel: %q", out) {
		return nil, false
	}

	var lines []string
	for _, line := range strings.Split(string(out)[idx+len(metricsOutputSentinel):], "\n") {
		line = strings.TrimSpace(strings.Map(func(r rune) rune {
			if r < 0x20 {
				return -1
			}
			return r
		}, line))
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, true
}

func metricValue(t assert.TestingT, lines []string, fragments ...string) (float64, bool) {
	var hits []string
	for _, line := range lines {
		matches := true
		for _, fragment := range fragments {
			if !strings.Contains(line, fragment) {
				matches = false
				break
			}
		}
		if matches {
			hits = append(hits, line)
		}
	}
	if len(hits) == 0 {
		return 0, false
	}
	if !assert.Len(t, hits, 1, "expected one series for %v, got %v", fragments, hits) {
		return 0, false
	}
	idx := strings.LastIndex(hits[0], " ")
	if !assert.Positive(t, idx, "malformed metric line %q", hits[0]) {
		return 0, false
	}
	value, err := strconv.ParseFloat(hits[0][idx+1:], 64)
	if !assert.NoError(t, err, "malformed metric line %q", hits[0]) {
		return 0, false
	}
	return value, true
}

func containerLogs(ctx context.Context, t assert.TestingT, container testcontainers.Container) (string, bool) {
	reader, err := container.Logs(ctx)
	if !assert.NoError(t, err) {
		return "", false
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if !assert.NoError(t, err) {
		return "", false
	}
	return string(raw), true
}
