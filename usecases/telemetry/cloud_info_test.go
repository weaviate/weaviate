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

package telemetry

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountIDFromARN(t *testing.T) {
	tests := []struct {
		name string
		arn  string
		want string
	}{
		{"task arn", "arn:aws:ecs:us-east-1:123456789012:task/cluster/task-id", "123456789012"},
		{"empty", "", ""},
		{"too few fields", "arn:aws:ecs", ""},
		{"exactly five fields, no resource", "arn:aws:ecs:us-east-1:999999999999", "999999999999"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, accountIDFromARN(tt.arn))
		})
	}
}

func TestExtractAWSAccountID(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"valid document", `{"accountId":"101","region":"us-east-1"}`, "101"},
		{"no accountId key", `{"region":"us-east-1"}`, ""},
		{"empty body", "", ""},
		{"accountId present but malformed json around it", `garbage "accountId" : "202" more garbage`, "202"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, extractAWSAccountID(tt.doc))
		})
	}
}

// awsIMDSStub serves the three IMDS paths used by readIMDSAccountID.
func awsIMDSStub(accountID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/latest/meta-data/"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "/latest/api/token"):
			_, _ = w.Write([]byte("test-token"))
		case strings.Contains(r.URL.Path, "/latest/dynamic/instance-identity/document"):
			fmt.Fprintf(w, `{"accountId":%q}`, accountID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

// deadAddr starts and immediately closes a local server, so a request
// against its URL fails fast with connection-refused instead of timing out
// against an unroutable address.
func deadAddr(t *testing.T) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := s.URL
	s.Close()
	return url
}

func TestAWSCloudInfo_IPv6Fallback(t *testing.T) {
	// Covers the IPv6 IMDS fallback: without it, an unreachable IPv4
	// endpoint means no account id at all, ever.
	ipv6Server := httptest.NewServer(awsIMDSStub("222"))
	defer ipv6Server.Close()

	logger, _ := test.NewNullLogger()
	c := newAWSCloudInfo(deadAddr(t), ipv6Server.URL, "", logger)

	info := c.getCloudInfo()
	require.NotNil(t, info)
	assert.Equal(t, "AWS", info.cloudProvider)
	assert.Equal(t, "222", info.uniqueID)
}

func TestAWSCloudInfo_ECSTaskMetadataFallback(t *testing.T) {
	// Covers the ECS/Fargate task metadata fallback: without it, a task
	// whose IMDS paths are both unreachable (the common ECS awsvpc shape)
	// never reports an account id, even though the ECS-injected task
	// metadata endpoint has it in the TaskARN.
	ecsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/task") {
			fmt.Fprint(w, `{"TaskARN":"arn:aws:ecs:us-east-1:555566667777:task/cluster/abc123"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ecsServer.Close()

	logger, _ := test.NewNullLogger()
	c := newAWSCloudInfo(deadAddr(t), "", ecsServer.URL, logger)

	info := c.getCloudInfo()
	require.NotNil(t, info)
	assert.Equal(t, "555566667777", info.uniqueID)

	// The ECS metadata URI env var alone is a reliable detection signal;
	// isDetected must not need a live IMDS endpoint to say "this is AWS".
	assert.True(t, c.isDetected())
}

func TestAWSCloudInfo_LogsOnceWhenNoAccountID(t *testing.T) {
	// All three sources fail (no ECS URI configured, both IMDS paths dead).
	// This pins the "log the reason once" requirement: repeated pushes with
	// no account id must not spam the log once per push.
	logger, hook := test.NewNullLogger()
	c := newAWSCloudInfo(deadAddr(t), "", "", logger)

	for i := 0; i < 3; i++ {
		info := c.getCloudInfo()
		require.NotNil(t, info)
		assert.Empty(t, info.uniqueID)
	}

	warnCount := 0
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel {
			warnCount++
		}
	}
	assert.Equal(t, 1, warnCount, "expected exactly one warning across repeated getCloudInfo calls")
}

func TestAWSCloudInfo_IPv4IMDSTakesPriorityOverFallbacks(t *testing.T) {
	// When the IPv4 IMDS endpoint answers, the IPv6 and ECS paths must never be reached.
	imdsServer := httptest.NewServer(awsIMDSStub("111"))
	defer imdsServer.Close()

	calledFallback := false
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calledFallback = true
		w.WriteHeader(http.StatusNotFound)
	}))
	defer fallbackServer.Close()

	logger, _ := test.NewNullLogger()
	c := newAWSCloudInfo(imdsServer.URL, fallbackServer.URL, fallbackServer.URL, logger)

	info := c.getCloudInfo()
	require.NotNil(t, info)
	assert.Equal(t, "111", info.uniqueID)
	assert.False(t, calledFallback, "IPv6/ECS fallbacks must not be tried once the IPv4 IMDS endpoint succeeds")
}

func TestSendRequest_IgnoresHTTPProxyEnv(t *testing.T) {
	// A proxy that would redirect every request to itself if honoured.
	proxyHit := false
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHit = true
		w.WriteHeader(http.StatusTeapot)
	}))
	defer proxy.Close()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("direct-hit"))
	}))
	defer target.Close()

	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("http_proxy", proxy.URL)

	body, status, err := sendRequest(target.URL, nil, "GET")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "direct-hit", body)
	assert.False(t, proxyHit, "the metadata probe must never be routed through HTTP_PROXY")
}

func TestCloudInfoHelper_LazyDetectionRetriesUntilFound(t *testing.T) {
	logger, _ := test.NewNullLogger()
	c := newCloudInfoHelper(logger, true)

	fake := &fakeCloudInfoProvider{}
	fake.On("getCloudInfo").Return(&cloudInfo{cloudProvider: "GCP", uniqueID: "id"})

	calls := 0
	c.detect = func() cloudInfoProvider {
		calls++
		if calls < 3 {
			return nil // not yet detected, e.g. a transient network blip
		}
		return fake
	}

	assert.Nil(t, c.getCloudInfo(), "not yet detected on call 1")
	assert.Nil(t, c.getCloudInfo(), "not yet detected on call 2")
	info := c.getCloudInfo()
	require.NotNil(t, info)
	assert.Equal(t, "GCP", info.cloudProvider)
	assert.Equal(t, 3, calls, "detect must be retried on every call until a provider is found")

	// Once cached, further calls must reuse the provider, not call detect again.
	info2 := c.getCloudInfo()
	require.NotNil(t, info2)
	assert.Equal(t, 3, calls, "a cached provider must short-circuit further detection")
}

func TestCloudInfoHelper_DisabledNeverDetects(t *testing.T) {
	logger, _ := test.NewNullLogger()
	c := newCloudInfoHelper(logger, false)

	called := false
	c.detect = func() cloudInfoProvider {
		called = true
		return &fakeCloudInfoProvider{}
	}

	assert.Nil(t, c.getCloudInfo())
	assert.False(t, called, "detect must never run when telemetry is disabled")
}

func TestCloudInfoHelper_ConstructionDoesNotDetect(t *testing.T) {
	// newCloudInfoHelper must do no network I/O: detection is lazy, deferred
	// to getCloudInfo, which is only ever called from inside the telemetry
	// goroutine, never on the synchronous startup path.
	logger, _ := test.NewNullLogger()
	c := newCloudInfoHelper(logger, true)

	called := false
	c.detect = func() cloudInfoProvider {
		called = true
		return nil
	}
	assert.False(t, called, "construction alone must not have invoked detect")

	c.getCloudInfo()
	assert.True(t, called, "getCloudInfo is what triggers detection, not construction")
}
