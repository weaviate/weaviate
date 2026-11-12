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

package weaviateembed

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcmetadata "google.golang.org/grpc/metadata"
)

func TestValidateBaseURL(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		allowlist string
		wantErr   bool
	}{
		{name: "empty means module default", baseURL: ""},
		{name: "default", baseURL: DefaultBaseURL},
		{name: "default with trailing slash", baseURL: DefaultBaseURL + "/"},
		{name: "default in upper case", baseURL: "https://API.EMBEDDING.WEAVIATE.IO"},

		{name: "public https host", baseURL: "https://other.example.com", wantErr: true},
		{name: "default as a prefix of another domain", baseURL: "https://api.embedding.weaviate.io.other.example.com", wantErr: true},
		{name: "default as a suffix of another label", baseURL: "https://otherapi.embedding.weaviate.io", wantErr: true},
		{name: "subdomain of default", baseURL: "https://x.api.embedding.weaviate.io", wantErr: true},
		{name: "parent domain of default", baseURL: "https://weaviate.io", wantErr: true},
		{name: "default as userinfo of another host", baseURL: "https://api.embedding.weaviate.io@other.example.com", wantErr: true},
		{name: "backslash before default", baseURL: `https://other.example.com\@api.embedding.weaviate.io`, wantErr: true},
		{name: "plaintext default", baseURL: "http://api.embedding.weaviate.io", wantErr: true},
		{name: "other port on default", baseURL: "https://api.embedding.weaviate.io:8443", wantErr: true},
		{name: "trailing dot on default", baseURL: "https://api.embedding.weaviate.io.", wantErr: true},
		{name: "no scheme", baseURL: "api.embedding.weaviate.io", wantErr: true},
		{name: "scheme relative", baseURL: "//api.embedding.weaviate.io", wantErr: true},
		{name: "opaque", baseURL: "https:api.embedding.weaviate.io", wantErr: true},
		{name: "not a URL", baseURL: "://", wantErr: true},
		{name: "loopback", baseURL: "http://127.0.0.1:9100", wantErr: true},

		{name: "allowlisted", baseURL: "https://embed.corp.example.com", allowlist: "https://embed.corp.example.com"},
		{name: "allowlisted plaintext internal", baseURL: "http://embed:8080", allowlist: "http://embed:8080"},
		{name: "allowlisted second entry with spaces", baseURL: "https://b.example.com", allowlist: "https://a.example.com , https://b.example.com"},
		{name: "allowlisted entry is case insensitive", baseURL: "https://b.example.com", allowlist: "https://B.EXAMPLE.COM"},
		{name: "default stays allowed with an allowlist", baseURL: DefaultBaseURL, allowlist: "https://a.example.com"},

		{name: "not on allowlist", baseURL: "https://c.example.com", allowlist: "https://a.example.com,https://b.example.com", wantErr: true},
		{name: "other scheme than allowlisted", baseURL: "http://a.example.com", allowlist: "https://a.example.com", wantErr: true},
		{name: "other port than allowlisted", baseURL: "https://a.example.com:8443", allowlist: "https://a.example.com", wantErr: true},
		{name: "subdomain of allowlisted", baseURL: "https://x.a.example.com", allowlist: "https://a.example.com", wantErr: true},
		{name: "wildcard is not a pattern", baseURL: "https://x.example.com", allowlist: "https://*.example.com", wantErr: true},
		{name: "bare star is not a pattern", baseURL: "https://x.example.com", allowlist: "*", wantErr: true},
		{name: "bare host entry is ignored", baseURL: "https://a.example.com", allowlist: "a.example.com", wantErr: true},
		{name: "empty entries allow nothing", baseURL: "https://a.example.com", allowlist: " , ,", wantErr: true},
		{name: "entry without a scheme is ignored", baseURL: "//embed:8080", allowlist: "//embed:8080", wantErr: true},
		{name: "other IPv6 zone case than allowlisted", baseURL: "http://[fe80::1%25lan0]:8080", allowlist: "http://[fe80::1%25LAN0]:8080", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(AllowedBaseURLsEnv, tt.allowlist)

			err := ValidateBaseURL(tt.baseURL)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEmbedURLIsBuiltFromOrigin(t *testing.T) {
	const want = "https://api.embedding.weaviate.io/v1/embeddings/embed"

	for _, baseURL := range []string{
		"https://API.EMBEDDING.WEAVIATE.IO/",
		"https://other.example.com@api.embedding.weaviate.io",
		"https://api.embedding.weaviate.io/other",
		"https://api.embedding.weaviate.io/../other",
		"https://api.embedding.weaviate.io?x=1",
		"https://api.embedding.weaviate.io#x",
	} {
		t.Run(baseURL, func(t *testing.T) {
			c := New[[]float32](0, 0, 0)

			got, err := c.getWeaviateEmbedURL(baseURL)

			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestClientParsesAllowlistOnceAtConstruction(t *testing.T) {
	t.Setenv(AllowedBaseURLsEnv, "https://a.example.com")
	c := New[[]float32](0, 0, 0)

	t.Setenv(AllowedBaseURLsEnv, "https://b.example.com")

	_, err := c.getWeaviateEmbedURL("https://a.example.com")
	assert.NoError(t, err)
	_, err = c.getWeaviateEmbedURL("https://b.example.com")
	assert.Error(t, err)
}

func TestWarnInvalidAllowedBaseURLs(t *testing.T) {
	tests := []struct {
		name         string
		allowlist    string
		wantWarnings []string
	}{
		{name: "unset"},
		{name: "valid entries", allowlist: "https://a.example.com, http://embed:8080"},
		{name: "empty entries", allowlist: " , ,"},
		{
			name:      "invalid entries next to a valid one",
			allowlist: "embed:8080, https://a.example.com, a.example.com, //embed:8080, https://*.example.com, *",
			wantWarnings: []string{
				`"embed:8080"`, `"a.example.com"`, `"//embed:8080"`, `"https://*.example.com"`, `"*"`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(AllowedBaseURLsEnv, tt.allowlist)
			logger, hook := logrustest.NewNullLogger()

			WarnInvalidAllowedBaseURLs(logger)

			require.Len(t, hook.AllEntries(), len(tt.wantWarnings))
			for i, want := range tt.wantWarnings {
				assert.Contains(t, hook.AllEntries()[i].Message, want)
			}
		})
	}
}

type recordingServer struct {
	*httptest.Server
	hits          atomic.Int32
	authorization atomic.Value
}

func newRecordingServer(t *testing.T) *recordingServer {
	t.Helper()
	recorder := &recordingServer{}
	recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.hits.Add(1)
		recorder.authorization.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"embeddings":[[0.1,0.2,0.3]]}`))
	}))
	t.Cleanup(recorder.Close)
	return recorder
}

func TestEmbedURLForAllowlistedHost(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    string
	}{
		{name: "IPv6", baseURL: "http://[fd00::1]:8080", want: "http://[fd00::1]:8080/v1/embeddings/embed"},
		{name: "scoped IPv6", baseURL: "http://[fe80::1%25eth0]:8080", want: "http://[fe80::1%25eth0]:8080/v1/embeddings/embed"},
		{name: "scoped IPv6 keeps zone case", baseURL: "http://[FE80::1%25LAN0]:8080", want: "http://[fe80::1%25LAN0]:8080/v1/embeddings/embed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(AllowedBaseURLsEnv, tt.baseURL)
			c := New[[]float32](0, 0, 0)

			got, err := c.getWeaviateEmbedURL(tt.baseURL)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestVectorizeCallsOnlyAllowedOrigins(t *testing.T) {
	const callerToken = "Bearer token"
	const clusterURL = "https://cluster.example.com"

	callerContexts := map[string]context.Context{
		"REST": context.WithValue(
			context.WithValue(context.Background(), "Authorization", []string{callerToken}),
			"X-Weaviate-Cluster-Url", []string{clusterURL}),
		"gRPC": grpcmetadata.NewIncomingContext(context.Background(), grpcmetadata.New(map[string]string{
			"authorization":          callerToken,
			"x-weaviate-cluster-url": clusterURL,
		})),
	}

	for transport, ctx := range callerContexts {
		for _, isSearchQuery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/searchQuery=%t", transport, isSearchQuery), func(t *testing.T) {
				t.Run("baseURL not on the allowlist is not called", func(t *testing.T) {
					t.Setenv(AllowedBaseURLsEnv, "")
					unlisted := newRecordingServer(t)
					c := New[[]float32](0, 0, 0)

					_, err := c.Vectorize(ctx, []string{"text"}, isSearchQuery, nil, "model", unlisted.URL)

					require.Error(t, err)
					assert.Contains(t, err.Error(), "is not allowed")
					assert.Zero(t, unlisted.hits.Load())
				})

				t.Run("allowlisted baseURL is called", func(t *testing.T) {
					listed := newRecordingServer(t)
					t.Setenv(AllowedBaseURLsEnv, listed.URL)
					c := New[[]float32](0, 0, 0)

					_, err := c.Vectorize(ctx, []string{"text"}, isSearchQuery, nil, "model", listed.URL)

					require.NoError(t, err)
					assert.Equal(t, callerToken, listed.authorization.Load())
				})
			})
		}
	}
}

func TestVectorizeDoesNotFollowRedirects(t *testing.T) {
	unlisted := newRecordingServer(t)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, unlisted.URL+URLPath, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	t.Setenv(AllowedBaseURLsEnv, redirector.URL)
	ctx := context.WithValue(context.Background(), "Authorization", []string{"Bearer token"})
	ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{"https://cluster.example.com"})
	c := New[[]float32](0, 0, 0)

	_, err := c.Vectorize(ctx, []string{"text"}, false, nil, "model", redirector.URL)

	require.Error(t, err)
	assert.Zero(t, unlisted.hits.Load())
}
