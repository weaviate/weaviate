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

package clusterapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/cluster"
)

func nullLogger() *logrus.Logger {
	logger, _ := logrustest.NewNullLogger()
	return logger
}

type stubCleanupProber struct {
	cleaningUp bool
	asked      string
}

func (s *stubCleanupProber) AnyCleanupInProgressForCollection(collection string) bool {
	s.asked = collection
	return s.cleaningUp
}

func TestInternalReindexCleanupActivity(t *testing.T) {
	tests := []struct {
		name       string
		prober     *stubCleanupProber
		query      string
		wantStatus int
		wantBody   string
		// wantPlainBody is the whole non-JSON body, trimmed.
		wantPlainBody string
		wantAsked     string
	}{
		{
			name:       "cancel seen or teardown running",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"probe":"weaviate/reindex-cleanup-activity","cleaningUp":true}`,
			wantAsked:  "Movies",
		},
		{
			name:       "nothing to confirm",
			prober:     &stubCleanupProber{},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"probe":"weaviate/reindex-cleanup-activity","cleaningUp":false}`,
			wantAsked:  "Movies",
		},
		{
			name:       "collection is required",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "",
			wantStatus: http.StatusBadRequest,
		},
		{
			// Must not answer "not cleaning up" from a node that cannot tell.
			// The sentinel body is what tells the caller this 503 is permanent
			// rather than transient, so it is part of the wire contract.
			name:          "probe not wired",
			prober:        nil,
			query:         "?collection=Movies",
			wantStatus:    http.StatusServiceUnavailable,
			wantPlainBody: clusterprobe.ProbeNotWiredMarker,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolve := func() clusterapi.ReindexCleanupProber { return nil }
			if tt.prober != nil {
				resolve = func() clusterapi.ReindexCleanupProber { return tt.prober }
			}
			handler := clusterapi.NewReindexCleanup(resolve, clusterapi.NewNoopAuthHandler(), nullLogger())
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity" + tt.query)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			if tt.wantPlainBody != "" {
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				assert.Equal(t, tt.wantPlainBody, strings.TrimSpace(string(body)))
				assert.Equal(t, "nosniff", res.Header.Get("X-Content-Type-Options"),
					"the client only trusts a sentinel a node marked as its own")
				return
			}
			if tt.wantBody == "" {
				return
			}
			assert.Equal(t, "application/json", res.Header.Get("Content-Type"))
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wantBody, string(body))
			assert.Equal(t, tt.wantAsked, tt.prober.asked)
		})
	}
}

func TestInternalReindexCleanupActivityRejectsNonGET(t *testing.T) {
	handler := clusterapi.NewReindexCleanup(
		func() clusterapi.ReindexCleanupProber { return &stubCleanupProber{} },
		clusterapi.NewNoopAuthHandler(), nullLogger())
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	assertRejectsNonGET(t, server, "/reindex/cleanup-activity?collection=Movies")
}

// Cluster-internal state must sit behind the same basic auth as every other
// internal route.
func TestInternalReindexCleanupActivityRequiresAuth(t *testing.T) {
	const (
		user = "alice"
		pass = "s3cret"
	)
	auth := clusterapi.NewBasicAuthHandler(cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{Username: user, Password: pass},
	})

	tests := []struct {
		name       string
		setAuth    bool
		user, pass string
		wantStatus int
		wantAsked  string
	}{
		{name: "no credentials", wantStatus: http.StatusUnauthorized},
		{name: "wrong user", setAuth: true, user: "mallory", pass: pass, wantStatus: http.StatusUnauthorized},
		{name: "wrong password", setAuth: true, user: user, pass: "guess", wantStatus: http.StatusUnauthorized},
		{
			name: "correct credentials", setAuth: true, user: user, pass: pass,
			wantStatus: http.StatusOK, wantAsked: "Movies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := &stubCleanupProber{}
			handler := clusterapi.NewReindexCleanup(
				func() clusterapi.ReindexCleanupProber { return prober }, auth, nullLogger())
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			req, err := http.NewRequest(http.MethodGet,
				server.URL+"/reindex/cleanup-activity?collection=Movies", nil)
			require.NoError(t, err)
			if tt.setAuth {
				req.SetBasicAuth(tt.user, tt.pass)
			}

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			assert.Equal(t, tt.wantAsked, prober.asked,
				"a refused caller must not reach the prober")
		})
	}
}

// Pins that a user-controlled collection name cannot forge log entries via
// newlines or blow out log size unbounded.
func TestInternalReindexCleanupActivityLogsABoundedQuotedCollection(t *testing.T) {
	tests := []struct {
		name       string
		collection string
		wantLogged string
	}{
		{
			name:       "newline forging a second entry",
			collection: "Movies\nlevel=error msg=forged",
			wantLogged: `"Movies\nlevel=error msg=forged"`,
		},
		{name: "carriage return", collection: "Movies\rmsg=forged", wantLogged: `"Movies\rmsg=forged"`},
		{
			name:       "longer than the cap",
			collection: strings.Repeat("A", 500),
			wantLogged: `"` + strings.Repeat("A", 128) + `…(truncated)"`,
		},
		{
			name:       "multi-byte runes, cap on a boundary",
			collection: strings.Repeat("é", 300),
			wantLogged: `"` + strings.Repeat("é", 64) + `…(truncated)"`,
		},
		{
			name:       "multi-byte runes, cap one byte into a rune",
			collection: "a" + strings.Repeat("é", 300),
			wantLogged: `"a` + strings.Repeat("é", 63) + `…(truncated)"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			prober := &stubCleanupProber{cleaningUp: true}
			handler := clusterapi.NewReindexCleanup(
				func() clusterapi.ReindexCleanupProber { return prober },
				clusterapi.NewNoopAuthHandler(), logger)
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=" +
				url.QueryEscape(tt.collection))
			require.NoError(t, err)
			defer res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)

			require.Equal(t, tt.collection, prober.asked,
				"the probe still has to be asked about the collection exactly as sent")

			entry := hook.LastEntry()
			require.NotNil(t, entry)
			logged, ok := entry.Data["collection"].(string)
			require.True(t, ok)
			assert.Equal(t, tt.wantLogged, logged)
			assert.NotContains(t, logged, "\n",
				"a raw newline ends the line and forges the next one")
			assert.NotContains(t, logged, "\r")
			assert.LessOrEqual(t, len(logged), 160,
				"one request must not be able to write an unbounded log line")
		})
	}
}

// Until the cleanup side is wired up, every production request takes the
// not-wired path, so that is the path an operator tracing a stalled cancel
// needs in the log.
func TestInternalReindexCleanupActivityLogsTheNotWiredAnswer(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	handler := clusterapi.NewReindexCleanup(nil, clusterapi.NewNoopAuthHandler(), logger)
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=Movies")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	entry := hook.LastEntry()
	require.NotNil(t, entry, "a node that cannot answer still has to log the question")
	assert.Equal(t, "reindex_cleanup_probe", entry.Data["action"])
	assert.Equal(t, `"Movies"`, entry.Data["collection"])
	assert.NotContains(t, entry.Data, "cleaning_up",
		"a node that cannot tell must not log an answer either way")
}
