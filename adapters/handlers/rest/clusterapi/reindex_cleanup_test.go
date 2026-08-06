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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/cluster"
)

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
		wantAsked  string
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
			// Never answer "not cleaning up" from a node that cannot tell:
			// a cancel's response depends on this.
			name:       "probe not wired",
			prober:     nil,
			query:      "?collection=Movies",
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolve := func() clusterapi.ReindexCleanupProber { return nil }
			if tt.prober != nil {
				resolve = func() clusterapi.ReindexCleanupProber { return tt.prober }
			}
			handler := clusterapi.NewReindexCleanup(resolve, clusterapi.NewNoopAuthHandler(), nil)
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity" + tt.query)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
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
		clusterapi.NewNoopAuthHandler(), nil)
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, server.URL+"/reindex/cleanup-activity?collection=Movies", nil)
			require.NoError(t, err)

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode,
				"a read-only probe must not answer writes")
		})
	}
}

// The route reports cluster-internal state, so it must sit behind the same
// basic auth as every other internal route: an unauthenticated caller is
// refused before the prober is ever asked.
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
				func() clusterapi.ReindexCleanupProber { return prober }, auth, nil)
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

// Pins the late binding described on the resolve field, through the same
// wiring and construction order production uses.
func TestInternalReindexCleanupActivityResolvesProviderLate(t *testing.T) {
	appState := &state.State{}

	handler := clusterapi.NewReindexCleanupFromState(appState, clusterapi.NewNoopAuthHandler())
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	get := func() int {
		res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=Movies")
		require.NoError(t, err, "the route must answer at every stage of bootstrap")
		defer res.Body.Close()
		return res.StatusCode
	}

	require.Equal(t, http.StatusServiceUnavailable, get(),
		"before the provider exists the route must say so")

	appState.ReindexProvider.Store(&db.ReindexProvider{})

	require.Equal(t, http.StatusOK, get(),
		"the route must pick the provider up once bootstrap assigns it")
}

// The route serves ~200 lines of startup before bootstrap assigns the
// provider, so the resolver reads the field on request goroutines while the
// startup goroutine writes it. Acceptance images are built with -race, where
// an unsynchronized pair aborts the process.
func TestInternalReindexCleanupActivityWiringIsRaceFree(t *testing.T) {
	appState := &state.State{}

	handler := clusterapi.NewReindexCleanupFromState(appState, clusterapi.NewNoopAuthHandler())
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=Movies")
				if err != nil {
					return
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
			}
		}()
	}

	// Land the bootstrap-time write in the middle of the polling.
	time.Sleep(20 * time.Millisecond)
	appState.ReindexProvider.Store(&db.ReindexProvider{})
	time.Sleep(20 * time.Millisecond)

	close(stop)
	wg.Wait()

	res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=Movies")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode,
		"the readers must observe the write, not a stale nil")
}

// The collection comes off the query string, so an unauthorized caller decides
// its bytes. A newline in a logrus field ends the line and everything after it
// reads as a second, fully forged entry; an unbounded one lets a single request
// write as much of the log as it likes.
func TestInternalReindexCleanupActivityLogsABoundedQuotedCollection(t *testing.T) {
	tests := []struct {
		name       string
		collection string
		wantLogged string
	}{
		{
			name:       "ordinary name",
			collection: "Movies",
			wantLogged: `"Movies"`,
		},
		{
			name:       "newline forging a second entry",
			collection: "Movies\nlevel=error msg=forged",
			wantLogged: `"Movies\nlevel=error msg=forged"`,
		},
		{
			name:       "carriage return",
			collection: "Movies\rmsg=forged",
			wantLogged: `"Movies\rmsg=forged"`,
		},
		{
			name:       "longer than the cap",
			collection: strings.Repeat("A", 500),
			wantLogged: `"` + strings.Repeat("A", 128) + `…(truncated)"`,
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
