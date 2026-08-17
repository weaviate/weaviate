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

package debug_endpoints

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

// removedReindexRoutes are the fourteen /debug/index/rebuild/inverted*
// routes that were removed. They were never part of an API contract; every
// capability they exposed is either served by /v1 or was a no-op.
var removedReindexRoutes = []string{
	"/debug/index/rebuild/inverted",
	"/debug/index/rebuild/inverted/cancelReindexContext",
	"/debug/index/rebuild/inverted/suspend",
	"/debug/index/rebuild/inverted/resume",
	"/debug/index/rebuild/inverted/rollback",
	"/debug/index/rebuild/inverted/unrollback",
	"/debug/index/rebuild/inverted/start",
	"/debug/index/rebuild/inverted/unstart",
	"/debug/index/rebuild/inverted/reset",
	"/debug/index/rebuild/inverted/unreset",
	"/debug/index/rebuild/inverted/setProperties",
	"/debug/index/rebuild/inverted/status",
	"/debug/index/rebuild/inverted/overrides",
	"/debug/index/rebuild/inverted/set_overrides",
}

// TestRemovedReindexDebugRoutes asserts the removed routes stay removed.
//
// The run happens with the debug surface fully enabled
// (WithWeaviateWithDebugPort sets DEBUG_ENDPOINTS_ENABLED=true), so a 404
// means the route does not exist rather than that the gate rejected it. The
// /debug/config control proves the listener is up and serving in the same
// run — without it, a container that failed to bind would pass vacuously.
func TestRemovedReindexDebugRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateWithDebugPort().Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	debugURI := compose.GetWeaviate().DebugURI()

	t.Run("control: debug surface is enabled and serving", func(t *testing.T) {
		status, body := get(t, debugURI, "/debug/config")
		require.Equal(t, http.StatusOK, status,
			"the debug listener must serve /debug/config, otherwise the 404s below prove nothing: body=%s", body)
	})

	t.Run("control: a surviving route under the same prefix still resolves", func(t *testing.T) {
		// Rules out the case where every path under /debug/index/rebuild/
		// 404s for an unrelated reason. This route is out of scope for the
		// removal, so it must answer something other than 404 — 501 when
		// async indexing is off, 400 on the missing arguments.
		status, body := get(t, debugURI, "/debug/index/rebuild/vector")
		require.NotEqual(t, http.StatusNotFound, status,
			"/debug/index/rebuild/vector must still be registered: body=%s", body)
	})

	t.Run("removed routes return 404", func(t *testing.T) {
		for _, route := range removedReindexRoutes {
			t.Run(route, func(t *testing.T) {
				// A query string the old handlers accepted, so a surviving
				// handler answers 200/400/500 rather than 404 on argument
				// validation.
				status, body := get(t, debugURI, route+"?collection=Foo&propertyNames=bar&shards=s1")
				assert.Equal(t, http.StatusNotFound, status,
					"route %s must not exist: body=%s", route, body)
			})
		}
	})
}

func get(t *testing.T, debugHost, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", debugHost, path), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}
