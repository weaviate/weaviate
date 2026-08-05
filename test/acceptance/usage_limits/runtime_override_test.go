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

package usage_limits

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/test/docker"
)

// TestRuntimeOverride_ObjectLimit covers RFC scenario #10: a container
// boots with no usage limit set; while it's running, an operator writes
// the limit into the runtime-overrides YAML file, and the change takes
// effect without restart.
//
// Pattern mirrors TestDefaultShardingCountRuntimeOverride on main
// (test/acceptance/schema/default_sharding_count_test.go in the
// equivalent main worktree): pre-mount an empty YAML file at
// startup, then exec a shell command into the container to overwrite
// it, then poll until the override fires. Polling cadence is the
// configured RUNTIME_OVERRIDES_LOAD_INTERVAL (1s for tests).
func TestRuntimeOverride_ObjectLimit(t *testing.T) {
	const overridePath = "/etc/weaviate/runtime-overrides.yaml"

	ctx, cancel := suiteContext(t)
	defer cancel()

	// Mount an empty YAML at startup so the runtime-overrides config
	// manager has a file to read; an absent path would surface as a
	// startup warning.
	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	c := docker.New().
		WithWeaviate().
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride)
	for k, v := range aggressiveFlushEnv() {
		c = c.WithWeaviateEnv(k, v)
	}
	compose, err := c.Start(ctx)
	require.NoError(t, err, "failed to start container")
	defer func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate testcontainer: %v", err)
		}
	}()

	httpURI := "http://" + compose.GetWeaviate().URI()

	// At startup no limit is set → adding a few objects must succeed.
	postOK(t, ctx, httpURI+"/v1/schema",
		[]byte(`{"class":"RuntimeCol","vectorizer":"none"}`))
	for i := 0; i < 3; i++ {
		body := []byte(fmt.Sprintf(`{"class":"RuntimeCol","properties":{"i":%d}}`, i))
		postOK(t, ctx, httpURI+"/v1/objects", body)
	}
	// Give the memtable flush trigger a beat to land — otherwise the
	// async count still says 0 when the override fires, and the probe
	// would succeed (count 0 + 1 < cap 3).
	time.Sleep(3 * time.Second)

	// Write a runtime override YAML pinning the object cap to 3 (i.e.
	// already at the cap given the inserts above).
	weaviateNode := compose.GetWeaviate()
	exitCode, _, err := weaviateNode.Container().Exec(ctx, []string{
		"sh", "-c",
		fmt.Sprintf("printf 'maximum_allowed_objects_count: 3\\n' > %s", overridePath),
	})
	require.NoError(t, err, "failed to write runtime override file")
	require.Equal(t, 0, exitCode, "exec returned non-zero")

	// Poll until the override takes effect — the next insert must 429.
	// RUNTIME_OVERRIDES_LOAD_INTERVAL=1s, so this should fire within a
	// few seconds; we give it a generous deadline to absorb host-vs-
	// container clock skew and CI variance.
	deadline := time.Now().Add(30 * time.Second)
	var sawLimit bool
	for time.Now().Before(deadline) {
		probeBody := []byte(`{"class":"RuntimeCol","properties":{"i":99}}`)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			httpURI+"/v1/objects", bytes.NewReader(probeBody))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			sawLimit = true
			assert.Contains(t, string(body), "USAGE_LIMIT_EXCEEDED",
				"runtime-overridden limit should produce the canonical body")
			break
		}
		// If we got 200, the override hasn't propagated yet — eat the
		// successful insert and retry. (Note: this means the in-flight
		// count climbs, but the cap is 3 and Get() will eventually catch
		// up; the bounded overshoot is documented in the RFC's "Accepted
		// imperfections".)
		time.Sleep(500 * time.Millisecond)
	}
	require.True(t, sawLimit,
		"runtime override did not take effect within 30s; expected an HTTP 429 once the YAML override propagated")
}
