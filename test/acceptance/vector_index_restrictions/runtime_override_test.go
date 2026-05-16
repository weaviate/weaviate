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

package vector_index_restrictions

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/test/docker"
)

// TestRuntimeOverride_AllowedVectorIndexTypes mirrors the usage-limits
// runtime-override pattern: boot a container with no restriction, then
// write the allow-list into the runtime-overrides YAML while the
// container is running and verify that subsequent class creates start
// to fail with HTTP 422.
//
// Setup intentionally pre-mounts an empty YAML at the configured
// overrides path so the manager has a file to watch from the start;
// once we write the override mid-flight, the file-watcher (1 s poll
// interval) picks up the change.
func TestRuntimeOverride_AllowedVectorIndexTypes(t *testing.T) {
	const overridePath = "/etc/weaviate/runtime-overrides.yaml"

	ctx, cancel := suiteContext(t)
	defer cancel()

	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	c := docker.New().WithWeaviate().
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride)
	compose, err := c.Start(ctx)
	require.NoError(t, err, "failed to start container")
	defer func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate testcontainer: %v", err)
		}
	}()

	httpURI := "http://" + compose.GetWeaviate().URI()

	// Before override: a flat class is accepted.
	flatBody := []byte(`{
		"class":"RuntimePreOverride",
		"vectorizer":"none",
		"vectorIndexType":"flat"
	}`)
	preResp := postRaw(t, ctx, httpURI+"/v1/schema", flatBody)
	preResp.Body.Close()
	require.LessOrEqual(t, preResp.StatusCode, 299,
		"pre-override flat-class create should succeed; got %d", preResp.StatusCode)

	// Write a runtime override that restricts vectors to hfresh only
	// (and seeds the default to hfresh, since hfresh becomes the sole
	// allowed value).
	weaviateNode := compose.GetWeaviate()
	// printf interprets \n inside single quotes; this matches the pattern
	// used in usage_limits/runtime_override_test.go.
	overrideCmd := fmt.Sprintf(
		"printf 'allowed_vector_index_types:\\n  - hfresh\\ndefault_vector_index: hfresh\\n' > %s",
		overridePath,
	)
	exitCode, _, err := weaviateNode.Container().Exec(ctx, []string{"sh", "-c", overrideCmd})
	require.NoError(t, err, "failed to write runtime override file")
	require.Equal(t, 0, exitCode, "exec returned non-zero")

	// Poll until the override propagates. RUNTIME_OVERRIDES_LOAD_INTERVAL=1s,
	// so this should fire within a few seconds. Generous deadline for
	// CI variance. Each probe uses a unique class name so a pre-propagation
	// success doesn't alias as a "class already exists" 422 once the
	// override fires.
	deadline := time.Now().Add(30 * time.Second)
	var saw422 bool
	for time.Now().Before(deadline) {
		probe := []byte(fmt.Sprintf(`{
			"class":"RuntimePostOverride%d",
			"vectorizer":"none",
			"vectorIndexType":"flat"
		}`, time.Now().UnixNano()))
		resp := postRaw(t, ctx, httpURI+"/v1/schema", probe)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnprocessableEntity {
			saw422 = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.True(t, saw422,
		"runtime override did not take effect within 30s; expected an HTTP 422 once the YAML override propagated")

	// --- grandfather behaviour: the flat class that was created
	// BEFORE the override (RuntimePreOverride) must remain mutable
	// after tightening. A no-op PUT (same body as create) should
	// succeed — restriction is meant to gate *new* configurations,
	// not police pre-existing ones. See QA-#2 comment thread.
	t.Run("grandfather PUT on pre-existing class passes after tighten", func(t *testing.T) {
		putBody := []byte(`{
			"class":"RuntimePreOverride",
			"vectorizer":"none",
			"vectorIndexType":"flat"
		}`)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut,
			httpURI+"/v1/schema/RuntimePreOverride", bytes.NewReader(putBody))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Less(t, resp.StatusCode, 300,
			"PUT on grandfathered class with unchanged body should succeed; got %d", resp.StatusCode)
	})

	// --- now push an INVALID override on top and verify it gracefully
	// degrades to "no restriction" (per the runtime hook's fail-safe
	// reset). Two flavours:
	//   1. unknown enum entry — per-value validator rejects, prior list
	//      stays in effect (still flat → 422).
	//   2. hfresh-only + compression — cross-field hook resets allow-lists
	//      to empty, flat class creates start succeeding again.
	t.Run("invalid runtime override gracefully degrades", func(t *testing.T) {
		// Cross-field violation: hfresh-only + compression set.
		overrideCmd := fmt.Sprintf(
			"printf 'allowed_vector_index_types:\\n  - hfresh\\nallowed_compression_types:\\n  - rq-8\\ndefault_vector_index: hfresh\\n' > %s",
			overridePath,
		)
		exitCode, _, err := weaviateNode.Container().Exec(ctx, []string{"sh", "-c", overrideCmd})
		require.NoError(t, err, "failed to write second override")
		require.Equal(t, 0, exitCode)

		// Wait for the hook to fire and reset Allowed* to empty. After
		// reset, a flat class should be accepted again — that's the
		// fail-safe behavior: a misconfigured operator gets "no
		// restriction" rather than a broken state.
		deadline := time.Now().Add(30 * time.Second)
		var sawAccepted bool
		for time.Now().Before(deadline) {
			probe := []byte(fmt.Sprintf(`{
				"class":"RuntimeInvalid%d",
				"vectorizer":"none",
				"vectorIndexType":"flat"
			}`, time.Now().UnixNano()))
			resp := postRaw(t, ctx, httpURI+"/v1/schema", probe)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				sawAccepted = true
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		require.True(t, sawAccepted,
			"after invalid cross-field override, allow-lists should reset to empty and flat class create should succeed")
	})
}
