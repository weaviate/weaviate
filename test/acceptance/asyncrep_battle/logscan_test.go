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

package asyncrep_battle

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// Log markers owned by adapters/repos/db/shard_async_replication.go; keep in sync.
const (
	markerInitFromCache  = "hashtree successfully initialized from cache"
	markerInitAny        = "hashtree successfully initialized"
	markerHashbeatFailed = "hashbeat iteration failed"
	markerNotReadySkip   = "hashbeat iteration skipped: target replica not ready"
	markerHeightMismatch = "cached hashtree height mismatch"
	markerDeserializeErr = "deserializing hashtree file"
	markerDemoted        = "demoted undeletable stale hashtree file"
	markerPanic          = "Recovered from panic:"
	markerDataRace       = "WARNING: DATA RACE"
)

// logCursor marks a byte offset into a node's cumulative docker log; the
// json-file log persists across stop/start of the same container.
type logCursor struct {
	node int
	off  int
}

func nodeLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int) string {
	t.Helper()
	rc, err := compose.GetWeaviateNode(n).Container().Logs(ctx)
	require.NoError(t, err, "read logs of node %d", n)
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	require.NoError(t, err)
	return string(raw)
}

func markLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int) logCursor {
	t.Helper()
	return logCursor{node: n, off: len(nodeLogs(ctx, t, compose, n))}
}

// logsSince returns log bytes after the cursor; a shrunken log (container
// recreated) clamps to the full log with a loud warning.
func logsSince(ctx context.Context, t *testing.T, compose *docker.DockerCompose, c logCursor) string {
	t.Helper()
	full := nodeLogs(ctx, t, compose, c.node)
	if c.off > len(full) {
		t.Logf("WARNING: node %d log shrank below cursor (%d > %d) — container recreated? scanning full log", c.node, c.off, len(full))
		return full
	}
	return full[c.off:]
}

// countMarker counts lines containing both the marker and classFrag (pass ""
// to skip class scoping).
func countMarker(logs, marker, classFrag string) int {
	count := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, marker) && (classFrag == "" || strings.Contains(line, classFrag)) {
			count++
		}
	}
	return count
}

// countFullScanInit disambiguates the prefix collision: the full-scan marker
// is a prefix of the from-cache marker.
func countFullScanInit(logs, classFrag string) int {
	return countMarker(logs, markerInitAny, classFrag) - countMarker(logs, markerInitFromCache, classFrag)
}

// requireCleanLogs scans the FULL logs of all nodes for panics and data races.
func requireCleanLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	for n := 1; n <= 3; n++ {
		logs := nodeLogs(ctx, t, compose, n)
		require.Zero(t, countMarker(logs, markerPanic, ""), "node %d recovered from a panic — see logs", n)
		require.Zero(t, countMarker(logs, markerDataRace, ""), "node %d hit a data race — see logs", n)
	}
}
