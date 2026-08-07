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
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/weaviate/weaviate/test/docker"
)

// hashtreeDir is the in-container snapshot dir: /data/<class-lc>/<shard>/hashtree_uuid.
func hashtreeDir(class, shard string) string {
	return fmt.Sprintf("/data/%s/%s/hashtree_uuid", strings.ToLower(class), shard)
}

func execOut(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, cmd string) (int, string) {
	t.Helper()
	exitCode, reader, err := compose.GetWeaviateNode(n).Container().Exec(ctx, []string{"sh", "-c", cmd}, tcexec.Multiplexed())
	require.NoError(t, err, "exec on node %d: %s", n, cmd)
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	return exitCode, string(raw)
}

// listHashtreeFiles lists snapshot files on a RUNNING node; missing dir = empty.
func listHashtreeFiles(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, class, shard string) []string {
	t.Helper()
	code, out := execOut(ctx, t, compose, n, fmt.Sprintf("ls -1 %s 2>/dev/null || true", hashtreeDir(class, shard)))
	require.Equal(t, 0, code)
	var files []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(stripExecArtifacts(line))
		if strings.Contains(line, "hashtree-") {
			files = append(files, line)
		}
	}
	return files
}

// stripExecArtifacts drops non-printable docker stream-header bytes.
func stripExecArtifacts(line string) string {
	return strings.Map(func(r rune) rune {
		if r < 32 || r > 126 {
			return -1
		}
		return r
	}, line)
}

// requireNoHashtreeFiles asserts the consumed-on-load contract: no .ht or .tmp survive.
func requireNoHashtreeFiles(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, class, shard string) {
	t.Helper()
	files := listHashtreeFiles(ctx, t, compose, n, class, shard)
	require.Empty(t, files, "node %d shard %s/%s still has snapshot files", n, class, shard)
}

// plantHashtreeJunk writes junk bytes as a snapshot file; the hex timestamp in
// the filename controls newest-first load ordering.
func plantHashtreeJunk(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, class, shard, filename string) {
	t.Helper()
	dir := hashtreeDir(class, shard)
	code, out := execOut(ctx, t, compose, n,
		fmt.Sprintf("mkdir -p %s && printf 'battle junk payload, not a hashtree' > %s/%s", dir, dir, filename))
	require.Equal(t, 0, code, "plant junk on node %d: %s", n, out)
}

// requireNoEphemeralTenantDirs asserts no deleted tenant left a shard dir (and
// hence no hashtree_uuid dir) behind on the node — the resurrection oracle.
func requireNoEphemeralTenantDirs(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, class string, deleted []string) {
	t.Helper()
	code, out := execOut(ctx, t, compose, n, fmt.Sprintf("ls -1 /data/%s 2>/dev/null || true", strings.ToLower(class)))
	require.Equal(t, 0, code)
	for _, tenant := range deleted {
		require.NotContains(t, out, tenant, "node %d resurrected deleted tenant %q shard dir", n, tenant)
	}
}
