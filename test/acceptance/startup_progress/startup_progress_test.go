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

package startup_progress

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestStartupProgressLogsShardLoading restarts a node that owns several shards
// and asserts the shard-loading progress reaches the logs.
//
// Only the final "local DB loaded from schema" line is asserted. It is emitted
// whenever the reload runs, with counts freshly scanned from the restored
// schema, so the assertion holds however fast the load finishes. The periodic
// "loading local DB from schema" line needs a load slower than its 5s ticker
// and is left unasserted.
func TestStartupProgressLogsShardLoading(t *testing.T) {
	ctx := context.Background()

	const (
		classCount     = 3
		shardsPerClass = 2
	)

	compose, err := docker.New().
		WithWeaviate().
		// Lazy-loaded shards are discounted from the progress totals; force
		// eager loading so every shard counts.
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	for i := 0; i < classCount; i++ {
		helper.CreateClass(t, &models.Class{
			Class: fmt.Sprintf("StartupProgress%d", i),
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]interface{}{"desiredCount": shardsPerClass},
		})
	}

	// The first boot of an empty node never reloads the DB, so everything
	// asserted below can only come from this restart.
	require.NoError(t, compose.RestartAt(ctx, 0, nil))

	reader, err := compose.GetWeaviate().Container().Logs(ctx)
	require.NoError(t, err)
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	logs := string(raw)

	require.Contains(t, logs, "local DB loaded from schema",
		"the reload's progress tracker must report the load")

	total := classCount * shardsPerClass
	assert.True(t,
		strings.Contains(logs, fmt.Sprintf("shards_total=%d", total)) ||
			strings.Contains(logs, fmt.Sprintf(`"shards_total":%d`, total)),
		"progress fields must carry the full shard count %d, logs:\n%s", total, tail(logs, 40))
	assert.True(t,
		strings.Contains(logs, "progress=100%") ||
			strings.Contains(logs, `"progress":"100%"`),
		"the final progress line must report 100%%, logs:\n%s", tail(logs, 40))
}

func tail(logs string, n int) string {
	lines := strings.Split(logs, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
