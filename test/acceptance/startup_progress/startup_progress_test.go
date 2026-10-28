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
	"regexp"
	"strings"
	"testing"
	"time"

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
//
// The node also recounts property lengths at startup, which must count the
// objects of every shard.
func TestStartupProgressLogsShardLoading(t *testing.T) {
	ctx := context.Background()

	const (
		classCount      = 3
		shardsPerClass  = 2
		objectsPerClass = 10
	)

	compose, err := docker.New().
		WithWeaviate().
		// Lazy-loaded shards are discounted from the progress totals; force
		// eager loading so every shard counts.
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("RECOUNT_PROPERTIES_AT_STARTUP", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	for i := 0; i < classCount; i++ {
		class := fmt.Sprintf("StartupProgress%d", i)
		helper.CreateClass(t, &models.Class{
			Class: class,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]interface{}{"desiredCount": shardsPerClass},
		})
		objects := make([]*models.Object, objectsPerClass)
		for j := range objects {
			objects[j] = &models.Object{Class: class, Properties: map[string]interface{}{"name": "some name"}}
		}
		helper.CreateObjectsBatch(t, objects)
	}

	// The first boot of an empty node never reloads the DB, so everything
	// asserted below can only come from this restart.
	require.NoError(t, compose.RestartAt(ctx, 0, nil))

	readLogs := func(t require.TestingT) string {
		reader, err := compose.GetWeaviate().Container().Logs(ctx)
		require.NoError(t, err)
		defer reader.Close()
		raw, err := io.ReadAll(reader)
		require.NoError(t, err)
		return string(raw)
	}
	logs := readLogs(t)

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

	// reported once the shards have loaded, after the restart returns
	objects := classCount * objectsPerClass
	objectsField := regexp.MustCompile(fmt.Sprintf(`\bobjects"?[=:]%d\b`, objects))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logs := readLogs(c)
		var recounted bool
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, "Recounting property lengths complete") && objectsField.MatchString(line) {
				recounted = true
			}
		}
		assert.True(c, recounted, "the recount must count all %d objects, logs:\n%s", objects, tail(logs, 40))
	}, time.Minute, time.Second)
}

func tail(logs string, n int) string {
	lines := strings.Split(logs, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
