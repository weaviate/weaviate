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
	"testing"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"

	"github.com/stretchr/testify/require"
)

// writeAsyncReplicationOverride rewrites the runtime-overrides file on every
// node (each reads its own copy); the 1s poll interval picks it up within a
// few seconds, and the hook fires only when the value actually changes.
func writeAsyncReplicationOverride(ctx context.Context, t *testing.T, compose *docker.DockerCompose, disabled bool) {
	t.Helper()
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		exitCode, _, err := node.Container().Exec(ctx, []string{
			"sh", "-c",
			fmt.Sprintf("printf 'async_replication_disabled: %t\\n' > %s", disabled, overridePath),
		})
		require.NoError(t, err, "write runtime override on node %d", i)
		require.Equal(t, 0, exitCode, "exec returned non-zero on node %d", i)
	}
}

// shardsAsyncReplicationLen sums len(asyncReplicationStatus) across every node
// and shard of the class; zero means async replication is registered nowhere.
// Uses the global helper client — call helper.SetupClient first, main goroutine only.
func shardsAsyncReplicationLen(t *testing.T, class string) (int, error) {
	verbose := verbosity.OutputVerbose
	params := nodes.NewNodesGetClassParams().WithClassName(class).WithOutput(&verbose)
	body, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
	if err != nil {
		return 0, err
	}
	if body.Payload == nil {
		return 0, fmt.Errorf("nil payload from NodesGetClass")
	}
	total := 0
	for _, n := range body.Payload.Nodes {
		for _, s := range n.Shards {
			total += len(s.AsyncReplicationStatus)
		}
	}
	return total, nil
}
