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

package selfrecovery

import (
	"context"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/test/docker"
)

// postDebug POSTs path on a debug port and returns the status and the body.
func postDebug(ctx context.Context, t *testing.T, debugURI, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+debugURI+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("close %s body: %v", path, err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func TestSelfRecoveryUnlicensedDoesNotRecover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{
		unlicensed: true, asyncDisabled: true, debugPort: true, persistentData: true,
	})

	const (
		class     = "UnlicensedMulti"
		objCount  = 300
		victimIdx = 2
	)
	victim := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	var (
		counts    map[string]int64
		removed   []string
		kept      string
		opsBefore map[string]string
	)
	dumpVictimOnFailure(t, compose, victimIdx, victim)

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "create RF=3 three-shard collection and ingest", func(t *testing.T) {
		ensureClass(t, srMultiShardClass(class, 3))
		waitShardsPresent(t, class, 3)
		waitShardsLoaded(t, class, 3)
		submitBatch(t, srParagraphObjects(class, "eeeeeeee-eeee-eeee-eeee", objCount, ""), types.ConsistencyLevelAll)
	})

	mustRun(t, "record per-shard counts and the ops registered so far", func(t *testing.T) {
		counts = waitShardObjectCounts(t, class, docker.Weaviate0, objCount)
		require.Len(t, counts, 3)
		assertShardObjectCounts(t, class, victim, counts)
		names := make([]string, 0, len(counts))
		for name := range counts {
			names = append(names, name)
		}
		sort.Strings(names)
		removed, kept = names[:2], names[2]
		for _, name := range removed {
			require.Positive(t, counts[name], "shard %s must hold data before removal", name)
		}
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "remove two shard dirs and restart", func(t *testing.T) {
		removeShardDirsAndKill(ctx, t, compose, victimIdx, class, removed...)
		startNode(ctx, t, compose, victimIdx)
		waitClusterHealthy(t)
	})

	mustRun(t, "startup warns about the missing license and falls back per shard", func(t *testing.T) {
		assertNodeLogContains(ctx, t, compose, victimIdx, "the self-recovery feature is enabled but this node holds no well-formed Weaviate license key")
		assertNodeLogContains(ctx, t, compose, victimIdx, "self-recovery: submission was not queued")
	})

	mustRun(t, "no recovery registered and the removed shards stay empty", func(t *testing.T) {
		assertNoActiveRecovery(t, []string{victim}, 45*time.Second)
		require.Empty(t, mustNewSelfRecoveryTargets(t, victim, opsBefore))
		waitNoShardRecovering(t, class, victim)
		assertShardLoadedState(t, class, victim, map[string]bool{removed[0]: true, removed[1]: true, kept: true})
		assertShardObjectCounts(t, class, victim, map[string]int64{removed[0]: 0, removed[1]: 0, kept: counts[kept]})
		assertShardObjectCounts(t, class, docker.Weaviate1, counts)
		assertShardObjectCounts(t, class, docker.Weaviate0, counts)
	})

	mustRun(t, "debug endpoints refuse every call with the license error", func(t *testing.T) {
		debugURI := compose.GetWeaviate().DebugURI()
		require.NotEmpty(t, debugURI)
		for _, tc := range []struct {
			name, path, bodyContains string
			wantStatus               int
		}{
			{
				name:         "restart on a live shard is forbidden",
				path:         "/debug/self-recovery/restart?collection=" + class + "&shard=" + kept,
				wantStatus:   http.StatusForbidden,
				bodyContains: "license",
			},
			{
				name:         "restart on an unknown class is forbidden",
				path:         "/debug/self-recovery/restart?collection=NoSuchClass&shard=X",
				wantStatus:   http.StatusForbidden,
				bodyContains: "license",
			},
			{
				name:         "accept-empty on a live shard is forbidden",
				path:         "/debug/self-recovery/accept-empty?collection=" + class + "&shard=" + kept,
				wantStatus:   http.StatusForbidden,
				bodyContains: "license",
			},
			{
				name:         "accept-empty on an unknown class is forbidden",
				path:         "/debug/self-recovery/accept-empty?collection=NoSuchClass&shard=X",
				wantStatus:   http.StatusForbidden,
				bodyContains: "license",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				status, body := postDebug(ctx, t, debugURI, tc.path)
				require.Equal(t, tc.wantStatus, status, "%s: body %q", tc.path, body)
				require.Contains(t, strings.ToLower(body), tc.bodyContains)
			})
		}
		forceRaftSnapshot(ctx, t, compose, 0)
	})
}
