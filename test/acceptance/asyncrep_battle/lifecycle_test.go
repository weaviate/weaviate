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
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/helper"
)

// TestBattleLifecycle exercises the .ht snapshot lifecycle: graceful-shutdown
// publication and trust-load, crash rescan, and stale-snapshot planting. No
// runtime-flag toggles here — a config transition would scrub planted files.
func TestBattleLifecycle(t *testing.T) {
	p := battleProfile()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	compose := buildCompose(ctx, t, nil)
	defer func() {
		if t.Failed() {
			compose.DumpWeaviateLogs(ctx, os.Stdout, 400)
		}
		require.NoError(t, compose.Terminate(ctx))
	}()

	t.Run("S2 graceful ht reuse", func(t *testing.T) {
		const class = "BattleS2"
		ensureAllRunning(ctx, t, compose)
		uri1 := compose.GetWeaviateNode(1).URI()
		helper.SetupClient(uri1)
		helper.CreateClass(t, battleClass(class, 1, false))
		probeID := seedSentinel(t, uri1, class)
		seedObjects(t, uri1, class, 500)
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)
		shard := common.DiscoverShards(t, uri1, class)[0]

		cur := markLogs(ctx, t, compose, 2)
		cycleNode(ctx, t, compose, 2, nil, class, probeID)

		w := logsSince(ctx, t, compose, cur)
		require.Equal(t, 1, countMarker(w, markerInitFromCache, class), "graceful restart must trust-load the published snapshot")
		require.Zero(t, countFullScanInit(w, class), "no full rescan expected after a clean shutdown")
		require.Zero(t, countMarker(w, markerHeightMismatch, class))
		requireNoHashtreeFiles(ctx, t, compose, 2, class, shard)

		seedObjects(t, compose.GetWeaviateNode(1).URI(), class, 50)
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)
	})

	t.Run("S3 crash rescan and repair", func(t *testing.T) {
		const class = "BattleS3"
		ensureAllRunning(ctx, t, compose)
		uri1 := compose.GetWeaviateNode(1).URI()
		helper.SetupClient(uri1)
		helper.CreateClass(t, battleClass(class, 1, false))
		probeID := seedSentinel(t, uri1, class)
		seeded := seedObjects(t, uri1, class, 300)
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)
		shard := common.DiscoverShards(t, uri1, class)[0]

		// One clean cycle first so a dump/consume round has already happened.
		cycleNode(ctx, t, compose, 2, nil, class, probeID)
		uri1 = compose.GetWeaviateNode(1).URI()

		seedObjects(t, uri1, class, 200)
		crashCur := markLogs(ctx, t, compose, 2)
		stopNode(ctx, t, compose, 2, &sigkill)
		time.Sleep(5 * time.Second)

		var newIDs []strfmt.UUID
		for i := 0; i < 100; i++ {
			id := strfmt.UUID(uuid.NewString())
			obj := &models.Object{ID: id, Class: class, Properties: map[string]interface{}{"contents": fmt.Sprintf("while-down-%d", i)}}
			upsertObjectEventually(t, uri1, obj, types.ConsistencyLevelQuorum)
			newIDs = append(newIDs, id)
		}
		deletedIDs := seeded[:50]
		for _, id := range deletedIDs {
			common.DeleteObject(t, uri1, class, id, types.ConsistencyLevelQuorum)
		}

		uri2 := startNodeAndWait(ctx, t, compose, 2, class, probeID)
		fcReady := captureMetric(ctx, t, compose, []int{1, 3}, "weaviate_async_replication_iteration_failure_count")
		readyCur1 := markLogs(ctx, t, compose, 1)
		readyCur3 := markLogs(ctx, t, compose, 3)

		w2 := logsSince(ctx, t, compose, crashCur)
		require.Zero(t, countMarker(w2, markerInitFromCache, class), "a crash must never trust a snapshot")
		require.GreaterOrEqual(t, countFullScanInit(w2, class), 1, "crash restart must rescan the object store")
		requireNoHashtreeFiles(ctx, t, compose, 2, class, shard)

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, id := range deletedIDs[:20] {
				exists, err := common.ObjectExistsCL(t, uri2, class, id, types.ConsistencyLevelOne)
				require.NoError(ct, err)
				require.False(ct, exists, "deleted id %s resurrected on rejoined node", id)
			}
			for _, id := range newIDs[:20] {
				exists, err := common.ObjectExistsCL(t, uri2, class, id, types.ConsistencyLevelOne)
				require.NoError(ct, err)
				require.True(ct, exists, "id %s written while down missing on rejoined node", id)
			}
		}, 3*time.Minute, 2*time.Second, "rejoined node was not repaired")
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)

		time.Sleep(15 * time.Second)
		for n, delta := range metricDelta(ctx, t, compose, fcReady, "weaviate_async_replication_iteration_failure_count") {
			require.Zero(t, delta, "peer node %d counted hashbeat failures after the restarted node became ready", n)
		}
		require.Zero(t, countMarker(logsSince(ctx, t, compose, readyCur1), markerHashbeatFailed, ""), "node 1 warned about hashbeat failures after peer became ready")
		require.Zero(t, countMarker(logsSince(ctx, t, compose, readyCur3), markerHashbeatFailed, ""), "node 3 warned about hashbeat failures after peer became ready")
	})

	t.Run("S8 stale snapshot plants", func(t *testing.T) {
		const class = "BattleS8"
		ensureAllRunning(ctx, t, compose)
		uri1 := compose.GetWeaviateNode(1).URI()
		helper.SetupClient(uri1)
		helper.CreateClass(t, battleClass(class, 1, false))
		probeID := seedSentinel(t, uri1, class)
		seedObjects(t, uri1, class, 200)
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)
		shard := common.DiscoverShards(t, uri1, class)[0]

		t.Run("past junk swept unattempted on graceful restart", func(t *testing.T) {
			plantHashtreeJunk(ctx, t, compose, 2, class, shard, "hashtree-0000000000000001.ht")
			cur := markLogs(ctx, t, compose, 2)
			cycleNode(ctx, t, compose, 2, nil, class, probeID)
			w := logsSince(ctx, t, compose, cur)
			require.Equal(t, 1, countMarker(w, markerInitFromCache, class), "the real newest dump must still be trusted")
			require.Zero(t, countMarker(w, markerDeserializeErr, ""), "older junk must be swept without being read")
			requireNoHashtreeFiles(ctx, t, compose, 2, class, shard)
		})

		t.Run("future junk forces rescan on graceful restart", func(t *testing.T) {
			plantHashtreeJunk(ctx, t, compose, 2, class, shard, "hashtree-ffffffffffffffff.ht")
			cur := markLogs(ctx, t, compose, 2)
			cycleNode(ctx, t, compose, 2, nil, class, probeID)
			w := logsSince(ctx, t, compose, cur)
			require.GreaterOrEqual(t, countMarker(w, markerDeserializeErr, ""), 1, "the newest (junk) file must be attempted and rejected")
			require.Zero(t, countMarker(w, markerInitFromCache, class), "no fallback to an older snapshot")
			require.GreaterOrEqual(t, countFullScanInit(w, class), 1)
			requireNoHashtreeFiles(ctx, t, compose, 2, class, shard)
		})

		t.Run("future junk after crash forces rescan", func(t *testing.T) {
			plantHashtreeJunk(ctx, t, compose, 2, class, shard, "hashtree-fffffffffffffffe.ht")
			cur := markLogs(ctx, t, compose, 2)
			cycleNode(ctx, t, compose, 2, &sigkill, class, probeID)
			w := logsSince(ctx, t, compose, cur)
			require.GreaterOrEqual(t, countMarker(w, markerDeserializeErr, ""), 1)
			require.Zero(t, countMarker(w, markerInitFromCache, class))
			require.GreaterOrEqual(t, countFullScanInit(w, class), 1)
			requireNoHashtreeFiles(ctx, t, compose, 2, class, shard)
		})

		t.Run("height mismatch discards the snapshot", func(t *testing.T) {
			cls := common.GetClass(t, uri1, class)
			cls.ReplicationConfig.AsyncConfig.HashtreeHeight = i64(12)
			common.UpdateClass(t, uri1, cls)
			cur := markLogs(ctx, t, compose, 2)
			cycleNode(ctx, t, compose, 2, nil, class, probeID)
			w := logsSince(ctx, t, compose, cur)
			require.Equal(t, 1, countMarker(w, markerHeightMismatch, class), "the height change must discard the cached snapshot")
			require.Zero(t, countMarker(w, markerInitFromCache, class))
			require.GreaterOrEqual(t, countFullScanInit(w, class), 1)
			requireNoHashtreeFiles(ctx, t, compose, 2, class, shard)
			cls = common.GetClass(t, uri1, class)
			cls.ReplicationConfig.AsyncConfig.HashtreeHeight = nil
			common.UpdateClass(t, uri1, cls)
		})

		for n := 1; n <= 3; n++ {
			require.Zero(t, countMarker(nodeLogs(ctx, t, compose, n), markerDemoted, ""), "no snapshot demotion expected in any lifecycle scenario")
		}
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)
	})

	requireCleanLogs(ctx, t, compose)
}

// seedObjects writes n objects at CL=ALL and returns their ids.
func seedObjects(t *testing.T, uri, class string, n int) []strfmt.UUID {
	t.Helper()
	batch := make([]*models.Object, n)
	ids := make([]strfmt.UUID, n)
	for i := 0; i < n; i++ {
		ids[i] = strfmt.UUID(uuid.NewString())
		batch[i] = &models.Object{
			ID:         ids[i],
			Class:      class,
			Properties: map[string]interface{}{"contents": fmt.Sprintf("seed-%d", i), "ver": 1},
		}
	}
	common.CreateObjectsCL(t, uri, batch, types.ConsistencyLevelAll)
	return ids
}
