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
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestBattleChurn runs a continuous writer while every node is cycled
// gracefully and by SIGKILL, folding in the not-ready quietness oracles
// (LOG_LEVEL=debug here so the Debug skip marker is visible).
func TestBattleChurn(t *testing.T) {
	p := battleProfile()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	compose := buildCompose(ctx, t, map[string]string{"LOG_LEVEL": "debug"})
	defer func() {
		if t.Failed() {
			compose.DumpWeaviateLogs(ctx, os.Stdout, 400)
		}
		require.NoError(t, compose.Terminate(ctx))
	}()

	t.Run("S1 convergence under node churn", func(t *testing.T) {
		const class = "BattleS1"
		ensureAllRunning(ctx, t, compose)
		uri1 := compose.GetWeaviateNode(1).URI()
		helper.SetupClient(uri1)
		helper.CreateClass(t, battleClass(class, 3, false))
		probeID := seedSentinel(t, uri1, class)

		w := newChurnWriter(class, p, nodeURIs(compose))
		w.start()
		time.Sleep(60 * time.Second)

		for n := 1; n <= 3; n++ {
			for _, mode := range []struct {
				name    string
				timeout *time.Duration
			}{{"graceful", nil}, {"sigkill", &sigkill}} {
				t.Logf("cycling node %d (%s)", n, mode.name)
				skipCur := map[int]logCursor{}
				for _, peer := range peersOf(n) {
					skipCur[peer] = markLogs(ctx, t, compose, peer)
				}
				w.setTargets(aliveURIs(compose, n))
				stopNode(ctx, t, compose, n, mode.timeout)
				time.Sleep(p.holdDown)

				startNodeAndWait(ctx, t, compose, n, class, probeID)
				fcReady := captureMetric(ctx, t, compose, peersOf(n), "weaviate_async_replication_iteration_failure_count")
				readyCur := map[int]logCursor{}
				for _, peer := range peersOf(n) {
					readyCur[peer] = markLogs(ctx, t, compose, peer)
				}
				w.setTargets(nodeURIs(compose))
				time.Sleep(15 * time.Second)

				for peer, delta := range metricDelta(ctx, t, compose, fcReady, "weaviate_async_replication_iteration_failure_count") {
					require.Zero(t, delta, "peer %d counted hashbeat failures after node %d became ready (%s)", peer, n, mode.name)
				}
				for _, peer := range peersOf(n) {
					require.Zero(t, countMarker(logsSince(ctx, t, compose, readyCur[peer]), markerHashbeatFailed, ""),
						"peer %d warned about hashbeat failures after node %d became ready (%s)", peer, n, mode.name)
				}
				skips := 0
				for _, peer := range peersOf(n) {
					skips += countMarker(logsSince(ctx, t, compose, skipCur[peer]), markerNotReadySkip, "")
				}
				// Soft: memberlist can rejoin after readiness, shrinking the
				// observable not-ready phase to zero beats.
				t.Logf("node %d (%s): %d not-ready skip lines on peers during the cycle window", n, mode.name, skips)
			}
		}

		acked, errs := w.stop()
		t.Logf("writer finished: %d acked ops, %d errors", acked, errs)
		require.Greater(t, acked, int64(0), "the writer must have applied load")

		requireConverged(ctx, t, compose, class, p.idSpace*2, p.convergeTimeout)
		// Counters reset on each node's own restart, so deltas are meaningless
		// under churn; the absolute post-convergence sum still proves repair ran
		// after the last restart (the killed node needed catching up).
		total := 0.0
		for n := 1; n <= 3; n++ {
			text := scrapeMetrics(ctx, t, compose, n)
			prop := sumMetric(text, "weaviate_async_replication_propagation_object_count")
			t.Logf("node %d propagation_object_count: %.0f, objects_diff_total: %.0f",
				n, prop, sumMetric(text, "weaviate_async_replication_objects_diff_total"))
			total += prop
		}
		require.Greater(t, total, 0.0, "repair must have propagated objects during churn")

		live, deleted := w.sample(p.perIDSamples/2, p.perIDSamples/2)
		for _, uri := range nodeURIs(compose) {
			requireSampleState(t, uri, class, live, deleted)
		}
	})

	t.Run("S4 rejoin repair including deletes", func(t *testing.T) {
		const class = "BattleS4"
		ensureAllRunning(ctx, t, compose)
		uri1 := compose.GetWeaviateNode(1).URI()
		helper.SetupClient(uri1)
		helper.CreateClass(t, battleClass(class, 3, false))
		probeID := seedSentinel(t, uri1, class)
		seeded := seedObjects(t, uri1, class, 500)
		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)

		stopNode(ctx, t, compose, 3, nil)
		time.Sleep(5 * time.Second)
		uri1 = compose.GetWeaviateNode(1).URI()

		var newIDs []strfmt.UUID
		for i := 0; i < 100; i++ {
			id := strfmt.UUID(uuid.NewString())
			obj := &models.Object{ID: id, Class: class, Properties: map[string]interface{}{"contents": fmt.Sprintf("rejoin-new-%d", i), "ver": 1}}
			upsertObjectEventually(t, uri1, obj, types.ConsistencyLevelQuorum)
			newIDs = append(newIDs, id)
		}
		overwritten := seeded[100:200]
		for i, id := range overwritten {
			obj := &models.Object{ID: id, Class: class, Properties: map[string]interface{}{"contents": fmt.Sprintf("rejoin-upd-%d", i), "ver": 2}}
			upsertObjectEventually(t, uri1, obj, types.ConsistencyLevelQuorum)
		}
		deleted := seeded[:100]
		for _, id := range deleted {
			common.DeleteObject(t, uri1, class, id, types.ConsistencyLevelQuorum)
		}

		uri3 := startNodeAndWait(ctx, t, compose, 3, class, probeID)

		// No further writes: convergence must come from hashbeat alone.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, id := range deleted[:30] {
				exists, err := common.ObjectExistsCL(t, uri3, class, id, types.ConsistencyLevelOne)
				require.NoError(ct, err)
				require.False(ct, exists, "deleted id %s resurrected on rejoined node", id)
			}
			for _, id := range newIDs[:30] {
				exists, err := common.ObjectExistsCL(t, uri3, class, id, types.ConsistencyLevelOne)
				require.NoError(ct, err)
				require.True(ct, exists)
			}
			for _, id := range overwritten[:30] {
				obj, err := common.GetObjectCL(t, uri3, class, id, types.ConsistencyLevelOne)
				require.NoError(ct, err)
				require.EqualValues(ct, 2, numericProp(obj, "ver"), "overwrite of %s not repaired", id)
			}
		}, 3*time.Minute, 2*time.Second, "rejoined node was not repaired by hashbeat alone")

		requireConverged(ctx, t, compose, class, 5000, p.convergeTimeout)
		total := 0.0
		for _, n := range []int{1, 2} {
			total += sumMetric(scrapeMetrics(ctx, t, compose, n), "weaviate_async_replication_propagation_object_count")
		}
		require.Greater(t, total, 0.0, "survivors must have propagated repair objects")
	})

	requireCleanLogs(ctx, t, compose)
}

// aliveURIs returns the URIs of all nodes except the one being cycled.
func aliveURIs(compose *docker.DockerCompose, down int) []string {
	var uris []string
	for i := 1; i <= 3; i++ {
		if i != down {
			uris = append(uris, compose.GetWeaviateNode(i).URI())
		}
	}
	return uris
}

// numericProp reads an int-ish property from a fetched object.
func numericProp(obj *models.Object, name string) int64 {
	props, ok := obj.Properties.(map[string]interface{})
	if !ok {
		return -1
	}
	switch v := props[name].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case fmt.Stringer:
		var out int64
		_, _ = fmt.Sscan(v.String(), &out)
		return out
	default:
		var out int64
		_, _ = fmt.Sscan(fmt.Sprint(v), &out)
		return out
	}
}
