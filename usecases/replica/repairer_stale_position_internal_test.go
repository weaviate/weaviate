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

package replica

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// staleAt says where the replica that is behind sits relative to the reply
// carrying the caller's copy.
type staleAt int

const (
	// staleServesRead puts the replica that is behind at contentIdx: the node
	// answering the read is the one holding the outdated version.
	staleServesRead staleAt = iota
	// stalePeer answers the read from a replica that already holds the winning
	// version, leaving a peer behind.
	stalePeer
)

// Read repair must close the same divergence whether the stale replica serves
// the read or is a peer: lastTimes seeds only from contentIdx's reply, so the
// stale replica's position is an axis of its own, independent of digest order.
//
// Both halves of convergence are asserted: the rounds stop doing work, and the
// replica that was behind reached the winner. A round that writes nothing also
// stops doing work, so on the write axis alone a repair that leaves the
// replicas divergent forever is indistinguishable from one that succeeded.
func TestRepairBatchPartConvergesWhereverTheStaleReplicaSits(t *testing.T) {
	const (
		shard      = "S1"
		numObjects = 5
		numReads   = 10

		tStale     = int64(100)
		tTombstone = int64(200)
		tWinner    = int64(300)
	)

	ids := make([]strfmt.UUID, numObjects)
	for i := range ids {
		ids[i] = strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", i))
	}

	cases := []struct {
		name     string
		order    []string // order[0] serves the read, the rest in RPC completion order
		position staleAt
		strategy string
		// Replicas a single repair round has to write. Bounds the write axis the
		// way numObjects bounds the fetch axis.
		staleReplicas int
	}{
		{
			name:          "stale replica serves the read, tombstone digest first",
			order:         []string{"A", "B", "C"},
			position:      staleServesRead,
			strategy:      models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			staleReplicas: 1,
		},
		{
			name:          "stale replica serves the read, winner digest first",
			order:         []string{"A", "C", "B"},
			position:      staleServesRead,
			strategy:      models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			staleReplicas: 1,
		},
		{
			name:          "winner serves the read, tombstone digest first",
			order:         []string{"C", "B", "A"},
			position:      stalePeer,
			strategy:      models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			staleReplicas: 1,
		},
		{
			name:          "winner serves the read, stale digest first",
			order:         []string{"C", "A", "B"},
			position:      stalePeer,
			strategy:      models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			staleReplicas: 1,
		},
		{
			name:     "winner serves the read, time based",
			order:    []string{"C", "A", "B"},
			position: stalePeer,
			strategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			// The tombstone loses to the newer write, so it is repairable too.
			staleReplicas: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := newFakeReplicas(tc.strategy, []string{"A", "B", "C"})
			for _, id := range ids {
				c.put("A", id, tStale, false)
				c.put("B", id, tTombstone, true)
				c.put("C", id, tWinner, false)
			}
			r := c.newRepairer(t)

			reader := tc.order[0]
			fetchesPerRead := make([]int, 0, numReads)
			writesPerRead := make([]int, 0, numReads)

			for read := 0; read < numReads; read++ {
				// A read only sees objects the replica answering it still serves.
				live := make([]strfmt.UUID, 0, len(ids))
				for _, id := range ids {
					if x := c.get(reader, id); !x.deleted && x.updateTime != 0 {
						live = append(live, id)
					}
				}
				if len(live) == 0 {
					break
				}

				vs := c.votes(tc.order, live)
				if read == 0 {
					requireStalePosition(t, vs, tc.position)
				}

				f0, w0, _ := c.counters()
				_, err := r.repairBatchPart(ctx, shard, live, vs, 0)
				require.NoError(t, err)
				f1, w1, _ := c.counters()
				fetchesPerRead = append(fetchesPerRead, f1-f0)
				writesPerRead = append(writesPerRead, w1-w0)
			}

			fetched, attempted, applied := c.counters()
			t.Logf("fetches per read: %v, writes per read: %v (total fetched %d, attempted %d, applied %d)",
				fetchesPerRead, writesPerRead, fetched, attempted, applied)

			require.NotEmpty(t, fetchesPerRead, "no read was performed")

			// Asserted before the bounds below, which a repair that never wrote
			// anything satisfies while leaving the replicas divergent.
			for _, id := range ids {
				require.Equal(t, replicaObj{updateTime: tWinner}, c.get("A", id),
					"the replica that was behind never reached the winning version")
			}

			// Anti-vacuity: without a fetch, every bound below holds trivially.
			require.Positive(t, fetched, "fixture never reached the fetch path")

			last := len(fetchesPerRead) - 1
			assert.Zero(t, fetchesPerRead[last],
				"repair has not converged: still re-fetching content on the last read")
			assert.Zero(t, writesPerRead[last],
				"repair has not converged: still writing on the last read")

			assert.LessOrEqual(t, attempted, tc.staleReplicas*numObjects,
				"unbounded write amplification: total overwrite attempts exceed one repair round")
			assert.LessOrEqual(t, fetched, numObjects,
				"unbounded read amplification: total object fetches exceed one repair round")
			assert.Equal(t, attempted, applied,
				"a repair write was refused, so the round spent an RPC that changed nothing")
		})
	}
}

// requireStalePosition asserts the fixture still builds the divergence its case
// names. A fixture that stopped placing the stale replica where the case says
// would still satisfy every convergence bound, because it would be exercising
// the other arm twice.
func requireStalePosition(t *testing.T, vs []Vote, position staleAt) {
	t.Helper()

	require.True(t, vs[0].IsLocal,
		"vote 0 does not carry the caller's copy, so contentIdx no longer marks the replica serving the read")

	var winning int64
	for _, v := range vs {
		if x := v.UpdateTimeAt(0); x > winning {
			winning = x
		}
	}

	behind := 0
	for _, v := range vs {
		if v.UpdateTimeAt(0) < winning {
			behind++
		}
	}
	require.Positive(t, behind,
		"fixture no longer holds a replica behind the winner, so there is nothing to repair")

	if position == staleServesRead {
		require.Less(t, vs[0].UpdateTimeAt(0), winning,
			"fixture no longer serves the read from the stale replica, so the condition under test is unreachable")
		return
	}
	require.Equal(t, winning, vs[0].UpdateTimeAt(0),
		"fixture no longer serves the read from a replica holding the winner, so the condition under test is unreachable")
}
