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

package replica_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/replica"
)

// breakerOpen is what the client returns instead of contacting a host whose breaker is open
func breakerOpen(host string) error {
	return fmt.Errorf("%w: %s (3 consecutive failures, retrying in 500ms)", replica.ErrHostCircuitOpen, host)
}

// A read must not fail because one replica's breaker is open while another can serve it.
func TestFinderReadSkipsReplicaWithOpenBreaker(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"0", "1"}
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	for _, tt := range []struct {
		name string
		cl   types.ConsistencyLevel
	}{
		{name: "ONE", cl: types.ConsistencyLevelOne},
		{name: "QUORUM", cl: types.ConsistencyLevelQuorum},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				f           = newFakeFactory(t, cls, shard, nodes, false)
				finder      = f.newFinder("A")
				xs, digestR = genInputs("A", shard, 1, ids)
			)
			// the breaker never lets this replica back in, not even as a last resort
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[1], cls, shard, ids, 0).
				RunAndReturn(func(context.Context, string, string, string, []strfmt.UUID, int) ([]types.RepairResponse, error) {
					return nil, breakerOpen(nodes[1])
				}).Maybe()
			f.RClient.EXPECT().DigestObjects(anyVal, nodes[2], cls, shard, ids, 0).Return(digestR, nil).Maybe()

			want := setObjectsConsistency(xs, true)
			require.NoError(t, finder.CheckConsistency(ctx, tt.cl, xs),
				"an open breaker on %q must cost the read a replica, not the read itself", nodes[1])
			assert.ElementsMatch(t, want, xs)
		})
	}
}

// With every replica's breaker open the read must still attempt one: a breaker must never make a shard unreadable.
func TestFinderReadIgnoresBreakerWhenNoReplicaIsLeft(t *testing.T) {
	var (
		ids   = []strfmt.UUID{"0", "1"}
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	for _, tt := range []struct {
		name string
		cl   types.ConsistencyLevel
	}{
		{name: "QUORUM", cl: types.ConsistencyLevelQuorum},
		{name: "ALL", cl: types.ConsistencyLevelAll},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				f           = newFakeFactory(t, cls, shard, nodes, false)
				finder      = f.newFinder("A")
				xs, digestR = genInputs("A", shard, 1, ids)
				contacted   atomic.Int64
			)
			// every remote replica is refused until the read runs out of alternatives
			for _, node := range nodes[1:] {
				node := node
				f.RClient.EXPECT().DigestObjects(anyVal, node, cls, shard, ids, 0).
					RunAndReturn(func(ctx context.Context, _, _, _ string, _ []strfmt.UUID, _ int,
					) ([]types.RepairResponse, error) {
						if !replica.HostBreakerBypassed(ctx) {
							return nil, breakerOpen(node)
						}
						contacted.Add(1)
						return digestR, nil
					}).Maybe()
			}

			want := setObjectsConsistency(xs, true)
			require.NoError(t, finder.CheckConsistency(ctx, tt.cl, xs),
				"the last replicas left must be contacted even with their breakers open")
			assert.ElementsMatch(t, want, xs)
			assert.Positive(t, contacted.Load(), "no replica was ever contacted, the shard was unreadable")
		})
	}
}

// count(*) polls replicas through the same read path, so an open breaker must not empty the count.
func TestFinderCountObjectsIgnoresBreakerWhenNoReplicaIsLeft(t *testing.T) {
	var (
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
		ctx   = context.Background()
	)

	f := newFakeFactory(t, cls, shard, nodes, false)
	finder := f.newFinder("A")
	for _, node := range nodes {
		node := node
		f.RClient.EXPECT().CountObjects(anyVal, node, cls, shard).
			RunAndReturn(func(ctx context.Context, _, _, _ string) (int, error) {
				if !replica.HostBreakerBypassed(ctx) {
					return 0, breakerOpen(node)
				}
				return 7, nil
			}).Maybe()
	}

	count, err := finder.CountObjects(ctx, shard, types.ConsistencyLevelAll)
	require.NoError(t, err)
	assert.Equal(t, 7, count, "an open breaker must never report a shard as empty")
}

// A shard whose replicas cannot be resolved must fail the count, never silently contribute zero.
func TestFinderCountObjectsSurfacesRoutingFailure(t *testing.T) {
	var (
		cls   = "C1"
		shard = "S1"
		nodes = []string{"A", "B", "C"}
	)

	f := newFakeFactory(t, cls, shard, nodes, false)
	finder := f.newFinder("A")

	count, err := finder.CountObjects(context.Background(), "unknown-shard", types.ConsistencyLevelAll)
	require.Error(t, err, "aggregateCount sums these: a routing failure must not read as an empty shard")
	assert.Zero(t, count)
}
