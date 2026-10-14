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

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A vertex can advertise more connections at a layer than the current
// maxConnections allows: indexes written before v1.12.0 could overflow
// (https://github.com/weaviate/weaviate/issues/1868) and the vertex layer
// never enforces the cap (vertex.appendConnectionsAtLevelNoLock ignores its
// maxConns argument). The SWEEPING branch of searchLayerByVectorWithDistancer
// has guarded against that since
// https://github.com/weaviate/weaviate/issues/1897; the ACORN branch sizes its
// frontier buffer from the pool at maximumConnectionsLayerZero and re-slices it
// to the advertised count, so the same graph panics with
// "slice bounds out of range" on every filtered search that pops the
// overloaded vertex.
func TestSearchOverloadedVertexConnections(t *testing.T) {
	const (
		seeded  = 400
		overbig = 69 // > maximumConnectionsLayerZero (2 * MaxConnections = 32)
	)

	tests := []struct {
		name           string
		filterStrategy string
	}{
		{name: "acorn", filterStrategy: ent.FilterStrategyAcorn},
		{name: "sweeping", filterStrategy: ent.FilterStrategySweeping},
	}

	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecs(seeded, 1, 8)

	// every tenth id, keeping the allow list under AcornFilterRatio so a
	// filtered search picks ACORN over RRE
	allowed := make([]uint64, 0, seeded/10)
	for id := uint64(0); id < seeded; id += 10 {
		allowed = append(allowed, id)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := newSeededIndex(t, "acorn-overloaded-vertex", vectors, seeded, test.filterStrategy)

			// Overload every vertex at layer zero. Searching the pool-backed
			// buffers has not happened yet, so the pool still hands out slices
			// sized at maximumConnectionsLayerZero.
			conns := make([]uint64, overbig)
			for i := range conns {
				conns[i] = uint64(i)
			}
			for id := uint64(0); id < seeded; id++ {
				node := index.nodes[id]
				require.NotNil(t, node)
				node.setConnectionsAtLevel(0, conns)
			}

			var (
				ids []uint64
				err error
			)
			require.NotPanics(t, func() {
				ids, _, err = index.SearchByVector(ctx, queries[0], 10,
					helpers.NewAllowList(allowed...))
			})
			require.Nil(t, err)
			require.NotEmpty(t, ids)
			for _, id := range ids {
				require.Contains(t, allowed, id)
			}
		})
	}
}
