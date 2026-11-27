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

package vector_index_params

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// fixture is a collection on the shared single node that several tests read.
// The parameters under test are per request, so one collection per index
// configuration serves every scenario. It is built by the first test that
// asks for it and lives until TestMain stops the node, so the inserts against
// the race-instrumented server are paid once per package run.
type fixture struct {
	class string
	build func(t *testing.T, f *fixture)

	once  sync.Once
	ds    *dataset
	ready bool
}

// get returns the collection name and its dataset, building the collection on
// first use. A build that failed in an earlier test fails every later caller
// with that message instead of a confusing downstream error.
func (f *fixture) get(t *testing.T) (string, *dataset) {
	t.Helper()
	f.once.Do(func() {
		f.build(t, f)
		f.ready = true
	})
	require.True(t, f.ready, "shared collection %s failed to build in an earlier test", f.class)
	return f.class, f.ds
}

// hnswShared is a legacy single-vector HNSW + RQ collection with the default
// flatSearchCutoff: the ef scenarios, the REST mapping, the caps and the
// per-target validation on a collection without named vectors.
var hnswShared = &fixture{
	class: "EfShared",
	build: func(t *testing.T, f *fixture) {
		f.ds = newDataset(42, numObjects, statQueries)
		defineSingleVectorCollection(t, f.class, "hnsw", rqConfig(), nil, f.ds, numObjects, types.ConsistencyLevelQuorum)
	},
}

// filterShared holds the same data as hnswShared with flatSearchCutoff 0, so
// a filtered search walks the graph and the filter strategy is exercised.
var filterShared = &fixture{
	class: "FilterStrategy",
	build: func(t *testing.T, f *fixture) {
		f.ds = newDataset(42, numObjects, statQueries)
		cfg := rqConfig()
		cfg["flatSearchCutoff"] = 0
		defineSingleVectorCollection(t, f.class, "hnsw", cfg, nil, f.ds, numObjects, types.ConsistencyLevelQuorum)
	},
}

// twoVectorShared has the graph (HNSW + RQ) and ivf (HFresh) named vectors of
// twoVectorIndexes, both holding the same vector per object: the per-target,
// search_probe and REST multi-target scenarios. HFresh rebalances postings in
// the background after the inserts, so the build waits for its results to
// settle once, here, instead of in every test.
var twoVectorShared = &fixture{
	class: "TwoVector",
	build: func(t *testing.T, f *fixture) {
		f.ds = newDataset(50, twoVectorObjects, statQueries)
		defineNamedVectorCollection(t, f.class, twoVectorIndexes(), nil, f.ds, twoVectorObjects, types.ConsistencyLevelQuorum)
		c := newGRPC(t, singleNode.GetWeaviate().GrpcURI())
		waitForStableResults(t, c, f.ds, func(q []float32) *pb.SearchRequest {
			return namedVectorRequest(f.class, ivfTarget, q, k, probeP(1))
		})
	},
}
