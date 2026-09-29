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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
)

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
