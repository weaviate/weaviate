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

package hfresh

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestHFreshSearchByVectorDistanceCutoffs(t *testing.T) {
	testinghelpers.RunSearchByVectorDistanceCutoffTests(t, func(t *testing.T, provider distancer.Provider, vectors [][]float32) testinghelpers.DistanceSearcher {
		tf := createHFreshIndex(t, withDistanceProvider(provider))
		t.Cleanup(func() { require.NoError(t, tf.Index.Shutdown(context.Background())) })
		for id, v := range vectors {
			addVectorToIndex(t, &tf, uint64(id), v)
		}
		tf.Index.waitForMaintenance(t)
		return tf.Index
	})
}
