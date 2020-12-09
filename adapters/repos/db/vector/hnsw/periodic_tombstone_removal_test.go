//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeriodicTombstoneRemoval(t *testing.T) {
	index, err := New(Config{
		RootPath:                 "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                       "automatic-tombstone-removal",
		MakeCommitLoggerThunk:    MakeNoopCommitLogger,
		MaximumConnections:       30,
		EFConstruction:           60,
		DistanceProvider:         distancer.NewCosineProvider(),
		VectorForIDThunk:         testVectorForID,
		TombstoneCleanupInterval: 100 * time.Millisecond,
	})
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(int64(i), vec)
		require.Nil(t, err)
	}

	t.Run("delete an entry and verify there is a tombstone", func(t *testing.T) {
		for i := range testVectors {
			if i%2 != 0 {
				continue
			}

			err := index.Delete(int64(i))
			require.Nil(t, err)
		}
	})

	t.Run("verify there are now tombstones", func(t *testing.T) {
		index.RLock()
		ts := len(index.tombstones)
		index.RUnlock()
		assert.True(t, ts > 0)
	})

	t.Run("wait for tombstones to disappear", func(t *testing.T) {
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			index.RLock()
			ts := len(index.tombstones)
			index.RUnlock()
			return ts == 0
		}, "wait until tombstones have been cleaned up")
	})
}
