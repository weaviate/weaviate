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

	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeriodicTombstoneRemoval(t *testing.T) {
	cl := &noopCommitLogger{}
	makeCL := func() (CommitLogger, error) {
		return cl, nil
	}

	index, err := New(Config{
		RootPath:                 "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                       "automatic-tombstone-removal",
		MakeCommitLoggerThunk:    makeCL,
		MaximumConnections:       30,
		EFConstruction:           60,
		VectorForIDThunk:         testVectorForID,
		TombstoneCleanupInterval: 100 * time.Millisecond,
	})
	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(i, vec)
		require.Nil(t, err)
	}

	t.Run("delete an entry and verify there is a tombstone", func(t *testing.T) {
		for i := range testVectors {
			if i%2 != 0 {
				continue
			}

			err := index.Delete(i)
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
