//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

func TestPeriodicTombstoneRemoval(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cleanupIntervalSeconds := 1
	tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstone", logger, 1)
	tombstoneCleanupCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(time.Duration(cleanupIntervalSeconds)*time.Second),
		tombstoneCallbacks.CycleCallback)
	tombstoneCleanupCycle.Start()

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "automatic-tombstone-removal",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
	}, ent.UserConfig{
		CleanupIntervalSeconds: cleanupIntervalSeconds,
		MaxConnections:         30,
		EFConstruction:         128,
	}, tombstoneCallbacks, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	index.PostStartup()

	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("delete an entry and verify there is a tombstone", func(t *testing.T) {
		for i := range testVectors {
			if i%2 != 0 {
				continue
			}

			err := index.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("verify there are now tombstones", func(t *testing.T) {
		index.tombstoneLock.RLock()
		ts := len(index.tombstones)
		index.tombstoneLock.RUnlock()
		assert.True(t, ts > 0)
	})

	t.Run("wait for tombstones to disappear", func(t *testing.T) {
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			index.tombstoneLock.RLock()
			ts := len(index.tombstones)
			index.tombstoneLock.RUnlock()
			return ts == 0
		}, "wait until tombstones have been cleaned up")
	})

	if err := index.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tombstoneCleanupCycle.StopAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
