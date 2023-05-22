//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCycles_PauseMaintenance(t *testing.T) {
	ctx := context.Background()

	t.Run("assert that context timeout works for long maintenance cycle", func(t *testing.T) {
		cycles := &MaintenanceCycles{}
		cycles.Init(
			cyclemanager.HnswCommitLoggerCycleTicker(),
			cyclemanager.NewFixedIntervalTicker(time.Second))

		expiredCtx, cancel := context.WithDeadline(ctx, time.Now())
		defer cancel()

		err := cycles.PauseMaintenance(expiredCtx)
		require.NotNil(t, err)
		assert.EqualError(t, err,
			"long-running commitlog maintenance in progress: context deadline exceeded, "+
				"long-running tombstone cleanup in progress: context deadline exceeded")

		assert.True(t, cycles.CommitLogMaintenance().Running())
		assert.True(t, cycles.TombstoneCleanup().Running())

		err = cycles.Shutdown(ctx)
		require.Nil(t, err)
	})

	t.Run("assert maintenance is successfully paused", func(t *testing.T) {
		cycles := &MaintenanceCycles{}
		cycles.Init(
			cyclemanager.HnswCommitLoggerCycleTicker(),
			cyclemanager.NewFixedIntervalTicker(time.Second))

		expireableCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		err := cycles.PauseMaintenance(expireableCtx)
		require.Nil(t, err)

		assert.False(t, cycles.CommitLogMaintenance().Running())
		assert.False(t, cycles.TombstoneCleanup().Running())

		err = cycles.Shutdown(ctx)
		require.Nil(t, err)
	})
}

func TestBackup_ResumeMaintenance(t *testing.T) {
	ctx := context.Background()

	t.Run("assert cleanup restarts after pausing", func(t *testing.T) {
		cycles := &MaintenanceCycles{}
		cycles.Init(
			cyclemanager.HnswCommitLoggerCycleTicker(),
			cyclemanager.NewFixedIntervalTicker(time.Second))

		expireableCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		err := cycles.PauseMaintenance(expireableCtx)
		require.Nil(t, err)

		err = cycles.ResumeMaintenance(expireableCtx)
		require.Nil(t, err)

		assert.True(t, cycles.CommitLogMaintenance().Running())
		assert.True(t, cycles.TombstoneCleanup().Running())

		err = cycles.Shutdown(ctx)
		require.Nil(t, err)
	})
}
