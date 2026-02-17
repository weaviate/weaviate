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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldShardInitHalted(t *testing.T) {
	t.Run("returns true when shard is explicitly in halted map", func(t *testing.T) {
		idx := &Index{
			haltedShardsForTransfer: map[string]struct{}{
				"shard1": {},
			},
		}

		assert.True(t, idx.shouldShardInitHalted("shard1"))
	})

	t.Run("returns false when shard is not in halted map", func(t *testing.T) {
		idx := &Index{
			haltedShardsForTransfer: map[string]struct{}{
				"shard1": {},
			},
		}

		assert.False(t, idx.shouldShardInitHalted("shard2"))
	})

	t.Run("returns false when halted map is nil", func(t *testing.T) {
		idx := &Index{}

		assert.False(t, idx.shouldShardInitHalted("shard1"))
	})

	t.Run("returns false when backup is in progress but shard not in halted map", func(t *testing.T) {
		idx := &Index{}
		idx.lastBackup.Store(&BackupState{
			BackupID:   "backup-1",
			InProgress: true,
		})

		assert.False(t, idx.shouldShardInitHalted("shard1"))
	})

	t.Run("does not add shard to halted map when backup is in progress", func(t *testing.T) {
		idx := &Index{}
		idx.lastBackup.Store(&BackupState{
			BackupID:   "backup-1",
			InProgress: true,
		})

		idx.shouldShardInitHalted("shard1")

		idx.haltedShardsForTransferLock.Lock()
		_, inMap := idx.haltedShardsForTransfer["shard1"]
		idx.haltedShardsForTransferLock.Unlock()
		assert.False(t, inMap)
	})
}
