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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/cluster/usage/types"
)

// TestUnloadedShardUsageSavedToDisk pins which saved usage a cold shard serves.
// Nothing deletes the file of a shard that stays cold, so a UsageDiskVersion bump
// only reaches those shards if a version it does not know makes them recompute.
func TestUnloadedShardUsageSavedToDisk(t *testing.T) {
	// distinguishable from the 20 objects the shard actually holds
	const savedObjectsCount = int64(7)

	tests := []struct {
		name             string
		savedVersion     int
		wantObjectsCount int64
	}{
		{
			name:             "usage of the current version is served",
			savedVersion:     types.UsageDiskVersion,
			wantObjectsCount: savedObjectsCount,
		},
		{
			name:             "usage of an older version is recomputed",
			savedVersion:     types.UsageDiskVersion - 1,
			wantObjectsCount: 20,
		},
		{
			name:             "usage of a newer version is recomputed",
			savedVersion:     types.UsageDiskVersion + 1,
			wantObjectsCount: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tenantName := "test-tenant"

			index := setupPopulatedLazyIndex(ctx, t)
			t.Cleanup(func() { _ = index.Shutdown(ctx) })

			writeSavedShardUsage(t, index.path(), tenantName, tt.savedVersion,
				&types.ShardUsage{Name: tenantName, ObjectsCount: savedObjectsCount})

			usage, err := index.usageForCollection(ctx, semaphore.NewWeighted(1), true, nil)
			require.NoError(t, err)
			require.Len(t, usage.Shards, 1)
			assert.Equal(t, tt.wantObjectsCount, usage.Shards[0].ObjectsCount)
		})
	}
}

// writeSavedShardUsage writes the file [shardusage.SaveComputedUsageData] writes,
// at an arbitrary version. It repeats that file's name because the package keeps
// it private and stamps the current version on everything it saves.
func writeSavedShardUsage(t *testing.T, indexPath, shardName string, version int, usage *types.ShardUsage) {
	t.Helper()

	data, err := json.Marshal(&types.UsageDisk{Version: version, ShardUsage: usage})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(indexPath, shardName, "usage.json.tmp"), data, 0o600))
}
