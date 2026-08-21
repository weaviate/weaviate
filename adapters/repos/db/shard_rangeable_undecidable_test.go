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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// TestRangeableReadinessOnUnreadableRecords pins the query-side answer for a
// shard whose migration records did not all decode. The readiness default is
// "ready if the rangeable bucket exists", and a filterable-to-rangeable
// migration pre-creates that bucket empty, so the default reads an in-flight
// migration as finished. The pessimistic entries that correct it come from the
// records — and a record that does not decode contributes none, silently.
// Serving range filters from the empty bucket returns zero counts once another
// replica flips the cluster-wide flag.
func TestRangeableReadinessOnUnreadableRecords(t *testing.T) {
	const propName = "price"

	tests := []struct {
		name       string
		unreadable bool
		wantReady  bool
	}{
		{
			name:      "records that read cleanly leave the bucket-existence default alone",
			wantReady: true,
		},
		{
			name:       "a record that does not decode is not evidence that no migration is in flight",
			unreadable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shd, _ := testShard(t, ctx, "RangeableUndecidable")
			shard := shd.(*Shard)

			// The state the migration's PreReindexHook leaves behind: the
			// bucket exists and is empty, which is what makes the default
			// answer "ready".
			require.NoError(t, shard.store.CreateOrLoadBucket(ctx,
				helpers.BucketRangeableFromPropNameLSM(propName),
				shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)...))

			if tt.unreadable {
				require.NoError(t, os.MkdirAll(shard.migrationRecords.Dir(), 0o755))
				require.NoError(t, os.WriteFile(
					filepath.Join(shard.migrationRecords.Dir(), "99_filterable_to_rangeable.json"),
					[]byte("{"), 0o600))
				require.NoError(t, shard.migrationRecords.Load())
			}
			markInFlightRangeableMigrationsNotReady(shard)

			assert.Equal(t, tt.wantReady, shard.IsRangeableLocallyReady(propName))
		})
	}
}
