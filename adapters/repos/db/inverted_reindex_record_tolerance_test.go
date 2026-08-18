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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A retained record is a tracker shape other .migrations/ readers never saw
// before finalize started keeping it. Each already answers correctly because
// a record still carries tidied.mig — a property of today's code, not a
// guarantee, so it gets pinned rather than assumed.
func TestReadersToleranceOfARetainedRecord(t *testing.T) {
	const propName = "category"
	recordName := "enable_filterable_" + propName + "_1"

	plantRecord := func(t *testing.T) string {
		t.Helper()
		lsmPath := t.TempDir()
		mkTrackerDir(t, lsmPath, recordName, recordedSentinels...)
		mkRecoveryPayload(t, lsmPath, recordName, propName)
		require.NoError(t, os.WriteFile(
			filepath.Join(lsmPath, ".migrations", recordName, "properties.mig"),
			[]byte(propName), 0o644))
		return lsmPath
	}

	t.Run("restart recovery does not re-register a completed migration", func(t *testing.T) {
		lsmPath := plantRecord(t)
		logger, _ := test.NewNullLogger()
		_, ok := loadReindexRecoveryRecord(filepath.Join(lsmPath, ".migrations", recordName), logger)
		assert.False(t, ok,
			"re-registering double-write callbacks for a migration that already swapped "+
				"would write every object twice")
	})

	t.Run("the orphan audit does not classify a record as an orphan", func(t *testing.T) {
		lsmPath := plantRecord(t)
		logger, _ := test.NewNullLogger()
		knownNothing := func(string, uint64) bool { return false }
		assert.Empty(t,
			collectOrphanTrackers(lsmPath, "SomeClass", "shard1", knownNothing, logger),
			"the audit destroys what it classifies; a completed migration is not abandoned state")
	})

	t.Run("the preserve pass still owns a record's sidecar names", func(t *testing.T) {
		lsmPath := plantRecord(t)
		scope := migrationDirsOf(lsmPath, nil, propName, "filterable").preserving("filterable")
		require.NotEmpty(t, completedMigrationSidecarSuffixes(scope),
			"a record a sweep no longer recognises is one whose sidecars it would delete")
	})
}

// The readiness scan must refuse queries only on an in-flight migration's
// unfilled bucket. A record is the opposite state — bucket filled and open —
// so refusing it would hide data the migration already produced.
func TestRangeableReadinessRefusesOnlyAnInFlightMigration(t *testing.T) {
	const propName = filterableToRangeablePropName
	tracker := "filterable_to_rangeable_" + propName + "_1"

	for _, tc := range []struct {
		name      string
		sentinels []string
		wantReady bool
	}{
		{
			name:      "a recorded promotion",
			sentinels: recordedSentinels,
			wantReady: true,
		},
		{
			name:      "a migration that has not swapped yet",
			sentinels: []string{"started.mig"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RangeableRecordReady_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			mkTrackerDir(t, shard.pathLSM(), tracker, tc.sentinels...)
			mkRecoveryPayload(t, shard.pathLSM(), tracker, propName)
			// What shard init does for a recorded promotion, and what the
			// migration's own hook does for one in flight: the bucket is open
			// either way, so only the scan tells the two apart.
			require.NoError(t, shard.store.CreateOrLoadBucket(ctx,
				helpers.BucketRangeableFromPropNameLSM(propName),
				shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)...))

			markInFlightRangeableMigrationsNotReady(shard)

			assert.Equal(t, tc.wantReady, shard.IsRangeableLocallyReady(propName))
		})
	}
}
