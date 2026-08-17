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
	"strings"
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
		mkTrackerDir(t, lsmPath, recordName,
			append(append([]string{}, completedSentinels...), finalizedSentinel)...)
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

	t.Run("the swap-completion probe reads a record like an absent dir", func(t *testing.T) {
		lsmPath := plantRecord(t)
		scope := migrationDirsOf(lsmPath, nil, propName, "filterable")
		assert.False(t, hasUntidiedTracker(scope),
			"an untidied tracker keeps the local callbacks registered; a record is tidied")
	})

	t.Run("a record's sidecar suffixes name nothing on disk", func(t *testing.T) {
		lsmPath := plantRecord(t)
		scope := migrationDirsOf(lsmPath, nil, propName, "filterable").preserving("filterable")
		suffixes := completedMigrationSidecarSuffixes(scope)
		require.NotEmpty(t, suffixes, "the record is still reported as a completed migration")
		for suffix := range suffixes {
			assert.NoDirExists(t, filepath.Join(lsmPath, helpers.BucketFromPropNameLSM(propName)+suffix),
				"a promotion leaves no sidecars, so preserving them is a no-op")
		}
	})

	// A record's generation is left readable like any other: a rehydrated
	// re-run converges through the already-tidied swap branch, and a fresh
	// task never reuses its number.
	t.Run("generation allocation counts a record like any other generation", func(t *testing.T) {
		lsmPath := plantRecord(t)
		assert.Equal(t, 1, maxMigrationGeneration(lsmPath, MigrationDirPrefixEnableFilterable, "_"+propName))
		assert.Equal(t, 2, nextMigrationGeneration(lsmPath, MigrationDirPrefixEnableFilterable, "_"+propName))
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
			sentinels: append(append([]string{}, completedSentinels...), finalizedSentinel),
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

// A cold tenant's backup reads its files from disk rather than from the
// store's loaded buckets, so a promoted index and its record are carried
// whether or not anything opened them.
func TestInactiveShardBackupCarriesAPromotedIndexAndItsRecord(t *testing.T) {
	const propName = "category"
	root := t.TempDir()
	lsmDir := filepath.Join(root, "lsm")
	bucket := helpers.BucketFromPropNameLSM(propName)
	require.NoError(t, os.MkdirAll(filepath.Join(lsmDir, bucket), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmDir, bucket, "segment-0.db"), []byte("x"), 0o644))
	record := "enable_filterable_" + propName + "_1"
	mkTrackerDir(t, lsmDir, record, finalizedSentinel)

	files, err := listInactiveLSMFiles(lsmDir, root)
	require.NoError(t, err)

	assert.Contains(t, files, filepath.Join("lsm", bucket, "segment-0.db"))
	assert.Contains(t, files, filepath.Join("lsm", ".migrations", record, finalizedSentinel))
	for _, f := range files {
		require.False(t, strings.HasSuffix(f, ".tmp"))
	}
}
