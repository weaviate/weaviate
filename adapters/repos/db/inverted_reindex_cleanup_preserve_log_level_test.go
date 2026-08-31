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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestCleanStaleMigrationDirsAt_PreservedGensLogAtDebug pins that preserving a
// deferred-finalize tracker dir logs at Debug: this runs inside the RAFT
// apply loop, and Info would serialize ~10k lines per property DELETE on a
// 10k-tenant class awaiting restart-finalize.
func TestCleanStaleMigrationDirsAt_PreservedGensLogAtDebug(t *testing.T) {
	lsm := t.TempDir()
	propName := "category"
	indexType := "filterable"

	const preservedGens = 3
	for gen := 1; gen <= preservedGens; gen++ {
		dir := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{propName}) + genSuffix(gen)
		mkTrackerDir(t, lsm, dir)
		mkMigrationRecord(t, lsm, dir, MigrationStateSwapped, map[string]string{
			propName: "property_" + propName + "__enable_filterable_ingest" + genSuffix(gen),
		})
	}

	hookLogger, hook := test.NewNullLogger()
	hookLogger.SetLevel(logrus.DebugLevel)

	cleanStaleMigrationDirsAt(t.Context(), lsm, propName, indexType, hookLogger, nil)

	var infoCount, preservedCount int
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.InfoLevel {
			infoCount++
		}
		if e.Level == logrus.DebugLevel && strings.Contains(e.Message, "preserving a tracker dir") {
			preservedCount++
		}
	}
	require.Equal(t, 0, infoCount,
		"preserving a deferred-finalize tracker dir must not log at Info inside the RAFT apply loop")
	require.Equal(t, preservedGens, preservedCount, "one Debug line per preserved generation")

	// The record-set read this path makes must not report itself either: it runs
	// once per shard inside the same apply, and the apply's own aggregate is the
	// one line it is allowed to emit.
	for _, e := range hook.AllEntries() {
		require.NotContains(t, e.Message, "read migration records",
			"the record-set read is accounted for by the caller's aggregate, not per read")
	}
}

// TestCleanStaleSidecarDirsPreservedLogAtDebug pins the sidecar half of the
// same rule: updatePropertyBuckets reaches this inside the RAFT apply loop, so
// preserving a sidecar dir costs one line per tenant at Info.
func TestCleanStaleSidecarDirsPreservedLogAtDebug(t *testing.T) {
	root := t.TempDir()
	hookLogger, hook := test.NewNullLogger()
	hookLogger.SetLevel(logrus.DebugLevel)

	shard := &Shard{
		name:  "s1",
		index: &Index{logger: hookLogger, Config: IndexConfig{RootPath: root, ClassName: "C"}},
	}
	require.NoError(t, os.MkdirAll(shard.pathLSM(), 0o777))

	const mainBucket = "property_category_searchable"
	const preserved = 3
	dirs := map[string]bool{}
	for gen := 1; gen <= preserved; gen++ {
		name := mainBucket + "__enable_searchable_ingest" + genSuffix(gen)
		require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), name), 0o777))
		dirs[name] = true
	}

	shard.cleanStaleSidecarDirsWithPreserved(mainBucket, migrationPreservingOnly(dirs))

	var infoCount, preservedCount int
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.InfoLevel {
			infoCount++
		}
		if e.Level == logrus.DebugLevel && strings.Contains(e.Message, "preserving the sidecar dir") {
			preservedCount++
		}
	}
	require.Equal(t, 0, infoCount,
		"preserving a committed migration's sidecar dir must not log at Info inside the RAFT apply loop")
	require.Equal(t, preserved, preservedCount, "one Debug line per preserved sidecar dir")
	for name := range dirs {
		require.DirExists(t, filepath.Join(shard.pathLSM(), name), "a preserved sidecar dir must survive")
	}
}

// TestApplyPathReadsAShardsRecordsOncePerProperty pins the read count the
// schema apply pays per shard. The sweep is built once for the whole
// index-type loop, so the number is one; building it inside the loop instead
// would multiply every tenant's cost by the index-type count, and the apply's
// aggregate is the only thing that would say so.
func TestApplyPathReadsAShardsRecordsOncePerProperty(t *testing.T) {
	ctx := testCtx()
	className := "ApplyReadCount" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	// Every index type off, so the loop this read has to outlive runs more than
	// once.
	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	require.Greater(t, len(disabledIndexTypes(prop)), 1,
		"the property must sweep several index types or this pins nothing")

	var counts migrationSweepCounts
	eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
	shard.updatePropertyBuckets(ctx, eg, prop, &counts)
	require.NoError(t, eg.Wait())

	require.Equal(t, int64(1), counts.recordSetReads.Load(),
		"one record-set read for the shard, whatever the index-type count")
}
