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
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The apply runs this sweep on every shard inside the RAFT apply loop, so going
// to disk would put a listing and a read per record file on it. Emptying the
// records directory behind the loaded store is what makes the two sources
// disagree: the map still holds the preserving record, the disk no longer does.
func TestTheApplyPathSweepsFromTheLoadedRecordStore(t *testing.T) {
	const (
		propName    = "title"
		keptTracker = "searchable_retokenize_title_1"
		staleTacker = "searchable_retokenize_title_2"
	)

	ctx := testCtx()
	className := "SweepSource" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	for _, tracker := range []string{keptTracker, staleTacker} {
		require.NoError(t, os.MkdirAll(
			filepath.Join(shard.pathLSM(), migrationsDir, tracker), 0o777))
	}

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
	subject.TrackerDir = keptTracker
	require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))

	// Put writes both file and map, so this is the only way to tell them apart.
	require.NoError(t, os.RemoveAll(shard.migrationRecords.Dir()))

	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off

	var counts migrationSweepCounts
	eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
	shard.updatePropertyBuckets(ctx, eg, prop, &counts)
	require.NoError(t, eg.Wait())

	require.DirExists(t, filepath.Join(shard.pathLSM(), migrationsDir, keptTracker),
		"the loaded store still names this tracker, so the sweep must preserve it")
	require.NoDirExists(t, filepath.Join(shard.pathLSM(), migrationsDir, staleTacker),
		"fixture: the sweep really ran, or preserving proves nothing")
}

// A cancelled apply must not pay for the sweep it is not going to use.
func TestACancelledApplyBuildsNoSweepState(t *testing.T) {
	ctx := testCtx()
	className := "SweepCancel" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	prop := class.Properties[0]
	off := false
	prop.IndexFilterable = &off
	prop.IndexSearchable = &off
	prop.IndexRangeFilters = &off

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	var counts migrationSweepCounts
	eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
	shard.updatePropertyBuckets(cancelled, eg, prop, &counts)
	require.Error(t, eg.Wait())

	require.Equal(t, int64(0), counts.recordSetReads.Load())
	require.Equal(t, int64(0), counts.payloadReads.Load())
}
