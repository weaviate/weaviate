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

// The property-update apply runs this sweep on every shard of the collection,
// inside the RAFT apply loop, whether or not any migration exists. The shard is
// loaded, so its record store already holds what the sweep asks for; going to
// disk for it puts a directory listing and a read per record file on the loop.
//
// Emptying the records directory behind the loaded store is what makes the two
// sources answer differently: the map still holds the record that says this
// tracker directory must be preserved, and the disk no longer does.
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

	// Put writes both the file and the map, so this is the only way to tell
	// which one the sweep read.
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
