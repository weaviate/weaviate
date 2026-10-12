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

//go:build integrationTest

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const recountTestClass = "RecountTest"

// The objects hold 2 + 3 words, so the recounted tally of "name" is sum 5, count 2.
var recountTestNames = []string{"a b", "c d e"}

func TestRecountPropertyLengthsAtStartup(t *testing.T) {
	tests := []struct {
		name    string
		recount bool
		lazy    bool
		// makeStale damages the property lengths of the shard while the db is down.
		makeStale func(t *testing.T, trackerPath string)
		// failBeforeLoad makes the recounts before and as the shard loads fail.
		failBeforeLoad bool
		wantSum        int
		wantCount      int
	}{
		{
			name:      "tracker file lost",
			recount:   true,
			makeStale: removeTracker,
			wantSum:   5, wantCount: 2,
		},
		{
			name:      "tracker file lost, lazy shard",
			recount:   true,
			lazy:      true,
			makeStale: removeTracker,
			wantSum:   5, wantCount: 2,
		},
		{
			name:      "tracker counts too much",
			recount:   true,
			makeStale: inflateTracker,
			wantSum:   5, wantCount: 2,
		},
		{
			name:    "leftover recount file of a crashed recount",
			recount: true,
			makeStale: func(t *testing.T, trackerPath string) {
				removeTracker(t, trackerPath)
				inflateTracker(t, trackerPath+".recount")
			},
			wantSum: 5, wantCount: 2,
		},
		{
			name:           "recount before load fails, shard keeps stale lengths",
			recount:        true,
			makeStale:      inflateTracker,
			failBeforeLoad: true,
			wantSum:        5 + 100, wantCount: 2 + 1,
		},
		{
			name:      "flag off keeps stale lengths",
			recount:   false,
			makeStale: inflateTracker,
			wantSum:   5 + 100, wantCount: 2 + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, shardState := t.TempDir(), singleShardState()

			repo, _ := openRecountTestDB(t, dir, shardState, false, tt.lazy)
			for _, name := range recountTestNames {
				require.NoError(t, repo.PutObject(testCtx(), &models.Object{
					Class:      recountTestClass,
					ID:         strfmt.UUID(uuid.NewString()),
					Properties: map[string]any{"name": name},
				}, []float32{1, 2, 3}, nil, nil, nil, 0))
			}
			trackerPath := recountTestTracker(t, repo).FileName()
			require.NoError(t, repo.Shutdown(context.Background()))

			tt.makeStale(t, trackerPath)
			// a non-empty directory cannot be removed as a leftover recount file
			blocker := filepath.Join(trackerPath+".recount", "blocker")
			if tt.failBeforeLoad {
				require.NoError(t, os.MkdirAll(blocker, 0o755))
			}

			repo, _ = openRecountTestDB(t, dir, shardState, tt.recount, tt.lazy)
			defer repo.Shutdown(context.Background())
			report := func() error {
				return NewMigrator(repo, repo.logger, "node1").ReportPropertyLengthsRecount(testCtx())
			}

			if tt.lazy {
				// read before the lazy shard is loaded in the background, a second after startup
				sum, count := diskTally(t, trackerPath)
				assert.Equal(t, []int{tt.wantSum, tt.wantCount}, []int{sum, count},
					"the shard must be recounted before it loads")
			}

			sum, count, _, err := recountTestTracker(t, repo).PropertyTally("name")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSum, sum)
			assert.Equal(t, tt.wantCount, count)
			switch {
			case !tt.recount:
			case tt.failBeforeLoad:
				require.ErrorContains(t, report(), "1 failed")
			default:
				require.NoError(t, report())
				_, err = os.Stat(trackerPath + ".recount")
				assert.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestRecountPropertyLengths(t *testing.T) {
	tests := []struct {
		name string
		// breakRecount makes the next recount of the shard fail.
		breakRecount func(cancel context.CancelFunc, schemaGetter *fakeSchemaGetter)
		wantErr      bool
	}{
		{
			name:         "shard load cancelled",
			breakRecount: func(cancel context.CancelFunc, _ *fakeSchemaGetter) { cancel() },
			wantErr:      true,
		},
		{
			name: "object cannot be analyzed",
			breakRecount: func(_ context.CancelFunc, schemaGetter *fakeSchemaGetter) {
				schemaGetter.schema = schema.Schema{Objects: &models.Schema{}}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, schemaGetter := openRecountTestDB(t, t.TempDir(), singleShardState(), true, false)
			defer repo.Shutdown(context.Background())
			require.NoError(t, repo.PutObject(testCtx(), &models.Object{
				Class:      recountTestClass,
				ID:         strfmt.UUID(uuid.NewString()),
				Properties: map[string]any{"name": "a b"},
			}, []float32{1, 2, 3}, nil, nil, nil, 0))
			shard := recountTestShard(t, repo)
			require.NoError(t, shard.GetPropertyLengthTracker().TrackProperty("name", 100))

			require.NoError(t, shard.recountPropertyLengthsOnLoad(testCtx()))
			requireTally(t, shard, 2+100, 1+1, "the shard was recounted before it loaded, so it must not be again")

			repo.propLengthsRecount.forget(shard.index.ID(), shard.name)
			ctx, cancel := context.WithCancel(testCtx())
			defer cancel()
			tt.breakRecount(cancel, schemaGetter)
			err := shard.recountPropertyLengthsOnLoad(ctx)
			assert.Equal(t, tt.wantErr, err != nil, "err: %v", err)
			requireTally(t, shard, 2+100, 1+1, "a failed recount must keep the previous lengths")
			_, err = os.Stat(shard.GetPropertyLengthTracker().FileName() + ".recount")
			assert.ErrorIs(t, err, os.ErrNotExist)

			schemaGetter.schema = recountTestSchema()
			require.NoError(t, shard.recountPropertyLengthsOnLoad(testCtx()))
			requireTally(t, shard, 2, 1, "a failed recount must be retried on the next load")
		})
	}
}

// The swap closes the shard's tracker before it opens the recounted one, so a
// failing swap leaves the shard without a usable tracker.
func TestRecountPropertyLengths_SwapFails(t *testing.T) {
	repo, _ := openRecountTestDB(t, t.TempDir(), singleShardState(), true, false)
	defer repo.Shutdown(context.Background())
	require.NoError(t, repo.PutObject(testCtx(), &models.Object{
		Class:      recountTestClass,
		ID:         strfmt.UUID(uuid.NewString()),
		Properties: map[string]any{"name": "a b"},
	}, []float32{1, 2, 3}, nil, nil, nil, 0))
	shard := recountTestShard(t, repo)
	trackerPath := shard.GetPropertyLengthTracker().FileName()
	require.NoError(t, os.Remove(trackerPath))
	// a non-empty directory cannot be replaced by a file
	require.NoError(t, os.MkdirAll(filepath.Join(trackerPath, "blocker"), 0o755))
	defer os.RemoveAll(trackerPath)

	repo.propLengthsRecount.forget(shard.index.ID(), shard.name)
	err := shard.recountPropertyLengthsOnLoad(testCtx())
	require.Error(t, err, "the shard has no usable tracker, so its load must fail")
	assert.True(t, repo.propLengthsRecount.pending(shard.index.ID(), shard.name), "the next load must recount")
}

// A shard whose recount failed before it loaded is recounted as it loads.
func TestRecountPropertyLengthsBeforeLoad_FailureRetriedOnLoad(t *testing.T) {
	repo, _ := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
	defer repo.Shutdown(context.Background())
	for _, name := range recountTestNames {
		require.NoError(t, repo.PutObject(testCtx(), &models.Object{
			Class:      recountTestClass,
			ID:         strfmt.UUID(uuid.NewString()),
			Properties: map[string]any{"name": name},
		}, []float32{1, 2, 3}, nil, nil, nil, 0))
	}
	shard := recountTestShard(t, repo)
	idx, name, class := shard.index, shard.name, shard.class
	trackerPath := shard.GetPropertyLengthTracker().FileName()
	require.NoError(t, shard.Shutdown(testCtx()))
	idx.shards.LoadAndDelete(name)
	inflateTracker(t, trackerPath)

	recount := &startupRebuild{enabled: true}
	idx.Config.PropertyLengthsRecount = recount
	// a non-empty directory cannot be removed as a leftover recount file
	require.NoError(t, os.MkdirAll(filepath.Join(trackerPath+".recount", "blocker"), 0o755))
	idx.recountPropertyLengthsOfShards(testCtx(), class, []string{name})
	_, failed, _ := recount.summary()
	require.Equal(t, 1, failed)
	sum, count := diskTally(t, trackerPath)
	require.Equal(t, []int{5 + 100, 2 + 1}, []int{sum, count}, "a failed recount must keep the previous lengths")

	require.NoError(t, os.RemoveAll(trackerPath+".recount"))
	loaded, err := idx.initShard(testCtx(), name, class, nil, true, true)
	require.NoError(t, err)
	defer loaded.Shutdown(testCtx())
	requireTally(t, loaded.(*Shard), 5, 2, "the shard must be recounted as it loads")
	recounted, failed, objects := recount.summary()
	assert.Equal(t, []int{1, 0, 2}, []int{recounted, failed, int(objects)})
}

// A corrupt segment makes the cursor panic, which must not fail the load.
func TestRecountPropertyLengthsOnLoad_CorruptObjectsSegment(t *testing.T) {
	ctx := testCtx()
	shard, idx, class := recalculationTestShard(t, ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)
	require.NoError(t, shard.Shutdown(ctx))
	// loaded once more, so the segment's sidecars are written before it is corrupted
	loaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	require.NoError(t, loaded.Shutdown(ctx))

	segments, err := filepath.Glob(filepath.Join(shard.pathLSM(), helpers.ObjectsBucketLSM, "segment-*.db"))
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	f, err := os.OpenFile(segments[0], os.O_RDWR, 0o600)
	require.NoError(t, err)
	// the value length of the first node, after its tombstone byte
	_, err = f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, int64(segmentindex.HeaderSize+1))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	recount := &startupRebuild{enabled: true}
	idx.Config.PropertyLengthsRecount = recount
	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err, "the shard must load")
	defer reloaded.Shutdown(ctx)
	_, failed, _ := recount.summary()
	assert.Equal(t, 1, failed)
}

func requireTally(t *testing.T, shard *Shard, wantSum, wantCount int, msg string) {
	t.Helper()
	sum, count, _, err := shard.GetPropertyLengthTracker().PropertyTally("name")
	require.NoError(t, err)
	require.Equal(t, []int{wantSum, wantCount}, []int{sum, count}, msg)
}

func removeTracker(t *testing.T, trackerPath string) {
	require.NoError(t, os.Remove(trackerPath))
}

func inflateTracker(t *testing.T, trackerPath string) {
	logger, _ := test.NewNullLogger()
	tracker, err := inverted.NewJsonShardMetaData(trackerPath, logger)
	require.NoError(t, err)
	require.NoError(t, tracker.TrackProperty("name", 100))
	require.NoError(t, tracker.Close())
}

func recountTestTracker(t *testing.T, repo *DB) *inverted.JsonShardMetaData {
	t.Helper()
	var tracker *inverted.JsonShardMetaData
	require.NoError(t, repo.GetIndex(recountTestClass).ForEachShard(func(_ string, shard ShardLike) error {
		tracker = shard.GetPropertyLengthTracker()
		return nil
	}))
	require.NotNil(t, tracker)
	require.Equal(t, filepath.Base(tracker.FileName()), propLengthsFile)
	return tracker
}

// diskTally reads the tally of "name" from the tracker file of a shard that is not loaded.
func diskTally(t *testing.T, trackerPath string) (sum, count int) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	tracker, err := inverted.NewJsonShardMetaData(trackerPath, logger)
	require.NoError(t, err)
	defer tracker.Close()
	sum, count, _, err = tracker.PropertyTally("name")
	require.NoError(t, err)
	return sum, count
}

func recountTestShard(t *testing.T, repo *DB) *Shard {
	t.Helper()
	var shard *Shard
	require.NoError(t, repo.GetIndex(recountTestClass).ForEachShard(func(_ string, s ShardLike) error {
		shard = s.(*Shard)
		return nil
	}))
	require.NotNil(t, shard)
	return shard
}

func recountTestSchema() schema.Schema {
	return schema.Schema{Objects: &models.Schema{Classes: []*models.Class{{
		Class:               recountTestClass,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:            "name",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWord,
			IndexSearchable: boolPtr(true),
		}},
	}}}}
}

// openRecountTestDB starts a db at dir that holds the recount test class. Reopen
// it with the same shardState, as that names the shard.
func openRecountTestDB(t *testing.T, dir string, shardState *sharding.State, recount, lazy bool) (*DB, *fakeSchemaGetter) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	sch := recountTestSchema()
	schemaGetter := &fakeSchemaGetter{schema: sch, shardState: shardState}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(sch.GetClass(className), shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(*sch.Objects).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                   dir,
		QueryMaximumResults:        1000,
		MaxImportGoroutinesFactor:  1,
		EnableLazyLoadShards:       boolPtr(lazy),
		RecountPropertiesAtStartup: recount,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	return repo, schemaGetter
}
