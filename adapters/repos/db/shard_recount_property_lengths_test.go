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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
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
		wantSum   int
		wantCount int
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

			repo, _ = openRecountTestDB(t, dir, shardState, tt.recount, tt.lazy)
			defer repo.Shutdown(context.Background())

			sum, count, _, err := recountTestTracker(t, repo).PropertyTally("name")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSum, sum)
			assert.Equal(t, tt.wantCount, count)
			_, err = os.Stat(trackerPath + ".recount")
			assert.ErrorIs(t, err, os.ErrNotExist)
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

			require.NoError(t, shard.recountPropertyLengths(testCtx()))
			requireTally(t, shard, 2+100, 1+1, "the shard was recounted when it loaded, so it must not be again")

			repo.propLengthsRecounted.Delete(shard.ID())
			ctx, cancel := context.WithCancel(testCtx())
			defer cancel()
			tt.breakRecount(cancel, schemaGetter)
			err := shard.recountPropertyLengths(ctx)
			assert.Equal(t, tt.wantErr, err != nil, "err: %v", err)
			requireTally(t, shard, 2+100, 1+1, "a failed recount must keep the previous lengths")
			_, err = os.Stat(shard.GetPropertyLengthTracker().FileName() + ".recount")
			assert.ErrorIs(t, err, os.ErrNotExist)

			schemaGetter.schema = recountTestSchema()
			require.NoError(t, shard.recountPropertyLengths(testCtx()))
			requireTally(t, shard, 2, 1, "a failed recount must be retried on the next load")
		})
	}
}

// The swap closes the shard's tracker before it opens the recounted one, so a
// failing swap leaves the shard without a usable tracker.
func TestRecountPropertyLengths_SwapFails(t *testing.T) {
	repo, _ := openRecountTestDB(t, t.TempDir(), singleShardState(), true, false)
	defer repo.Shutdown(context.Background())
	shard := recountTestShard(t, repo)
	trackerPath := shard.GetPropertyLengthTracker().FileName()
	require.NoError(t, os.Remove(trackerPath))
	// a non-empty directory cannot be replaced by a file
	require.NoError(t, os.MkdirAll(filepath.Join(trackerPath, "blocker"), 0o755))
	defer os.RemoveAll(trackerPath)

	repo.propLengthsRecounted.Delete(shard.ID())
	err := shard.recountPropertyLengths(testCtx())
	require.Error(t, err, "the shard has no usable tracker, so its load must fail")
	_, recounted := repo.propLengthsRecounted.Load(shard.ID())
	assert.False(t, recounted, "the next load must recount")
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
