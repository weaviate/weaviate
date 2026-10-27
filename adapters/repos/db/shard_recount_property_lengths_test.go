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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
			name:      "tracker file lost, lazy shard",
			recount:   true,
			lazy:      true,
			makeStale: removeTracker,
			wantSum:   5, wantCount: 2,
		},
		{
			name:    "tracker file unreadable",
			recount: true,
			makeStale: func(t *testing.T, trackerPath string) {
				require.NoError(t, os.WriteFile(trackerPath, nil, 0o644))
			},
			wantSum: 5, wantCount: 2,
		},
		{
			name:    "leftover recount file of a crashed recount",
			recount: true,
			makeStale: func(t *testing.T, trackerPath string) {
				removeTracker(t, trackerPath)
				inflateTracker(t, recountFileOf(trackerPath))
			},
			wantSum: 5, wantCount: 2,
		},
		{
			name:           "recount fails before and as the shard loads, shard keeps stale lengths",
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
			putRecountTestObjects(t, repo)
			trackerPath := recountTestTracker(t, repo).FileName()
			require.NoError(t, repo.Shutdown(context.Background()))

			tt.makeStale(t, trackerPath)
			// a non-empty directory cannot be removed as a leftover recount file
			blocker := filepath.Join(recountFileOf(trackerPath), "blocker")
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
				_, err = os.Stat(recountFileOf(trackerPath))
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
			requireTally(t, shard, 2+100, 1+1, "the shard was recorded done when it loaded empty, so it must not be recounted")

			repo.propLengthsRecount.forget(shard.index.ID(), shard.name)
			ctx, cancel := context.WithCancel(testCtx())
			defer cancel()
			tt.breakRecount(cancel, schemaGetter)
			err := shard.recountPropertyLengthsOnLoad(ctx)
			assert.Equal(t, tt.wantErr, err != nil, "err: %v", err)
			requireTally(t, shard, 2+100, 1+1, "a failed recount must keep the previous lengths")
			_, err = os.Stat(recountFileOf(shard.GetPropertyLengthTracker().FileName()))
			assert.ErrorIs(t, err, os.ErrNotExist)

			schemaGetter.schema = recountTestSchema()
			require.NoError(t, shard.recountPropertyLengthsOnLoad(testCtx()))
			requireTally(t, shard, 2, 1, "a failed recount must be retried on the next load")
		})
	}
}

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

func TestRecountPropertyLengths_SwapFailsToReplaceTrackerFile(t *testing.T) {
	repo, _ := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
	defer repo.Shutdown(context.Background())
	require.NoError(t, repo.PutObject(testCtx(), &models.Object{
		Class:      recountTestClass,
		ID:         strfmt.UUID(uuid.NewString()),
		Properties: map[string]any{"name": "a b"},
	}, []float32{1, 2, 3}, nil, nil, nil, 0))
	shard := recountTestShard(t, repo)

	r := newPropertyLengthsRecounter(shard.path(), shard.index.logger)
	_, err := r.count(testCtx(), shard)
	require.NoError(t, err)
	require.NoError(t, os.Remove(r.recountPath))
	require.Error(t, r.swapIn(shard))
}

// The recount must be done before the shard serves, so a load cancelled during it fails.
func TestRecountPropertyLengthsOnLoad_CancelledFailsLoad(t *testing.T) {
	repo, schemaGetter := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
	defer repo.Shutdown(context.Background())
	putRecountTestObjects(t, repo)
	shard := recountTestShard(t, repo)
	idx, name, class := shard.index, shard.name, shard.class
	recountPath := filepath.Join(shard.path(), propLengthsRecountFile)
	require.NoError(t, shard.Shutdown(testCtx()))
	idx.shards.LoadAndDelete(name)

	ctx, cancel := context.WithCancel(testCtx())
	defer cancel()
	idx.getSchema = &recountHookSchemaGetter{SchemaGetter: schemaGetter, onReadOnlyClass: func() {
		if _, err := os.Stat(recountPath); err == nil {
			cancel()
		}
	}}
	idx.Config.PropertyLengthsRecount = &startupRebuild{enabled: true}
	loaded, err := idx.initShard(ctx, name, class, nil, true, true)
	if loaded != nil {
		defer loaded.Shutdown(testCtx())
	}
	require.ErrorContains(t, err, "recount property lengths")
	require.ErrorIs(t, err, context.Canceled)
}

func TestRecountPropertyLengthsBeforeLoad_FailureRetriedOnLoad(t *testing.T) {
	repo, _ := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
	defer repo.Shutdown(context.Background())
	putRecountTestObjects(t, repo)
	shard := recountTestShard(t, repo)
	idx, name, class := shard.index, shard.name, shard.class
	trackerPath := shard.GetPropertyLengthTracker().FileName()
	require.NoError(t, shard.Shutdown(testCtx()))
	idx.shards.LoadAndDelete(name)
	inflateTracker(t, trackerPath)

	recount := &startupRebuild{enabled: true}
	idx.Config.PropertyLengthsRecount = recount
	// a non-empty directory cannot be removed as a leftover recount file
	require.NoError(t, os.MkdirAll(filepath.Join(recountFileOf(trackerPath), "blocker"), 0o755))
	idx.recountPropertyLengthsOfShards(testCtx(), class, []string{name})
	_, failed, _ := recount.summary()
	require.Equal(t, 1, failed)
	sum, count := diskTally(t, trackerPath)
	require.Equal(t, []int{5 + 100, 2 + 1}, []int{sum, count}, "a failed recount must keep the previous lengths")

	require.NoError(t, os.RemoveAll(recountFileOf(trackerPath)))
	loaded, err := idx.initShard(testCtx(), name, class, nil, true, true)
	require.NoError(t, err)
	defer loaded.Shutdown(testCtx())
	requireTally(t, loaded.(*Shard), 5, 2, "the shard must be recounted as it loads")
	recounted, failed, objects := recount.summary()
	assert.Equal(t, []int{1, 0, 2}, []int{recounted, failed, int(objects)})
}

// An empty tracker file cannot be read. A shard with objects gets a new one from the
// recount, and one without must get a new one too.
func TestRecountPropertyLengthsBeforeLoad_UnreadableTrackerOfEmptyShard(t *testing.T) {
	repo, _ := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
	defer repo.Shutdown(context.Background())
	shard := recountTestShard(t, repo)
	idx, name, class := shard.index, shard.name, shard.class
	trackerPath := shard.GetPropertyLengthTracker().FileName()
	require.NoError(t, shard.Shutdown(testCtx()))
	idx.shards.LoadAndDelete(name)
	require.NoError(t, os.WriteFile(trackerPath, nil, 0o644))

	idx.Config.PropertyLengthsRecount = &startupRebuild{enabled: true}
	idx.recountPropertyLengthsOfShards(testCtx(), class, []string{name})
	sum, count := diskTally(t, trackerPath)
	require.Equal(t, []int{0, 0}, []int{sum, count})

	loaded, err := idx.initShard(testCtx(), name, class, nil, true, true)
	require.NoError(t, err)
	require.NoError(t, loaded.Shutdown(testCtx()))
}

// Async replication can delete objects as soon as it starts, so the recount must
// be done by then. Starting it sweeps the shard's hashtree directory.
func TestRecountPropertyLengthsOnLoad_BeforeAsyncReplication(t *testing.T) {
	repo, schemaGetter := openRecountTestDB(t, t.TempDir(), singleShardState(), false, false)
	defer repo.Shutdown(context.Background())
	putRecountTestObjects(t, repo)
	shard := recountTestShard(t, repo)
	idx, name, class := shard.index, shard.name, shard.class
	recountPath := filepath.Join(shard.path(), propLengthsRecountFile)
	stale := filepath.Join(shard.pathHashTree(), "stale.tmp")
	require.NoError(t, shard.Shutdown(testCtx()))
	idx.shards.LoadAndDelete(name)
	require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o755))
	require.NoError(t, os.WriteFile(stale, nil, 0o644))

	var counted, sweptWhileCounting int
	idx.getSchema = &recountHookSchemaGetter{SchemaGetter: schemaGetter, onReadOnlyClass: func() {
		if _, err := os.Stat(recountPath); err != nil {
			return
		}
		counted++
		if _, err := os.Stat(stale); err != nil {
			sweptWhileCounting++
		}
	}}
	idx.Config.PropertyLengthsRecount = &startupRebuild{enabled: true}
	loaded, err := idx.initShard(testCtx(), name, class, nil, true, true)
	require.NoError(t, err)
	defer loaded.Shutdown(testCtx())
	require.Equal(t, len(recountTestNames), counted)
	assert.Zero(t, sweptWhileCounting, "async replication started during the recount")
	assert.NoFileExists(t, stale)
}

type recountHookSchemaGetter struct {
	schemaUC.SchemaGetter
	onReadOnlyClass func()
}

func (g *recountHookSchemaGetter) ReadOnlyClass(name string) *models.Class {
	g.onReadOnlyClass()
	return g.SchemaGetter.ReadOnlyClass(name)
}

func putRecountTestObjects(t *testing.T, repo *DB) {
	t.Helper()
	for _, name := range recountTestNames {
		require.NoError(t, repo.PutObject(testCtx(), &models.Object{
			Class:      recountTestClass,
			ID:         strfmt.UUID(uuid.NewString()),
			Properties: map[string]any{"name": name},
		}, []float32{1, 2, 3}, nil, nil, nil, 0))
	}
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

func recountFileOf(trackerPath string) string {
	return filepath.Join(filepath.Dir(trackerPath), propLengthsRecountFile)
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
	repo := openTestDatabase(t, dir, shardState, monitoring.GetMetrics(), func(config *Config) {
		config.EnableLazyLoadShards = boolPtr(lazy)
		config.RecountPropertiesAtStartup = recount
	}, recountTestSchema().Objects.Classes...)
	return repo, repo.schemaGetter.(*fakeSchemaGetter)
}
