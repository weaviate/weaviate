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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	tokChangeProp  = "title"
	tokChangeValue = "Hello World"
)

// tokenizationChange: three "Hello World" objects under field tokenization, with both halves of a
// change to word tokenization prepared, so only the swap is left.
type tokenizationChange struct {
	ctx      context.Context
	class    *models.Class
	idx      *Index
	shard    *Shard
	provider *ReindexProvider
	payload  *ReindexTaskPayload
	desc     distributedtask.TaskDescriptor
	unit     string
	tasks    []*ShardReindexTaskGeneric
}

func newTokenizationChange(t *testing.T) *tokenizationChange {
	t.Helper()
	ctx := testCtx()
	className := "TokChange" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{tokChangeProp})
	class.Properties[0].Tokenization = models.PropertyTokenizationField

	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
	shard, err := unwrapShard(ctx, hot)
	require.NoError(t, err)
	registerIndex(idx, className)

	logger, _ := logrustest.NewNullLogger()
	f := &tokenizationChange{
		ctx: ctx, class: class, idx: idx, shard: shard,
		provider: NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx),
		desc:     distributedtask.TaskDescriptor{ID: "T_" + className, Version: 1},
		unit:     shard.migrationUnit(),
	}
	t.Cleanup(func() { f.shard.Shutdown(context.Background()) })
	f.payload = &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeChangeTokenization,
		Properties: []string{tokChangeProp}, TargetTokenization: models.PropertyTokenizationWord,
		OriginalTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
		UnitToShard: map[string]string{f.unit: shard.Name()},
		UnitToNode:  map[string]string{f.unit: "node1"},
	}

	for range 3 {
		require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, tokChangeValue)))
	}
	require.Equal(t, allFound, f.counts(t), "the fixture must find every object before the change")

	f.tasks = f.freshTasks(t)
	require.Len(t, f.tasks, 2, "a property with both indexes changes tokenization in two halves")
	for _, task := range f.tasks {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	}
	return f
}

func (f *tokenizationChange) freshTasks(t *testing.T) []*ShardReindexTaskGeneric {
	t.Helper()
	tasks, err := f.provider.createReindexTasks(f.desc, f.unit, f.payload)
	require.NoError(t, err)
	return tasks
}

// swapWithOverlay swaps tasks as one unit with the overlay the swap phase sets.
func swapWithOverlay(ctx context.Context, shard ShardLike, payload *ReindexTaskPayload,
	tasks ...*ShardReindexTaskGeneric,
) error {
	loggers := make([]logrus.FieldLogger, len(tasks))
	for i, task := range tasks {
		loggers[i] = task.logger
	}
	return errors.Join(swapUnitOnShard(ctx, shard, tasks, loggers, propertyOverlayFor(payload, tasks))...)
}

func (f *tokenizationChange) swap(tasks []*ShardReindexTaskGeneric) phaseResult {
	logger, _ := logrustest.NewNullLogger()
	return f.provider.runShardSwapPhase(f.ctx, f.payload, f.unit, f.shard.Name(), f.shard, tasks, logger)
}

// decide leaves what a crash right after task's flip decision leaves.
func (f *tokenizationChange) decide(t *testing.T, task *ShardReindexTaskGeneric) {
	t.Helper()
	rec, ok := task.migrationRecord(f.shard)
	require.True(t, ok)
	subject := rec.Subject()
	displaced := map[string]string{tokChangeProp: subject.Props[tokChangeProp].Canonical}
	require.NoError(t, task.putMigrationRecord(f.shard,
		NewMigrationRecordSwapped(subject, []string{tokChangeProp}, displaced)))
}

func (f *tokenizationChange) reload(t *testing.T) {
	t.Helper()
	f.shard = reloadShardFromDisk(t, f.ctx, f.idx, f.shard, f.class)
}

func (f *tokenizationChange) tokenization() string {
	return queryTokenization(f.shard, tokChangeProp, models.PropertyTokenizationField)
}

// allFound is what counts returns while tokenization and buckets agree.
var allFound = [7]int{3, 3, 3, 3, 3, 3, 3}

// counts asks every query entry point for the stored value: Equal, Like, BM25,
// BM25 with an Equal filter, the vector search allow list, the batch delete
// match and an Equal-filtered aggregation.
func (f *tokenizationChange) counts(t *testing.T) [7]int {
	t.Helper()
	search := func(filter *filters.LocalFilter, keyword *searchparams.KeywordRanking) int {
		found, _, err := f.shard.ObjectSearch(f.ctx, 10, filter, keyword, nil, nil,
			additional.Properties{}, []string{tokChangeProp})
		require.NoError(t, err)
		return len(found)
	}
	equal := propEqualsFilter(f.class.Class, tokChangeProp, tokChangeValue)
	like := propEqualsFilter(f.class.Class, tokChangeProp, "Hello*")
	like.Root.Operator = filters.OperatorLike
	bm25 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{tokChangeProp}, Query: tokChangeValue}

	allow, err := f.shard.buildAllowList(f.ctx, equal, additional.Properties{})
	require.NoError(t, err)
	defer allow.Close()
	deletes, err := f.shard.FindUUIDs(f.ctx, equal, 10)
	require.NoError(t, err)
	agg, err := f.shard.Aggregate(f.ctx, aggregation.Params{
		ClassName: schema.ClassName(f.class.Class), Filters: equal, IncludeMetaCount: true,
	}, nil)
	require.NoError(t, err)
	require.Len(t, agg.Groups, 1)

	return [7]int{
		search(equal, nil), search(like, nil), search(nil, bm25), search(equal, bm25),
		allow.Len(), len(deletes), agg.Groups[0].Count,
	}
}

func (f *tokenizationChange) states(t *testing.T) []MigrationState {
	t.Helper()
	out := make([]MigrationState, 0, len(f.tasks))
	for _, task := range f.tasks {
		rec, ok := task.migrationRecord(f.shard)
		require.True(t, ok)
		out = append(out, rec.State())
	}
	return out
}

func TestChangeTokenizationFiltersWhileASwapTearsDown(t *testing.T) {
	t.Run("a reader holds the displaced searchable bucket", func(t *testing.T) {
		f := newTokenizationChange(t)
		_, release := f.shard.Store().AcquireBucketForRead(helpers.BucketSearchableFromPropNameLSM(tokChangeProp))
		released := false
		defer func() {
			if !released {
				release()
			}
		}()

		done := make(chan phaseResult, 1)
		enterrors.GoWrapper(func() { done <- f.swap(f.tasks) }, f.idx.logger)
		require.Eventually(t, func() bool { return f.tokenization() == models.PropertyTokenizationWord },
			10*time.Second, time.Millisecond)

		assert.Equal(t, allFound, f.counts(t), "while the teardown waits")

		release()
		released = true
		require.Empty(t, (<-done).Errs)
	})

	for _, tc := range []struct {
		name string
		fail func(t *testing.T, f *tokenizationChange) (repair func())
	}{
		{
			name: "the filterable staged bucket is gone",
			fail: func(t *testing.T, f *tokenizationChange) func() {
				staged := f.tasks[1].ingestBucketName(tokChangeProp)
				require.NoError(t, f.shard.Store().ShutdownBucket(f.ctx, staged))
				require.NoError(t, os.RemoveAll(filepath.Join(f.shard.pathLSM(), staged)))
				return func() {}
			},
		},
		{
			name: "the decisions cannot be recorded",
			fail: func(t *testing.T, f *tokenizationChange) func() {
				records := f.shard.migrationRecords.Dir()
				denyDirectoryWrites(t, records)
				return func() { require.NoError(t, os.Chmod(records, 0o700)) }
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokenizationChange(t)
			repair := tc.fail(t, f)

			require.NotEmpty(t, f.swap(f.tasks).Errs)
			assert.Equal(t, models.PropertyTokenizationField, f.tokenization())
			assert.Equal(t, allFound, f.counts(t))

			repair()
			f.reload(t)
			assert.Equal(t, models.PropertyTokenizationField, f.tokenization())
			assert.Equal(t, allFound, f.counts(t))
		})
	}
}

// A forced drop drains a query's pin while the query waits to pin its next
// bucket, so a flip must not hold that query back while it waits on the drop.
func TestAFlipDuringADropLetsQueriesPin(t *testing.T) {
	f := newTokenizationChange(t)
	store := f.shard.Store()
	searchable := helpers.BucketSearchableFromPropNameLSM(tokChangeProp)
	_, releaseFirstPin := store.AcquireBucketForRead(searchable)

	dropped := make(chan error, 1)
	enterrors.GoWrapper(func() { dropped <- store.Shutdown(f.ctx) }, f.idx.logger)
	require.Eventually(t, func() bool { return store.Bucket(searchable) == nil }, 10*time.Second, time.Millisecond)

	flipped := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := f.shard.SwapBucketsAndSetOverlay(
			map[string]inverted.PropertyOverlay{tokChangeProp: {Tokenization: models.PropertyTokenizationWord}}, nil)
		flipped <- err
	}, f.idx.logger)
	assert.Never(t, func() bool {
		if f.shard.propertyOverlayMu.TryRLock() {
			f.shard.propertyOverlayMu.RUnlock()
			return false
		}
		return true
	}, 200*time.Millisecond, time.Millisecond, "the flip holds the lock the query needs for its next pin")

	releaseFirstPin()
	require.NoError(t, <-dropped)
	require.ErrorIs(t, <-flipped, lsmkv.ErrAlreadyClosed)
}

// A load that withholds promotion leaves both decisions with the staged buckets on disk.
func TestDecidedSwapsLeftByALoadFlipAsOneUnit(t *testing.T) {
	f := newTokenizationChange(t)
	f.decide(t, f.tasks[0])
	f.decide(t, f.tasks[1])
	f.provider.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		f.desc: {f.unit: f.tasks},
	})

	raw, err := json.Marshal(f.payload)
	require.NoError(t, err)
	rec := newFakeRecorder()
	f.provider.processOneUnit(f.ctx, &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: f.desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
	}, f.payload, f.idx, f.unit, rec)

	require.Empty(t, rec.failed)
	assert.Equal(t, models.PropertyTokenizationWord, f.tokenization())
	assert.Equal(t, allFound, f.counts(t))
}
