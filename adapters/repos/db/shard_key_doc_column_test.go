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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A reindex names only props whose bucket holds a column, in one warning per run with each shard counted once.
func TestKeyDocColumnDropsReportOncePerRun(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	storeLogger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, storeLogger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(ctx) })

	roaringSet := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	}
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM("sku"),
		append(roaringSet, lsmkv.WithKeyDocColumn(dir, func() uint64 { return 1 }))...))
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM("title"),
		roaringSet...))

	props := propsWithKeyDocColumn(store, []string{"sku", "title", "missing"})
	require.Equal(t, []string{"sku"}, props)

	var drops, other keyDocColumnDrops
	drops.record("Article", "shard1", props)
	drops.record("Article", "shard1", props) // a resumed run
	drops.record("Article", "shard2", nil)
	drops.record("Book", "shard1", props) // the same shard name in another collection
	other.record("Article", "shard3", props)
	drops.absorb(&other)

	entry := reportedKeyDocColumnDrop(t, &drops)
	assert.Equal(t, 3, entry.Data["shards"])
	assert.Equal(t, []string{"Article", "Book"}, entry.Data["collections"])
	assert.Equal(t, []string{"sku"}, entry.Data["props"])
	assert.Contains(t, entry.Message, "3 shard(s) of 2 collection(s)")
}

// A run over many collections names a bounded few of them, and counts them all.
func TestKeyDocColumnDropsReportBoundsCollections(t *testing.T) {
	var drops keyDocColumnDrops
	const collections = keyDocColumnDropsNamedCollections + 3
	for i := range collections {
		drops.record(fmt.Sprintf("C%02d", i), "shard1", []string{"sku"})
	}

	entry := reportedKeyDocColumnDrop(t, &drops)
	assert.Len(t, entry.Data["collections"], keyDocColumnDropsNamedCollections)
	assert.Contains(t, entry.Message, fmt.Sprintf("of %d collection(s)", collections))
}

// reportedKeyDocColumnDrop reports drops and returns the single warning it must make.
func reportedKeyDocColumnDrop(t *testing.T, drops *keyDocColumnDrops) *logrus.Entry {
	t.Helper()
	logger, hook := test.NewNullLogger()
	drops.report(logger)
	require.Len(t, hook.AllEntries(), 1, "one line per run")
	entry := hook.LastEntry()
	require.Equal(t, logrus.WarnLevel, entry.Level)
	return entry
}

// newKeyDocColumnShard opens a shard of className whose "sku" property is configured
// for a key/doc column, logging to logger.
func newKeyDocColumnShard(t *testing.T, className string, logger logrus.FieldLogger) (*Shard, *Index) {
	t.Helper()
	class := newTestClassWithProps(className, []string{"sku"})
	class.Properties[0].Tokenization = models.PropertyTokenizationField
	return openKeyDocColumnShard(t, class, map[string][]string{className: {"sku"}}, logger)
}

// openKeyDocColumnShard opens a shard of class with configured as its key/doc column configuration.
func openKeyDocColumnShard(t *testing.T, class *models.Class, configured map[string][]string,
	logger logrus.FieldLogger,
) (*Shard, *Index) {
	t.Helper()
	shd, idx := testShardWithSettings(t, testCtx(), class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) {
			i.logger = logger
			i.Config.KeyDocColumnProps = configured[class.Class]
		})
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })
	return shard, idx
}

// putSKUs writes one object per value to shard's "sku" property and flushes them.
func putSKUs(t *testing.T, shard *Shard, values ...string) {
	t.Helper()
	ctx := testCtx()
	for _, v := range values {
		obj := testObject(shard.Index().Config.ClassName.String())
		obj.Object.Properties = map[string]any{"sku": v}
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	require.NoError(t, shard.Store().FlushMemtables(ctx))
}

// requireSKUColumn fails unless shard's "sku" bucket holds a key/doc column.
func requireSKUColumn(t *testing.T, shard *Shard) {
	t.Helper()
	b := shard.Store().Bucket(helpers.BucketFromPropNameLSM("sku"))
	require.NotNil(t, b)
	require.NotNil(t, b.KeyDocColumn(), "the shard builds the configured column")
}

// A collection configured with "*" gets a column only on properties the query path reads one for.
func TestShardOpensKeyDocColumnOnlyForEligibleProperties(t *testing.T) {
	className := "KeyDocColumnEligible_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"sku", "title"})
	class.Properties[0].Tokenization = models.PropertyTokenizationField
	logger, _ := test.NewNullLogger()
	shard, _ := openKeyDocColumnShard(t, class, map[string][]string{className: {"*"}}, logger)

	requireSKUColumn(t, shard)
	title := shard.Store().Bucket(helpers.BucketFromPropNameLSM("title"))
	require.NotNil(t, title)
	assert.Nil(t, title.KeyDocColumn(), "a word-tokenized property is never read from a column")
}

// processOneUnit records the column its bucket swap drops, and the provider reports it once with the shard counted.
func TestReindexProviderReportsKeyDocColumnDrop(t *testing.T) {
	ctx := testCtx()
	className := "KeyDocColumnDrop_" + uuid.NewString()[:8]
	logger, _ := test.NewNullLogger()
	shard, idx := newKeyDocColumnShard(t, className, logger)
	putSKUs(t, shard, "a", "b", "c")
	requireSKUColumn(t, shard)

	p, _ := newTestProvider(t)
	rec := newFakeRecorder()
	dtmTask := &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-1", Version: 1},
	}
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeRepairFilterable,
		Collection:    className,
		Properties:    []string{"sku"},
		UnitToShard:   map[string]string{"unit-1": shard.Name()},
	}
	var drops keyDocColumnDrops
	p.processOneUnit(ctx, dtmTask, payload, idx, "unit-1", rec, &drops)
	require.Equal(t, []string{"unit-1"}, rec.completed, "the unit fails: %v", rec.failed)

	entry := reportedKeyDocColumnDrop(t, &drops)
	assert.Equal(t, 1, entry.Data["shards"])
	assert.Equal(t, []string{className}, entry.Data["collections"])
	assert.Equal(t, []string{"sku"}, entry.Data["props"])
}

// A task's drops were split across collectors, mixed with another task's, or reported again on a repeated completion.
func TestReindexProviderReportsColumnDropsOncePerTask(t *testing.T) {
	p, hook := newTestProvider(t)
	task := &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-1", Version: 1},
		Status:         distributedtask.TaskStatusFinished,
	}
	for _, tenant := range []string{"tenant1", "tenant2"} {
		p.taskColumnDrops(task.TaskDescriptor).record("Class", tenant, []string{"sku"})
	}
	other := distributedtask.TaskDescriptor{ID: "task-2", Version: 1}
	p.taskColumnDrops(other).record("Class", "tenant3", []string{"sku"})

	dropWarnings := func() []*logrus.Entry {
		var out []*logrus.Entry
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "key/doc columns are absent") {
				out = append(out, e)
			}
		}
		return out
	}
	require.NoError(t, p.OnTaskCompleted(task))
	warnings := dropWarnings()
	require.Len(t, warnings, 1, "one warning for the task's groups")
	assert.Equal(t, 2, warnings[0].Data["shards"], "and not the other task's")

	require.NoError(t, p.OnTaskCompleted(task))
	assert.Len(t, dropWarnings(), 1, "a repeated completion reports nothing again")
}

// The swap, not prep, reports a dropped column, also for a configured property that never had one.
func TestReindexSwapPhaseReportsKeyDocColumnDrop(t *testing.T) {
	tests := []struct {
		name       string
		filterable bool
		migration  ReindexMigrationType
		newTask    func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric
	}{
		{
			name:       "retokenizing drops the column",
			filterable: true,
			migration:  ReindexTypeChangeTokenizationFilterable,
			newTask: func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric {
				task, _ := newFilterableRetokenizeTask(t, idx, className, "sku", models.PropertyTokenizationWord, unitID)
				return task
			},
		},
		{
			name:      "enabling filterable leaves the configured property without one",
			migration: ReindexTypeEnableFilterable,
			newTask: func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, unitID, "sku")
				return task
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "KeyDocColumnSwapDrop_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"sku"})
			class.Properties[0].Tokenization = models.PropertyTokenizationField
			class.Properties[0].IndexFilterable = boolPtr(tc.filterable)
			logger, _ := test.NewNullLogger()
			shard, idx := openKeyDocColumnShard(t, class, map[string][]string{className: {"sku"}}, logger)
			putSKUs(t, shard, "a", "b", "c")

			task := tc.newTask(t, idx, className, shard.migrationUnit())
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			var drops keyDocColumnDrops
			drops.absorb(task.keyDocColumnDrops)
			require.Empty(t, drops.shards, "nothing is dropped before the swap")

			p := &ReindexProvider{logger: logger, localNode: "node1", serverCtx: ctx}
			res := p.runShardSwapPhase(ctx, &ReindexTaskPayload{
				MigrationType: tc.migration,
				Collection:    className,
				Properties:    []string{"sku"},
			}, "unit-1", shard.Name(), shard, []*ShardReindexTaskGeneric{task}, &drops, logger)
			require.Empty(t, res.Errs)

			entry := reportedKeyDocColumnDrop(t, &drops)
			assert.Equal(t, 1, entry.Data["shards"])
			assert.Equal(t, []string{"sku"}, entry.Data["props"])
		})
	}
}

// ShardInvertedReindexer records the column a bucket swap drops.
func TestShardInvertedReindexerRecordsKeyDocColumnDrop(t *testing.T) {
	className := "KeyDocColumnLegacyDrop_" + uuid.NewString()[:8]
	logger, _ := test.NewNullLogger()
	shard, _ := newKeyDocColumnShard(t, className, logger)
	putSKUs(t, shard, "a", "b", "c")
	requireSKUColumn(t, shard)

	var drops keyDocColumnDrops
	reindexer := NewShardInvertedReindexer(shard, logger, &drops)
	require.NotNil(t, reindexer)
	reindexer.AddTask(&keyDocColumnPropValueTask{ShardInvertedReindexTask_SpecifiedIndex{
		classNamesWithPropertyNames: map[string]map[string]struct{}{className: {"sku": {}}},
	}})
	require.NoError(t, reindexer.Do(testCtx()))

	entry := reportedKeyDocColumnDrop(t, &drops)
	assert.Equal(t, 1, entry.Data["shards"])
	assert.Equal(t, []string{className}, entry.Data["collections"])
	assert.Equal(t, []string{"sku"}, entry.Data["props"])
}

// keyDocColumnPropValueTask reindexes only the filterable bucket of its properties.
type keyDocColumnPropValueTask struct {
	ShardInvertedReindexTask_SpecifiedIndex
}

func (t *keyDocColumnPropValueTask) GetPropertiesToReindex(ctx context.Context,
	shard ShardLike,
) ([]ReindexableProperty, error) {
	props, err := t.ShardInvertedReindexTask_SpecifiedIndex.GetPropertiesToReindex(ctx, shard)
	return slices.DeleteFunc(props, func(p ReindexableProperty) bool {
		return p.IndexType != IndexTypePropValue
	}), err
}

// Build warnings fire once per Class.prop: a collection's shards share one, another collection's same-named property gets its own.
func TestShardKeyDocColumnNameIsCollectionAndProperty(t *testing.T) {
	ctx := testCtx()
	suffix := uuid.NewString()[:8]
	nonUniqueWarnings := func(hook *test.Hook) map[logrus.Level]int {
		levels := map[logrus.Level]int{}
		for _, e := range hook.AllEntries() {
			if e.Data["action"] == "keydoccolumn_build" && strings.Contains(e.Message, "not unique") {
				levels[e.Level]++
			}
		}
		return levels
	}
	// reopen builds the column over sku values that are not unique, as a restart does
	reopen := func(t *testing.T, idx *Index, shard ShardLike) {
		putSKUs(t, shard.(*Shard), "dup", "dup")
		name := shard.Name()
		require.NoError(t, shard.Shutdown(ctx))
		reopened, err := idx.initShard(ctx, name, idx.getClass(), nil, true, true)
		require.NoError(t, err)
		idx.shards.Store(name, reopened)
		t.Cleanup(func() { reopened.Shutdown(context.Background()) })
		requireSKUColumn(t, reopened.(*Shard))
	}

	articleLogger, articleHook := test.NewNullLogger()
	articleLogger.SetLevel(logrus.DebugLevel)
	article, articleIdx := newKeyDocColumnShard(t, "Article"+suffix, articleLogger)
	reopen(t, articleIdx, article)
	second, err := articleIdx.initShard(ctx, "shard2", articleIdx.getClass(), nil, true, true)
	require.NoError(t, err)
	articleIdx.shards.Store("shard2", second)
	reopen(t, articleIdx, second)

	bookLogger, bookHook := test.NewNullLogger()
	bookLogger.SetLevel(logrus.DebugLevel)
	book, bookIdx := newKeyDocColumnShard(t, "Book"+suffix, bookLogger)
	reopen(t, bookIdx, book)

	assert.Equal(t, map[logrus.Level]int{logrus.WarnLevel: 1, logrus.DebugLevel: 1},
		nonUniqueWarnings(articleHook), "one warning across a collection's shards")
	assert.Equal(t, map[logrus.Level]int{logrus.WarnLevel: 1}, nonUniqueWarnings(bookHook),
		"another collection's property warns on its own")
}

// TestKeyDocColumnConfigured pins which configured property names give a shard's property a column.
func TestKeyDocColumnConfigured(t *testing.T) {
	tests := []struct {
		name       string
		configured []string // the class's entry, as migrator.go and init.go narrow it
		prop       string
		want       bool
	}{
		{name: "nothing configured", prop: "prop1"},
		{name: "the property is listed", configured: []string{"prop1", "prop2"}, prop: "prop2", want: true},
		{name: "the property is not listed", configured: []string{"prop1"}, prop: "other"},
		{name: "every property, by wildcard", configured: []string{"*"}, prop: "anything", want: true},
		{
			// the schema holds "sKU"; "SKU" reaches it, as a filter written that way does
			name:       "a property whose first letter was capitalised",
			configured: []string{"SKU"}, prop: "sKU", want: true,
		},
		{name: "and a later letter's case still has to match", configured: []string{"sku"}, prop: "sKU"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Shard{index: &Index{Config: IndexConfig{KeyDocColumnProps: tt.configured}}}
			assert.Equal(t, tt.want, s.keyDocColumnConfigured(tt.prop))
		})
	}
}
