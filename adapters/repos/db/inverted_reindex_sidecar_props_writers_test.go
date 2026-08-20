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
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// writerSidecar is the properties.mig content a task's own writers produce.
// Mirrors selectedProps, which both getPropsToReindex and SaveSelectedProps
// answer from.
func writerSidecar(cfg reindexTaskConfig, collectionName string) []byte {
	var props []string
	for p := range cfg.selectedPropsByCollection[collectionName] {
		props = append(props, p)
	}
	sort.Strings(props)
	return []byte(strings.Join(props, ","))
}

// Drives every per-property strategy through its real constructor and pins
// that the dir name and properties.mig are two renderings of one property
// list — and where that breaks: a repeated property makes the sidecar (built
// from a set) unable to rebuild the name (built from the raw slice), so that
// tracker safely falls back to its payload.
func TestEveryPerPropertyStrategyWritesASidecarThatRebuildsItsDirName(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const collection = "Docs"

	strategies := []struct {
		name      string
		prefix    string
		multiProp bool
		newTask   func(l logrus.FieldLogger, props []string, gen int) *ShardReindexTaskGeneric
	}{
		{
			name: "enable filterable", prefix: MigrationDirPrefixEnableFilterable, multiProp: true,
			newTask: func(l logrus.FieldLogger, p []string, g int) *ShardReindexTaskGeneric {
				return NewRuntimeEnableFilterableTask(l, p, collection, g)
			},
		},
		{
			name: "enable searchable", prefix: MigrationDirPrefixEnableSearchable, multiProp: true,
			newTask: func(l logrus.FieldLogger, p []string, g int) *ShardReindexTaskGeneric {
				return NewRuntimeEnableSearchableTask(l, p, collection, "word", g)
			},
		},
		{
			name: "rebuild searchable", prefix: MigrationDirPrefixRebuildSearchable, multiProp: true,
			newTask: func(l logrus.FieldLogger, p []string, g int) *ShardReindexTaskGeneric {
				return NewRuntimeRebuildSearchableTask(l, p, collection, g)
			},
		},
		{
			name: "filterable to rangeable", prefix: MigrationDirPrefixFilterableToRangeable, multiProp: true,
			newTask: func(l logrus.FieldLogger, p []string, g int) *ShardReindexTaskGeneric {
				return NewRuntimeFilterableToRangeableTask(l, nil, p, collection, g)
			},
		},
		{
			name: "searchable retokenize", prefix: MigrationDirPrefixSearchableRetokenize,
			newTask: func(l logrus.FieldLogger, p []string, g int) *ShardReindexTaskGeneric {
				return NewRuntimeSearchableRetokenizeTask(l, p[0], "field", collection, "", collection, g)
			},
		},
		{
			name: "filterable retokenize", prefix: MigrationDirPrefixFilterableRetokenize,
			newTask: func(l logrus.FieldLogger, p []string, g int) *ShardReindexTaskGeneric {
				return NewRuntimeFilterableRetokenizeTask(l, p[0], "field", collection, collection, g)
			},
		},
	}

	propLists := []struct {
		name  string
		props []string
		// rebuilt is whether the sidecar the writer produces can still rebuild
		// the dir name.
		rebuilt bool
	}{
		{name: "one property", props: []string{"cat"}, rebuilt: true},
		{name: "property carrying the join character", props: []string{"price_cents"}, rebuilt: true},
		{name: "two properties", props: []string{"a", "b"}, rebuilt: true},
		{name: "three properties given out of order", props: []string{"zebra", "b", "cat"}, rebuilt: true},
		{name: "a property repeated", props: []string{"a", "a"}, rebuilt: false},
	}

	for _, s := range strategies {
		for _, pl := range propLists {
			if !s.multiProp && len(pl.props) > 1 {
				// One property is all these strategies are ever given;
				// createReindexTasks rejects a longer list for them.
				continue
			}
			t.Run(s.name+"/"+pl.name, func(t *testing.T) {
				task := s.newTask(logger, pl.props, 1)
				dirName := task.strategy.MigrationDirName()

				require.Equal(t, migrationDirWithProps(s.prefix, pl.props),
					migrationDirBase(dirName),
					"the dir name must be this property list, sorted and joined")

				lsm := t.TempDir()
				mkTrackerDir(t, lsm, dirName, "started.mig")
				mkRecoveryPayload(t, lsm, dirName, pl.props...)
				require.NoError(t, os.WriteFile(
					filepath.Join(lsm, ".migrations", dirName, "properties.mig"),
					writerSidecar(task.config, collection), 0o644))

				migDir := filepath.Join(lsm, ".migrations", dirName)
				got, readPayload := readTaskProps(migDir, []string{s.prefix})

				require.True(t, got.ok)
				require.False(t, got.unreadable)
				require.Equal(t, !pl.rebuilt, readPayload,
					"a sidecar that rebuilds the name must spare the payload, and only then")

				// Whichever side answered, the property set is the task's.
				require.ElementsMatch(t, dedupedProps(pl.props), dedupedProps(got.props))
			})
		}
	}
}

// Pins the explicit empty-list refusal: a bare-prefix dir is the one name an
// empty sidecar would rebuild, and accepting it would claim "the task
// recorded an empty property list" — a claim only the payload may make.
func TestPropsFromSidecarRefusesAnEmptyListOnABarePrefixDir(t *testing.T) {
	for _, prefix := range migrationDirPrefixesForIndexType("filterable") {
		lsm := t.TempDir()
		dirName := prefix + genSuffix(1)
		mkTrackerDir(t, lsm, dirName, "started.mig")
		mkRecoveryPayload(t, lsm, dirName, "cat")
		migDir := filepath.Join(lsm, ".migrations", dirName)
		require.NoError(t, os.WriteFile(
			filepath.Join(migDir, "properties.mig"), nil, 0o644))

		props, ok := propsFromSidecar(migDir, []string{prefix})
		require.False(t, ok, "prefix %s", prefix)
		require.Nil(t, props)
	}
}

// Drives the real payload-save path and pins that the property sidecar lands
// with payload.mig rather than waiting for the shard's first reindex pass. A
// property DELETE arriving in between is answered from the sidecar instead of
// parsing payload.mig, which runs inside the RAFT apply that holds the FSM loop
// cluster-wide.
func TestPersistRecoveryRecordWritesThePropsSidecar(t *testing.T) {
	tests := []struct {
		name      string
		migration ReindexMigrationType
		props     []string
		// tokenization and bucketStrategy are only read by the migration types
		// that require them.
		tokenization   string
		bucketStrategy string
		// sweptProp and sweptIndexType are the property DELETE the sidecar has
		// to answer.
		sweptProp      string
		sweptIndexType string
		// sweptDirs are the tracker dirs that DELETE removes; every one must
		// carry a sidecar and cost no payload read.
		sweptDirs []string
	}{
		{
			// ["cat","dog"] sorts to the same dir name a lone "cat_dog"
			// property would produce, so only a recorded list settles it.
			name:           "two properties share a dir name with one",
			migration:      ReindexTypeEnableFilterable,
			props:          []string{"dog", "cat"},
			sweptProp:      "cat",
			sweptIndexType: "filterable",
			sweptDirs:      []string{"enable_filterable_cat_dog_1"},
		},
		{
			name:           "one property carrying the join character",
			migration:      ReindexTypeEnableRangeable,
			props:          []string{"cat_dog"},
			sweptProp:      "cat_dog",
			sweptIndexType: "rangeable",
			sweptDirs:      []string{"filterable_to_rangeable_cat_dog_1"},
		},
		{
			// Two tasks, two migration dirs, one payload — the sidecar has to
			// reach both.
			name:           "change-tokenization writes one sidecar per sub-task",
			migration:      ReindexTypeChangeTokenization,
			props:          []string{"cat_dog"},
			tokenization:   "field",
			bucketStrategy: "MapCollection",
			sweptProp:      "cat_dog",
			sweptIndexType: "filterable",
			sweptDirs:      []string{"filterable_retokenize_cat_dog_1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "PropsSidecar" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx,
				newTestClassWithProps(className, tc.props),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			p, _ := newTestProvider(t)
			payload := &ReindexTaskPayload{
				MigrationType:      tc.migration,
				Collection:         className,
				Properties:         tc.props,
				TargetTokenization: tc.tokenization,
				BucketStrategy:     tc.bucketStrategy,
				UnitToShard:        map[string]string{"unit-1": shard.Name()},
			}
			tasks, err := p.createReindexTasks(payload, lsm, false)
			require.NoError(t, err)
			require.NotEmpty(t, tasks)

			dtmTask := &distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-1", Version: 1},
			}
			require.NoError(t, p.persistRecoveryRecord(dtmTask, payload, "unit-1", shard, tasks, &selectedPropsFailures{}))

			want := slices.Clone(tc.props)
			sort.Strings(want)
			for _, task := range tasks {
				migDir := task.migrationPath(lsm)
				got, err := readMigrationProps(migDir)
				require.NoErrorf(t, err, "task %q wrote no properties.mig", task.Name())
				require.Equal(t, want, got)
			}

			logger, _ := test.NewNullLogger()
			// A real memo, never nil: the read count accrues into it, and a nil
			// one reads every payload again while counting nothing, which would
			// make the assertion below hold no matter what the sweep read.
			props := &taskPropsCache{}
			cleanStaleMigrationDirsAt(t.Context(), lsm, tc.sweptProp, tc.sweptIndexType, logger, props)
			require.Zero(t, props.count(),
				"every swept tracker was answerable from its sidecar")
			// Without this the zero above would also hold for a sweep that
			// matched nothing at all.
			for _, dir := range tc.sweptDirs {
				require.NoDirExistsf(t, filepath.Join(lsm, ".migrations", dir),
					"the sweep must have owned %s to have read its properties", dir)
			}
		})
	}
}

// Pins the guard the early write rests on: a task that has to discover its
// properties per shard records nothing, leaving properties.mig absent so the
// discovery still runs on the shard.
func TestSaveSelectedPropsWritesNoSidecarWithoutASelectedList(t *testing.T) {
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name    string
		newTask func(className, shardName string) *ShardReindexTaskGeneric
	}{
		{
			name: "whole-collection task selects no properties",
			newTask: func(className, shardName string) *ShardReindexTaskGeneric {
				return newTestTask(logger, &MapToBlockmaxStrategy{generation: 1})
			},
		},
		{
			name: "properties are selected for another collection",
			newTask: func(className, shardName string) *ShardReindexTaskGeneric {
				return NewRuntimeEnableFilterableTask(logger, []string{"cat"}, className+"Other", 1)
			},
		},
		{
			name: "properties are selected for another shard",
			newTask: func(className, shardName string) *ShardReindexTaskGeneric {
				task := NewRuntimeEnableFilterableTask(logger, []string{"cat"}, className, 1)
				task.constrainToShard(className, shardName+"-other")
				return task
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "NoSidecar" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx,
				newTestClassWithProps(className, []string{"cat"}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			task := tc.newTask(className, shard.Name())
			migDir := task.migrationPath(shard.pathLSM())
			require.NoError(t, os.MkdirAll(migDir, 0o777))

			require.NoError(t, task.SaveSelectedProps(shard))
			require.NoFileExists(t, filepath.Join(migDir, "properties.mig"))
		})
	}
}

// A zero-byte properties.mig is what a machine crash between the sidecar
// write's create and its content leaves behind. Nothing else on the shard
// records that a property list was ever computed, so every writer that meets
// one has to rebuild it — reading it as a finished discovery retires the
// shard's reindex for good.
func TestAZeroBytePropsSidecarIsRebuiltNotObeyed(t *testing.T) {
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name string
		// newTask builds the task whose writer has to notice the torn file.
		newTask func(className string) *ShardReindexTaskGeneric
		// repair is that writer, returning the props it resolved (nil where
		// the writer does not report any).
		repair func(task *ShardReindexTaskGeneric, shard *Shard, rt *fileReindexTracker) ([]string, error)
		want   []string
	}{
		{
			name: "whole-collection discovery",
			newTask: func(className string) *ShardReindexTaskGeneric {
				return newTestTask(logger, &MapToBlockmaxStrategy{generation: 1})
			},
			repair: func(task *ShardReindexTaskGeneric, shard *Shard, rt *fileReindexTracker) ([]string, error) {
				return task.getPropsToReindex(shard, rt)
			},
			want: []string{"cat", "dog"},
		},
		{
			name: "the early selected-props write",
			newTask: func(className string) *ShardReindexTaskGeneric {
				return NewRuntimeEnableFilterableTask(logger, []string{"cat", "dog"}, className, 1)
			},
			repair: func(task *ShardReindexTaskGeneric, shard *Shard, rt *fileReindexTracker) ([]string, error) {
				return nil, task.SaveSelectedProps(shard)
			},
			want: []string{"cat", "dog"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "TornSidecar" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx,
				newTestClassWithProps(className, []string{"cat", "dog"}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			task := tc.newTask(className)
			rt := NewFileReindexTracker(shard.pathLSM(), task.strategy.MigrationDirName(), &UuidKeyParser{})
			require.NoError(t, rt.init())
			migDir := rt.config.migrationPath
			require.NoError(t, os.WriteFile(filepath.Join(migDir, "properties.mig"), nil, 0o644))

			props, err := tc.repair(task, shard, rt)
			require.NoError(t, err)
			if props != nil {
				require.ElementsMatch(t, tc.want, props)
			}

			// The repair has to reach disk: the next boot reads the file, not
			// this call's return value.
			onDisk, err := readMigrationProps(migDir)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, onDisk)

			entries, err := os.ReadDir(migDir)
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotContains(t, entry.Name(), ".tmp",
					"the atomic write must not leave its temp file behind")
			}
		})
	}
}

// properties.mig is the shard's only record of what it has to reindex, so
// content that names no property is a corrupt file, not an empty list. Reading
// it as an empty list makes the shard report "nothing to do" forever.
func TestAPropsSidecarNamingNoPropertyIsAnError(t *testing.T) {
	ctx := testCtx()
	logger, _ := test.NewNullLogger()
	className := "BlankSidecar" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, ctx,
		newTestClassWithProps(className, []string{"cat", "dog"}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	task := newTestTask(logger, &MapToBlockmaxStrategy{generation: 1})
	rt := NewFileReindexTracker(shard.pathLSM(), task.strategy.MigrationDirName(), &UuidKeyParser{})
	require.NoError(t, rt.init())
	// Non-empty, so the file reads as a recorded list, yet it names nothing.
	require.NoError(t, os.WriteFile(
		filepath.Join(rt.config.migrationPath, "properties.mig"), []byte(" \n"), 0o644))

	_, err := task.getPropsToReindex(shard, rt)
	require.Error(t, err)
	_, err = task.readPropsToReindex(rt)
	require.Error(t, err)
}

// ambiguousSweepDirs is how many tracker dirs the hot cell carries.
const ambiguousSweepDirs = 100

// ambiguousSweepPayloadBytes pads each payload.mig so the fixture has the shape
// that made this path expensive: a payload far larger than the sidecar beside
// it. Kept well under a production payload so the tree stays cheap to build.
const ambiguousSweepPayloadBytes = 256 << 10

// writeAmbiguousSweepTree lays down tracker dirs whose names cannot settle the
// property on their own, which is the only cell that reaches the payload at all.
func writeAmbiguousSweepTree(t testing.TB, withSidecar bool) string {
	t.Helper()
	lsm := t.TempDir()
	props := []string{"a", "b"}
	for gen := 1; gen <= ambiguousSweepDirs; gen++ {
		dirName := migrationDirWithProps(MigrationDirPrefixEnableFilterable, props) + genSuffix(gen)
		dir := filepath.Join(lsm, ".migrations", dirName)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), []byte("x"), 0o644))

		payload, err := json.Marshal(map[string]any{
			"payload": map[string]any{"properties": props},
			// Ignored by the reader, but it still has to scan past it.
			"filler": strings.Repeat("x", ambiguousSweepPayloadBytes),
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, reindexRecoveryPayloadFile), payload, 0o644))

		if withSidecar {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "properties.mig"),
				[]byte(strings.Join(props, ",")), 0o644))
		}
	}
	return lsm
}

// Regression guard on the cost this change removes: a refactor that puts
// the payload parse back on this path fails here rather than in production,
// where it blocks the RAFT apply loop cluster-wide.
func TestAmbiguousSweepReadsNoPayloadWhenTheSidecarsAreThere(t *testing.T) {
	logger, _ := test.NewNullLogger()

	lsm := writeAmbiguousSweepTree(t, true)
	withSidecars := &taskPropsCache{}
	cleanStaleMigrationDirsAt(t.Context(), lsm, "a_b", "filterable", logger, withSidecars)
	require.Zero(t, withSidecars.count(), "every tracker was answerable from its sidecar")

	// Without sidecars the same sweep has to open every payload, which is what
	// pins the count above as a property of the sidecars and not of the names.
	bare := writeAmbiguousSweepTree(t, false)
	noSidecars := &taskPropsCache{}
	cleanStaleMigrationDirsAt(t.Context(), bare, "a_b", "filterable", logger, noSidecars)
	require.Equal(t, ambiguousSweepDirs, noSidecars.count(),
		"without a sidecar there is nothing to answer from")

	const maxBytesPerSweep = 2 << 20
	tree := writeAmbiguousSweepTree(t, true)
	scope := migrationDirsOf(tree, nil, "a_b", "filterable")
	names := trackerDirNames(t, tree)

	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pass := scope.cachingProps(&taskPropsCache{})
			for _, name := range names {
				pass.inScope(name)
			}
		}
	})
	require.Less(t, result.AllocedBytesPerOp(), int64(maxBytesPerSweep),
		"a %d-dir sweep allocated %d bytes; the payload parse is back",
		ambiguousSweepDirs, result.AllocedBytesPerOp())
}

func trackerDirNames(t testing.TB, lsm string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(lsm, ".migrations"))
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names
}

// BenchmarkAmbiguousSweep measures one deletion sweep's matching pass over the
// cell the sidecar shortcut targets: tracker dirs whose names cannot settle the
// property. Run it with and without properties.mig on disk to see the cost the
// shortcut removes.
func BenchmarkAmbiguousSweep(b *testing.B) {
	for _, withSidecar := range []bool{false, true} {
		name := "without-sidecar"
		if withSidecar {
			name = "with-sidecar"
		}
		b.Run(name, func(b *testing.B) {
			lsm := writeAmbiguousSweepTree(b, withSidecar)
			scope := migrationDirsOf(lsm, nil, "a_b", "filterable")
			names := trackerDirNames(b, lsm)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				pass := scope.cachingProps(&taskPropsCache{})
				for _, n := range names {
					pass.inScope(n)
				}
			}
		})
	}
}
