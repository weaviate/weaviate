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
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// writerSidecar is the properties.mig content getPropsToReindex writes for a
// task. Mirrors findPropsToReindex, the only non-test caller of saveProps.
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
				dirName := task.MigrationDirName()

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
	require.Zero(t, cleanStaleMigrationDirsAt(lsm, "a_b", "filterable", logger),
		"every tracker was answerable from its sidecar")

	// Without sidecars the same sweep has to open every payload, which is what
	// pins the count above as a property of the sidecars and not of the names.
	bare := writeAmbiguousSweepTree(t, false)
	require.Equal(t, ambiguousSweepDirs,
		cleanStaleMigrationDirsAt(bare, "a_b", "filterable", logger),
		"without a sidecar there is nothing to answer from")

	const maxBytesPerSweep = 2 << 20
	tree := writeAmbiguousSweepTree(t, true)
	scope := migrationDirsOf(tree, nil, "a_b", "filterable")
	names := trackerDirNames(t, tree)

	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pass := scope.cachingProps(&taskPropsCache{})
			for _, name := range names {
				pass.matches(name)
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
					pass.matches(n)
				}
			}
		})
	}
}
