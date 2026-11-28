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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const legacyObjects = 20

// The marker set a v1.39 node leaves once its migration's task FINISHED,
// taken from a real run; mergedOnly is a swap that never completed.
var (
	legacyFinishedMarkers = []string{
		"started.mig", "reindexed.mig", "prepended.mig", "merged.mig", "swapped.mig", "tidied.mig",
	}
	legacyMergedOnlyMarkers = []string{"started.mig", "reindexed.mig", "prepended.mig", "merged.mig"}
)

// Each row plants the on-disk state a v1.39 node leaves after a FINISHED
// migration: the data under the staged name, no canonical directory, and the
// tracker. Verified against a real v1.39 run for every code except map to
// blockmax, which that run could not start.
func TestFinalizeLegacyMigrations(t *testing.T) {
	finished := legacyFinishedMarkers
	tests := []struct {
		name    string
		code    MigrationStrategyCode
		props   []string
		markers []string
		// alreadyMoved are properties a crashed earlier pass already renamed.
		alreadyMoved []string
		// disables turns the migrated index off, as before a cluster commit.
		disables    bool
		edit        func(t *testing.T, lsm, tracker string)
		wantErr     string
		wantTracker bool
		// fix repairs the state for a second load, which must serve every object.
		fix func(t *testing.T, lsm string, class *models.Class)
	}{
		{name: "map to blockmax", code: StrategyCodeSearchableMapToBlockmax, props: []string{"title"}, markers: finished},
		{name: "repair filterable", code: StrategyCodeFilterableRoaringsetRefresh, props: []string{"title"}, markers: finished},
		{name: "enable or repair rangeable", code: StrategyCodeFilterableToRangeable, props: []string{"score"}, markers: finished},
		{name: "retokenize searchable", code: StrategyCodeSearchableRetokenize, props: []string{"title"}, markers: finished},
		{name: "retokenize filterable", code: StrategyCodeFilterableRetokenize, props: []string{"title"}, markers: finished},
		{name: "enable filterable", code: StrategyCodeEnableFilterable, props: []string{"title"}, markers: finished},
		{name: "enable searchable", code: StrategyCodeEnableSearchable, props: []string{"title"}, markers: finished},
		{name: "rebuild searchable", code: StrategyCodeRebuildSearchable, props: []string{"title"}, markers: finished},
		{
			name: "crash after the first of two renames", code: StrategyCodeEnableFilterable, props: []string{"body", "title"},
			markers: finished, alreadyMoved: []string{"body"},
		},
		{
			name: "merged, before the swap", code: StrategyCodeFilterableRetokenize, props: []string{"title"},
			markers: legacyMergedOnlyMarkers, wantTracker: true,
			edit: func(t *testing.T, lsm, _ string) {
				require.NoError(t, copyTree(filepath.Join(lsm, "property_title__filt_retokenize_ingest_7"),
					filepath.Join(lsm, "property_title")))
			},
		},
		{
			name: "failed promotion", code: StrategyCodeEnableFilterable, props: []string{"body", "title"}, markers: finished,
			edit: func(t *testing.T, lsm, _ string) {
				require.NoError(t, os.Mkdir(filepath.Join(lsm, "property_body"), 0o755))
			},
			wantErr: `property "body" holds both`,
			fix: func(t *testing.T, lsm string, _ *models.Class) {
				require.NoError(t, os.Remove(filepath.Join(lsm, "property_body")))
			},
		},
		{
			name: "unreadable finish marker", code: StrategyCodeEnableFilterable, props: []string{"title"}, markers: finished,
			edit: func(t *testing.T, _, tracker string) {
				marker := filepath.Join(tracker, "tidied.mig")
				require.NoError(t, os.Remove(marker))
				require.NoError(t, os.Symlink(marker, marker))
			},
			wantErr: "tidied.mig",
		},
		{
			name: "index off locally", code: StrategyCodeEnableFilterable, props: []string{"title"}, markers: finished,
			disables: true, wantTracker: true,
			fix: func(_ *testing.T, _ string, class *models.Class) { class.Properties[0].IndexFilterable = boolPtr(true) },
		},
		{
			name: "index off, nothing left", code: StrategyCodeEnableFilterable, props: []string{"title"}, markers: finished,
			disables: true,
			edit: func(t *testing.T, lsm, _ string) {
				require.NoError(t, os.RemoveAll(filepath.Join(lsm, "property_title__enable_filterable_ingest_7")))
			},
		},
		{
			// A newer rebuild-searchable run (generation 1) renamed this older run's staged folder to its backup.
			name: "two trackers, older first", code: StrategyCodeSearchableRetokenize, props: []string{"title"}, markers: finished,
			edit: func(t *testing.T, lsm, _ string) {
				plantLegacyTracker(t, lsm, StrategyCodeRebuildSearchable, 1, []string{"title"}, finished)
				require.NoError(t, os.Rename(filepath.Join(lsm, "property_title_searchable__retokenize_ingest_7"),
					filepath.Join(lsm, "property_title_searchable__rebuild_searchable_ingest_1")))
				require.NoError(t, os.Mkdir(filepath.Join(lsm, "property_title_searchable__rebuild_searchable_backup_1"), 0o755))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			class := newLegacyFinalizeClass("LegacyFinalize_" + uuid.NewString()[:8])
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			for i := 0; i < legacyObjects; i++ {
				require.NoError(t, shard.PutObject(ctx, newLegacyFinalizeObject(class.Class, i)))
			}
			require.NoError(t, shard.Shutdown(ctx))

			lsm := shard.pathLSM()
			layout := legacyMigrationLayouts[tt.code]
			tracker := plantLegacyTracker(t, lsm, tt.code, 7, tt.props, tt.markers)
			for _, prop := range tt.props {
				canonical := layout.canonical(prop)
				require.NoError(t, os.Mkdir(filepath.Join(lsm, canonical+layout.backup+"_7"), 0o755))
				if !slices.Contains(tt.alreadyMoved, prop) {
					require.NoError(t, os.Rename(filepath.Join(lsm, canonical), filepath.Join(lsm, canonical+layout.staged+"_7")))
				}
			}
			if tt.edit != nil {
				tt.edit(t, lsm, tracker)
			}
			if tt.disables {
				class.Properties[0].IndexFilterable = boolPtr(false)
			}

			before := legacyFolders(t, lsm)
			loaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, before, legacyFolders(t, lsm), "a failed load changes nothing")
			} else {
				require.NoError(t, err)
				requireLegacyFinalized(t, loaded.(*Shard), class, tt.code, tt.props, tt.wantTracker, tt.disables)
			}
			if tt.fix == nil {
				return
			}
			tt.fix(t, lsm, class)
			loaded, err = idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err)
			requireLegacyFinalized(t, loaded.(*Shard), class, tt.code, tt.props, false, false)
		})
	}
}

// The folder kept for an index turned off holds data from before it went off.
// A later enable run serves its own generation, and the next load deletes the folder.
func TestLegacyKeptFolderIsNotServedAfterReEnable(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 1)
	n := c.nodes[0]
	n.sm.SetMutationGuard(n.dtm)
	c.addTenants("t1")
	c.write("t1", 3)
	idx, lsm, stale := n.index(), filepath.Join(n.index().path(), "t1", "lsm"), filepath.Join(t.TempDir(), "stale")
	unload := func() {
		require.NoError(t, n.shard("t1").Shutdown(c.ctx))
		idx.shards.LoadAndDelete("t1")
	}
	load := func() {
		shd, err := idx.initShard(c.ctx, "t1", n.sr.ReadOnlyClass(c.class), nil, true, true)
		require.NoError(t, err)
		idx.shards.Store("t1", shd)
	}
	unload()
	require.NoError(t, copyTree(filepath.Join(lsm, "property_title"), stale))
	load()
	off := false
	require.NoError(t, c.apply(api.ApplyRequest_TYPE_UPDATE_PROPERTY, c.class, api.UpdatePropertyRequest{
		Property: &models.Property{Name: genProp, IndexFilterable: &off}, FieldsToUpdate: []string{api.PropertyFieldIndexFilterable},
	}))
	c.write("t1", 2)
	unload()
	require.NoError(t, os.Rename(stale, filepath.Join(lsm, "property_title__enable_filterable_ingest_7")))
	tracker := plantLegacyTracker(t, lsm, StrategyCodeEnableFilterable, 7, []string{genProp}, legacyFinishedMarkers)
	load()
	require.DirExists(t, filepath.Join(lsm, "property_title"))
	require.DirExists(t, tracker)

	id, err := c.submit(ReindexTypeEnableFilterable, "")
	require.NoError(t, err)
	c.waitGone(id)
	hello := n.filter(genProp, "hello", schema.DataTypeText, filters.OperatorEqual)
	require.Equal(t, 5, n.count("t1", hello), "the kept folder holds only 3")
	n.reload("t1")
	require.Equal(t, 5, n.count("t1", hello))
	require.NoDirExists(t, filepath.Join(lsm, "property_title"))
	require.NoDirExists(t, tracker)
}

func plantLegacyTracker(t *testing.T, lsm string, code MigrationStrategyCode, gen int, props, markers []string) string {
	tracker := filepath.Join(lsm, migrationsDir, fmt.Sprintf("%s_%s_%d", code, strings.Join(props, "_"), gen))
	require.NoError(t, os.MkdirAll(tracker, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tracker, "properties.mig"), []byte(strings.Join(props, ",")), 0o644))
	for _, marker := range markers {
		require.NoError(t, os.WriteFile(filepath.Join(tracker, marker), nil, 0o644))
	}
	return tracker
}

// requireLegacyFinalized checks a loaded shard, then shuts it down.
func requireLegacyFinalized(t *testing.T, shard *Shard, class *models.Class, code MigrationStrategyCode,
	props []string, wantTracker, indexOff bool,
) {
	defer func() { require.NoError(t, shard.Shutdown(context.Background())) }()
	lsm, layout := shard.pathLSM(), legacyMigrationLayouts[code]
	trackers, err := os.ReadDir(filepath.Join(lsm, migrationsDir))
	require.NoError(t, err)
	require.Equal(t, wantTracker, len(trackers) > 0, "trackers left: %v", trackers)
	for _, prop := range props {
		canonical := layout.canonical(prop)
		for _, dir := range []string{canonical + layout.staged + "_7", canonical + layout.backup + "_7"} {
			require.NoDirExists(t, filepath.Join(lsm, dir))
		}
		if indexOff {
			_, err := os.Stat(filepath.Join(lsm, canonical))
			require.Equal(t, wantTracker, err == nil, "the canonical folder of %q stays exactly with the tracker", prop)
			continue
		}
		require.Lenf(t, legacyFinalizeSearch(t, context.Background(), shard, class.Class, code, prop), legacyObjects,
			"property %q must serve every object", prop)
	}
}

func newLegacyFinalizeClass(className string) *models.Class {
	text := func(name string) *models.Property {
		return &models.Property{
			Name: name, DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord,
			IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true),
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			UsingBlockMaxWAND:      true,
		},
		Properties: []*models.Property{
			text("title"), text("body"),
			{
				Name: "score", DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: boolPtr(true), IndexRangeFilters: boolPtr(true),
			},
		},
	}
}

func newLegacyFinalizeObject(className string, i int) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]any{
				"title": fmt.Sprintf("apple %d", i), "body": fmt.Sprintf("apple %d", i), "score": int64(i),
			},
		},
	}
}

// Queries through the index the migration rebuilt, so an empty canonical
// bucket answers with nothing.
func legacyFinalizeSearch(t *testing.T, ctx context.Context, shard *Shard, className string,
	code MigrationStrategyCode, prop string,
) []*storobj.Object {
	t.Helper()
	var (
		filter  *filters.LocalFilter
		keyword *searchparams.KeywordRanking
	)
	on := &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(prop)}
	switch legacyMigrationLayouts[code].canonical(prop) {
	case helpers.BucketSearchableFromPropNameLSM(prop):
		keyword = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{prop}, Query: "apple"}
	case helpers.BucketRangeableFromPropNameLSM(prop):
		filter = &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorGreaterThanEqual, On: on,
			Value: &filters.Value{Value: 0, Type: schema.DataTypeInt},
		}}
	default:
		filter = &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual, On: on,
			Value: &filters.Value{Value: "apple", Type: schema.DataTypeText},
		}}
	}
	found, _, err := shard.ObjectSearch(ctx, 2*legacyObjects, filter, keyword, nil, nil,
		additional.Properties{}, nil)
	require.NoError(t, err)
	return found
}

func TestPropertyIndexRemoved(t *testing.T) {
	for _, tt := range []struct {
		name     string
		setting  *bool
		expected bool
	}{
		{name: "nil setting", setting: nil, expected: false},
		{name: "true setting", setting: boolPtr(true), expected: false},
		{name: "false setting", setting: boolPtr(false), expected: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, propertyIndexRemoved(tt.setting))
		})
	}
}

type bucketDirsFixture struct {
	t       *testing.T
	lsmPath string
	dirs    bucketDirs
}

func newBucketDirsFixture(t *testing.T) bucketDirsFixture {
	t.Helper()
	lsmPath := filepath.Join(t.TempDir(), "lsm")
	require.NoError(t, os.MkdirAll(lsmPath, 0o777))
	return bucketDirsFixture{t: t, lsmPath: lsmPath, dirs: shardBucketDirs(lsmPath)}
}

func (f bucketDirsFixture) mkdirs(names ...string) {
	f.t.Helper()
	for _, name := range names {
		require.NoError(f.t, os.MkdirAll(filepath.Join(f.lsmPath, name), 0o777))
		require.NoError(f.t, os.WriteFile(filepath.Join(f.lsmPath, name, "segment-1.db"), []byte(name), 0o600))
	}
}

func (f bucketDirsFixture) exists(name string) bool {
	info, err := os.Stat(filepath.Join(f.lsmPath, name))
	return err == nil && info.IsDir()
}

func TestOnlyAHandleNamingOneDirectoryBecomesAPath(t *testing.T) {
	tests := []struct {
		name             string
		dir              string
		refusedAsPath    bool
		refusedAsRemoval bool
	}{
		{name: "names none", dir: "", refusedAsPath: true, refusedAsRemoval: false},
		{name: "the root itself", dir: ".", refusedAsPath: true, refusedAsRemoval: true},
		{name: "the parent of the root", dir: "..", refusedAsPath: true, refusedAsRemoval: true},
		{name: "a join back to the root", dir: "x/..", refusedAsPath: true, refusedAsRemoval: true},
		{name: "a nested path", dir: "sub/dir", refusedAsPath: true, refusedAsRemoval: true},
		{name: "an absolute path", dir: "/etc", refusedAsPath: true, refusedAsRemoval: true},
		{name: "one directory", dir: "property_title_searchable", refusedAsPath: false, refusedAsRemoval: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newBucketDirsFixture(t)
			f.mkdirs(helpers.ObjectsBucketLSM, "property_title_searchable")

			path, err := f.dirs.Path(tt.dir, "a recorded directory")
			if tt.refusedAsPath {
				require.Error(t, err)
				require.Empty(t, path)
			} else {
				require.NoError(t, err)
				require.Equal(t, filepath.Join(f.lsmPath, tt.dir), path)
			}

			err = f.dirs.Discard(tt.dir, "a recorded directory")
			if tt.refusedAsRemoval {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.DirExists(t, f.lsmPath, "the shard's LSM directory")
			require.True(t, f.exists(helpers.ObjectsBucketLSM), "the shard's object store")
			require.Equal(t, tt.refusedAsPath, f.exists("property_title_searchable"),
				"only a handle that names one directory removes one")
		})
	}
}

func TestPromoteGuardsBothHandles(t *testing.T) {
	tests := []struct {
		name    string
		from    string
		to      string
		refused bool
	}{
		{name: "the directory to promote escapes", from: "..", to: "property_title_searchable", refused: true},
		{name: "the name to promote onto escapes", from: "property_title__g42_ingest", to: "..", refused: true},
		{name: "neither names a directory", from: "", to: "", refused: true},
		{name: "both name one directory", from: "property_title__g42_ingest", to: "property_title_rangeable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newBucketDirsFixture(t)
			f.mkdirs("property_title__g42_ingest", "property_title_searchable")

			err := f.dirs.Promote(tt.from, tt.to)

			require.DirExists(t, f.lsmPath, "the shard's LSM directory")
			if tt.refused {
				require.Error(t, err)
				require.True(t, f.exists("property_title__g42_ingest"), "a refused promotion renames nothing")
				require.True(t, f.exists("property_title_searchable"))
				return
			}
			require.NoError(t, err)
			require.False(t, f.exists("property_title__g42_ingest"))
			data, err := os.ReadFile(filepath.Join(f.lsmPath, tt.to, "segment-1.db"))
			require.NoError(t, err)
			require.Equal(t, "property_title__g42_ingest", string(data),
				"the canonical name holds the staged data")
		})
	}
}

func TestDirExistsSeparatesAbsentFromUnreadable(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		sealRoot bool
		want     bool
		wantErr  bool
	}{
		{name: "a directory that is there", dir: "property_title_searchable", want: true},
		{name: "nothing at that name", dir: "property_gone_searchable"},
		{name: "a regular file is not a directory", dir: "afile"},
		{name: "a handle naming none reads as absent, not as an error", dir: ""},
		{name: "a handle that does not name one directory under the shard", dir: "sub/dir", wantErr: true},
		{
			name: "a directory the process may not stat is not an absent one",
			dir:  "property_title_searchable", sealRoot: true, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newBucketDirsFixture(t)
			f.mkdirs("property_title_searchable")
			require.NoError(t, os.WriteFile(filepath.Join(f.lsmPath, "afile"), []byte("x"), 0o600))
			if tt.sealRoot {
				if os.Geteuid() == 0 {
					t.Skip("root traverses a 0o000 directory, so the permission error cannot arise")
				}
				require.NoError(t, os.Chmod(f.lsmPath, 0o000))
				t.Cleanup(func() { os.Chmod(f.lsmPath, 0o700) })
			}

			there, err := f.dirs.Exists(tt.dir)
			if tt.wantErr {
				require.Error(t, err)
				require.False(t, there, "a probe that could not answer must not report a directory")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, there)
		})
	}
}
