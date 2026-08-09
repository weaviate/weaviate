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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestUnexplainedEmptyRangeableProps drives the startup audit against real
// directory layouts. The damaged layout is the durable state a cluster
// is left in by weaviate/0-weaviate-issues#464: the schema claims a range
// index, the shard holds objects, and the canonical rangeable bucket is an
// empty directory that no migration explains.
//
// The two negative cases matter as much as the positive one — an audit
// that cries wolf during every legitimate migration would be turned off.
func TestUnexplainedEmptyRangeableProps(t *testing.T) {
	trueVal, falseVal := true, false
	class := &models.Class{
		Class: "Readings",
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexRangeFilters: &trueVal},
			{Name: "ignored", DataType: []string{"int"}, IndexRangeFilters: &falseVal},
			{Name: "title", DataType: []string{"text"}},
		},
	}
	rangeableBucket := helpers.BucketRangeableFromPropNameLSM("score")
	trackerDir := migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, []string{"score"}) + "_1"
	trackerDirGen2 := migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, []string{"score"}) + "_2"
	multiPropTrackerDir := migrationDirWithProps(
		MigrationDirPrefixFilterableToRangeable, []string{"depth", "score", "weight"}) + "_1"
	otherPropTrackerDir := migrationDirWithProps(
		MigrationDirPrefixFilterableToRangeable, []string{"weight"}) + "_1"
	filterableTrackerDir := migrationDirWithProps(
		MigrationDirPrefixEnableFilterable, []string{"score"}) + "_1"

	// A property at the front or the back of a multi-prop tracker needs a
	// class that promises a range index on that property.
	rangeableOn := func(propName string) *models.Class {
		return &models.Class{
			Class:      "Readings",
			Properties: []*models.Property{{Name: propName, DataType: []string{"int"}, IndexRangeFilters: &trueVal}},
		}
	}

	tests := []struct {
		name string
		// layout maps a path relative to lsmPath to its file contents.
		// A path ending in "/" is created as an empty directory.
		layout map[string]string
		// class defaults to the shared multi-property class above.
		class *models.Class
		want  []string
		// wantState is the tracker state the enumerator must report for
		// every prop in want. It decides which of the two WARNs fires, so
		// it is pinned here rather than only at the shard-init call site.
		wantState rangeableMigrationExplanation
	}{
		{
			name: "464-damaged: schema promises the index, bucket dir is empty, no tracker",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/":                        "",
			},
			want: []string{"score"},
		},
		{
			name: "464-damaged: bucket dir absent entirely",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
			},
			want: []string{"score"},
		},
		{
			name: "healthy: the rangeable bucket has a segment",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/segment-001.db":          "range data",
			},
			want: nil,
		},
		{
			name: "healthy: data still only in the write-ahead log",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/segment-001.wal":         "range data",
			},
			want: nil,
		},
		{
			name: "mid-migration: empty bucket explained by an in-flight tracker",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/":                        "",
				".migrations/" + trackerDir + "/started.mig": "x",
			},
			want: nil,
		},
		{
			// The audit runs right after FinalizeCompletedMigrations, which
			// promotes every tidied generation and removes its tracker. A
			// tidied tracker that is still here means that promotion failed,
			// and the empty index is what the failure left behind.
			name: "failed promotion: a tidied tracker survived this startup's finalize",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/":                        "",
				".migrations/" + trackerDir + "/started.mig": "x",
				".migrations/" + trackerDir + "/tidied.mig":  "x",
			},
			want:      []string{"score"},
			wantState: rangeableMigrationPromotionFailed,
		},
		{
			// A generation still running explains the empty index no matter
			// what an older, unpromotable one left behind. ReadDir returns
			// the older one first, so this is what stops the tidied arm
			// from short-circuiting the scan: it goes red if that arm
			// returns instead of recording the state and reading on.
			name: "mid-migration: a running generation outranks a failed older one",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":     "objects",
				rangeableBucket + "/":                            "",
				".migrations/" + trackerDir + "/started.mig":     "x",
				".migrations/" + trackerDir + "/tidied.mig":      "x",
				".migrations/" + trackerDirGen2 + "/started.mig": "x",
			},
			want: nil,
		},
		{
			// A migration submitted for several properties at once writes
			// ONE tracker naming all of them. Matching only the
			// single-property dir name would miss it and report every
			// shard the migration is still running on as damaged.
			name: "mid-migration: tracker names several properties at once",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":          "objects",
				rangeableBucket + "/":                                 "",
				".migrations/" + multiPropTrackerDir + "/started.mig": "x",
			},
			want: nil,
		},
		{
			name: "mid-migration: the property is the first one the tracker names",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":          "objects",
				helpers.BucketRangeableFromPropNameLSM("depth") + "/": "",
				".migrations/" + multiPropTrackerDir + "/started.mig": "x",
			},
			class: rangeableOn("depth"),
			want:  nil,
		},
		{
			name: "mid-migration: the property is the last one the tracker names",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":           "objects",
				helpers.BucketRangeableFromPropNameLSM("weight") + "/": "",
				".migrations/" + multiPropTrackerDir + "/started.mig":  "x",
			},
			class: rangeableOn("weight"),
			want:  nil,
		},
		{
			name: "unrelated property's tracker does not explain this one",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":          "objects",
				rangeableBucket + "/":                                 "",
				".migrations/" + otherPropTrackerDir + "/started.mig": "x",
			},
			want: []string{"score"},
		},
		{
			name: "a tracker from another migration family does not explain it",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":           "objects",
				rangeableBucket + "/":                                  "",
				".migrations/" + filterableTrackerDir + "/started.mig": "x",
			},
			want: []string{"score"},
		},
		{
			// Several paths create the tracker dir before finding there
			// is nothing to do. A leftover explains nothing, and counting
			// it would suppress this warning on every boot from then on.
			name: "an empty tracker dir explains nothing",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/":                        "",
				".migrations/" + trackerDir + "/":            "",
			},
			want: []string{"score"},
		},
		{
			// The marker the finalizer leaves behind records a migration
			// that is over; it must not stand in for one in flight.
			name: "a finalized marker does not explain it",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db":   "objects",
				rangeableBucket + "/":                          "",
				".migrations/" + trackerDir + ".finalized.mig": "",
			},
			want: []string{"score"},
		},
		{
			name: "empty shard: no objects, so an empty index is correct",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/": "",
				rangeableBucket + "/":          "",
			},
			want: nil,
		},
		{
			name: "empty shard: zero-length segment does not count as data",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "",
				rangeableBucket + "/":                        "",
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			for rel, content := range tc.layout {
				full := filepath.Join(lsmPath, rel)
				if rel[len(rel)-1] == '/' {
					require.NoError(t, os.MkdirAll(full, 0o755))
					continue
				}
				require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
				require.NoError(t, os.WriteFile(full, []byte(content), 0o644))
			}
			tcClass := tc.class
			if tcClass == nil {
				tcClass = class
			}
			got := unexplainedEmptyRangeableProps(lsmPath, tcClass)
			var gotNames []string
			for _, prop := range got {
				gotNames = append(gotNames, prop.name)
				require.Equal(t, tc.wantState, prop.state,
					"the enumerator must hand its caller the state that picks the WARN")
			}
			require.Equal(t, tc.want, gotNames)
		})
	}
}

// TestUnexplainedEmptyRangeableProps_NoClass pins the nil guard: shard init
// must not panic on a class the schema could not supply.
func TestUnexplainedEmptyRangeableProps_NoClass(t *testing.T) {
	require.Nil(t, unexplainedEmptyRangeableProps(t.TempDir(), nil))
}

// TestShardInit_WarnsOnUnexplainedEmptyRangeableIndex pins the audit's
// call site and the WARN an operator greps for. The table above drives
// the pure decision helper; nothing else reaches shard init, and the
// message is the branch's only operator-facing deliverable.
//
// The damage state is reproduced the way #464 produces it: a shard that
// holds objects is restarted under a schema that has since turned the
// range index on, with no migration to explain the missing data.
func TestShardInit_WarnsOnUnexplainedEmptyRangeableIndex(t *testing.T) {
	ctx := testCtx()
	className := "RangeableAudit_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	shd, idx := testShardWithSettings(t, ctx, newFilterableToRangeableTestClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeFilterableToRangeableTestObjects(t, 5, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	logger, hook := logrustest.NewNullLogger()
	idx.logger = logger

	enabled := true
	restartClass := newFilterableToRangeableTestClass(className)
	restartClass.Properties[0].IndexRangeFilters = &enabled
	shd2, err := idx.initShard(ctx, shardName, restartClass, nil, true, true)
	require.NoError(t, err)
	defer shd2.Shutdown(ctx)

	var audits []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "rangeable_index_audit" {
			audits = append(audits, entry)
		}
	}
	require.Len(t, audits, 1, "shard init must emit exactly one audit WARN for the damaged property")
	entry := audits[0]
	require.Equal(t, logrus.WarnLevel, entry.Level)
	require.Equal(t, className, entry.Data["collection"])
	require.Equal(t, shardName, entry.Data["shard"])
	require.Equal(t, propName, entry.Data["property"])
	require.Contains(t, entry.Message,
		`PUT /v1/schema/`+className+`/indexes/`+propName+` {"rangeable":{"rebuild":true}}`,
		"the repair command must stay copy-pasteable")
}

// TestShardInit_WarnsOnFailedPromotion pins the second audit message: when
// the reason the index is empty is a promotion this startup's finalize pass
// could not complete, the WARN must say so and point at the retry rather
// than tell the operator to rebuild an index that is already built and only
// needs to be moved into place.
//
// The tracker that survives the failed promotion is also what would silence
// the audit if a finished migration still counted as an explanation.
func TestShardInit_WarnsOnFailedPromotion(t *testing.T) {
	ctx := testCtx()
	className := "RangeableAuditRetry_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	shd, idx := testShardWithSettings(t, ctx, newFilterableToRangeableTestClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeFilterableToRangeableTestObjects(t, 5, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	shardName, lsmPath := shard.Name(), shard.pathLSM()
	require.NoError(t, shard.Shutdown(ctx))

	// A tidied tracker whose generation cannot be promoted: finalize needs
	// swapped.mig too, so it fails, keeps the tracker and writes no marker.
	trackerDir := filepath.Join(lsmPath, ".migrations",
		migrationDirWithProps(MigrationDirPrefixFilterableToRangeable, []string{propName})+"_1")
	require.NoError(t, os.MkdirAll(trackerDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(trackerDir, "tidied.mig"), []byte("x"), 0o644))

	logger, hook := logrustest.NewNullLogger()
	idx.logger = logger

	enabled := true
	restartClass := newFilterableToRangeableTestClass(className)
	restartClass.Properties[0].IndexRangeFilters = &enabled
	shd2, err := idx.initShard(ctx, shardName, restartClass, nil, true, true)
	require.NoError(t, err)
	defer shd2.Shutdown(ctx)

	require.DirExists(t, trackerDir, "the failed promotion must keep its tracker for the retry")

	var audits []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "rangeable_index_audit" {
			audits = append(audits, entry)
		}
	}
	require.Len(t, audits, 1,
		"a tracker left behind by a failed promotion must not silence the audit")
	require.Equal(t, logrus.WarnLevel, audits[0].Level)
	require.Equal(t, propName, audits[0].Data["property"])
	require.Contains(t, audits[0].Message, "could not be promoted to its canonical directory")
	require.Contains(t, audits[0].Message, "retried at every startup")
}
