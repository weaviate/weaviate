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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
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
			name: "mid-migration: swap committed, rename deferred to this startup",
			layout: map[string]string{
				helpers.ObjectsBucketLSM + "/segment-001.db": "objects",
				rangeableBucket + "/":                        "",
				".migrations/" + trackerDir + "/started.mig": "x",
				".migrations/" + trackerDir + "/tidied.mig":  "x",
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
			require.Equal(t, tc.want, unexplainedEmptyRangeableProps(lsmPath, tcClass))
		})
	}
}

// TestUnexplainedEmptyRangeableProps_NoClass pins the nil guard: shard init
// must not panic on a class the schema could not supply.
func TestUnexplainedEmptyRangeableProps_NoClass(t *testing.T) {
	require.Nil(t, unexplainedEmptyRangeableProps(t.TempDir(), nil))
}
