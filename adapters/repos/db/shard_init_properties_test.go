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
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// createsGeoIndex decides which props leave the per-property fan-out, so it has
// to agree with the branch createPropertyValueIndex actually takes.
func TestCreatesGeoIndex(t *testing.T) {
	geoDataType := []string{string(schema.DataTypeGeoCoordinates)}

	tests := []struct {
		name string
		prop *models.Property
		want bool
	}{
		{
			name: "geo prop with filterable defaulted",
			prop: &models.Property{Name: "location", DataType: geoDataType},
			want: true,
		},
		{
			name: "geo prop with filterable set",
			prop: &models.Property{Name: "location", DataType: geoDataType, IndexFilterable: boolPtr(true)},
			want: true,
		},
		{
			// createPropertyValueIndex never reaches initGeoProp for these, so
			// batching them would claim work that is not happening
			name: "geo prop with filterable off",
			prop: &models.Property{Name: "location", DataType: geoDataType, IndexFilterable: boolPtr(false)},
			want: false,
		},
		{
			name: "text prop",
			prop: &models.Property{Name: "name", DataType: schema.DataTypeText.PropString()},
			want: false,
		},
		{
			name: "int prop",
			prop: &models.Property{Name: "count", DataType: schema.DataTypeInt.PropString()},
			want: false,
		},
		{
			name: "reference prop",
			prop: &models.Property{Name: "parkedAt", DataType: []string{"MultiRefParkingGarage"}},
			want: false,
		},
		{
			name: "prop naming two datatypes",
			prop: &models.Property{Name: "parkedAt", DataType: []string{"MultiRefParkingGarage", "MultiRefParkingLot"}},
			want: false,
		},
		{
			name: "prop with no datatype",
			prop: &models.Property{Name: "broken"},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, createsGeoIndex(test.prop))
		})
	}
}

// The load sweep deletes every sidecar a v1.39 migration wrote only if isSidecarDirOf knows it.
func TestEverySidecarSuffixIsASidecar(t *testing.T) {
	const main = "property_category"
	suffixes := []string{
		"__blockmax_reindex", "__roaringset_reindex", "__rangeable_reindex", "__retokenize_reindex",
		"__filt_retokenize_reindex", "__enable_filterable_reindex", "__enable_searchable_reindex",
		"__rebuild_searchable_reindex",
	}
	for _, layout := range legacyMigrationLayouts {
		suffixes = append(suffixes, layout.staged, layout.backup)
	}

	roles := map[string]struct{}{}
	for _, gen := range []int{1, 7} {
		for _, suffix := range suffixes {
			dir := main + suffix + genSuffix(gen)
			assert.Truef(t, isSidecarDirOf(dir, main), "%q is not recognized as a sidecar", dir)
			roles[sidecarRoleWord(strings.TrimPrefix(suffix+genSuffix(gen), "__"))] = struct{}{}
		}
	}
	require.ElementsMatch(t, sidecarRoleWords, slices.Collect(maps.Keys(roles)))
}

func TestIsSidecarDirOfRejectsOtherPropertiesBuckets(t *testing.T) {
	const main = "property_category"

	tests := []struct {
		name string
		dir  string
		want bool
	}{
		{name: "the main bucket itself", dir: main},
		{name: "a property whose name carries the separator", dir: main + "__extra"},
		{name: "a generation-suffixed main bucket", dir: main + "__gen2"},
		{name: "a longer property's main bucket", dir: main + "_x"},
		{name: "a sidecar", dir: main + "__enable_filterable_ingest_1", want: true},
		{name: "category__reindex's own bucket, wrongly accepted", dir: main + "__reindex", want: true},
		{name: "category__ingest_0's own bucket, wrongly accepted", dir: main + "__ingest_0", want: true},
		{name: "a property named after a number", dir: main + "__12", want: false},
		{name: "a blockmax backup dir", dir: main + "__blockmax_map_3", want: true},
		{name: "a filterable backup dir", dir: main + "__enable_filterable_backup_1", want: true},
		{name: "a property whose name extends a role word", dir: main + "__ingest_x", want: false},
		{name: "a property whose name ends in a role word and a separator", dir: main + "__ingest_", want: false},
		{name: "a sidecar whose suffix carries a strategy word", dir: main + "__blockmax_ingest", want: true},
		{
			name: "the dir a crashed bucket replacement left behind",
			dir:  main + lsmkv.ReplacedBucketDirSuffix, want: true,
		},
		// weaviate/weaviate#12621: sweeping "category" deletes this property's own main bucket.
		{name: "a property whose name ends in a role word", dir: main + "__ingest", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSidecarDirOf(tc.dir, main))
		})
	}
}
