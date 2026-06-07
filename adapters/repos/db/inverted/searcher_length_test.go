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

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Regression test for https://github.com/weaviate/weaviate/issues/11015
//
// Filtering by the length of a property whose inverted index is disabled
// (e.g. an int[] with indexFilterable=false) used to read a property-length
// bucket that was never created, surfacing as a confusing "bucket not found"
// error. When length indexing is enabled globally but the property has no
// inverted index, it should instead return the same MissingFilterableIndex
// error that other meta properties (e.g. reference count) already return.
func TestSearcher_extractPropertyLength_missingInvertedIndex(t *testing.T) {
	s := &Searcher{}

	classWithLengthIndex := func() *models.Class {
		return &models.Class{
			Class: "BugReportLengthIndex",
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexPropertyLength: true,
			},
		}
	}

	t.Run("returns clear error when property has no inverted index", func(t *testing.T) {
		notFilterable := false
		class := classWithLengthIndex()
		prop := &models.Property{
			Name:            "ints_non_filterable",
			DataType:        []string{"int[]"},
			IndexFilterable: &notFilterable,
		}

		pv, err := s.extractPropertyLength(prop, schema.DataTypeInt, 2,
			filters.OperatorEqual, class)

		require.Error(t, err)
		assert.Nil(t, pv)
		assert.Contains(t, err.Error(), "requires inverted index")
	})

	t.Run("succeeds when property is filterable", func(t *testing.T) {
		filterable := true
		class := classWithLengthIndex()
		prop := &models.Property{
			Name:            "ints_filterable",
			DataType:        []string{"int[]"},
			IndexFilterable: &filterable,
		}

		pv, err := s.extractPropertyLength(prop, schema.DataTypeInt, 2,
			filters.OperatorEqual, class)

		require.NoError(t, err)
		require.NotNil(t, pv)
		assert.True(t, pv.hasFilterableIndex)
	})

	t.Run("defers to downstream global-config error when length indexing is off", func(t *testing.T) {
		notFilterable := false
		// IndexPropertyLength is off globally; this is reported with a dedicated
		// message later in readFromBucket, so the extractor must not shadow it.
		class := &models.Class{
			Class:               "BugReportLengthIndex",
			InvertedIndexConfig: &models.InvertedIndexConfig{IndexPropertyLength: false},
		}
		prop := &models.Property{
			Name:            "ints_non_filterable",
			DataType:        []string{"int[]"},
			IndexFilterable: &notFilterable,
		}

		_, err := s.extractPropertyLength(prop, schema.DataTypeInt, 2,
			filters.OperatorEqual, class)

		require.NoError(t, err)
	})
}

// Same defect family as #11015 on the adjacent null-state journey: a `null`/
// `isNull` filter on a property with no inverted index read a property-null
// bucket that was never created. It must return the same clear error.
func TestSearcher_extractPropertyNull_missingInvertedIndex(t *testing.T) {
	s := &Searcher{}

	classWithNullState := func() *models.Class {
		return &models.Class{
			Class: "BugReportNullIndex",
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexNullState: true,
			},
		}
	}

	t.Run("returns clear error when property has no inverted index", func(t *testing.T) {
		notFilterable := false
		class := classWithNullState()
		prop := &models.Property{
			Name:            "ints_non_filterable",
			DataType:        []string{"int[]"},
			IndexFilterable: &notFilterable,
		}

		pv, err := s.extractPropertyNull(prop, schema.DataTypeBoolean, true,
			filters.OperatorEqual, class)

		require.Error(t, err)
		assert.Nil(t, pv)
		assert.Contains(t, err.Error(), "requires inverted index")
	})

	t.Run("succeeds when property is filterable", func(t *testing.T) {
		filterable := true
		class := classWithNullState()
		prop := &models.Property{
			Name:            "ints_filterable",
			DataType:        []string{"int[]"},
			IndexFilterable: &filterable,
		}

		pv, err := s.extractPropertyNull(prop, schema.DataTypeBoolean, true,
			filters.OperatorEqual, class)

		require.NoError(t, err)
		require.NotNil(t, pv)
		assert.True(t, pv.hasFilterableIndex)
	})

	t.Run("defers to downstream global-config error when null-state indexing is off", func(t *testing.T) {
		notFilterable := false
		class := &models.Class{
			Class:               "BugReportNullIndex",
			InvertedIndexConfig: &models.InvertedIndexConfig{IndexNullState: false},
		}
		prop := &models.Property{
			Name:            "ints_non_filterable",
			DataType:        []string{"int[]"},
			IndexFilterable: &notFilterable,
		}

		_, err := s.extractPropertyNull(prop, schema.DataTypeBoolean, true,
			filters.OperatorEqual, class)

		require.NoError(t, err)
	})
}
