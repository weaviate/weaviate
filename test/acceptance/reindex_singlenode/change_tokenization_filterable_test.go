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

package reindex_singlenode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testChangeTokenizationFilterable pins the journey class around
// change-tokenization for properties with various index configurations.
// A filterable-only text property is retokenized via
// PUT .../index/filterable {"tokenization":X}; PUT .../index/searchable on
// the same property is rejected ("searchable bucket not found"). This test
// covers every (data type, IndexFilterable, IndexSearchable) state against
// both index types.
func testChangeTokenizationFilterable(t *testing.T, restURI string) {
	t.Run("filterable_only_text__filterable_tokenization", func(t *testing.T) {
		testFilterableTokenizationFilterableOnly(t, restURI, "text")
	})
	t.Run("filterable_only_text_array__filterable_tokenization", func(t *testing.T) {
		testFilterableTokenizationFilterableOnly(t, restURI, "text[]")
	})
	t.Run("filterable_only_text__searchable_tokenization_rejected", func(t *testing.T) {
		testSearchableTokenizationOnFilterableOnlyRejected(t, restURI)
	})
	t.Run("searchable_only_text__filterable_tokenization_rejected", func(t *testing.T) {
		testFilterableTokenizationOnSearchableOnlyRejected(t, restURI)
	})
	t.Run("both_indexes_text__filterable_tokenization_retokenizes_filterable_only", func(t *testing.T) {
		testFilterableTokenizationOnBothIndexes(t, restURI)
	})
	t.Run("non_text_filterable__filterable_tokenization_rejected", func(t *testing.T) {
		testFilterableTokenizationOnNonText(t, restURI)
	})
}

func testFilterableTokenizationFilterableOnly(t *testing.T, restURI, dataType string) {
	className := "FilterableTokOnly_" + dataTypeSlug(dataType)
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{dataType},
				IndexFilterable: &trueVal,
				IndexSearchable: &falseVal,
				Tokenization:    "field",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	// Insert a few objects so Equal() can match post-migration.
	if dataType == "text" {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: className, Properties: map[string]interface{}{"name": "alpha"},
		}))
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: className, Properties: map[string]interface{}{"name": "beta"},
		}))
	} else {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: className, Properties: map[string]interface{}{"name": []string{"alpha", "beta"}},
		}))
	}

	// Pre-migration: filterable with "field" tokenization. Equal('alpha')
	// matches by exact field value.
	require.Equal(t, 1, equalFilterHits(t, className, "name", "alpha"),
		"pre-migration: Equal('alpha') with field tokenization must match exactly one object")

	// Submit PUT .../index/filterable {"tokenization":"word"} — the
	// filterable-only retokenize shape.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "name", "filterable",
		`{"tokenization":"word"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// Schema flag: Tokenization must now be "word".
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, className)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == "name" {
				return p.Tokenization == "word"
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond,
		"property tokenization must flip to 'word' after change-tokenization-filterable")

	// Post-migration: Equal() now uses word tokenization so an exact
	// match against the unchanged value still hits.
	require.Equal(t, 1, equalFilterHits(t, className, "name", "alpha"),
		"post-migration: Equal('alpha') must still match exactly one object with word tokenization")
}

// testSearchableTokenizationOnFilterableOnlyRejected pins that creating a
// searchable index with a tokenization diverging from the property's
// existing filterable tokenization is rejected (400 naming the filterable
// index) rather than silently desynchronizing the two buckets.
func testSearchableTokenizationOnFilterableOnlyRejected(t *testing.T, restURI string) {
	const className = "SearchableTokOnFilterableOnly"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &falseVal,
				Tokenization:    "field",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	resp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, className, "name", "searchable",
		`{"tokenization":"word"}`)
	require.Equal(t, 400, resp.StatusCode,
		"PUT .../index/searchable {tokenization:word} on filterable-only (tok=field) must 400, not 5xx or 202")
	require.Contains(t, resp.Body, "filterable",
		"the 400 body must name the filterable index so the caller knows to retokenize it first; "+
			"current body: %s", resp.Body)
}

func testFilterableTokenizationOnSearchableOnlyRejected(t *testing.T, restURI string) {
	const className = "FilterableTokOnSearchableOnly"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            "body",
				DataType:        []string{"text"},
				IndexFilterable: &falseVal,
				IndexSearchable: &trueVal,
				Tokenization:    "word",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	resp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, className, "body", "filterable",
		`{"tokenization":"field"}`)
	require.Equal(t, 400, resp.StatusCode,
		"PUT .../index/filterable {tokenization:field} on searchable-only (tok=word) must 400")
	// The property has no filterable index, so this takes the create path;
	// creating one with a tokenization that diverges from the property's
	// current tokenization ("word") is rejected.
	require.Contains(t, resp.Body, "must match the property's current tokenization",
		"the 400 body must explain the tokenization must match on filterable creation; current body: %s", resp.Body)
}

func testFilterableTokenizationOnBothIndexes(t *testing.T, restURI string) {
	const className = "FilterableTokBothIndexes"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
				Tokenization:    "field",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	require.NoError(t, helper.CreateObject(t, &models.Object{
		Class: className, Properties: map[string]interface{}{"name": "gamma"},
	}))

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "name", "filterable",
		`{"tokenization":"word"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	require.Eventually(t, func() bool {
		c := helper.GetClass(t, className)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == "name" {
				return p.Tokenization == "word"
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond,
		"property tokenization must flip to 'word' even though both indexes exist")

	require.Equal(t, 1, equalFilterHits(t, className, "name", "gamma"),
		"post-migration: filterable Equal('gamma') still matches")
}

func testFilterableTokenizationOnNonText(t *testing.T, restURI string) {
	const className = "FilterableTokNonText"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	resp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, className, "score", "filterable",
		`{"tokenization":"word"}`)
	require.Equal(t, 400, resp.StatusCode,
		"PUT .../index/filterable {tokenization:word} on a non-text property must 400")
	require.Contains(t, resp.Body, "text type",
		"the 400 body must say the property is not a text type; current body: %s", resp.Body)
}

func dataTypeSlug(dt string) string {
	switch dt {
	case "text[]":
		return "textArray"
	case "text":
		return "text"
	}
	return dt
}

// TestSuppress_ChangeTokenizationFilterable ensures this file compiles in
// isolation; the suite entry point is the t.Run("ChangeTokenizationFilterable")
// in suite_test.go.
func TestSuppress_ChangeTokenizationFilterable(t *testing.T) {
	assert.NotNil(t, testChangeTokenizationFilterable)
}
