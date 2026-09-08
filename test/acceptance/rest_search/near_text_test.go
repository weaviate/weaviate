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

// Package rest_search covers POST /v1/search/{collection}/near-text end to
// end: the raw wire contract and the live error-status mapping.
package rest_search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	authorID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300001")
	studioID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300011")
	bookID   = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300012")
	comicID  = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300013")
	movie1ID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300002")
	movie2ID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300003")
)

// postSearch POSTs a raw JSON search of the given type and decodes the raw
// JSON reply, so assertions run against the wire shape, not generated
// models.
func postSearch(t *testing.T, collection, searchType string, body map[string]any) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s:%s/v1/search/%s/%s",
		helper.ServerHost, helper.ServerPort, collection, searchType)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

func postNearText(t *testing.T, collection string, body map[string]any) (int, map[string]any) {
	return postSearch(t, collection, "near-text", body)
}

// errMessage extracts the message from either error shape: the handler's
// ErrorResponse ({"error":[{"message":...}]}) or the swagger bind-tier
// ({"code":...,"message":...}).
func errMessage(t *testing.T, out map[string]any) string {
	t.Helper()
	if items, ok := out["error"].([]any); ok && len(items) > 0 {
		if item, ok := items[0].(map[string]any); ok {
			if msg, ok := item["message"].(string); ok {
				return msg
			}
		}
	}
	if msg, ok := out["message"].(string); ok {
		return msg
	}
	t.Fatalf("no error message in response: %v", out)
	return ""
}

func results(t *testing.T, out map[string]any) []any {
	t.Helper()
	res, ok := out["results"].([]any)
	require.True(t, ok, "response has no results array: %v", out)
	return res
}

func hit(t *testing.T, out map[string]any, i int) map[string]any {
	t.Helper()
	res := results(t, out)
	require.Greater(t, len(res), i)
	h, ok := res[i].(map[string]any)
	require.True(t, ok)
	return h
}

func metadataOf(t *testing.T, h map[string]any) map[string]any {
	t.Helper()
	metadata, ok := h["metadata"].(map[string]any)
	require.True(t, ok, "hit has no metadata key: %v", h)
	return metadata
}

func propertiesOf(t *testing.T, h map[string]any) map[string]any {
	t.Helper()
	props, ok := h["properties"].(map[string]any)
	require.True(t, ok, "hit has no properties key: %v", h)
	return props
}

// referencesOf reads one reference selection off a hit (or off a referenced
// object, which carries the same envelope).
func referencesOf(t *testing.T, h map[string]any, name string) []map[string]any {
	t.Helper()
	references, ok := h["references"].(map[string]any)
	require.True(t, ok, "no references key: %v", h)
	raw, ok := references[name].([]any)
	require.True(t, ok, "reference %q missing or not an array: %v", name, references)
	refs := make([]map[string]any, len(raw))
	for i := range raw {
		ref, ok := raw[i].(map[string]any)
		require.True(t, ok, "reference %q entry %d is not an object: %v", name, i, raw[i])
		refs[i] = ref
	}
	return refs
}

// beaconsTo builds the reference value pointing at one object.
func beaconsTo(collection string, id strfmt.UUID) []any {
	return []any{
		map[string]any{"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", collection, id)},
	}
}

func idOf(t *testing.T, h map[string]any) string {
	t.Helper()
	id, ok := h["id"].(string)
	require.True(t, ok, "hit has no id: %v", h)
	return id
}

// unsearchableLedgerClass has no searchable property: ints are never
// keyword-searchable and its sole text property disables its searchable
// index.
func unsearchableLedgerClass(vectorizer string) *models.Class {
	return &models.Class{
		Class:      "Ledger",
		Vectorizer: vectorizer,
		Properties: []*models.Property{
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			{
				Name: "code", DataType: schema.DataTypeText.PropString(),
				IndexSearchable: func() *bool { b := false; return &b }(),
			},
		},
	}
}

// assertScoredHits asserts the scored-envelope shape shared by the keyword
// and hybrid endpoints: tookMs present, every hit a UUID id with a score in
// its metadata, scores descending. requirePositive additionally demands
// every score be above zero (bm25; hybrid's relative-score fusion may
// normalize the last hit to 0).
func assertScoredHits(t *testing.T, out map[string]any, wantHits int, requirePositive bool) {
	t.Helper()
	res := results(t, out)
	require.Len(t, res, wantHits)
	_, ok := out["tookMs"].(float64)
	assert.True(t, ok, "tookMs missing or not a number: %v", out)

	prev := float64(-1)
	for i := range res {
		h := hit(t, out, i)
		require.True(t, strfmt.IsUUID(idOf(t, h)), "id is not a UUID: %v", h)
		score, ok := metadataOf(t, h)["score"].(float64)
		require.True(t, ok, "score missing in metadata: %v", h)
		if requirePositive {
			assert.Greater(t, score, float64(0))
		}
		if prev >= 0 {
			assert.LessOrEqual(t, score, prev, "scores must descend")
		}
		prev = score
	}
}

func movieClass() *models.Class {
	return &models.Class{
		Class:      "Movie",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			// no inverted index: filtering on this property cannot run
			{
				Name: "rating", DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: func() *bool { b := false; return &b }(),
			},
			// must behave as ordinary user data under "properties", despite
			// sharing its name with the envelope's metadata field
			{Name: "metadata", DataType: schema.DataTypeText.PropString()},
			{
				Name: "details", DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "duration", DataType: schema.DataTypeInt.PropString()},
					{Name: "summary", DataType: schema.DataTypeText.PropString()},
				},
			},
			{Name: "hasAuthor", DataType: []string{"Author"}},
			// multi-target: the reference points at either collection
			{Name: "basedOn", DataType: []string{"Book", "Comic"}},
		},
	}
}

func TestRESTSearchNearText(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		// the endpoint is experimental and off by default; enable it
		WithWeaviateEnv("EXPERIMENTAL_REST_SEARCH_ENABLED", "true").
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	studioClass := &models.Class{
		Class:      "Studio",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
			{Name: "logo", DataType: schema.DataTypeBlob.PropString()},
		},
	}
	authorClass := &models.Class{
		Class:      "Author",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
			// second hop: Movie -> hasAuthor -> Author -> worksFor -> Studio
			{Name: "worksFor", DataType: []string{"Studio"}},
		},
	}
	bookClass := &models.Class{
		Class:      "Book",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "isbn", DataType: schema.DataTypeText.PropString()},
		},
	}
	comicClass := &models.Class{
		Class:      "Comic",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "issue", DataType: schema.DataTypeInt.PropString()},
		},
	}
	// vectorizer "none": near-text has nothing to embed the query with
	notesClass := &models.Class{
		Class:      "Notes",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	// non-cosine index: certainty cannot be computed
	paintingClass := &models.Class{
		Class:             "Painting",
		Vectorizer:        "text2vec-contextionary",
		VectorIndexConfig: map[string]any{"distance": "l2-squared"},
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	journalClass := &models.Class{
		Class:      "Journal",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	// referenced collections first: a beacon is validated against the
	// pre-existing schema and objects
	classes := []*models.Class{
		studioClass, authorClass, bookClass, comicClass,
		movieClass(), notesClass, paintingClass, journalClass,
	}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Journal", []*models.Tenant{{Name: "tenantA"}})

	// each referenced object must exist before the beacon that points at it
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID:    studioID,
		Class: "Studio",
		Properties: map[string]any{
			"name": "big studio",
			"logo": "aGVsbG8=",
		},
	}))
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID:    authorID,
		Class: "Author",
		Properties: map[string]any{
			"name":     "famous writer",
			"worksFor": beaconsTo("Studio", studioID),
		},
	}))
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID:    bookID,
		Class: "Book",
		Properties: map[string]any{
			"title": "the source novel",
			"isbn":  "978-0",
		},
	}))
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID:    comicID,
		Class: "Comic",
		Properties: map[string]any{
			"title": "the source comic",
			"issue": 7,
		},
	}))

	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:    movie1ID,
			Class: "Movie",
			Properties: map[string]any{
				"title":    "spaceship galaxy adventure",
				"year":     2021,
				"metadata": "user data",
				"details": map[string]any{
					"duration": 120,
					"summary":  "a journey through space",
				},
				"hasAuthor": beaconsTo("Author", authorID),
				"basedOn":   append(beaconsTo("Book", bookID), beaconsTo("Comic", comicID)...),
			},
		},
		{
			ID:    movie2ID,
			Class: "Movie",
			Properties: map[string]any{
				"title":    "cooking dinner recipes",
				"year":     1999,
				"metadata": "more user data",
			},
		},
		{
			ID:     strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300004"),
			Class:  "Journal",
			Tenant: "tenantA",
			Properties: map[string]any{
				"title": "travel diary",
			},
		},
		{
			ID:    strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300005"),
			Class: "Painting",
			Properties: map[string]any{
				"title": "sunflowers",
			},
		},
	})

	t.Run("happy path: envelope with id, properties, metadata, tookMs", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
			"returnMetadata":   []string{"distance"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		res := results(t, out)
		require.Len(t, res, 2)
		_, ok := out["tookMs"].(float64)
		assert.True(t, ok, "tookMs missing or not a number: %v", out)

		first := hit(t, out, 0)
		props := propertiesOf(t, first)
		assert.Equal(t, "spaceship galaxy adventure", props["title"])
		assert.NotContains(t, props, "year", "unselected property must not be returned")

		var prev float64
		for i := range res {
			h := hit(t, out, i)
			id := idOf(t, h)
			require.True(t, strfmt.IsUUID(id), "id is not a UUID: %q", id)
			metadata := metadataOf(t, h)
			assert.NotContains(t, metadata, "id", "the id lives on the envelope, not in metadata")
			distance, ok := metadata["distance"].(float64)
			require.True(t, ok, "distance missing in metadata: %v", metadata)
			assert.GreaterOrEqual(t, distance, prev, "distances must ascend")
			prev = distance
		}
		assert.Equal(t, movie1ID.String(), idOf(t, first))
	})

	t.Run("metadata comes back under camelCase wire keys", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":          []string{"spaceship galaxy"},
			"returnMetadata": []string{"distance", "certainty", "score", "explainScore", "creationTime", "lastUpdateTime"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		metadata := metadataOf(t, hit(t, out, 0))
		// always computable for a cosine near-text search
		for _, key := range []string{"distance", "certainty", "creationTime", "lastUpdateTime"} {
			assert.Contains(t, metadata, key)
		}
		// score/explainScore may be absent for pure vector search, but any
		// key that is present must use its camelCase wire name
		for key := range metadata {
			assert.Contains(t, []string{
				"distance", "certainty", "score", "explainScore", "creationTime", "lastUpdateTime",
			}, key, "unexpected metadata wire key")
		}
	})

	t.Run("the id is always returned, even without returnMetadata", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		assert.Equal(t, movie1ID.String(), idOf(t, first))
		// no non-id metadata was requested: the metadata block is omitted
		assert.NotContains(t, first, "metadata")
	})

	t.Run("id is not a returnMetadata value", func(t *testing.T) {
		// returnMetadata selects metadata keys only; "id" is outside the
		// swagger enum and is rejected at bind time with the standard
		// ErrorResponse body
		status, out := postNearText(t, "Movie", map[string]any{
			"query":          []string{"spaceship galaxy"},
			"returnMetadata": []string{"id"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "returnMetadata")
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})

	t.Run("a property named metadata is ordinary user data under properties", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":          []string{"spaceship galaxy"},
			"returnMetadata": []string{"distance"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		// the user property and the envelope's metadata coexist: no shadowing
		assert.Equal(t, "user data", propertiesOf(t, first)["metadata"])
		metadata := metadataOf(t, first)
		_, ok := metadata["distance"].(float64)
		assert.True(t, ok, "distance missing in metadata: %v", metadata)
		assert.Equal(t, movie1ID.String(), idOf(t, first))
	})

	t.Run("where filter narrows results", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query": []string{"spaceship galaxy"},
			"where": map[string]any{
				"path":     []string{"year"},
				"operator": "LessThan",
				"valueInt": 2000,
			},
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		res := results(t, out)
		require.Len(t, res, 1)
		assert.Equal(t, "cooking dinner recipes", propertiesOf(t, hit(t, out, 0))["title"])
	})

	t.Run("filter on a property without an inverted index is a 422", func(t *testing.T) {
		// live guard for the inverted.MissingIndexError mapping
		status, out := postNearText(t, "Movie", map[string]any{
			"query": []string{"spaceship galaxy"},
			"where": map[string]any{
				"path":     []string{"rating"},
				"operator": "GreaterThan",
				"valueInt": 3,
			},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "indexFilterable")
	})

	t.Run("selected reference properties come back under references", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
			"returnReferences": []map[string]any{
				{"linkOn": "hasAuthor", "returnProperties": []string{"name"}},
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		assert.NotContains(t, propertiesOf(t, first), "hasAuthor",
			"reference selections must not appear under properties")
		refs := referencesOf(t, first, "hasAuthor")
		require.Len(t, refs, 1)
		assert.Equal(t, "famous writer", propertiesOf(t, refs[0])["name"])
		// single-target, nothing else requested
		assert.NotContains(t, refs[0], "collection")
		assert.NotContains(t, refs[0], "metadata")
		assert.NotContains(t, refs[0], "references")
	})

	t.Run("references omitted when the request selects none", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.NotContains(t, hit(t, out, 0), "references")
	})

	t.Run("a reference in returnProperties is a 400", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"hasAuthor"},
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "returnReferences")
	})

	t.Run("a second hop returns all non-ref non-blob properties by default", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
			"returnReferences": []map[string]any{
				{"linkOn": "hasAuthor", "returnReferences": []map[string]any{{"linkOn": "worksFor"}}},
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		author := referencesOf(t, hit(t, out, 0), "hasAuthor")[0]
		assert.Equal(t, "famous writer", propertiesOf(t, author)["name"])
		studio := referencesOf(t, author, "worksFor")[0]
		props := propertiesOf(t, studio)
		assert.Equal(t, "big studio", props["name"])
		// blobs stay out at every level
		assert.NotContains(t, props, "logo")
	})

	t.Run("per-reference metadata", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
			"returnReferences": []map[string]any{
				{
					"linkOn":           "hasAuthor",
					"returnProperties": []string{"name"},
					"returnMetadata":   []string{"id", "creationTime", "lastUpdateTime"},
				},
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		ref := referencesOf(t, hit(t, out, 0), "hasAuthor")[0]
		metadata, ok := ref["metadata"].(map[string]any)
		require.True(t, ok, "referenced object has no metadata: %v", ref)
		assert.Equal(t, authorID.String(), metadata["id"])
		assert.NotZero(t, metadata["creationTime"])
		assert.NotZero(t, metadata["lastUpdateTime"])
	})

	t.Run("a multi-target reference carries the collection of each object", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
			"returnReferences": []map[string]any{
				{"linkOn": "basedOn", "targetCollection": "Book", "returnProperties": []string{"isbn"}},
				{"linkOn": "basedOn", "targetCollection": "Comic", "returnProperties": []string{"issue"}},
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		refs := referencesOf(t, hit(t, out, 0), "basedOn")
		require.Len(t, refs, 2)
		byCollection := map[string]map[string]any{}
		for _, ref := range refs {
			collection, ok := ref["collection"].(string)
			require.True(t, ok, "multi-target reference without a collection: %v", ref)
			byCollection[collection] = propertiesOf(t, ref)
		}
		require.Contains(t, byCollection, "Book")
		require.Contains(t, byCollection, "Comic")
		assert.Equal(t, "978-0", byCollection["Book"]["isbn"])
		assert.Equal(t, float64(7), byCollection["Comic"]["issue"])
		assert.NotContains(t, byCollection["Book"], "title")
	})

	t.Run("selecting one target of a multi-target reference returns only that one", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"title"},
			"returnReferences": []map[string]any{
				{"linkOn": "basedOn", "targetCollection": "Book", "returnProperties": []string{"isbn"}},
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		refs := referencesOf(t, hit(t, out, 0), "basedOn")
		require.Len(t, refs, 1)
		assert.Equal(t, "Book", refs[0]["collection"])
	})

	for name, tc := range map[string]struct {
		selector map[string]any
		status   int
		message  string
	}{
		"unknown reference property": {
			map[string]any{"linkOn": "nope"}, http.StatusBadRequest, "no such prop",
		},
		"linkOn naming a plain property": {
			map[string]any{"linkOn": "title"}, http.StatusBadRequest, "returnProperties",
		},
		"multi-target without targetCollection": {
			map[string]any{"linkOn": "basedOn"}, http.StatusBadRequest, "needs targetCollection",
		},
		"target the reference does not point at": {
			map[string]any{"linkOn": "hasAuthor", "targetCollection": "Book"},
			http.StatusBadRequest, "does not target collection",
		},
		"unknown property on the referenced collection": {
			map[string]any{"linkOn": "hasAuthor", "returnProperties": []string{"nope"}},
			http.StatusBadRequest, "no such prop",
		},
		"missing linkOn": {
			map[string]any{"returnProperties": []string{"name"}},
			http.StatusUnprocessableEntity, "linkOn",
		},
		"metadata outside the selector vocabulary": {
			map[string]any{"linkOn": "hasAuthor", "returnMetadata": []string{"distance"}},
			http.StatusUnprocessableEntity, "returnMetadata",
		},
	} {
		t.Run("returnReferences: "+name, func(t *testing.T) {
			status, out := postNearText(t, "Movie", map[string]any{
				"query":            []string{"spaceship galaxy"},
				"returnReferences": []map[string]any{tc.selector},
			})
			require.Equal(t, tc.status, status, "%v", out)
			assert.Contains(t, errMessage(t, out), tc.message)
		})
	}

	t.Run("nested object properties are returned as nested maps", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship galaxy"},
			"returnProperties": []string{"details"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		details, ok := propertiesOf(t, first)["details"].(map[string]any)
		require.True(t, ok, "details missing or not an object: %v", first)
		assert.Equal(t, "a journey through space", details["summary"])
		assert.Equal(t, float64(120), details["duration"])
	})

	t.Run("no vectorizer is a 422, not a 500", func(t *testing.T) {
		// live guard for the typed-error ordering (config 422, not 500)
		status, out := postNearText(t, "Notes", map[string]any{
			"query": []string{"anything"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "vectorizer")
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postNearText(t, "Ghosts", map[string]any{
			"query": []string{"anything"},
		})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postNearText(t, "Journal", map[string]any{
			"query":  []string{"travel"},
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postNearText(t, "Journal", map[string]any{
			"query":  []string{"travel"},
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postNearText(t, "Journal", map[string]any{
			"query": []string{"travel"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postNearText(t, "Movie", map[string]any{
			"query":  []string{"spaceship"},
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})

	t.Run("certainty on a non-cosine index is a 422", func(t *testing.T) {
		status, out := postNearText(t, "Painting", map[string]any{
			"query":     []string{"sunflowers"},
			"certainty": 0.7,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "certainty")
	})

	t.Run("unknown property in returnProperties is a 400", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"query":            []string{"spaceship"},
			"returnProperties": []string{"nonexistent"},
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "no such prop")
	})

	t.Run("absent query is rejected at bind time", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]any{
			"limit": 1,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "query")
		// bind-tier errors use the same ErrorResponse shape as handler errors
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})
}

// TestRESTSearchDisabled pins the opt-in default: with
// EXPERIMENTAL_REST_SEARCH_ENABLED unset, every search answers 422 before
// any schema access.
func TestRESTSearchDisabled(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		// no EXPERIMENTAL_REST_SEARCH_ENABLED: the feature is off by default
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	status, out := postNearText(t, "Anything", map[string]any{
		"query": []string{"anything"},
	})
	require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
	assert.Contains(t, errMessage(t, out), "not enabled")

	// the gate covers every search endpoint
	status, out = postBm25(t, "Anything", map[string]any{
		"query": "anything",
	})
	require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
	assert.Contains(t, errMessage(t, out), "not enabled")

	status, out = postHybrid(t, "Anything", map[string]any{
		"query": "anything",
	})
	require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
	assert.Contains(t, errMessage(t, out), "not enabled")

	status, out = postNearObject(t, "Anything", map[string]any{
		"id": "dd44bbee-ca5f-4db7-a412-5fc6a2300001",
	})
	require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
	assert.Contains(t, errMessage(t, out), "not enabled")

	// the gate also covers the sibling aggregate endpoint
	status, out = postAggregate(t, "Anything", map[string]any{})
	require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
	assert.Contains(t, errMessage(t, out), "not enabled")
}
