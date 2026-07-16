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

package search

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	nearTextArgs "github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
)

func namedVectorsClass(vectors ...string) *models.Class {
	class := &models.Class{
		Class: "MultiVec",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		VectorConfig: map[string]models.VectorConfig{},
	}
	for _, name := range vectors {
		class.VectorConfig[name] = models.VectorConfig{
			Vectorizer:        map[string]any{"text2vec-contextionary": map[string]any{}},
			VectorIndexConfig: hnsw.UserConfig{Distance: "cosine"},
		}
	}
	return class
}

// decodeModel unmarshals a JSON body into the typed request model, the way
// the swagger JSON consumer does (unknown fields ignored, type mismatches
// fail). A decode failure maps to the 400 the consumer returns live.
func decodeModel(body string) (*models.SearchNearTextRequest, *APIError) {
	var req models.SearchNearTextRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, newAPIError(http.StatusBadRequest, "invalid request body: %v", err)
	}
	return &req, nil
}

// fixtureGetClass resolves collections from the fixture schema with the same
// not-found sentinel the real classGetterWithAuthz produces.
func fixtureGetClass(deps *testDeps) classGetterFunc {
	return func(name string) (*models.Class, error) {
		if c, ok := deps.schemaReader.classes[name]; ok {
			return c, nil
		}
		return nil, fmt.Errorf("%w %s in schema", errCollectionNotFound, name)
	}
}

// buildParams runs the full body -> dto.GetParams conversion against the
// Movie/Author fixture schema, including the reserved-field 422 check that
// the handler runs before buildNearTextParams.
func buildParams(t *testing.T, class *models.Class, body string) (*fakeSearcher, *APIError) {
	t.Helper()
	deps := newTestHandler(t)
	deps.schemaReader.classes[class.Class] = class

	parsed, apiErr := decodeModel(body)
	if apiErr != nil {
		return nil, apiErr
	}
	if apiErr := checkReservedFields(&parsed.SearchCommon); apiErr != nil {
		return nil, apiErr
	}

	params, apiErr := deps.handler.buildNearTextParams(class, class.Class, parsed, fixtureGetClass(deps), nil)
	if apiErr != nil {
		return nil, apiErr
	}
	deps.searcher.lastParams = params
	return deps.searcher, nil
}

// decodeBm25Model unmarshals a JSON body into the typed bm25 request model,
// the way the swagger JSON consumer does (unknown fields ignored, type
// mismatches fail). A decode failure maps to the 400 the consumer returns
// live.
func decodeBm25Model(body string) (*models.SearchBm25Request, *APIError) {
	var req models.SearchBm25Request
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, newAPIError(http.StatusBadRequest, "invalid request body: %v", err)
	}
	return &req, nil
}

// buildBm25 runs the full bm25 body -> dto.GetParams conversion against the
// fixture schema, including the reserved-field 422 check that the handler
// runs before buildBm25Params.
func buildBm25(t *testing.T, class *models.Class, body string) (*fakeSearcher, *APIError) {
	t.Helper()
	deps := newTestHandler(t)
	deps.schemaReader.classes[class.Class] = class

	parsed, apiErr := decodeBm25Model(body)
	if apiErr != nil {
		return nil, apiErr
	}
	if apiErr := checkReservedFields(&parsed.SearchCommon); apiErr != nil {
		return nil, apiErr
	}

	params, apiErr := deps.handler.buildBm25Params(class, class.Class, parsed, fixtureGetClass(deps), nil)
	if apiErr != nil {
		return nil, apiErr
	}
	deps.searcher.lastParams = params
	return deps.searcher, nil
}

func nearTextFromParams(t *testing.T, searcher *fakeSearcher) *nearTextArgs.NearTextParams {
	t.Helper()
	raw, ok := searcher.lastParams.ModuleParams["nearText"]
	require.True(t, ok)
	params, ok := raw.(*nearTextArgs.NearTextParams)
	require.True(t, ok)
	return params
}

func TestUnknownFieldIgnored(t *testing.T) {
	// unknown fields are silently dropped at decode
	searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"not_a_field":1}`)
	require.Nil(t, apiErr)
	assert.Equal(t, []string{"space"}, nearTextFromParams(t, searcher).Values)
}

func TestReservedFieldsRejected(t *testing.T) {
	// each reserved field, set to a type-appropriate non-null value, is a
	// 422 "not yet supported"
	reserved := map[string]string{
		"single_prompt":     `"x"`,
		"grouped_task":      `"x"`,
		"group_by":          `"x"`,
		"number_of_groups":  `2`,
		"objects_per_group": `2`,
		"rerank_property":   `"x"`,
		"rerank_query":      `"x"`,
	}
	for field, value := range reserved {
		t.Run(field, func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(),
				fmt.Sprintf(`{"query":["space"],"%s":%s}`, field, value))
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "not yet supported")
			assert.Contains(t, apiErr.Error(), field)
		})
	}
}

func TestReservedFieldsAbsentOrNullAllowed(t *testing.T) {
	// a null (or omitted) reserved field leaves its pointer nil, so it is
	// treated as absent and the request proceeds
	for _, body := range []string{
		`{"query":["space"]}`,
		`{"query":["space"],"group_by":null}`,
	} {
		t.Run(body, func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			assert.Nil(t, apiErr)
		})
	}
}

func TestParseQuery(t *testing.T) {
	t.Run("single string", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space opera"]}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"space opera"}, nearTextFromParams(t, searcher).Values)
	})

	t.Run("array of strings", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space","opera"]}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"space", "opera"}, nearTextFromParams(t, searcher).Values)
	})

	for name, body := range map[string]string{
		"missing":       `{}`,
		"null":          `{"query":null}`,
		"empty string":  `{"query":[""]}`,
		"empty array":   `{"query":[]}`,
		"empty concept": `{"query":["space",""]}`,
		"number":        `{"query":42}`,
		"object":        `{"query":{"text":"space"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		})
	}
}

func TestParsePagination(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"]}`)
		require.Nil(t, apiErr)
		pagination := searcher.lastParams.Pagination
		assert.Equal(t, 10, pagination.Limit) // handler default
		assert.Equal(t, 0, pagination.Offset)
		assert.Equal(t, 0, pagination.Autocut)
	})

	t.Run("explicit values", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"limit":3,"offset":6,"auto_limit":2}`)
		require.Nil(t, apiErr)
		pagination := searcher.lastParams.Pagination
		assert.Equal(t, 3, pagination.Limit)
		assert.Equal(t, 6, pagination.Offset)
		assert.Equal(t, 2, pagination.Autocut)
		// the near-text limit follows the pagination limit, like gRPC
		assert.Equal(t, 3, nearTextFromParams(t, searcher).Limit)
	})

	for name, body := range map[string]string{
		"negative limit":      `{"query":["space"],"limit":-1}`,
		"negative offset":     `{"query":["space"],"offset":-1}`,
		"negative auto_limit": `{"query":["space"],"auto_limit":-1}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		})
	}

	// pages beyond QUERY_MAXIMUM_RESULTS (fixture: 10000) are a 400 here,
	// not a db-layer error; huge values must not overflow into the negative
	// special limit flags
	for name, body := range map[string]string{
		"limit above maximum":          `{"query":["space"],"limit":10001}`,
		"offset above maximum":         `{"query":["space"],"offset":10001}`,
		"offset + limit above maximum": `{"query":["space"],"offset":6000,"limit":6000}`,
		"int64-boundary limit":         `{"query":["space"],"limit":9223372036854775807}`,
		"overflowing sum":              `{"query":["space"],"offset":9223372036854775806,"limit":2}`,
	} {
		t.Run(name+" is a 400", func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "QUERY_MAXIMUM_RESULTS")
		})
	}

	t.Run("offset + limit at the maximum is valid", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"offset":5000,"limit":5000}`)
		require.Nil(t, apiErr)
		assert.Equal(t, 5000, searcher.lastParams.Pagination.Limit)
		assert.Equal(t, 5000, searcher.lastParams.Pagination.Offset)
	})
}

func TestParseCertaintyAndDistance(t *testing.T) {
	t.Run("distance", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"distance":0.4}`)
		require.Nil(t, apiErr)
		params := nearTextFromParams(t, searcher)
		assert.Equal(t, 0.4, params.Distance)
		assert.True(t, params.WithDistance)
	})

	t.Run("certainty", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"certainty":0.8}`)
		require.Nil(t, apiErr)
		params := nearTextFromParams(t, searcher)
		assert.Equal(t, 0.8, params.Certainty)
		assert.False(t, params.WithDistance)
	})

	t.Run("both is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"certainty":0.8,"distance":0.4}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("certainty on non-cosine is a 422", func(t *testing.T) {
		class := movieClass()
		class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
		_, apiErr := buildParams(t, class, `{"query":["space"],"certainty":0.8}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	})

	// certainty is normalized: values outside [0,1] are a 400, the
	// boundaries are valid
	for name, body := range map[string]string{
		"negative certainty": `{"query":["space"],"certainty":-0.1}`,
		"certainty above 1":  `{"query":["space"],"certainty":1.5}`,
	} {
		t.Run(name+" is a 400", func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "certainty")
		})
	}
	for name, body := range map[string]string{
		"certainty 0": `{"query":["space"],"certainty":0}`,
		"certainty 1": `{"query":["space"],"certainty":1}`,
	} {
		t.Run(name+" is valid", func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			require.Nil(t, apiErr)
		})
	}
}

func TestTargetVectors(t *testing.T) {
	t.Run("single named vector is selected implicitly", func(t *testing.T) {
		searcher, apiErr := buildParams(t, namedVectorsClass("title_vec"), `{"query":["space"]}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"title_vec"}, nearTextFromParams(t, searcher).TargetVectors)
	})

	t.Run("multiple named vectors require target_vector", func(t *testing.T) {
		_, apiErr := buildParams(t, namedVectorsClass("title_vec", "plot_vec"), `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "multiple vectors")
	})

	t.Run("explicit target_vector", func(t *testing.T) {
		searcher, apiErr := buildParams(t, namedVectorsClass("title_vec", "plot_vec"),
			`{"query":["space"],"target_vector":"plot_vec"}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"plot_vec"}, nearTextFromParams(t, searcher).TargetVectors)
	})

	t.Run("unknown target_vector is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, namedVectorsClass("title_vec"),
			`{"query":["space"],"target_vector":"nope"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestNoVectorizerIsA422(t *testing.T) {
	t.Run("legacy vectorizer none", func(t *testing.T) {
		class := movieClass()
		class.Vectorizer = "none"
		_, apiErr := buildParams(t, class, `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	})

	t.Run("named vector with vectorizer none", func(t *testing.T) {
		class := namedVectorsClass("title_vec")
		class.VectorConfig["title_vec"] = models.VectorConfig{
			Vectorizer:        map[string]any{"none": map[string]any{}},
			VectorIndexConfig: hnsw.UserConfig{Distance: "cosine"},
		}
		_, apiErr := buildParams(t, class, `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	})
}

func TestParseReturnMetadata(t *testing.T) {
	t.Run("omitted still requests the id", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.False(t, addl.Distance)
	})

	t.Run("empty list still requests the id", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_metadata":[]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.False(t, addl.Distance)
	})

	t.Run("id is not a metadata key", func(t *testing.T) {
		// return_metadata selects metadata keys only; the id is a top-level
		// result field. Live, the swagger enum rejects "id" at bind (422);
		// the parser's own 400 covers the direct-call path.
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_metadata":["id"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(),
			"expected one of distance, certainty, score, explain_score, creation_time, last_update_time")
	})

	t.Run("all supported values", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_metadata":["distance","certainty","score","explain_score","creation_time","last_update_time"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.True(t, addl.Distance)
		assert.True(t, addl.Certainty)
		assert.True(t, addl.Score)
		assert.True(t, addl.ExplainScore)
		assert.True(t, addl.CreationTimeUnix)
		assert.True(t, addl.LastUpdateTimeUnix)
	})

	t.Run("certainty dropped on non-cosine", func(t *testing.T) {
		class := movieClass()
		class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
		searcher, apiErr := buildParams(t, class, `{"query":["space"],"return_metadata":["certainty"]}`)
		require.Nil(t, apiErr)
		assert.False(t, searcher.lastParams.AdditionalProperties.Certainty)
	})

	t.Run("unknown value is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_metadata":["vector"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestParseReturnProperties(t *testing.T) {
	t.Run("omitted selects all non-ref non-blob properties", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		names := make([]string, len(props))
		for i, prop := range props {
			names[i] = prop.Name
		}
		// poster (blob) and hasAuthor (ref) are excluded
		assert.ElementsMatch(t, []string{"title", "year"}, names)
		assert.False(t, searcher.lastParams.AdditionalProperties.NoProps)
	})

	t.Run("subset", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_properties":["title"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		require.Len(t, props, 1)
		assert.Equal(t, "title", props[0].Name)
		assert.True(t, props[0].IsPrimitive)
	})

	t.Run("empty list means no properties", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_properties":[]}`)
		require.Nil(t, apiErr)
		assert.Empty(t, searcher.lastParams.Properties)
		assert.True(t, searcher.lastParams.AdditionalProperties.NoProps)
	})

	t.Run("unknown property is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_properties":["nope"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("dot-path selects across a reference", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_properties":["title","hasAuthor.name"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		require.Len(t, props, 2)
		refProp := props[1]
		assert.Equal(t, "hasAuthor", refProp.Name)
		require.Len(t, refProp.Refs, 1)
		assert.Equal(t, "Author", refProp.Refs[0].ClassName)
		require.Len(t, refProp.Refs[0].RefProperties, 1)
		assert.Equal(t, "name", refProp.Refs[0].RefProperties[0].Name)
	})

	t.Run("dot-paths with the same root merge", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_properties":["hasAuthor.name","hasAuthor.age"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		require.Len(t, props, 1)
		require.Len(t, props[0].Refs, 1)
		require.Len(t, props[0].Refs[0].RefProperties, 2)
	})

	t.Run("bare reference name selects all target properties", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_properties":["hasAuthor"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		require.Len(t, props, 1)
		require.Len(t, props[0].Refs, 1)
		refProps := props[0].Refs[0].RefProperties
		names := make([]string, len(refProps))
		for i, prop := range refProps {
			names[i] = prop.Name
		}
		assert.ElementsMatch(t, []string{"name", "age"}, names)
	})

	t.Run("dot-path on a non-ref property is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_properties":["title.name"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("two reference hops are deferred with a 422", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_properties":["hasAuthor.name.first"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "not yet supported")
	})

	t.Run("unknown property on the referenced class is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_properties":["hasAuthor.nope"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

// TestParseWhere covers the handler's filter validation. Structural
// WhereFilter violations are caught by swagger validation (422) before the
// handler; the 400s here are the semantic failures the handler owns.
func TestParseWhere(t *testing.T) {
	t.Run("valid filter", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"where":{"operator":"And","operands":[
				{"path":["year"],"operator":"GreaterThanEqual","valueInt":1980},
				{"path":["title"],"operator":"Equal","valueText":"Dune"}]}}`)
		require.Nil(t, apiErr)
		require.NotNil(t, searcher.lastParams.Filters)
		assert.Len(t, searcher.lastParams.Filters.Root.Operands, 2)
	})

	t.Run("malformed filter is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"where":{"operator":42}}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("unknown operator is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"where":{"operator":"Nope","path":["year"],"valueInt":1}}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("unknown property is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"where":{"operator":"Equal","path":["nope"],"valueText":"x"}}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestBm25KeywordRanking(t *testing.T) {
	t.Run("query maps to KeywordRanking, not module params", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(), `{"query":"space opera"}`)
		require.Nil(t, apiErr)
		kw := searcher.lastParams.KeywordRanking
		require.NotNil(t, kw)
		assert.Equal(t, "bm25", kw.Type)
		assert.Equal(t, "space opera", kw.Query)
		assert.Empty(t, kw.Properties)
		assert.False(t, kw.AdditionalExplanations)
		// a keyword search must not carry any vector-search params
		assert.Empty(t, searcher.lastParams.ModuleParams)
	})

	// swagger's required validation rejects an absent or null query with 422
	// before the handler; the handler's own 400 covers the explicit empty
	// string (which passes bind) and the direct-call path
	for name, body := range map[string]string{
		"empty string": `{"query":""}`,
		"missing":      `{}`,
		"null":         `{"query":null}`,
	} {
		t.Run(name+" query is a 400", func(t *testing.T) {
			_, apiErr := buildBm25(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "query")
		})
	}

	t.Run("query is string-only: the array form fails decode", func(t *testing.T) {
		_, apiErr := buildBm25(t, movieClass(), `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

// TestBm25QueryProperties pins the gRPC-parity property handling: names pass
// through with their first letter lowercased, and a "^boost" suffix is not
// interpreted here — the searcher parses it.
func TestBm25QueryProperties(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{"pass through", `{"query":"space","query_properties":["title"]}`, []string{"title"}},
		{"boost suffix passes through", `{"query":"space","query_properties":["title^2"]}`, []string{"title^2"}},
		{
			"first letter lowercased (gRPC parity)",
			`{"query":"space","query_properties":["Title^2","Year"]}`,
			[]string{"title^2", "year"},
		},
		{
			// only the FIRST letter is lowercased (schema.LowercaseFirstLetterOfStrings),
			// interior caps are preserved — a whole-string strings.ToLower would
			// yield "camelcaseprop" and fail this case.
			"first letter only, interior caps preserved",
			`{"query":"space","query_properties":["CamelCaseProp"]}`,
			[]string{"camelCaseProp"},
		},
		{"omitted searches all searchable properties", `{"query":"space"}`, nil},
		{"empty searches all searchable properties", `{"query":"space","query_properties":[]}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher, apiErr := buildBm25(t, movieClass(), tt.body)
			require.Nil(t, apiErr)
			kw := searcher.lastParams.KeywordRanking
			require.NotNil(t, kw)
			if tt.want == nil {
				assert.Empty(t, kw.Properties)
			} else {
				assert.Equal(t, tt.want, kw.Properties)
			}
		})
	}
}

func TestBm25ReturnMetadata(t *testing.T) {
	t.Run("score and explain_score", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","return_metadata":["score","explain_score"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.True(t, addl.Score)
		assert.True(t, addl.ExplainScore)
		// explain_score also switches on the ranker's explanations (gRPC
		// parity: AdditionalExplanations follows ExplainScore)
		assert.True(t, searcher.lastParams.KeywordRanking.AdditionalExplanations)
	})

	t.Run("explain_score omitted leaves explanations off", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(), `{"query":"space","return_metadata":["score"]}`)
		require.Nil(t, apiErr)
		assert.False(t, searcher.lastParams.KeywordRanking.AdditionalExplanations)
	})

	t.Run("certainty is inapplicable and silently dropped", func(t *testing.T) {
		// gRPC parity: a non-vector search clears the certainty flag. The
		// distance flag stays set but is inert — the explorer emits distance
		// only for vector searches, so the response omits it either way
		// (silent drop; covered end to end in the reply tests).
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","return_metadata":["distance","certainty","score"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.False(t, addl.Certainty)
		assert.True(t, addl.Distance)
		assert.True(t, addl.Score)
	})

	t.Run("creation and update times", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","return_metadata":["creation_time","last_update_time"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.CreationTimeUnix)
		assert.True(t, addl.LastUpdateTimeUnix)
	})
}

// TestBm25NeedsNoVectorizer: bm25 is a pure keyword search — collections
// without any vectorizer module are fully searchable (unlike near-text,
// which 422s on them).
func TestBm25NeedsNoVectorizer(t *testing.T) {
	t.Run("legacy vectorizer none", func(t *testing.T) {
		class := movieClass()
		class.Vectorizer = "none"
		_, apiErr := buildBm25(t, class, `{"query":"space"}`)
		assert.Nil(t, apiErr)
	})

	t.Run("named vector with vectorizer none", func(t *testing.T) {
		class := namedVectorsClass("title_vec")
		class.VectorConfig["title_vec"] = models.VectorConfig{
			Vectorizer:        map[string]any{"none": map[string]any{}},
			VectorIndexConfig: hnsw.UserConfig{Distance: "cosine"},
		}
		_, apiErr := buildBm25(t, class, `{"query":"space"}`)
		assert.Nil(t, apiErr)
	})
}

// TestBm25SharedFields smoke-tests that the SearchCommon fields flow through
// the shared parsers for bm25 exactly as they do for near-text.
func TestBm25SharedFields(t *testing.T) {
	t.Run("pagination and consistency", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","limit":3,"offset":6,"auto_limit":2,"consistency_level":"QUORUM"}`)
		require.Nil(t, apiErr)
		pagination := searcher.lastParams.Pagination
		assert.Equal(t, 3, pagination.Limit)
		assert.Equal(t, 6, pagination.Offset)
		assert.Equal(t, 2, pagination.Autocut)
		require.NotNil(t, searcher.lastParams.ReplicationProperties)
		assert.Equal(t, "QUORUM", searcher.lastParams.ReplicationProperties.ConsistencyLevel)
	})

	t.Run("where filter", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","where":{"path":["year"],"operator":"GreaterThanEqual","valueInt":1980}}`)
		require.Nil(t, apiErr)
		require.NotNil(t, searcher.lastParams.Filters)
	})

	t.Run("return_properties subset", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(), `{"query":"space","return_properties":["title"]}`)
		require.Nil(t, apiErr)
		require.Len(t, searcher.lastParams.Properties, 1)
		assert.Equal(t, "title", searcher.lastParams.Properties[0].Name)
	})

	t.Run("near-text-only fields are unknown fields and ignored", func(t *testing.T) {
		// option-2 contract: fields outside the bm25 schema (target_vector,
		// certainty, distance live in the near-text extension) drop at decode
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","target_vector":"nope","certainty":0.9,"distance":0.1,"not_a_field":1}`)
		require.Nil(t, apiErr)
		require.NotNil(t, searcher.lastParams.KeywordRanking)
	})
}

// decodeHybridModel unmarshals a JSON body into the typed hybrid request
// model, the way the swagger JSON consumer does (unknown fields ignored, type
// mismatches fail). A decode failure maps to the 400 the consumer returns
// live.
func decodeHybridModel(body string) (*models.SearchHybridRequest, *APIError) {
	var req models.SearchHybridRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, newAPIError(http.StatusBadRequest, "invalid request body: %v", err)
	}
	return &req, nil
}

// buildHybrid runs the full hybrid body -> dto.GetParams conversion against
// the fixture schema, including the reserved-field 422 check that the handler
// runs before buildHybridParams.
func buildHybrid(t *testing.T, class *models.Class, body string) (*fakeSearcher, *APIError) {
	t.Helper()
	deps := newTestHandler(t)
	deps.schemaReader.classes[class.Class] = class

	parsed, apiErr := decodeHybridModel(body)
	if apiErr != nil {
		return nil, apiErr
	}
	if apiErr := checkReservedFields(&parsed.SearchCommon); apiErr != nil {
		return nil, apiErr
	}

	params, apiErr := deps.handler.buildHybridParams(class, class.Class, parsed, fixtureGetClass(deps), nil)
	if apiErr != nil {
		return nil, apiErr
	}
	deps.searcher.lastParams = params
	return deps.searcher, nil
}

func TestHybridParams(t *testing.T) {
	t.Run("query maps to HybridSearch, not module or keyword params", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space opera"}`)
		require.Nil(t, apiErr)
		hybrid := searcher.lastParams.HybridSearch
		require.NotNil(t, hybrid)
		assert.Equal(t, "space opera", hybrid.Query)
		assert.Empty(t, hybrid.Properties)
		assert.Empty(t, searcher.lastParams.ModuleParams)
		assert.Nil(t, searcher.lastParams.KeywordRanking)
	})

	// swagger's required validation rejects an absent or null query with 422
	// before the handler; the handler's own 400 covers the explicit empty
	// string (which passes bind) and the direct-call path
	for name, body := range map[string]string{
		"empty string": `{"query":""}`,
		"missing":      `{}`,
		"null":         `{"query":null}`,
	} {
		t.Run(name+" query is a 400", func(t *testing.T) {
			_, apiErr := buildHybrid(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "query")
		})
	}

	t.Run("query is string-only: the array form fails decode", func(t *testing.T) {
		_, apiErr := buildHybrid(t, movieClass(), `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

// TestHybridAlpha pins the gRPC/GraphQL-parity alpha semantics: omitted
// defaults to the shared 0.75, 0 and 1 are valid endpoints of the range, and
// anything outside [0, 1] is the GraphQL parser's rejection.
func TestHybridAlpha(t *testing.T) {
	t.Run("omitted defaults to 0.75", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space"}`)
		require.Nil(t, apiErr)
		assert.Equal(t, 0.75, searcher.lastParams.HybridSearch.Alpha)
	})

	for body, want := range map[string]float64{
		`{"query":"space","alpha":0}`:    0,
		`{"query":"space","alpha":0.3}`:  0.3,
		`{"query":"space","alpha":1}`:    1,
		`{"query":"space","alpha":null}`: 0.75,
	} {
		t.Run(body, func(t *testing.T) {
			searcher, apiErr := buildHybrid(t, movieClass(), body)
			require.Nil(t, apiErr)
			assert.Equal(t, want, searcher.lastParams.HybridSearch.Alpha)
		})
	}

	for _, body := range []string{
		`{"query":"space","alpha":-0.1}`,
		`{"query":"space","alpha":1.1}`,
	} {
		t.Run(body+" is a 400", func(t *testing.T) {
			_, apiErr := buildHybrid(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "alpha")
		})
	}
}

// TestHybridFusionType pins the fusion mapping and the shared default:
// omitted means relative_score (common_filters.HybridFusionDefault, the same
// default gRPC and GraphQL apply).
func TestHybridFusionType(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{"omitted defaults to relative_score", `{"query":"space"}`, common_filters.HybridRelativeScoreFusion},
		{"ranked", `{"query":"space","fusion_type":"ranked"}`, common_filters.HybridRankedFusion},
		{"relative_score", `{"query":"space","fusion_type":"relative_score"}`, common_filters.HybridRelativeScoreFusion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher, apiErr := buildHybrid(t, movieClass(), tt.body)
			require.Nil(t, apiErr)
			assert.Equal(t, tt.want, searcher.lastParams.HybridSearch.FusionAlgorithm)
		})
	}

	t.Run("unknown value is a 400", func(t *testing.T) {
		// the swagger enum rejects it with 422 before the handler; the
		// handler's 400 is the defensive fallback for the direct-call path
		_, apiErr := buildHybrid(t, movieClass(), `{"query":"space","fusion_type":"best"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "fusion_type")
	})
}

func TestHybridMaxVectorDistance(t *testing.T) {
	t.Run("sets the distance cutoff", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space","max_vector_distance":0.4}`)
		require.Nil(t, apiErr)
		hybrid := searcher.lastParams.HybridSearch
		assert.Equal(t, float32(0.4), hybrid.Distance)
		assert.True(t, hybrid.WithDistance)
	})

	t.Run("omitted leaves the cutoff off", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space"}`)
		require.Nil(t, apiErr)
		assert.False(t, searcher.lastParams.HybridSearch.WithDistance)
	})
}

// TestHybridQueryProperties pins the gRPC-parity property handling shared
// with bm25: names pass through with their first letter lowercased, and a
// "^boost" suffix is not interpreted here — the searcher parses it.
func TestHybridQueryProperties(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{"pass through", `{"query":"space","query_properties":["title"]}`, []string{"title"}},
		{"boost suffix passes through", `{"query":"space","query_properties":["title^2"]}`, []string{"title^2"}},
		{
			"first letter lowercased (gRPC parity)",
			`{"query":"space","query_properties":["Title^2","Year"]}`,
			[]string{"title^2", "year"},
		},
		{"omitted searches all searchable properties", `{"query":"space"}`, nil},
		{"empty searches all searchable properties", `{"query":"space","query_properties":[]}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher, apiErr := buildHybrid(t, movieClass(), tt.body)
			require.Nil(t, apiErr)
			hybrid := searcher.lastParams.HybridSearch
			require.NotNil(t, hybrid)
			if tt.want == nil {
				assert.Empty(t, hybrid.Properties)
			} else {
				assert.Equal(t, tt.want, hybrid.Properties)
			}
		})
	}
}

// TestHybridVectorizerOnlyAboveAlphaZero: the vector leg is skipped entirely
// at alpha 0 (engine behavior), so a pure keyword hybrid search runs on
// collections without any vectorizer module; any alpha above 0 needs one.
func TestHybridVectorizerOnlyAboveAlphaZero(t *testing.T) {
	noVectorizer := movieClass()
	noVectorizer.Vectorizer = "none"

	t.Run("alpha 0 needs no vectorizer", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, noVectorizer, `{"query":"space","alpha":0}`)
		require.Nil(t, apiErr)
		assert.Equal(t, float64(0), searcher.lastParams.HybridSearch.Alpha)
	})

	for name, body := range map[string]string{
		"default alpha": `{"query":"space"}`,
		"explicit":      `{"query":"space","alpha":0.5}`,
	} {
		t.Run(name+" above 0 without a vectorizer is a 422", func(t *testing.T) {
			_, apiErr := buildHybrid(t, noVectorizer, body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "vectorizer")
			assert.Contains(t, apiErr.Error(), "alpha")
		})
	}

	t.Run("named vector with vectorizer none is a 422 above alpha 0", func(t *testing.T) {
		class := namedVectorsClass("title_vec")
		class.VectorConfig["title_vec"] = models.VectorConfig{
			Vectorizer:        map[string]any{"none": map[string]any{}},
			VectorIndexConfig: hnsw.UserConfig{Distance: "cosine"},
		}
		_, apiErr := buildHybrid(t, class, `{"query":"space"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)

		_, apiErr = buildHybrid(t, class, `{"query":"space","alpha":0}`)
		assert.Nil(t, apiErr)
	})
}

func TestHybridTargetVectors(t *testing.T) {
	t.Run("legacy vector collection needs no target", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space"}`)
		require.Nil(t, apiErr)
		assert.Empty(t, searcher.lastParams.HybridSearch.TargetVectors)
	})

	t.Run("sole named vector selected implicitly", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, namedVectorsClass("title_vec"), `{"query":"space"}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"title_vec"}, searcher.lastParams.HybridSearch.TargetVectors)
	})

	t.Run("multiple named vectors require target_vector", func(t *testing.T) {
		_, apiErr := buildHybrid(t, namedVectorsClass("title_vec", "summary_vec"), `{"query":"space"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)

		searcher, apiErr := buildHybrid(t, namedVectorsClass("title_vec", "summary_vec"),
			`{"query":"space","target_vector":"summary_vec"}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"summary_vec"}, searcher.lastParams.HybridSearch.TargetVectors)
	})

	t.Run("unknown target_vector is a 400", func(t *testing.T) {
		_, apiErr := buildHybrid(t, namedVectorsClass("title_vec"), `{"query":"space","target_vector":"nope"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestHybridReturnMetadata(t *testing.T) {
	t.Run("score and explain_score", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(),
			`{"query":"space","return_metadata":["score","explain_score"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.True(t, addl.Score)
		assert.True(t, addl.ExplainScore)
	})

	t.Run("certainty stays requested on a cosine index", func(t *testing.T) {
		// hybrid is a vector search: unlike bm25, the certainty flag is NOT
		// force-cleared (gRPC parity — vectorSearch keeps the flag, subject
		// only to the cosine-compatibility silent drop)
		searcher, apiErr := buildHybrid(t, movieClass(),
			`{"query":"space","return_metadata":["distance","certainty"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.Certainty)
		assert.True(t, addl.Distance)
	})

	t.Run("certainty silently dropped on a non-cosine index", func(t *testing.T) {
		class := movieClass()
		class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
		searcher, apiErr := buildHybrid(t, class,
			`{"query":"space","return_metadata":["distance","certainty"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.False(t, addl.Certainty)
		assert.True(t, addl.Distance)
	})
}

// TestHybridSharedFields smoke-tests that the SearchCommon fields flow
// through the shared parsers for hybrid exactly as they do for near-text and
// bm25.
func TestHybridSharedFields(t *testing.T) {
	t.Run("pagination and consistency", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(),
			`{"query":"space","limit":3,"offset":6,"auto_limit":2,"consistency_level":"QUORUM"}`)
		require.Nil(t, apiErr)
		pagination := searcher.lastParams.Pagination
		assert.Equal(t, 3, pagination.Limit)
		assert.Equal(t, 6, pagination.Offset)
		assert.Equal(t, 2, pagination.Autocut)
		require.NotNil(t, searcher.lastParams.ReplicationProperties)
		assert.Equal(t, "QUORUM", searcher.lastParams.ReplicationProperties.ConsistencyLevel)
	})

	t.Run("where filter", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(),
			`{"query":"space","where":{"path":["year"],"operator":"GreaterThanEqual","valueInt":1980}}`)
		require.Nil(t, apiErr)
		require.NotNil(t, searcher.lastParams.Filters)
	})

	t.Run("return_properties subset", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space","return_properties":["title"]}`)
		require.Nil(t, apiErr)
		require.Len(t, searcher.lastParams.Properties, 1)
		assert.Equal(t, "title", searcher.lastParams.Properties[0].Name)
	})
}

func TestParseConsistencyLevel(t *testing.T) {
	// the swagger enum rejects lowercase/unknown values (422) before the
	// handler; the handler's tolerant ToUpper + 400 is the defensive
	// fallback covered here.
	t.Run("valid levels", func(t *testing.T) {
		for _, level := range []string{"ONE", "QUORUM", "ALL", "quorum"} {
			searcher, apiErr := buildParams(t, movieClass(),
				fmt.Sprintf(`{"query":["space"],"consistency_level":"%s"}`, level))
			require.Nil(t, apiErr)
			require.NotNil(t, searcher.lastParams.ReplicationProperties)
			assert.Equal(t, strings.ToUpper(level), searcher.lastParams.ReplicationProperties.ConsistencyLevel)
		}
	})

	t.Run("omitted", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"]}`)
		require.Nil(t, apiErr)
		assert.Nil(t, searcher.lastParams.ReplicationProperties)
	})

	t.Run("invalid is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"consistency_level":"MOST"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}
