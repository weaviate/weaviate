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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/dto"
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
		"singlePrompt":    `"x"`,
		"groupedTask":     `"x"`,
		"groupBy":         `"x"`,
		"numberOfGroups":  `2`,
		"objectsPerGroup": `2`,
		"rerank":          `{"property":"x"}`,
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
		`{"query":["space"],"groupBy":null}`,
		`{"query":["space"],"rerank":null}`,
	} {
		t.Run(body, func(t *testing.T) {
			_, apiErr := buildParams(t, movieClass(), body)
			assert.Nil(t, apiErr)
		})
	}
}

func TestRerankValidationNamesPropertyOnce(t *testing.T) {
	req, apiErr := decodeModel(`{"query":["space"],"rerank":{}}`)
	require.Nil(t, apiErr)

	err := req.Validate(strfmt.Default)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rerank.property in body is required")
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
			`{"query":["space"],"limit":3,"offset":6,"autoLimit":2}`)
		require.Nil(t, apiErr)
		pagination := searcher.lastParams.Pagination
		assert.Equal(t, 3, pagination.Limit)
		assert.Equal(t, 6, pagination.Offset)
		assert.Equal(t, 2, pagination.Autocut)
		// the near-text limit follows the pagination limit, like gRPC
		assert.Equal(t, 3, nearTextFromParams(t, searcher).Limit)
	})

	for name, body := range map[string]string{
		"negative limit":     `{"query":["space"],"limit":-1}`,
		"negative offset":    `{"query":["space"],"offset":-1}`,
		"negative autoLimit": `{"query":["space"],"autoLimit":-1}`,
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

	t.Run("multiple named vectors require targetVector", func(t *testing.T) {
		_, apiErr := buildParams(t, namedVectorsClass("title_vec", "plot_vec"), `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "multiple vectors")
	})

	t.Run("explicit targetVector", func(t *testing.T) {
		searcher, apiErr := buildParams(t, namedVectorsClass("title_vec", "plot_vec"),
			`{"query":["space"],"targetVector":"plot_vec"}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"plot_vec"}, nearTextFromParams(t, searcher).TargetVectors)
	})

	t.Run("unknown targetVector is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, namedVectorsClass("title_vec"),
			`{"query":["space"],"targetVector":"nope"}`)
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
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnMetadata":[]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.False(t, addl.Distance)
	})

	t.Run("id is not a metadata key", func(t *testing.T) {
		// returnMetadata selects metadata keys only; the id is a top-level
		// result field. Live, the swagger enum rejects "id" at bind (422);
		// the parser's own 400 covers the direct-call path.
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnMetadata":["id"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(),
			"expected one of distance, certainty, score, explainScore, creationTime, lastUpdateTime")
	})

	t.Run("all supported values", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"returnMetadata":["distance","certainty","score","explainScore","creationTime","lastUpdateTime"]}`)
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
		searcher, apiErr := buildParams(t, class, `{"query":["space"],"returnMetadata":["certainty"]}`)
		require.Nil(t, apiErr)
		assert.False(t, searcher.lastParams.AdditionalProperties.Certainty)
	})

	t.Run("unknown value is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnMetadata":["vector"]}`)
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
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnProperties":["title"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		require.Len(t, props, 1)
		assert.Equal(t, "title", props[0].Name)
		assert.True(t, props[0].IsPrimitive)
	})

	t.Run("empty list means no properties", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnProperties":[]}`)
		require.Nil(t, apiErr)
		assert.Empty(t, searcher.lastParams.Properties)
		assert.True(t, searcher.lastParams.AdditionalProperties.NoProps)
	})

	t.Run("unknown property is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnProperties":["nope"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("dot-path selects across a reference", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"returnProperties":["title","hasAuthor.name"]}`)
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
			`{"query":["space"],"returnProperties":["hasAuthor.name","hasAuthor.age"]}`)
		require.Nil(t, apiErr)
		props := searcher.lastParams.Properties
		require.Len(t, props, 1)
		require.Len(t, props[0].Refs, 1)
		require.Len(t, props[0].Refs[0].RefProperties, 2)
	})

	t.Run("bare reference name selects all target properties", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"returnProperties":["hasAuthor"]}`)
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
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"returnProperties":["title.name"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})

	t.Run("two reference hops are deferred with a 422", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"returnProperties":["hasAuthor.name.first"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "not yet supported")
	})

	t.Run("unknown property on the referenced class is a 400", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"returnProperties":["hasAuthor.nope"]}`)
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

	t.Run("explicit empty-string query is a 400", func(t *testing.T) {
		// absent/null query is swagger's 422 at bind (pinned live in the
		// acceptance suite); the handler's own 400 covers the empty string
		_, apiErr := buildBm25(t, movieClass(), `{"query":""}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "query")
	})

	t.Run("query is string-only: the array form fails decode", func(t *testing.T) {
		_, apiErr := buildBm25(t, movieClass(), `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

// assertQueryPropertiesParsing runs the gRPC-parity property-handling
// contract shared by the keyword endpoints (first letter lowercased,
// "^boost" passes through to the searcher) against one endpoint's builder.
func assertQueryPropertiesParsing(t *testing.T,
	build func(*testing.T, *models.Class, string) (*fakeSearcher, *APIError),
	props func(dto.GetParams) []string,
) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{"pass through", `{"query":"space","queryProperties":["title"]}`, []string{"title"}},
		{"boost suffix passes through", `{"query":"space","queryProperties":["title^2"]}`, []string{"title^2"}},
		{
			"first letter lowercased (gRPC parity)",
			`{"query":"space","queryProperties":["Title^2","Year"]}`,
			[]string{"title^2", "year"},
		},
		{"omitted searches all searchable properties", `{"query":"space"}`, nil},
		{"empty searches all searchable properties", `{"query":"space","queryProperties":[]}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher, apiErr := build(t, movieClass(), tt.body)
			require.Nil(t, apiErr)
			got := props(searcher.lastParams)
			if tt.want == nil {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}

	t.Run("first letter only, interior caps preserved", func(t *testing.T) {
		class := movieClass()
		class.Properties = append(class.Properties,
			&models.Property{Name: "camelCaseProp", DataType: schema.DataTypeText.PropString()})
		searcher, apiErr := build(t, class, `{"query":"space","queryProperties":["CamelCaseProp"]}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"camelCaseProp"}, props(searcher.lastParams))
	})
}

// assertSharedFieldsFlow drives the SearchCommon fields through one
// endpoint's builder and asserts they land in dto.GetParams.
func assertSharedFieldsFlow(t *testing.T,
	build func(*testing.T, *models.Class, string) (*fakeSearcher, *APIError),
) {
	searcher, apiErr := build(t, movieClass(),
		`{"query":"space","limit":3,"offset":6,"autoLimit":2,"consistencyLevel":"QUORUM",`+
			`"where":{"path":["year"],"operator":"GreaterThanEqual","valueInt":1980},`+
			`"returnProperties":["title"]}`)
	require.Nil(t, apiErr)
	pagination := searcher.lastParams.Pagination
	assert.Equal(t, 3, pagination.Limit)
	assert.Equal(t, 6, pagination.Offset)
	assert.Equal(t, 2, pagination.Autocut)
	require.NotNil(t, searcher.lastParams.ReplicationProperties)
	assert.Equal(t, "QUORUM", searcher.lastParams.ReplicationProperties.ConsistencyLevel)
	require.NotNil(t, searcher.lastParams.Filters)
	require.Len(t, searcher.lastParams.Properties, 1)
	assert.Equal(t, "title", searcher.lastParams.Properties[0].Name)
}

func TestBm25QueryProperties(t *testing.T) {
	assertQueryPropertiesParsing(t, buildBm25, func(p dto.GetParams) []string {
		return p.KeywordRanking.Properties
	})
}

func TestBm25UnknownQueryProperty(t *testing.T) {
	// an entry naming no schema property is a 400 like returnProperties;
	// only an existing property without a searchable index is the
	// searcher's typed 422
	for name, body := range map[string]string{
		"unknown":            `{"query":"space","queryProperties":["titel"]}`,
		"unknown with boost": `{"query":"space","queryProperties":["titel^2"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, apiErr := buildBm25(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "no such prop")
		})
	}

	t.Run("existing but non-searchable is the searcher's to reject", func(t *testing.T) {
		_, apiErr := buildBm25(t, unsearchableClass(), `{"query":"space","queryProperties":["code"]}`)
		assert.Nil(t, apiErr)
	})
}

func TestBm25ReturnMetadata(t *testing.T) {
	t.Run("score and explainScore", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","returnMetadata":["score","explainScore"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.True(t, addl.Score)
		assert.True(t, addl.ExplainScore)
		// explainScore also switches on the ranker's explanations (gRPC
		// parity: AdditionalExplanations follows ExplainScore)
		assert.True(t, searcher.lastParams.KeywordRanking.AdditionalExplanations)
	})

	t.Run("explainScore omitted leaves explanations off", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(), `{"query":"space","returnMetadata":["score"]}`)
		require.Nil(t, apiErr)
		assert.False(t, searcher.lastParams.KeywordRanking.AdditionalExplanations)
	})

	t.Run("certainty is inapplicable and silently dropped", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","returnMetadata":["distance","certainty","score"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.False(t, addl.Certainty)
		assert.True(t, addl.Score)
	})

	t.Run("creation and update times", func(t *testing.T) {
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","returnMetadata":["creationTime","lastUpdateTime"]}`)
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
	class := movieClass()
	class.Vectorizer = "none"
	_, apiErr := buildBm25(t, class, `{"query":"space"}`)
	assert.Nil(t, apiErr)
}

// TestBm25SharedFields smoke-tests that the SearchCommon fields flow through
// the shared parsers for bm25 exactly as they do for near-text.
func TestBm25SharedFields(t *testing.T) {
	t.Run("shared parsers flow through", func(t *testing.T) {
		assertSharedFieldsFlow(t, buildBm25)
	})

	t.Run("near-text-only fields are unknown fields and ignored", func(t *testing.T) {
		// option-2 contract: fields outside the bm25 schema (targetVector,
		// certainty, distance live in the near-text extension) drop at decode
		searcher, apiErr := buildBm25(t, movieClass(),
			`{"query":"space","targetVector":"nope","certainty":0.9,"distance":0.1,"not_a_field":1}`)
		require.Nil(t, apiErr)
		require.NotNil(t, searcher.lastParams.KeywordRanking)
	})
}

// unsearchableClass has no searchable property: ints are never
// keyword-searchable and its sole text property disables its searchable
// index.
func unsearchableClass() *models.Class {
	searchable := false
	return &models.Class{
		Class:             "Ledger",
		Vectorizer:        "text2vec-contextionary",
		VectorIndexConfig: hnsw.UserConfig{Distance: "cosine"},
		Properties: []*models.Property{
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			{Name: "code", DataType: schema.DataTypeText.PropString(), IndexSearchable: &searchable},
		},
	}
}

func TestBm25NoSearchableProperties(t *testing.T) {
	// empty queryProperties expand to all searchable properties; with none,
	// the engine errors untyped (a 500), so the handler pre-checks with 422
	for name, body := range map[string]string{
		"omitted": `{"query":"space"}`,
		"empty":   `{"query":"space","queryProperties":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, apiErr := buildBm25(t, unsearchableClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "no searchable properties")
		})
	}

	t.Run("explicit queryProperties are the searcher's to reject", func(t *testing.T) {
		// the pre-check only guards the empty-list expansion; an explicit
		// non-searchable property reaches the searcher (typed
		// MissingIndexError, mapped to 422 live)
		_, apiErr := buildBm25(t, unsearchableClass(), `{"query":"space","queryProperties":["code"]}`)
		assert.Nil(t, apiErr)
	})
}

// decodeNearObjectModel unmarshals a JSON body into the typed near-object
// request model, the way the swagger JSON consumer does (unknown fields
// ignored, type mismatches fail). A decode failure maps to the 400 the
// consumer returns live.
func decodeNearObjectModel(body string) (*models.SearchNearObjectRequest, *APIError) {
	var req models.SearchNearObjectRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return nil, newAPIError(http.StatusBadRequest, "invalid request body: %v", err)
	}
	return &req, nil
}

// buildNearObject runs the full near-object body -> dto.GetParams conversion
// against the fixture schema, including the reserved-field 422 check that
// the handler runs before buildNearObjectParams.
func buildNearObject(t *testing.T, class *models.Class, body string) (*fakeSearcher, *APIError) {
	t.Helper()
	deps := newTestHandler(t)
	deps.schemaReader.classes[class.Class] = class

	parsed, apiErr := decodeNearObjectModel(body)
	if apiErr != nil {
		return nil, apiErr
	}
	if apiErr := checkReservedFields(&parsed.SearchCommon); apiErr != nil {
		return nil, apiErr
	}

	params, apiErr := deps.handler.buildNearObjectParams(class, class.Class, parsed, fixtureGetClass(deps), nil)
	if apiErr != nil {
		return nil, apiErr
	}
	deps.searcher.lastParams = params
	return deps.searcher, nil
}

const nearObjectSourceID = "11111111-2222-4333-8444-555555555555"

func TestNearObjectParams(t *testing.T) {
	t.Run("id maps to NearObject, not module or keyword params", func(t *testing.T) {
		searcher, apiErr := buildNearObject(t, movieClass(),
			fmt.Sprintf(`{"id":%q}`, nearObjectSourceID))
		require.Nil(t, apiErr)
		nearObject := searcher.lastParams.NearObject
		require.NotNil(t, nearObject)
		assert.Equal(t, nearObjectSourceID, nearObject.ID)
		assert.Empty(t, nearObject.Beacon)
		assert.Empty(t, searcher.lastParams.ModuleParams)
		assert.Nil(t, searcher.lastParams.KeywordRanking)
		assert.Nil(t, searcher.lastParams.HybridSearch)
	})

	// swagger's required validation rejects an absent or null id with 422 and
	// its uuid format rejects a structurally invalid one; the handler's own
	// 400 covers the direct-call path
	for name, body := range map[string]string{
		"empty string": `{"id":""}`,
		"missing":      `{}`,
		"null":         `{"id":null}`,
	} {
		t.Run(name+" id is a 400", func(t *testing.T) {
			_, apiErr := buildNearObject(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "id")
		})
	}
}

// TestNearObjectCertaintyAndDistance mirrors the near-text handling on the
// near-object body: mutual exclusion (gRPC parity), the certainty range
// check, and the deterministic cosine-only 422.
func TestNearObjectCertaintyAndDistance(t *testing.T) {
	body := func(fields string) string {
		return fmt.Sprintf(`{"id":%q%s}`, nearObjectSourceID, fields)
	}

	t.Run("distance sets the cutoff", func(t *testing.T) {
		searcher, apiErr := buildNearObject(t, movieClass(), body(`,"distance":0.4`))
		require.Nil(t, apiErr)
		nearObject := searcher.lastParams.NearObject
		assert.Equal(t, 0.4, nearObject.Distance)
		assert.True(t, nearObject.WithDistance)
	})

	t.Run("certainty on a cosine index", func(t *testing.T) {
		searcher, apiErr := buildNearObject(t, movieClass(), body(`,"certainty":0.8`))
		require.Nil(t, apiErr)
		nearObject := searcher.lastParams.NearObject
		assert.Equal(t, 0.8, nearObject.Certainty)
		assert.False(t, nearObject.WithDistance)
	})

	t.Run("both certainty and distance is a 400", func(t *testing.T) {
		_, apiErr := buildNearObject(t, movieClass(), body(`,"certainty":0.8,"distance":0.4`))
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "certainty")
	})

	t.Run("certainty outside [0,1] is a 400", func(t *testing.T) {
		for _, fields := range []string{`,"certainty":-0.1`, `,"certainty":1.1`} {
			_, apiErr := buildNearObject(t, movieClass(), body(fields))
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		}
	})

	t.Run("certainty on a non-cosine index is a 422", func(t *testing.T) {
		class := movieClass()
		class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
		_, apiErr := buildNearObject(t, class, body(`,"certainty":0.8`))
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "certainty")
	})
}

// TestNearObjectNeedsNoVectorizer: the source object's stored vector anchors
// the search, so collections without any vectorizer module are fully
// searchable (unlike near-text, and unlike hybrid above alpha 0).
func TestNearObjectNeedsNoVectorizer(t *testing.T) {
	class := movieClass()
	class.Vectorizer = "none"
	_, apiErr := buildNearObject(t, class, fmt.Sprintf(`{"id":%q}`, nearObjectSourceID))
	assert.Nil(t, apiErr)
}

func TestNearObjectTargetVectors(t *testing.T) {
	t.Run("sole named vector selected implicitly", func(t *testing.T) {
		searcher, apiErr := buildNearObject(t, namedVectorsClass("title_vec"),
			fmt.Sprintf(`{"id":%q}`, nearObjectSourceID))
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"title_vec"}, searcher.lastParams.NearObject.TargetVectors)
	})

	t.Run("multiple named vectors require targetVector", func(t *testing.T) {
		_, apiErr := buildNearObject(t, namedVectorsClass("title_vec", "summary_vec"),
			fmt.Sprintf(`{"id":%q}`, nearObjectSourceID))
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)

		searcher, apiErr := buildNearObject(t, namedVectorsClass("title_vec", "summary_vec"),
			fmt.Sprintf(`{"id":%q,"targetVector":"summary_vec"}`, nearObjectSourceID))
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"summary_vec"}, searcher.lastParams.NearObject.TargetVectors)
	})

	t.Run("unknown targetVector is a 400", func(t *testing.T) {
		_, apiErr := buildNearObject(t, namedVectorsClass("title_vec"),
			fmt.Sprintf(`{"id":%q,"targetVector":"nope"}`, nearObjectSourceID))
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestNearObjectReturnMetadata(t *testing.T) {
	t.Run("certainty stays requested on a cosine index", func(t *testing.T) {
		// near-object is a vector search: the certainty flag is NOT
		// force-cleared (gRPC parity), subject only to the
		// cosine-compatibility silent drop
		searcher, apiErr := buildNearObject(t, movieClass(),
			fmt.Sprintf(`{"id":%q,"returnMetadata":["distance","certainty"]}`, nearObjectSourceID))
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.Certainty)
		assert.True(t, addl.Distance)
	})

	t.Run("certainty silently dropped on a non-cosine index", func(t *testing.T) {
		class := movieClass()
		class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
		searcher, apiErr := buildNearObject(t, class,
			fmt.Sprintf(`{"id":%q,"returnMetadata":["distance","certainty"]}`, nearObjectSourceID))
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.False(t, addl.Certainty)
		assert.True(t, addl.Distance)
	})
}

// TestNearObjectSharedFields smoke-tests that the SearchCommon fields flow
// through the shared parsers for near-object exactly as for the other
// search types.
func TestNearObjectSharedFields(t *testing.T) {
	t.Run("pagination, consistency, where, properties", func(t *testing.T) {
		searcher, apiErr := buildNearObject(t, movieClass(), fmt.Sprintf(
			`{"id":%q,"limit":3,"offset":6,"consistencyLevel":"ALL",`+
				`"where":{"path":["year"],"operator":"GreaterThanEqual","valueInt":1980},`+
				`"returnProperties":["title"]}`, nearObjectSourceID))
		require.Nil(t, apiErr)
		assert.Equal(t, 3, searcher.lastParams.Pagination.Limit)
		assert.Equal(t, 6, searcher.lastParams.Pagination.Offset)
		require.NotNil(t, searcher.lastParams.ReplicationProperties)
		assert.Equal(t, "ALL", searcher.lastParams.ReplicationProperties.ConsistencyLevel)
		require.NotNil(t, searcher.lastParams.Filters)
		require.Len(t, searcher.lastParams.Properties, 1)
		assert.Equal(t, "title", searcher.lastParams.Properties[0].Name)
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
// omitted means relativeScore (common_filters.HybridFusionDefault, the same
// default gRPC and GraphQL apply).
func TestHybridFusionType(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{"omitted defaults to relativeScore", `{"query":"space"}`, common_filters.HybridRelativeScoreFusion},
		{"ranked", `{"query":"space","fusionType":"ranked"}`, common_filters.HybridRankedFusion},
		{"relativeScore", `{"query":"space","fusionType":"relativeScore"}`, common_filters.HybridRelativeScoreFusion},
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
		_, apiErr := buildHybrid(t, movieClass(), `{"query":"space","fusionType":"best"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "fusionType")
	})
}

func TestHybridMaxVectorDistance(t *testing.T) {
	t.Run("sets the distance cutoff", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space","maxVectorDistance":0.4}`)
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

	t.Run("alpha 0 rejects the cutoff", func(t *testing.T) {
		// at alpha 0 the vector leg never runs, so the cutoff would be
		// silently ignored rather than applied
		_, apiErr := buildHybrid(t, movieClass(), `{"query":"space","alpha":0,"maxVectorDistance":0.4}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "maxVectorDistance requires alpha > 0")
	})

	t.Run("any alpha above 0 accepts the cutoff", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(), `{"query":"space","alpha":0.5,"maxVectorDistance":0.4}`)
		require.Nil(t, apiErr)
		assert.True(t, searcher.lastParams.HybridSearch.WithDistance)
	})
}

func TestHybridQueryProperties(t *testing.T) {
	assertQueryPropertiesParsing(t, buildHybrid, func(p dto.GetParams) []string {
		return p.HybridSearch.Properties
	})
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

	t.Run("above 0 without a vectorizer is a 422", func(t *testing.T) {
		_, apiErr := buildHybrid(t, noVectorizer, `{"query":"space"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
		assert.Contains(t, apiErr.Error(), "vectorizer")
		assert.Contains(t, apiErr.Error(), "alpha")
	})
}

func TestHybridNoSearchableProperties(t *testing.T) {
	// the keyword leg expands empty queryProperties to all searchable
	// properties; below alpha 1 a collection with none is a 422, at alpha 1
	// the keyword leg is skipped and the search is legitimately pure-vector
	for name, body := range map[string]string{
		"default alpha": `{"query":"space"}`,
		"alpha 0":       `{"query":"space","alpha":0}`,
		"alpha 0.5":     `{"query":"space","alpha":0.5}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, apiErr := buildHybrid(t, unsearchableClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "no searchable properties")
		})
	}

	t.Run("alpha 1 skips the keyword leg", func(t *testing.T) {
		_, apiErr := buildHybrid(t, unsearchableClass(), `{"query":"space","alpha":1}`)
		assert.Nil(t, apiErr)
	})

	t.Run("explicit queryProperties are the searcher's to reject", func(t *testing.T) {
		_, apiErr := buildHybrid(t, unsearchableClass(), `{"query":"space","alpha":0,"queryProperties":["code"]}`)
		assert.Nil(t, apiErr)
	})
}

func TestHybridUnknownQueryProperty(t *testing.T) {
	// same contract as bm25: an entry naming no schema property is a 400;
	// an existing property without a searchable index is the searcher's 422
	for name, body := range map[string]string{
		"unknown":            `{"query":"space","queryProperties":["titel"]}`,
		"unknown with boost": `{"query":"space","queryProperties":["titel^2"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, apiErr := buildHybrid(t, movieClass(), body)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Contains(t, apiErr.Error(), "no such prop")
		})
	}
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

	t.Run("multiple named vectors require targetVector", func(t *testing.T) {
		_, apiErr := buildHybrid(t, namedVectorsClass("title_vec", "summary_vec"), `{"query":"space"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)

		searcher, apiErr := buildHybrid(t, namedVectorsClass("title_vec", "summary_vec"),
			`{"query":"space","targetVector":"summary_vec"}`)
		require.Nil(t, apiErr)
		assert.Equal(t, []string{"summary_vec"}, searcher.lastParams.HybridSearch.TargetVectors)
	})

	t.Run("unknown targetVector is a 400", func(t *testing.T) {
		_, apiErr := buildHybrid(t, namedVectorsClass("title_vec"), `{"query":"space","targetVector":"nope"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}

func TestHybridReturnMetadata(t *testing.T) {
	t.Run("score and explainScore", func(t *testing.T) {
		searcher, apiErr := buildHybrid(t, movieClass(),
			`{"query":"space","returnMetadata":["score","explainScore"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.True(t, addl.Score)
		assert.True(t, addl.ExplainScore)
	})

	t.Run("certainty stays requested on a cosine index", func(t *testing.T) {
		// unlike bm25, hybrid does not force-clear certainty (gRPC parity)
		searcher, apiErr := buildHybrid(t, movieClass(),
			`{"query":"space","returnMetadata":["distance","certainty"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.Certainty)
		assert.True(t, addl.Distance)
	})

	t.Run("certainty silently dropped on a non-cosine index", func(t *testing.T) {
		class := movieClass()
		class.VectorIndexConfig = hnsw.UserConfig{Distance: "l2-squared"}
		searcher, apiErr := buildHybrid(t, class,
			`{"query":"space","returnMetadata":["distance","certainty"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.False(t, addl.Certainty)
		assert.True(t, addl.Distance)
	})
}

func TestHybridSharedFields(t *testing.T) {
	assertSharedFieldsFlow(t, buildHybrid)
}

func TestParseConsistencyLevel(t *testing.T) {
	// the swagger enum rejects lowercase/unknown values (422) before the
	// handler; the handler's tolerant ToUpper + 400 is the defensive
	// fallback covered here.
	t.Run("valid levels", func(t *testing.T) {
		for _, level := range []string{"ONE", "QUORUM", "ALL", "quorum"} {
			searcher, apiErr := buildParams(t, movieClass(),
				fmt.Sprintf(`{"query":["space"],"consistencyLevel":"%s"}`, level))
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
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"consistencyLevel":"MOST"}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	})
}
