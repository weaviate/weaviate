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
			Vectorizer:        map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
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

	getClass := func(name string) (*models.Class, error) {
		if c, ok := deps.schemaReader.classes[name]; ok {
			return c, nil
		}
		// same sentinel the real classGetterWithAuthz produces
		return nil, fmt.Errorf("%w %s in schema", errCollectionNotFound, name)
	}

	params, apiErr := deps.handler.buildNearTextParams(class, class.Class, parsed, getClass, nil)
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
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			VectorIndexConfig: hnsw.UserConfig{Distance: "cosine"},
		}
		_, apiErr := buildParams(t, class, `{"query":["space"]}`)
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusUnprocessableEntity, apiErr.Status)
	})
}

func TestParseReturnMetadata(t *testing.T) {
	t.Run("omitted means id", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(), `{"query":["space"]}`)
		require.Nil(t, apiErr)
		addl := searcher.lastParams.AdditionalProperties
		assert.True(t, addl.ID)
		assert.False(t, addl.Distance)
	})

	t.Run("all supported values", func(t *testing.T) {
		searcher, apiErr := buildParams(t, movieClass(),
			`{"query":["space"],"return_metadata":["id","distance","certainty","score","explain_score","creation_time","last_update_time"]}`)
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

	t.Run("_additional is reserved", func(t *testing.T) {
		_, apiErr := buildParams(t, movieClass(), `{"query":["space"],"return_properties":["_additional"]}`)
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
