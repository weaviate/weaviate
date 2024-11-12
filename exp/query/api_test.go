//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package query

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
)

const (
	// This is pre-generated based on popular jeopardy dataset.
	// https://weaviate.io/developers/wcs/quickstart#query-your-data
	lsmRoot = "./testdata/lsmroot/lsm"

	// This is pre-generated vectorized query via text2vec-contextionary
	textVectorized = "./testdata/vectorized.json"

	// Collection name for data in `testdata`
	testCollection = "Question"

	// testTenant for data in `testdata`
	testTenant = "weaviate-tenant"
)

func TestAPI_propertyFilters(t *testing.T) {
	vectorize := newMockVectorizer(t)
	offmod := modsloads3.Module{}
	config := Config{}
	logger := testLogger()
	lsm := NewLSMFetcher("", &offmod, logger)
	ctx := context.Background()
	st, err := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
	require.NoError(t, err)

	store, err := lsmkv.New(lsmRoot, lsmRoot, logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(ctx)

	api := NewAPI(testSchemaInfo, lsm, vectorize, st, &config, logger)

	cases := []struct {
		name     string
		filters  *filters.LocalFilter
		limit    int
		expCount int
	}{
		{
			name: "single_property_filter",
			filters: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Question"),
					Property: schema.AssertValidPropertyName("category"),
				},
				Value: &filters.Value{
					Value: "ANIMALS",
					Type:  schema.DataTypeText,
				},
			}},
			expCount: 4,
		},
		{
			name: "multiple_property_filter",
			filters: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorAnd,
				Operands: []filters.Clause{
					{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    schema.AssertValidClassName("Question"),
							Property: schema.AssertValidPropertyName("category"),
						},
						Value: &filters.Value{
							Value: "ANIMALS",
							Type:  schema.DataTypeText,
						},
					},
					{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    schema.AssertValidClassName("Question"),
							Property: schema.AssertValidPropertyName("answer"),
						},
						Value: &filters.Value{
							Value: "Elephant",
							Type:  schema.DataTypeText,
						},
					},
				},
			}},
			expCount: 1,
		},
		{
			name: "property_filter_with_limit",
			filters: &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("Question"),
					Property: schema.AssertValidPropertyName("category"),
				},
				Value: &filters.Value{
					Value: "ANIMALS",
					Type:  schema.DataTypeText,
				},
			}},
			limit:    2,
			expCount: 2, // even though matched results are >2, we should see only 2 results because of `limit=2`
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := api.propertyFilters(ctx, store, testCollection, nil, testTenant, tc.filters, tc.limit)
			require.NoError(t, err)
			assert.Equal(t, tc.expCount, len(res))
		})
	}
}

func TestAPI_vectorSearch(t *testing.T) {
	vectorize := newMockVectorizer(t)

	// we don't use this in test. As it's hard to mock it.
	// It's here because it's needed as dependency for `query.API`.
	offmod := modsloads3.Module{}
	config := Config{}
	logger := testLogger()
	lsm := NewLSMFetcher("", &offmod, logger)
	ctx := context.Background()
	st, err := stopwords.NewDetectorFromPreset(stopwords.EnglishPreset)
	require.NoError(t, err)

	store, err := lsmkv.New(lsmRoot, lsmRoot, logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(ctx)

	api := NewAPI(testSchemaInfo, lsm, vectorize, st, &config, logger)

	nearText := []string{"biology"}

	cases := []struct {
		name      string
		threshold float32
		limit     int
		expCount  int
	}{
		{
			name:     "vector_match",
			expCount: 10, // return all objects
		},
		{
			name:      "vector_match_with_threshold",
			threshold: 0.4,
			expCount:  7,
		},
		{
			name:      "vector_match_with_threshold_and_limit",
			threshold: 0.4,
			limit:     2,
			expCount:  7, // limit - not working as expected. See TODO in api.go
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, _, err := api.vectorSearch(ctx, store, lsmRoot, nearText, tc.threshold, tc.limit)
			require.NoError(t, err)
			assert.Equal(t, tc.expCount, len(res))
		})
	}
}

type mockSchemaInfo struct {
	mu         sync.Mutex
	tenantinfo map[string]*tenantInfo
}

type tenantInfo struct {
	tenant     string
	collection string
	status     string
}

var testSchemaInfo = &mockSchemaInfo{
	tenantinfo: map[string]*tenantInfo{
		"ada-active": {
			tenant:     "ada-active",
			collection: "computer",
			status:     "ACTIVE",
		},
		"feynman-frozen": {
			tenant:     "feynman-frozen",
			collection: "physics",
			status:     "FROZEN",
		},
	},
}

func (m *mockSchemaInfo) TenantStatus(_ context.Context, collection, tenant string) (string, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.tenantinfo[tenant]
	if !ok || info.collection != collection {
		return "", 0, ErrInvalidTenant
	}
	return info.status, 0, nil
}

func (m *mockSchemaInfo) Collection(_ context.Context, collection string) (*models.Class, error) {
	var class models.Class
	err := json.Unmarshal([]byte(classQuestion), &class)

	return &class, err
}

func newMockVectorizer(t *testing.T) text2vecbase.TextVectorizer[[]float32] {
	f, err := os.Open(textVectorized)
	require.NoError(t, err)
	defer f.Close()

	var (
		vv []Vectorized
		mv = mockVectorizer{
			vectors: make(map[string][]float32),
		}
	)

	err = json.NewDecoder(f).Decode(&vv)
	require.NoError(t, err)

	for _, v := range vv {
		mv.vectors[v.Term] = v.Vectors
	}

	return &mv
}

type mockVectorizer struct {
	// pre-generated `vectors` for any token(string) using text2vec-contextionary
	vectors map[string][]float32
}

func (mv *mockVectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error) {
	panic("unimplemented")
}

func (mv *mockVectorizer) Texts(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// assume only vectorize 0th index of input.
	return mv.vectors[input[0]], nil
}

type Vectorized struct {
	Term    string    `yaml:"term" json:"term"`
	Vectors []float32 `yaml:"vectors" json:"vectors"`
}

func testLogger() logrus.FieldLogger {
	log := logrus.StandardLogger()
	log.SetOutput(io.Discard)
	return log
}

const (
	classQuestion = `
{
  "class": "Question",
  "invertedIndexConfig": {
    "bm25": {
      "b": 0.75,
      "k1": 1.2
    },
    "cleanupIntervalSeconds": 60,
    "stopwords": {
      "additions": null,
      "preset": "en",
      "removals": null
    }
  },
  "moduleConfig": {
    "text2vec-contextionary": {
      "vectorizeClassName": true
    }
  },
  "multiTenancyConfig": {
    "autoTenantActivation": false,
    "autoTenantCreation": false,
    "enabled": true
  },
  "properties": [
    {
      "dataType": [
        "text"
      ],
      "indexFilterable": true,
      "indexRangeFilters": false,
      "indexSearchable": true,
      "moduleConfig": {
        "text2vec-contextionary": {
          "skip": false,
          "vectorizePropertyName": false
        }
      },
      "name": "answer",
      "tokenization": "whitespace"
    },
    {
      "dataType": [
        "text"
      ],
      "indexFilterable": true,
      "indexRangeFilters": false,
      "indexSearchable": true,
      "moduleConfig": {
        "text2vec-contextionary": {
          "skip": false,
          "vectorizePropertyName": false
        }
      },
      "name": "question",
      "tokenization": "whitespace"
    },
    {
      "dataType": [
        "text"
      ],
      "indexFilterable": true,
      "indexRangeFilters": false,
      "indexSearchable": true,
      "moduleConfig": {
        "text2vec-contextionary": {
          "skip": false,
          "vectorizePropertyName": false
        }
      },
      "name": "category",
      "tokenization": "whitespace"
    }
  ],
  "replicationConfig": {
    "asyncEnabled": false,
    "factor": 1,
    "objectDeletionConflictResolution": "PermanentDeletion"
  },
  "shardingConfig": {
    "virtualPerPhysical": 0,
    "desiredCount": 0,
    "actualCount": 0,
    "desiredVirtualCount": 0,
    "actualVirtualCount": 0,
    "key": "",
    "strategy": "",
    "function": ""
  },
  "vectorIndexConfig": {
    "distance": "cosine",
    "vectorCacheMaxObjects": 1000000000000,
    "pq": {
      "enabled": false,
      "rescoreLimit": -1,
      "cache": false
    },
    "bq": {
      "enabled": true,
      "rescoreLimit": -1,
      "cache": false
    },
    "sq": {
      "enabled": false,
      "rescoreLimit": -1,
      "cache": false
    }
  },
  "vectorIndexType": "flat",
  "vectorizer": "text2vec-contextionary"
}
`
)
