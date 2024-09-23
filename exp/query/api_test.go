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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
)

const (
	// This is pre-generated based on popular jeopardy dataset.
	// https://weaviate.io/developers/wcs/quickstart#query-your-data
	lsmRoot = "./testdata/lsmroot/lsm"

	// This is pre-generated vectorized query via text2vec-contextionary
	textVectorized = "./testdata/vectorized.json"
)

func TestAPI_vectorSearch(t *testing.T) {
	vectorize := newMockVectorizer(t)

	// we don't use this in test. As it's hard to mock it.
	// It's here because it's needed as dependency for `query.API`.
	offmod := modsloads3.Module{}
	config := Config{}
	logger := testLogger()
	ctx := context.Background()

	store, err := lsmkv.New(lsmRoot, lsmRoot, logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	api := NewAPI(testTenantInfo, &offmod, vectorize, &config, logger)

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
			assert.Equal(t, len(res), tc.expCount)
		})
	}
}

type mockTenantInfo struct {
	mu   sync.Mutex
	info map[string]*tenantInfo
}

type tenantInfo struct {
	tenant     string
	collection string
	status     string
}

var testTenantInfo = &mockTenantInfo{
	info: map[string]*tenantInfo{
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

func (m *mockTenantInfo) TenantStatus(_ context.Context, collection, tenant string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.info[tenant]
	if !ok || info.collection != collection {
		return "", ErrInvalidTenant
	}
	return info.status, nil
}

func (m *mockTenantInfo) Collection(_ context.Context, collection string) (*models.Class, error) {
	return nil, nil
}

func (m *mockTenantInfo) With(info *tenantInfo) *mockTenantInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.info[info.tenant] = info
	return m
}

func newMockVectorizer(t *testing.T) text2vecbase.TextVectorizer {
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
