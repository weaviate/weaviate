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

package clusterapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// capturingShards records the vectorIndexParams argument of Search.
type capturingShards struct {
	shards
	got    *searchparams.VectorIndexParams
	called bool
}

func (c *capturingShards) Search(_ context.Context, _, _ string,
	_ []models.Vector, _ []string, _ float32, _ int, _ *filters.LocalFilter, _ *searchparams.KeywordRanking,
	_ []filters.Sort, _ *filters.Cursor, _ *searchparams.GroupBy, _ additional.Properties,
	_ *dto.TargetCombination, _ []string, params *searchparams.VectorIndexParams,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	c.got = params
	c.called = true
	return nil, nil, nil, nil
}

// TestSearchObjectsPropagatesVectorIndexParams pins that the per-request ef
// carried in the node-to-node search payload reaches shards.Search, so a hop
// dropping it fails here without Docker.
func TestSearchObjectsPropagatesVectorIndexParams(t *testing.T) {
	tests := []struct {
		name   string
		params *searchparams.VectorIndexParams
		wantEF int
	}{
		{"ef override is forwarded", &searchparams.VectorIndexParams{EF: 800}, 800},
		{"absent params stay nil", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &capturingShards{}
			logger := logrus.New()
			logger.SetOutput(&bytes.Buffer{})
			idx := NewIndices(fake, startedDB{}, NewNoopAuthHandler(), func() bool { return false }, logger)

			body, err := shared.IndicesPayloads.SearchParams.Marshal(
				[]models.Vector{[]float32{0.1, 0.2}}, []string{""}, 0, 10,
				nil, nil, nil, nil, nil, additional.Properties{}, nil, nil, tt.params)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost,
				"/indices/MyClass/shards/myshard/objects/_search", bytes.NewReader(body))
			shared.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)
			rec := httptest.NewRecorder()
			idx.Indices().ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.True(t, fake.called)
			if tt.params == nil {
				assert.Nil(t, fake.got)
				return
			}
			require.NotNil(t, fake.got)
			assert.Equal(t, tt.wantEF, fake.got.EF)
		})
	}
}
