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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// versionShards records the schema version a read reaches the shards with.
type versionShards struct {
	shards
	got *uint64
}

func (s versionShards) Search(_ context.Context, _, _ string, _ []models.Vector, _ []string, _ float32, _ int,
	_ *filters.LocalFilter, _ *searchparams.KeywordRanking, _ []filters.Sort, _ *filters.Cursor,
	_ *searchparams.GroupBy, _ additional.Properties, _ *dto.TargetCombination, _ []string, v uint64,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	*s.got = v
	return nil, nil, nil, nil
}

func (s versionShards) Aggregate(_ context.Context, _, _ string, _ aggregation.Params, v uint64) (*aggregation.Result, error) {
	*s.got = v
	return &aggregation.Result{}, nil
}

func (s versionShards) Exists(_ context.Context, _, _ string, _ strfmt.UUID, v uint64) (bool, error) {
	*s.got = v
	return true, nil
}

// TestShardReadsPassTheSchemaVersion checks the read handlers hand the
// coordinator's schema version to the shards, and treat a missing one as 0.
func TestShardReadsPassTheSchemaVersion(t *testing.T) {
	searchBody, err := shared.IndicesPayloads.SearchParams.Marshal(
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	aggregateBody, err := shared.IndicesPayloads.AggregationParams.Marshal(aggregation.Params{})
	require.NoError(t, err)

	reads := []struct {
		name string
		req  func(query string) *http.Request
	}{
		{"Search", func(q string) *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/indices/C/shards/S/objects/_search?"+q, bytes.NewReader(searchBody))
			shared.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(r)
			return r
		}},
		{"Aggregate", func(q string) *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/indices/C/shards/S/objects/_aggregations?"+q, bytes.NewReader(aggregateBody))
			shared.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(r)
			return r
		}},
		{"Exists", func(q string) *http.Request {
			return httptest.NewRequest(http.MethodGet,
				"/indices/C/shards/S/objects/c5e2ea4f-7d4c-4b6e-8a1e-2a9b1f0d3c11?check_exists=true&"+q, nil)
		}},
	}
	cases := []struct {
		name     string
		query    string
		wantCode int
		want     uint64
	}{
		{name: "with a version", query: "schema_version=7", want: 7},
		{name: "without a version", query: "", want: 0},
		{name: "a malformed version", query: "schema_version=x", wantCode: http.StatusBadRequest, want: 99},
	}
	for _, r := range reads {
		for _, c := range cases {
			t.Run(r.name+"/"+c.name, func(t *testing.T) {
				logger := logrus.New()
				logger.SetOutput(io.Discard)
				got := uint64(99)
				idx := NewIndices(versionShards{got: &got}, startedDB{}, NewNoopAuthHandler(), func() bool { return false }, logger)

				rec := httptest.NewRecorder()
				idx.Indices().ServeHTTP(rec, r.req(c.query))

				if c.wantCode != 0 {
					require.Equal(t, c.wantCode, rec.Code, rec.Body.String())
				} else {
					require.Less(t, rec.Code, 300, rec.Body.String())
				}
				require.Equal(t, c.want, got)
			})
		}
	}
}
