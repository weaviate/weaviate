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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestReadMissStatusSeparatesLagFromFinal pins the two shapes a replica returns when it cannot
// serve a read, and the status each must get. 503 is the one a coordinator fails over from; 422
// is terminal. Answering 503 for a shard the replica will never hold is what kept coordinators
// cycling a dead end for the whole read budget, and 500 for either is worse still, because
// shouldRetry asks the same replica again.
func TestReadMissStatusSeparatesLagFromFinal(t *testing.T) {
	const shard = "tenant-7"

	// The shapes usecases/sharding.classifyReadMiss produces.
	lagging := enterrors.NewErrUnprocessable(fmt.Errorf("applied schema index %d, read resolved at version %d: %w",
		90, 100, enterrors.ErrLocalShardNotFound{Shard: shard}))
	final := enterrors.NewErrUnprocessable(enterrors.ErrNotServedHere{
		Index: "MyClass", Shard: shard, Version: 100,
	})

	tests := []struct {
		name     string
		readErr  error
		wantCode int
	}{
		{"behind on schema reads as unavailable", lagging, http.StatusServiceUnavailable},
		{"caught up and still missing is terminal", final, http.StatusUnprocessableEntity},
		{"an unrelated failure is a fault", io.ErrUnexpectedEOF, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("search", func(t *testing.T) {
				rec := serveShardSearch(t, tt.readErr)
				require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
			})
			t.Run("aggregate", func(t *testing.T) {
				rec := serveShardAggregate(t, tt.readErr)
				require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
			})
		})
	}
}

// versionRecordingShards records the schema version a read handler forwards.
type versionRecordingShards struct {
	shards
	searchVersion    uint64
	aggregateVersion uint64
}

func (v *versionRecordingShards) Search(_ context.Context, _, _ string,
	_ []models.Vector, _ []string, _ float32, _ int, _ *filters.LocalFilter, _ *searchparams.KeywordRanking,
	_ []filters.Sort, _ *filters.Cursor, _ *searchparams.GroupBy, _ additional.Properties,
	_ *dto.TargetCombination, _ []string, schemaVersion uint64,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	v.searchVersion = schemaVersion
	return nil, nil, nil, nil
}

func (v *versionRecordingShards) Aggregate(_ context.Context, _, _ string,
	_ aggregation.Params, schemaVersion uint64,
) (*aggregation.Result, error) {
	v.aggregateVersion = schemaVersion
	return &aggregation.Result{}, nil
}

// TestReadForwardsSchemaVersion checks the version survives the wire. Without it the replica has
// nothing to compare against and has to treat every miss as lag.
func TestReadForwardsSchemaVersion(t *testing.T) {
	newIndices := func(sh shards) *indices {
		logger := logrus.New()
		logger.SetOutput(&bytes.Buffer{})
		return NewIndices(sh, startedDB{}, NewNoopAuthHandler(), func() bool { return false }, logger)
	}

	searchReq := func(t *testing.T, query string) *http.Request {
		t.Helper()
		body, err := shared.IndicesPayloads.SearchParams.Marshal(
			nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost,
			"/indices/MyClass/shards/myshard/objects/_search?"+query, bytes.NewReader(body))
		shared.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)
		return req
	}

	t.Run("search forwards the version it was sent", func(t *testing.T) {
		sh := &versionRecordingShards{}
		rec := httptest.NewRecorder()
		newIndices(sh).Indices().ServeHTTP(rec, searchReq(t, "schema_version=4711"))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		assert.Equal(t, uint64(4711), sh.searchVersion)
	})

	t.Run("aggregate forwards the version it was sent", func(t *testing.T) {
		sh := &versionRecordingShards{}
		body, err := shared.IndicesPayloads.AggregationParams.Marshal(aggregation.Params{})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost,
			"/indices/MyClass/shards/myshard/objects/_aggregations?schema_version=4711",
			bytes.NewReader(body))
		shared.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(req)

		rec := httptest.NewRecorder()
		newIndices(sh).Indices().ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		assert.Equal(t, uint64(4711), sh.aggregateVersion)
	})

	t.Run("a coordinator too old to send one reads as version 0", func(t *testing.T) {
		sh := &versionRecordingShards{}
		rec := httptest.NewRecorder()
		newIndices(sh).Indices().ServeHTTP(rec, searchReq(t, ""))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		assert.Equal(t, uint64(0), sh.searchVersion)
	})

	t.Run("a version that is not a number is the caller's fault", func(t *testing.T) {
		sh := &versionRecordingShards{}
		rec := httptest.NewRecorder()
		newIndices(sh).Indices().ServeHTTP(rec, searchReq(t, "schema_version=later"))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
