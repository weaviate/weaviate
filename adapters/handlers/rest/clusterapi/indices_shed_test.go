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
	"errors"
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
	clusterTypes "github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

// overloadedShards embeds shards so only Search needs implementing; any other
// call nil-panics.
type overloadedShards struct {
	shards
	err error
}

func (o overloadedShards) Search(context.Context, string, string,
	[]models.Vector, []string, float32, int, *filters.LocalFilter, *searchparams.KeywordRanking,
	[]filters.Sort, *filters.Cursor, *searchparams.GroupBy, additional.Properties,
	*dto.TargetCombination, []string, uint64,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	return nil, nil, nil, o.err
}

type startedDB struct{}

func (startedDB) StartupComplete() bool { return true }

// TestSearchErrorStatusMapping verifies a shed query maps to HTTP 429, while
// unrelated errors still map to 500.
func TestSearchErrorStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		searcher error
		wantCode int
	}{
		{"shed maps to 429", queryadmission.ErrOverloaded, http.StatusTooManyRequests},
		{"generic error maps to 500", io.ErrUnexpectedEOF, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serveShardSearch(t, tt.searcher)
			require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func serveShardSearch(t *testing.T, searchErr error) *httptest.ResponseRecorder {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(&bytes.Buffer{})
	idx := NewIndices(overloadedShards{err: searchErr}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger)

	body, err := shared.IndicesPayloads.SearchParams.Marshal(
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost,
		"/indices/MyClass/shards/myshard/objects/_search", bytes.NewReader(body))
	shared.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)

	rec := httptest.NewRecorder()
	idx.Indices().ServeHTTP(rec, req)
	return rec
}

func (o overloadedShards) Aggregate(context.Context, string, string,
	aggregation.Params, uint64,
) (*aggregation.Result, error) {
	return nil, o.err
}

// TestAggregateErrorStatusMapping verifies a shed surfacing from a shard
// aggregation (a ref filter's nested search is admitted) maps to HTTP 429,
// so the coordinator's retryClient backs off as it does for _search.
func TestAggregateErrorStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		aggErr   error
		wantCode int
	}{
		{"shed maps to 429", queryadmission.ErrOverloaded, http.StatusTooManyRequests},
		{"generic error maps to 500", io.ErrUnexpectedEOF, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serveShardAggregate(t, tt.aggErr)
			require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func serveShardAggregate(t *testing.T, aggErr error) *httptest.ResponseRecorder {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(&bytes.Buffer{})
	idx := NewIndices(overloadedShards{err: aggErr}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger)

	body, err := shared.IndicesPayloads.AggregationParams.Marshal(aggregation.Params{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost,
		"/indices/MyClass/shards/myshard/objects/_aggregations", bytes.NewReader(body))
	shared.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(req)

	rec := httptest.NewRecorder()
	idx.Indices().ServeHTTP(rec, req)
	return rec
}

// A node behind on schema must read as unavailable: 500 is retryable, so the caller spends the
// ladder on a node that already said no.
// TestMissingShardIsLagNotFault pins the multi-tenant case: a lagging replica is missing a shard
// rather than a class, and as a fault that answered a retryable 500.
func TestMissingShardIsLagNotFault(t *testing.T) {
	missing := enterrors.ErrLocalShardNotFound{Shard: "sim55298380815951463"}

	t.Run("a missing shard answers not-ready on writes", func(t *testing.T) {
		assert.Equal(t, http.StatusServiceUnavailable, operationStatus(missing))
	})

	t.Run("and on reads", func(t *testing.T) {
		assert.Equal(t, http.StatusServiceUnavailable, unprocessableStatus(missing))
	})

	t.Run("still classified once Index has wrapped it", func(t *testing.T) {
		wrapped := fmt.Errorf("search shard %q: %w", "sim55298380815951463", missing)
		assert.Equal(t, http.StatusServiceUnavailable, operationStatus(wrapped))
	})

	t.Run("the message is unchanged, so text matching still works", func(t *testing.T) {
		assert.Equal(t, `local sim55298380815951463 shard not found`, missing.Error())
	})

	t.Run("the same text without the cause is not classified", func(t *testing.T) {
		assert.Equal(t,
			http.StatusInternalServerError,
			operationStatus(errors.New("local sim55298380815951463 shard not found")),
		)
	})
}

func TestOperationStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "the sender asked for a version this node has not applied",
			err:  fmt.Errorf("wait for schema version 55: %w", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "wrapped per shard, as Index does",
			err:  fmt.Errorf("shard %q: wait for schema version 55: %w", "S1", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "the same text without the cause is not classified",
			err:  errors.New("shard \"S1\": wait for schema version 55: deadline exceeded"),
			want: http.StatusInternalServerError,
		},
		{
			name: "the sentinel on its own",
			err:  fmt.Errorf("something: %w", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a class this node does not have yet",
			err:  enterrors.ErrLocalIndexNotFound{Index: "Product_v2"},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "reached through the unprocessable wrapper the shards layer adds",
			err:  enterrors.NewErrUnprocessable(enterrors.ErrLocalIndexNotFound{Index: "Product_v2"}),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a real failure stays an internal error",
			err:  errors.New("write to disk: no space left on device"),
			want: http.StatusInternalServerError,
		},
		{name: "no error", err: nil, want: http.StatusInternalServerError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, operationStatus(test.err))
		})
	}
}

// A batch blocked entirely by the wait answers unavailable, not a per-object list reading as
// partial success.
func TestWroteNotCaughtUp(t *testing.T) {
	lagging := fmt.Errorf("wait for schema version 55: %w", clusterTypes.ErrDeadlineExceeded)

	tests := []struct {
		name     string
		errs     []error
		wantCode int
	}{
		{name: "no errors", errs: []error{nil, nil}},
		{name: "every object blocked by the wait", errs: []error{lagging, lagging}, wantCode: http.StatusServiceUnavailable},
		{name: "the wait plus a real failure stays per object", errs: []error{lagging, errors.New("disk full")}},
		{name: "a partial success stays per object", errs: []error{nil, errors.New("invalid vector")}},
		{name: "blocked with gaps", errs: []error{nil, lagging, nil}, wantCode: http.StatusServiceUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			wrote := wroteNotCaughtUp(rec, test.errs)

			if test.wantCode == 0 {
				assert.False(t, wrote, "the caller must get its per-object error list")
				return
			}
			assert.True(t, wrote)
			assert.Equal(t, test.wantCode, rec.Code)
		})
	}
}

// A read for a class this node does not hold yet says unavailable: a 422 loses the replica's vote.
func TestUnprocessableStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "a class this node has not applied yet",
			err:  enterrors.NewErrUnprocessable(enterrors.ErrLocalIndexNotFound{Index: "Product_v2"}),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a request that really is unprocessable",
			err:  enterrors.NewErrUnprocessable(errors.New("vector lengths don't match")),
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "a missing shard is not a missing class",
			err:  enterrors.NewErrUnprocessable(errors.New("shard not found")),
			want: http.StatusUnprocessableEntity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, unprocessableStatus(test.err))
		})
	}
}
