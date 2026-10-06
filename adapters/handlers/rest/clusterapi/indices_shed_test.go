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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
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
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
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
// The multi-tenant case: a lagging replica is missing a shard rather than a class, and as a
// fault that answered a retryable 500.
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

// One table, because the failure mode is forgetting a route: classification lives in each
// handler, so an overlooked one answers 500 and nothing complains. The subtest below fails
// when a route is registered without a decision here.
func TestLaggingNodeAnswers503OnEveryReplicatedRoute(t *testing.T) {
	const (
		class = "MyClass"
		shard = "myshard"
		id    = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
	)
	base := "/indices/" + class + "/shards/" + shard

	lag := enterrors.NewErrUnprocessable(enterrors.ErrLocalIndexNotFound{Index: class})

	searchBody, err := shared.IndicesPayloads.SearchParams.Marshal(
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	aggBody, err := shared.IndicesPayloads.AggregationParams.Marshal(aggregation.Params{})
	require.NoError(t, err)
	findBody, err := shared.IndicesPayloads.FindUUIDsParams.Marshal(nil, 10)
	require.NoError(t, err)
	batchDeleteBody, err := shared.IndicesPayloads.BatchDeleteParams.Marshal(
		[]strfmt.UUID{strfmt.UUID(id)}, time.Now(), false)
	require.NoError(t, err)

	// getObject and getObjectsMulti reject a malformed url param before reaching the shard,
	// so these have to be well formed for the classification to be what is under test.
	b64 := func(v any) string {
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		return base64.StdEncoding.EncodeToString(raw)
	}
	addParam := b64(additional.Properties{})
	selParam := b64(search.SelectProperties{})
	idsParam := b64([]strfmt.UUID{strfmt.UUID(id)})

	tests := []struct {
		route string // the regexp field on indices that this exercises
		name  string
		req   func() *http.Request
	}{
		{
			route: "regexpObject", name: "get object",
			req: func() *http.Request {
				return httptest.NewRequest(http.MethodGet,
					base+"/objects/"+id+"?additional="+addParam+"&selectProperties="+selParam, nil)
			},
		},
		{
			route: "regexpObject", name: "exists",
			req: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, base+"/objects/"+id+"?check_exists=true", nil)
			},
		},
		{
			route: "regexpObjects", name: "multi get objects",
			req: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, base+"/objects?ids="+idsParam, nil)
			},
		},
		{
			route: "regexpObjectsSearch", name: "search",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, base+"/objects/_search", bytes.NewReader(searchBody))
				shared.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(r)
				return r
			},
		},
		{
			route: "regexpObjectsAggregations", name: "aggregate",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, base+"/objects/_aggregations", bytes.NewReader(aggBody))
				shared.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(r)
				return r
			},
		},
		{
			route: "regexpObjectsFind", name: "find uuids",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, base+"/objects/_find", bytes.NewReader(findBody))
				shared.IndicesPayloads.FindUUIDsParams.SetContentTypeHeaderReq(r)
				return r
			},
		},
		{
			route: "regexpObject", name: "delete object",
			req: func() *http.Request {
				return httptest.NewRequest(http.MethodDelete, base+"/objects/"+id, nil)
			},
		},
		{
			route: "regexpObjects", name: "batch delete objects",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodDelete, base+"/objects", bytes.NewReader(batchDeleteBody))
				shared.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(r)
				return r
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serveLagging(t, lag, tt.req())
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
				"a lagging node must answer 503 here, body=%q", strings.TrimSpace(rec.Body.String()))
		})
	}

	t.Run("every registered route is accounted for", func(t *testing.T) {
		covered := map[string]struct{}{}
		for _, tt := range tests {
			covered[tt.route] = struct{}{}
		}
		for name := range exemptRoutes {
			covered[name] = struct{}{}
		}

		v := reflect.TypeOf(indices{})
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i).Name
			if !strings.HasPrefix(f, "regexp") {
				continue
			}
			_, ok := covered[f]
			assert.True(t, ok,
				"%s is a registered route with no entry in this table and no entry in exemptRoutes: "+
					"decide whether a lagging node must answer 503 on it", f)
		}
	})
}

// exemptRoutes are registered routes that carry no replicated read or write, so the 503
// contract does not apply. Each needs a reason, because the default must be to classify.
var exemptRoutes = map[string]string{
	"regexpObjectsOverwrite":           "repair write, driven by the replication FSM rather than a client read",
	"regexpObjectsDigest":              "repair digest, same path as overwrite",
	"regexpObjectsDigestsInRange":      "repair digest, same path as overwrite",
	"regexpHashTreeLevel":              "async replication internals, not a client read",
	"regexpReferences":                 "batch references, covered by TestWroteNotCaughtUp",
	"regexpShardsQueueSize":            "shard metadata, not a replicated read",
	"regexpShardsStatus":               "shard metadata, not a replicated read",
	"regexpShardFiles":                 "file transfer for shard movement",
	"regexpShard":                      "shard create/delete lifecycle",
	"regexpShardReinit":                "shard lifecycle",
	"regexpAsyncReplicationTargetNode": "async replication target wiring; answers 404 for a missing index",
}

func serveLagging(t *testing.T, lagErr error, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(&bytes.Buffer{})
	idx := NewIndices(laggingShards{err: lagErr}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger)

	rec := httptest.NewRecorder()
	idx.Indices().ServeHTTP(rec, req)
	return rec
}

// laggingShards embeds shards, so a route reaching a method it does not stub nil-panics
// rather than passing on a call that never happened.
type laggingShards struct {
	shards
	err error
}

func (l laggingShards) GetObject(context.Context, string, string, strfmt.UUID,
	search.SelectProperties, additional.Properties,
) (*storobj.Object, error) {
	return nil, l.err
}

func (l laggingShards) Exists(context.Context, string, string, strfmt.UUID) (bool, error) {
	return false, l.err
}

func (l laggingShards) MultiGetObjects(context.Context, string, string,
	[]strfmt.UUID,
) ([]*storobj.Object, error) {
	return nil, l.err
}

func (l laggingShards) Search(context.Context, string, string,
	[]models.Vector, []string, float32, int, *filters.LocalFilter, *searchparams.KeywordRanking,
	[]filters.Sort, *filters.Cursor, *searchparams.GroupBy, additional.Properties,
	*dto.TargetCombination, []string,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	return nil, nil, nil, l.err
}

func (l laggingShards) Aggregate(context.Context, string, string,
	aggregation.Params,
) (*aggregation.Result, error) {
	return nil, l.err
}

func (l laggingShards) FindUUIDs(context.Context, string, string,
	*filters.LocalFilter, int,
) ([]strfmt.UUID, error) {
	return nil, l.err
}

func (l laggingShards) DeleteObject(context.Context, string, string,
	strfmt.UUID, time.Time, uint64,
) error {
	return l.err
}

func (l laggingShards) DeleteObjectBatch(context.Context, string, string,
	[]strfmt.UUID, time.Time, bool, uint64,
) objects.BatchSimpleObjects {
	return objects.BatchSimpleObjects{{Err: l.err}}
}
