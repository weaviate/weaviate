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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
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
	*dto.TargetCombination, []string,
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

	body, err := IndicesPayloads.SearchParams.Marshal(
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost,
		"/indices/MyClass/shards/myshard/objects/_search", bytes.NewReader(body))
	IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)

	rec := httptest.NewRecorder()
	idx.Indices().ServeHTTP(rec, req)
	return rec
}
