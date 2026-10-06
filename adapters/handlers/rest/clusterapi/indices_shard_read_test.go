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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (f failingShards) GetObject(context.Context, string, string, strfmt.UUID,
	search.SelectProperties, additional.Properties,
) (*storobj.Object, error) {
	return nil, f.err
}

func (f failingShards) MultiGetObjects(context.Context, string, string, []strfmt.UUID,
) ([]*storobj.Object, error) {
	return nil, f.err
}

func (f failingShards) Exists(context.Context, string, string, strfmt.UUID) (bool, error) {
	return false, f.err
}

func (f failingShards) FindUUIDs(context.Context, string, string, *filters.LocalFilter, int,
) ([]strfmt.UUID, error) {
	return nil, f.err
}

func (f failingShards) GetShardQueueSize(context.Context, string, string) (int64, error) {
	return 0, f.err
}

func (f failingShards) GetShardStatus(context.Context, string, string) (string, error) {
	return "", f.err
}

// TestShardReadErrorStatusMapping verifies each shard read endpoint returns 422
// for a shard that is not ready and 500 for any other error. check_exists
// returns 500 for both.
func TestShardReadErrorStatusMapping(t *testing.T) {
	encode := func(v any) string {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return base64.StdEncoding.EncodeToString(b)
	}
	const (
		id         = strfmt.UUID("8c7c6a2e-3b5c-4b0e-9a3c-1f2d3e4f5a6b")
		shardPath  = "/indices/MyClass/shards/myshard"
		objectPath = shardPath + "/objects/" + string(id)
	)
	get := func(target string) func(t *testing.T) *http.Request {
		return func(t *testing.T) *http.Request {
			return httptest.NewRequest(http.MethodGet, target, nil)
		}
	}
	endpoints := []struct {
		name         string
		request      func(t *testing.T) *http.Request
		notReadyCode int
	}{
		{"get object", get(objectPath + "?" + url.Values{
			"additional":       {encode(additional.Properties{})},
			"selectProperties": {encode(search.SelectProperties{})},
		}.Encode()), http.StatusUnprocessableEntity},
		{"object exists", get(objectPath + "?check_exists=true"), http.StatusInternalServerError},
		{"get objects", get(shardPath + "/objects?" + url.Values{
			"ids": {encode([]strfmt.UUID{id})},
		}.Encode()), http.StatusUnprocessableEntity},
		{"find uuids", func(t *testing.T) *http.Request {
			body, err := shared.IndicesPayloads.FindUUIDsParams.Marshal(nil, 10)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, shardPath+"/objects/_find", bytes.NewReader(body))
			shared.IndicesPayloads.FindUUIDsParams.SetContentTypeHeaderReq(req)
			return req
		}, http.StatusUnprocessableEntity},
		{"shard queue size", get(shardPath + "/queuesize"), http.StatusUnprocessableEntity},
		{"shard status", get(shardPath + "/status"), http.StatusUnprocessableEntity},
	}
	errs := []struct {
		name     string
		err      error
		notReady bool
	}{
		{"not ready", enterrors.NewErrUnprocessable(errors.New("shard is not ready")), true},
		{"generic error maps to 500", io.ErrUnexpectedEOF, false},
	}
	for _, ep := range endpoints {
		for _, tt := range errs {
			t.Run(ep.name+"/"+tt.name, func(t *testing.T) {
				logger := logrus.New()
				logger.SetOutput(&bytes.Buffer{})
				idx := NewIndices(failingShards{err: tt.err}, startedDB{},
					NewNoopAuthHandler(), func() bool { return false }, logger)

				rec := httptest.NewRecorder()
				idx.Indices().ServeHTTP(rec, ep.request(t))
				wantCode := http.StatusInternalServerError
				if tt.notReady {
					wantCode = ep.notReadyCode
				}
				require.Equal(t, wantCode, rec.Code, "body: %s", rec.Body.String())
			})
		}
	}
}
