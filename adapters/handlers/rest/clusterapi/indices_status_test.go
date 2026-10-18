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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// uncalledShards fails the test when HashTreeLevel reaches it. Any other
// method nil-panics through the embedded shards.
type uncalledShards struct {
	shards
	t *testing.T
}

func (s uncalledShards) HashTreeLevel(context.Context, string, string, int,
	*hashtree.Bitset,
) ([]hashtree.Digest, error) {
	s.t.Error("HashTreeLevel called after the 405")
	return nil, nil
}

func TestIndicesPostOnlyRoutesRejectOtherMethods(t *testing.T) {
	logger, _ := test.NewNullLogger()
	idx := NewIndices(uncalledShards{t: t}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger)

	body, err := hashtree.NewBitset(8).Marshal()
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet,
		"/indices/MyClass/shards/myshard/objects/hashtree/0", bytes.NewReader(body))

	rec := httptest.NewRecorder()
	idx.Indices().ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "body: %s", rec.Body.String())
}

func (o failingShards) PutObject(context.Context, string, string, *storobj.Object, uint64) error {
	return o.err
}

func (o failingShards) GetObject(context.Context, string, string, strfmt.UUID,
	search.SelectProperties, additional.Properties,
) (*storobj.Object, error) {
	return nil, o.err
}

func (o failingShards) Exists(context.Context, string, string, strfmt.UUID) (bool, error) {
	return false, o.err
}

func (o failingShards) DeleteObject(context.Context, string, string, strfmt.UUID, time.Time, uint64) error {
	return o.err
}

func (o failingShards) MergeObject(context.Context, string, string, objects.MergeDocument, uint64) error {
	return o.err
}

func (o failingShards) MultiGetObjects(context.Context, string, string, []strfmt.UUID) ([]*storobj.Object, error) {
	return nil, o.err
}

func (o failingShards) FindUUIDs(context.Context, string, string, *filters.LocalFilter, int) ([]strfmt.UUID, error) {
	return nil, o.err
}

func (o failingShards) DigestObjects(context.Context, string, string, []strfmt.UUID) ([]types.RepairResponse, error) {
	return nil, o.err
}

func (o failingShards) GetShardQueueSize(context.Context, string, string) (int64, error) {
	return 0, o.err
}

func (o failingShards) GetShardStatus(context.Context, string, string) (string, error) {
	return "", o.err
}

func (o failingShards) UpdateShardStatus(context.Context, string, string, string, uint64) error {
	return o.err
}

func (o failingShards) FilePutter(context.Context, string, string, string) (io.WriteCloser, error) {
	return nil, o.err
}

func (o failingShards) CreateShard(context.Context, string, string) error {
	return o.err
}

func (o failingShards) ReInitShard(context.Context, string, string) error {
	return o.err
}

func (o failingShards) AddAsyncReplicationTargetNode(context.Context, string, string,
	additional.AsyncReplicationTargetNodeOverride, uint64,
) error {
	return o.err
}

func (o failingShards) RemoveAsyncReplicationTargetNode(context.Context, string, string,
	additional.AsyncReplicationTargetNodeOverride,
) error {
	return o.err
}

func TestIndicesNamespaceRefusalStatus(t *testing.T) {
	logger, _ := test.NewNullLogger()
	refusal := fmt.Errorf("%w: shard %q", namespaces.ErrNamespaceSuspended, "s")
	idx := NewIndices(failingShards{err: refusal}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger)

	const id = "73f2eb5f-5abf-447a-81ca-74b1dd168247"
	objBody, err := shared.IndicesPayloads.SingleObject.Marshal(
		&storobj.Object{MarshallerVersion: 1, Object: models.Object{Class: "MyClass", ID: id}}, shared.MethodPut)
	require.NoError(t, err)
	searchBody, err := shared.IndicesPayloads.SearchParams.Marshal(
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	aggBody, err := shared.IndicesPayloads.AggregationParams.Marshal(aggregation.Params{})
	require.NoError(t, err)
	findBody, err := shared.IndicesPayloads.FindUUIDsParams.Marshal(nil, 10)
	require.NoError(t, err)
	statusBody, err := shared.IndicesPayloads.UpdateShardStatusParams.Marshal("READONLY")
	require.NoError(t, err)
	getQuery := url.Values{
		"additional":       {base64.StdEncoding.EncodeToString([]byte("{}"))},
		"selectProperties": {base64.StdEncoding.EncodeToString([]byte("[]"))},
	}.Encode()
	multiGetQuery := url.Values{"ids": {base64.StdEncoding.EncodeToString([]byte("[]"))}}.Encode()

	tests := []struct {
		handler     string
		method      string
		path        string
		body        []byte
		contentType string
	}{
		{"postObjectSingle", http.MethodPost, "/objects", objBody, shared.IndicesPayloads.SingleObject.MIME()},
		{"getObject", http.MethodGet, "/objects/" + id + "?" + getQuery, nil, ""},
		{"checkExists", http.MethodGet, "/objects/" + id + "?check_exists=true", nil, ""},
		{"deleteObject", http.MethodDelete, "/objects/" + id, nil, ""},
		{"mergeObject", http.MethodPatch, "/objects/" + id, []byte("{}"), shared.IndicesPayloads.MergeDoc.MIME()},
		{"getObjectsMulti", http.MethodGet, "/objects?" + multiGetQuery, nil, ""},
		{"postSearchObjects", http.MethodPost, "/objects/_search", searchBody, shared.IndicesPayloads.SearchParams.MIME()},
		{"postAggregateObjects", http.MethodPost, "/objects/_aggregations", aggBody, shared.IndicesPayloads.AggregationParams.MIME()},
		{"postFindUUIDs", http.MethodPost, "/objects/_find", findBody, shared.IndicesPayloads.FindUUIDsParams.MIME()},
		{"getObjectsDigest", http.MethodGet, "/objects:digest", []byte("[]"), ""},
		{"getGetShardQueueSize", http.MethodGet, "/queuesize", nil, ""},
		{"getGetShardStatus", http.MethodGet, "/status", nil, ""},
		{"postUpdateShardStatus", http.MethodPost, "/status", statusBody, shared.IndicesPayloads.UpdateShardStatusParams.MIME()},
		{"postShardFile", http.MethodPost, "/files/f", nil, shared.IndicesPayloads.ShardFiles.MIME()},
		{"postShard", http.MethodPost, "", nil, ""},
		{"putShardReinit", http.MethodPut, ":reinit", nil, ""},
		{"postAddAsyncReplicationTargetNode", http.MethodPost, "/async-replication-target-node", []byte("{}"), ""},
		{"deleteAsyncReplicationTargetNode", http.MethodDelete, "/async-replication-target-node", []byte("{}"), ""},
	}
	for _, tc := range tests {
		t.Run(tc.handler, func(t *testing.T) {
			req := httptest.NewRequest(tc.method,
				"/indices/MyClass/shards/myshard"+tc.path, bytes.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("content-type", tc.contentType)
			}

			rec := httptest.NewRecorder()
			idx.Indices().ServeHTTP(rec, req)
			body := strings.TrimSpace(rec.Body.String())
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, "body: %s", body)
			require.True(t, strings.HasSuffix(body, namespaces.ErrNamespaceSuspended.Error()), "body: %s", body)
		})
	}
}

func TestWriteShardRefusal(t *testing.T) {
	rec := httptest.NewRecorder()
	require.True(t, writeShardRefusal(rec, fmt.Errorf("%w: shard %q", namespaces.ErrNamespaceResuming, "s")))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
