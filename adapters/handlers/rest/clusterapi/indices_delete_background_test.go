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
	"net/url"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/backgroundaccess"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

type deleteObjectBatchShards struct {
	shards
	background []bool
}

func (f *deleteObjectBatchShards) DeleteObjectBatch(ctx context.Context, _, _ string, _ []strfmt.UUID, _ time.Time, _ bool, _ uint64) objects.BatchSimpleObjects {
	f.background = append(f.background, backgroundaccess.Is(ctx))
	return objects.BatchSimpleObjects{}
}

func TestDeleteObjectsHandlersHonourTheBackgroundMark(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   bool
	}{
		{name: "no header"},
		{name: "background header", header: "true", want: true},
		{name: "unknown header value", header: "yes"},
	}
	uuids, deletionTime := []strfmt.UUID{"a"}, time.UnixMilli(1)
	body, err := shared.IndicesPayloads.BatchDeleteParams.Marshal(uuids, deletionTime, false)
	require.NoError(t, err)
	query := url.Values{replica.RequestKey: {"r1"}, replica.SchemaVersionKey: {"1"}}.Encode()
	newRequest := func(path, header string) *http.Request {
		req := httptest.NewRequest(http.MethodDelete, path+"?"+query, bytes.NewReader(body))
		shared.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(req)
		if header != "" {
			req.Header.Set(backgroundaccess.Header, header)
		}
		return req
	}
	for _, tt := range tests {
		t.Run(tt.name+"/indices", func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			fake := &deleteObjectBatchShards{}
			h := NewIndices(fake, nil, NewNoopAuthHandler(), func() bool { return false }, logger).Indices()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, newRequest("/indices/C1/shards/S1/objects", tt.header))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, []bool{tt.want}, fake.background)
		})
		t.Run(tt.name+"/replicas", func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			r := replicaTypes.NewMockReplicator(t)
			r.EXPECT().ReplicateDeletions(mock.MatchedBy(func(ctx context.Context) bool {
				return backgroundaccess.Is(ctx) == tt.want
			}), "C1", "S1", "r1", uuids, mock.Anything, false, uint64(1)).Return(replica.SimpleResponse{}).Once()
			on, off := func() bool { return true }, func() bool { return false }
			h := NewReplicatedIndices(r, NewNoopAuthHandler(), off, off, logger, on).Indices()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, newRequest("/replicas/indices/C1/shards/S1/objects", tt.header))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		})
	}
}
