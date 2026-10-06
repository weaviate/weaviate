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

package clusterapi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

const (
	testIndex = "MyClass"
	testShard = "tenant-7"
	testUUID  = "8c29da7a-600a-43b4-ab0f-9ba6e2e3d9b3"
)

func newReplicaServer(t *testing.T, r replicaTypes.Replicator) *httptest.Server {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(r, clusterapi.NewNoopAuthHandler(),
		func() bool { return false }, logger, func() bool { return true })
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	mux.Handle("/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func laggingMiss() error {
	return enterrors.ClassifyReadMiss(enterrors.ErrLocalShardNotFound{Shard: testShard},
		testIndex, testShard, 100, 90)
}

func finalMiss() error {
	return enterrors.ClassifyReadMiss(enterrors.ErrLocalShardNotFound{Shard: testShard},
		testIndex, testShard, 100, 100)
}

// TestReplicaReadMissStatus pins the status each read endpoint answers for the two kinds of
// miss. These used to answer a flat 422 for any miss, telling the coordinator "never" when the
// replica only needed to catch up, while _count answered a retryable 500.
func TestReplicaReadMissStatus(t *testing.T) {
	endpoints := []struct {
		name    string
		expect  func(m *replicaTypes.MockReplicator, readErr error)
		request func(t *testing.T, base string) *http.Request
	}{
		{
			name: "_count",
			expect: func(m *replicaTypes.MockReplicator, readErr error) {
				m.EXPECT().CountObjects(mock.Anything, testIndex, testShard, mock.Anything).Return(0, readErr)
			},
			request: func(t *testing.T, base string) *http.Request {
				return mustRequest(t, http.MethodGet, base+"/indices/"+testIndex+"/shards/"+testShard+"/objects/_count", nil)
			},
		},
		{
			name: "_digest",
			expect: func(m *replicaTypes.MockReplicator, readErr error) {
				m.EXPECT().DigestObjects(mock.Anything, testIndex, testShard, mock.Anything, mock.Anything).
					Return(nil, readErr)
			},
			request: func(t *testing.T, base string) *http.Request {
				body, err := json.Marshal([]strfmt.UUID{testUUID})
				require.NoError(t, err)
				return mustRequest(t, http.MethodGet,
					base+"/indices/"+testIndex+"/shards/"+testShard+"/objects/_digest", bytes.NewReader(body))
			},
		},
		{
			name: "single object",
			expect: func(m *replicaTypes.MockReplicator, readErr error) {
				m.EXPECT().FetchObject(mock.Anything, testIndex, testShard, strfmt.UUID(testUUID), mock.Anything).
					Return(replica.Replica{}, readErr)
			},
			request: func(t *testing.T, base string) *http.Request {
				return mustRequest(t, http.MethodGet,
					base+"/replicas/indices/"+testIndex+"/shards/"+testShard+"/objects/"+testUUID, nil)
			},
		},
		{
			name: "multiple objects",
			expect: func(m *replicaTypes.MockReplicator, readErr error) {
				m.EXPECT().FetchObjects(mock.Anything, testIndex, testShard, mock.Anything, mock.Anything).
					Return(nil, readErr)
			},
			request: func(t *testing.T, base string) *http.Request {
				ids, err := json.Marshal([]strfmt.UUID{testUUID})
				require.NoError(t, err)
				q := url.Values{"ids": []string{base64.StdEncoding.EncodeToString(ids)}}
				return mustRequest(t, http.MethodGet,
					base+"/replicas/indices/"+testIndex+"/shards/"+testShard+"/objects?"+q.Encode(), nil)
			},
		},
	}

	misses := []struct {
		name     string
		readErr  error
		wantCode int
	}{
		{"behind on schema reads as unavailable", laggingMiss(), http.StatusServiceUnavailable},
		{"caught up and still missing is terminal", finalMiss(), http.StatusUnprocessableEntity},
		{"an unrelated failure is a fault", io.ErrUnexpectedEOF, http.StatusInternalServerError},
	}

	for _, ep := range endpoints {
		for _, miss := range misses {
			t.Run(fmt.Sprintf("%s/%s", ep.name, miss.name), func(t *testing.T) {
				m := replicaTypes.NewMockReplicator(t)
				ep.expect(m, miss.readErr)
				server := newReplicaServer(t, m)

				res, err := http.DefaultClient.Do(ep.request(t, server.URL))
				require.NoError(t, err)
				defer res.Body.Close()
				body, _ := io.ReadAll(res.Body)
				assert.Equal(t, miss.wantCode, res.StatusCode, "body: %s", body)
			})
		}
	}
}

// TestReplicaReadForwardsSchemaVersion checks the version survives the wire, and that a
// malformed one is the caller's fault rather than a silent 0.
func TestReplicaReadForwardsSchemaVersion(t *testing.T) {
	countURL := func(base, query string) string {
		u := base + "/indices/" + testIndex + "/shards/" + testShard + "/objects/_count"
		if query != "" {
			u += "?" + query
		}
		return u
	}

	t.Run("the version it was sent", func(t *testing.T) {
		var got uint64
		m := replicaTypes.NewMockReplicator(t)
		m.EXPECT().CountObjects(mock.Anything, testIndex, testShard, mock.Anything).
			RunAndReturn(func(_ context.Context, _, _ string, schemaVersion uint64) (int, error) {
				got = schemaVersion
				return 7, nil
			})
		server := newReplicaServer(t, m)

		res, err := http.DefaultClient.Do(mustRequest(t, http.MethodGet,
			countURL(server.URL, replica.SchemaVersionKey+"=4711"), nil))
		require.NoError(t, err)
		defer res.Body.Close()
		require.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, uint64(4711), got)
	})

	t.Run("a coordinator too old to send one reads as version 0", func(t *testing.T) {
		var got uint64 = 1
		m := replicaTypes.NewMockReplicator(t)
		m.EXPECT().CountObjects(mock.Anything, testIndex, testShard, mock.Anything).
			RunAndReturn(func(_ context.Context, _, _ string, schemaVersion uint64) (int, error) {
				got = schemaVersion
				return 7, nil
			})
		server := newReplicaServer(t, m)

		res, err := http.DefaultClient.Do(mustRequest(t, http.MethodGet, countURL(server.URL, ""), nil))
		require.NoError(t, err)
		defer res.Body.Close()
		require.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, uint64(0), got)
	})

	t.Run("a version that is not a number is the caller's fault", func(t *testing.T) {
		server := newReplicaServer(t, replicaTypes.NewMockReplicator(t))

		res, err := http.DefaultClient.Do(mustRequest(t, http.MethodGet,
			countURL(server.URL, replica.SchemaVersionKey+"=later"), nil))
		require.NoError(t, err)
		defer res.Body.Close()
		assert.Equal(t, http.StatusBadRequest, res.StatusCode)
	})
}

func mustRequest(t *testing.T, method, url string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	require.NoError(t, err)
	return req
}
