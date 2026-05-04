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

package clients

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	RequestError             = "RIDNotFound"
	RequestSuccess           = "RIDSuccess"
	RequestInternalError     = "RIDInternal"
	RequestMalFormedResponse = "RIDMalFormed"
)

const (
	UUID1 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")
	UUID2 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242")
)

type fakeServer struct {
	method                string
	path                  string
	RequestError          replica.SimpleResponse
	RequestSuccess        replica.SimpleResponse
	host                  string
	ExpectedSchemaVersion string
}

func newFakeReplicationServer(t *testing.T, method, path string, schemaVersion uint64) *fakeServer {
	t.Helper()
	return &fakeServer{
		method:                method,
		path:                  path,
		RequestError:          replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}},
		RequestSuccess:        replica.SimpleResponse{},
		ExpectedSchemaVersion: fmt.Sprint(schemaVersion),
	}
}

func (f *fakeServer) server(t *testing.T) *httptest.Server {
	t.Helper()
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != f.method {
			t.Errorf("method want %s got %s", f.method, r.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if f.path != r.URL.Path {
			t.Errorf("path want %s got %s", f.path, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		schemaVersion := r.URL.Query().Get(replica.SchemaVersionKey)
		if f.ExpectedSchemaVersion != "0" && schemaVersion != f.ExpectedSchemaVersion {
			t.Errorf("schemaVersion want %s got %s", f.ExpectedSchemaVersion, schemaVersion)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		switch requestID {
		case RequestInternalError:
			w.WriteHeader(http.StatusInternalServerError)
		case RequestError:
			bytes, _ := json.Marshal(&f.RequestError)
			w.Write(bytes)
		case RequestSuccess:
			bytes, _ := json.Marshal(&replica.SimpleResponse{})
			w.Write(bytes)
		case RequestMalFormedResponse:
			w.Write([]byte(`mal formed`))
		}
	}
	serv := httptest.NewServer(http.HandlerFunc(handler))
	f.host = serv.URL[7:]
	return serv
}

func anyObject(uuid strfmt.UUID) models.Object {
	return models.Object{
		Class:              "C1",
		CreationTimeUnix:   900000000001,
		LastUpdateTimeUnix: 900000000002,
		ID:                 uuid,
		Properties: map[string]interface{}{
			"stringProp":    "string",
			"textProp":      "text",
			"datePropArray": []string{"1980-01-01T00:00:00+02:00"},
		},
	}
}

func TestReplicationPutObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFakeReplicationServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects", 0)
	ts := f.server(t)
	defer ts.Close()

	client := newReplicationClient(t, ts.Client())
	t.Run("EncodeRequest", func(t *testing.T) {
		obj := &storobj.Object{}
		_, err := client.PutObject(ctx, "Node1", "C1", "S1", "RID", obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "encode")
	})

	obj := &storobj.Object{MarshallerVersion: 1, Object: anyObject(UUID1)}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.PutObject(ctx, "", "C1", "S1", "", obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.PutObject(ctx, f.host, "C1", "S1", RequestError, obj, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: f.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.PutObject(ctx, f.host, "C1", "S1", RequestMalFormedResponse, obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.PutObject(ctx, f.host, "C1", "S1", RequestInternalError, obj, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationDeleteObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uuid := UUID1
	deletionTime := time.Now()
	path := fmt.Sprintf("/replicas/indices/C1/shards/S1/objects/%s/%d", uuid.String(), deletionTime.UnixMilli())
	fs := newFakeReplicationServer(t, http.MethodDelete, path, 0)
	ts := fs.server(t)
	defer ts.Close()

	client := newReplicationClient(t, ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, "", "C1", "S1", "", uuid, deletionTime, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObject(ctx, fs.host, "C1", "S1", RequestError, uuid, deletionTime, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, uuid, deletionTime, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, fs.host, "C1", "S1", RequestInternalError, uuid, deletionTime, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationPutObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := newFakeReplicationServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects", 123)
	fs.RequestError.Errors = append(fs.RequestError.Errors, replica.Error{Msg: "error2"})
	ts := fs.server(t)
	defer ts.Close()

	client := newReplicationClient(t, ts.Client())
	t.Run("EncodeRequest", func(t *testing.T) {
		objs := []*storobj.Object{{}}
		_, err := client.PutObjects(ctx, "Node1", "C1", "S1", "RID", objs, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "encode")
	})

	objects := []*storobj.Object{
		{MarshallerVersion: 1, Object: anyObject(UUID1)},
		{MarshallerVersion: 1, Object: anyObject(UUID2)},
	}

	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.PutObjects(ctx, "", "C1", "S1", "", objects, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.PutObjects(ctx, fs.host, "C1", "S1", RequestError, objects, 123)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.PutObjects(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, objects, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.PutObjects(ctx, fs.host, "C1", "S1", RequestInternalError, objects, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationMergeObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uuid := UUID1
	f := newFakeReplicationServer(t, http.MethodPatch, "/replicas/indices/C1/shards/S1/objects/"+uuid.String(), 0)
	ts := f.server(t)
	defer ts.Close()

	client := newReplicationClient(t, ts.Client())
	doc := &objects.MergeDocument{ID: uuid}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.MergeObject(ctx, "", "C1", "S1", "", doc, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.MergeObject(ctx, f.host, "C1", "S1", RequestError, doc, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: f.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.MergeObject(ctx, f.host, "C1", "S1", RequestMalFormedResponse, doc, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.MergeObject(ctx, f.host, "C1", "S1", RequestInternalError, doc, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationAddReferences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := newFakeReplicationServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects/references", 0)
	fs.RequestError.Errors = append(fs.RequestError.Errors, replica.Error{Msg: "error2"})
	ts := fs.server(t)
	defer ts.Close()

	client := newReplicationClient(t, ts.Client())
	refs := []objects.BatchReference{{OriginalIndex: 1}, {OriginalIndex: 2}}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.AddReferences(ctx, "", "C1", "S1", "", refs, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.AddReferences(ctx, fs.host, "C1", "S1", RequestError, refs, 0)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.AddReferences(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, refs, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.AddReferences(ctx, fs.host, "C1", "S1", RequestInternalError, refs, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationDeleteObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := newFakeReplicationServer(t, http.MethodDelete, "/replicas/indices/C1/shards/S1/objects", 0)
	fs.RequestError.Errors = append(fs.RequestError.Errors, replica.Error{Msg: "error2"})
	ts := fs.server(t)
	defer ts.Close()
	client := newReplicationClient(t, ts.Client())

	uuids := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
	deletionTime := time.Now()

	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, "", "C1", "S1", "", uuids, deletionTime, false, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestError, uuids, deletionTime, false, 123)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, uuids, deletionTime, false, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestInternalError, uuids, deletionTime, false, 123)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationAbort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/replicas/indices/C1/shards/S1:abort"
	fs := newFakeReplicationServer(t, http.MethodPost, path, 0)
	ts := fs.server(t)
	defer ts.Close()
	client := newReplicationClient(t, ts.Client())

	t.Run("ConnectionError", func(t *testing.T) {
		client := newReplicationClient(t, ts.Client())
		client.maxBackOff = client.timeoutUnit * 20
		_, err := client.Abort(ctx, "", "C1", "S1", "")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.Abort(ctx, fs.host, "C1", "S1", RequestError)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.Abort(ctx, fs.host, "C1", "S1", RequestMalFormedResponse)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})
	// timeoutUnit * 5 drives the per-request context deadline. Setting it to
	// maxBackOff*100 (800ms) makes the deadline >> MaxElapsedTime (72ms), so
	// the retry loop always exhausts retries rather than the context, even under
	// CI scheduling pressure.
	client.timeoutUnit = client.maxBackOff * 100
	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.Abort(ctx, fs.host, "C1", "S1", RequestInternalError)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/replicas/indices/C1/shards/S1:commit"
	fs := newFakeReplicationServer(t, http.MethodPost, path, 0)
	ts := fs.server(t)
	defer ts.Close()
	resp := replica.SimpleResponse{}
	client := newReplicationClient(t, ts.Client())

	t.Run("ConnectionError", func(t *testing.T) {
		err := client.Commit(ctx, "", "C1", "S1", "", &resp)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		err := client.Commit(ctx, fs.host, "C1", "S1", RequestError, &resp)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		err := client.Commit(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, &resp)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		err := client.Commit(ctx, fs.host, "C1", "S1", RequestInternalError, &resp)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationFetchObject(t *testing.T) {
	t.Parallel()

	expected := replica.Replica{
		ID: UUID1,
		Object: &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID: UUID1,
				Properties: map[string]interface{}{
					"stringProp": "abc",
				},
			},
			Vector:    []float32{1, 2, 3, 4, 5},
			VectorLen: 5,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := expected.MarshalBinary()
		w.Write(b)
	}))

	c := newReplicationClient(t, server.Client())
	resp, err := c.FetchObject(context.Background(), server.URL[7:],
		"C1", "S1", expected.ID, nil, additional.Properties{}, 9)
	require.Nil(t, err)
	assert.Equal(t, expected.ID, resp.ID)
	assert.Equal(t, expected.Deleted, resp.Deleted)
	assert.EqualValues(t, expected.Object, resp.Object)
}

func TestReplicationFetchObjects(t *testing.T) {
	t.Parallel()
	expected := replica.Replicas{
		{
			ID: UUID1,
			Object: &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID: UUID1,
					Properties: map[string]interface{}{
						"stringProp": "abc",
					},
				},
				Vector:    []float32{1, 2, 3, 4, 5},
				VectorLen: 5,
			},
		},
		{
			ID: UUID2,
			Object: &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID: UUID2,
					Properties: map[string]interface{}{
						"floatProp": float64(123),
					},
				},
				Vector:    []float32{10, 20, 30, 40, 50},
				VectorLen: 5,
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := expected.MarshalBinary()
		w.Write(b)
	}))

	c := newReplicationClient(t, server.Client())
	resp, err := c.FetchObjects(context.Background(), server.URL[7:], "C1", "S1", []strfmt.UUID{expected[0].ID})
	require.Nil(t, err)
	require.Len(t, resp, 2)
	assert.Equal(t, expected[0].ID, resp[0].ID)
	assert.Equal(t, expected[0].Deleted, resp[0].Deleted)
	assert.EqualValues(t, expected[0].Object, resp[0].Object)
	assert.Equal(t, expected[1].ID, resp[1].ID)
	assert.Equal(t, expected[1].Deleted, resp[1].Deleted)
	assert.EqualValues(t, expected[1].Object, resp[1].Object)
}

func TestReplicationDigestObjects(t *testing.T) {
	t.Parallel()

	now := time.Now()
	expected := []types.RepairResponse{
		{
			ID:         UUID1.String(),
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
		{
			ID:         UUID2.String(),
			Deleted:    true,
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := json.Marshal(expected)
		w.Write(b)
	}))

	c := newReplicationClient(t, server.Client())
	resp, err := c.DigestObjects(context.Background(), server.URL[7:], "C1", "S1", []strfmt.UUID{
		strfmt.UUID(expected[0].ID),
		strfmt.UUID(expected[1].ID),
	}, 9)
	require.Nil(t, err)
	require.Len(t, resp, 2)
	assert.Equal(t, expected[0].ID, resp[0].ID)
	assert.Equal(t, expected[0].Deleted, resp[0].Deleted)
	assert.Equal(t, expected[0].UpdateTime, resp[0].UpdateTime)
	assert.Equal(t, expected[0].Version, resp[0].Version)
	assert.Equal(t, expected[1].ID, resp[1].ID)
	assert.Equal(t, expected[1].Deleted, resp[1].Deleted)
	assert.Equal(t, expected[1].UpdateTime, resp[1].UpdateTime)
	assert.Equal(t, expected[1].Version, resp[1].Version)
}

func TestReplicationOverwriteObjects(t *testing.T) {
	t.Parallel()

	now := time.Now()
	input := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 UUID1,
				Class:              "C1",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(),
				Properties: map[string]interface{}{
					"stringProp": "abc",
				},
				Vector: []float32{1, 2, 3, 4, 5},
			},
			StaleUpdateTime: now.UnixMilli(),
			Version:         0,
		},
	}
	expected := []types.RepairResponse{
		{
			ID:         UUID1.String(),
			Version:    1,
			UpdateTime: now.Add(time.Hour).UnixMilli(),
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := json.Marshal(expected)
		w.Write(b)
	}))

	c := newReplicationClient(t, server.Client())
	resp, err := c.OverwriteObjects(context.Background(), server.URL[7:], "C1", "S1", input)
	require.Nil(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, expected[0].ID, resp[0].ID)
	assert.Equal(t, expected[0].Version, resp[0].Version)
	assert.Equal(t, expected[0].UpdateTime, resp[0].UpdateTime)
}

func TestReplicationHashTreeLevel(t *testing.T) {
	t.Parallel()

	expected := []hashtree.Digest{
		{0x0102030405060708, 0x090a0b0c0d0e0f10},
		{0xdeadbeefcafebabe, 0x0123456789abcdef},
	}

	discriminant := hashtree.NewBitset(4)
	discriminant.Set(0)
	discriminant.Set(2)

	t.Run("BinaryResponse", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "zstd", r.Header.Get("X-Request-Compression"))
			assert.Equal(t, "binary", r.Header.Get("X-Accept-Response-Encoding"))
			buf := make([]byte, len(expected)*hashtree.DigestLength)
			for i, d := range expected {
				b, _ := d.MarshalBinary()
				copy(buf[i*hashtree.DigestLength:], b)
			}
			w.Header().Set("X-Response-Encoding", "binary")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(buf)
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		c.timeoutUnit = time.Second
		got, err := c.HashTreeLevel(context.Background(), server.URL[7:], "C1", "S1", 3, discriminant)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	})

	t.Run("JSONFallback", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// No X-Response-Encoding header → legacy JSON path
			b, _ := json.Marshal(expected)
			w.Write(b)
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		c.timeoutUnit = time.Second
		got, err := c.HashTreeLevel(context.Background(), server.URL[7:], "C1", "S1", 3, discriminant)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	})

	t.Run("ConnectionError", func(t *testing.T) {
		c := newReplicationClient(t, &http.Client{})
		_, err := c.HashTreeLevel(context.Background(), "127.0.0.1:0", "C1", "S1", 3, discriminant)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("ServerError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		c.timeoutUnit = time.Second
		_, err := c.HashTreeLevel(context.Background(), server.URL[7:], "C1", "S1", 3, discriminant)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "status code")
	})

	t.Run("InvalidBinaryLength", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Response-Encoding", "binary")
			w.Write([]byte{0x01, 0x02, 0x03}) // not a multiple of DigestLength
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		c.timeoutUnit = time.Second
		_, err := c.HashTreeLevel(context.Background(), server.URL[7:], "C1", "S1", 3, discriminant)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read digest")
	})
}

func TestReplicationDigestObjectsInRange(t *testing.T) {
	t.Parallel()

	now := time.Now()
	expected := []types.RepairResponse{
		{ID: UUID1.String(), UpdateTime: now.UnixMilli()},
		{ID: UUID2.String(), UpdateTime: now.Add(time.Hour).UnixMilli()},
	}

	// buildBinaryPayload encodes expected records using the same layout as
	// writeDigestsInRangeResponse: 16-byte UUID + 8-byte UpdateTime big-endian.
	buildBinaryPayload := func(records []types.RepairResponse) []byte {
		buf := make([]byte, len(records)*replica.DigestObjectsInRangeRecordLength)
		for i, r := range records {
			id, err := uuid.Parse(r.ID)
			require.NoError(t, err)
			idBytes, err := id.MarshalBinary()
			require.NoError(t, err)
			copy(buf[i*replica.DigestObjectsInRangeRecordLength:], idBytes)
			binary.BigEndian.PutUint64(buf[i*replica.DigestObjectsInRangeRecordLength+16:], uint64(r.UpdateTime))
		}
		return buf
	}

	t.Run("BinaryResponse", func(t *testing.T) {
		payload := buildBinaryPayload(expected) // build in test goroutine; buildBinaryPayload uses require.*
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "binary", r.Header.Get("X-Accept-Response-Encoding"))
			w.Header().Set("X-Response-Encoding", "binary")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(payload) //nolint:errcheck
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		// timeoutUnit*20 is the per-request deadline; set it large enough to
		// survive CI scheduling jitter without relying on retry.
		c.timeoutUnit = time.Second
		got, err := c.DigestObjectsInRange(context.Background(), server.URL[7:], "C1", "S1", UUID1, UUID2, 10)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, expected[0].ID, got[0].ID)
		assert.Equal(t, expected[0].UpdateTime, got[0].UpdateTime)
		assert.Equal(t, expected[1].ID, got[1].ID)
		assert.Equal(t, expected[1].UpdateTime, got[1].UpdateTime)
	})

	t.Run("JSONFallback", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// No X-Response-Encoding header → legacy JSON path.
			b, _ := json.Marshal(replica.DigestObjectsInRangeResp{Digests: expected})
			w.Write(b) //nolint:errcheck
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		c.timeoutUnit = time.Second
		got, err := c.DigestObjectsInRange(context.Background(), server.URL[7:], "C1", "S1", UUID1, UUID2, 10)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, expected[0].ID, got[0].ID)
		assert.Equal(t, expected[0].UpdateTime, got[0].UpdateTime)
		assert.Equal(t, expected[1].ID, got[1].ID)
		assert.Equal(t, expected[1].UpdateTime, got[1].UpdateTime)
	})

	t.Run("EmptyBinaryResponse", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Response-Encoding", "binary")
			w.Header().Set("Content-Type", "application/octet-stream")
			// empty body — zero records
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		c.timeoutUnit = time.Second
		got, err := c.DigestObjectsInRange(context.Background(), server.URL[7:], "C1", "S1", UUID1, UUID2, 10)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("ConnectionError", func(t *testing.T) {
		c := newReplicationClient(t, &http.Client{})
		_, err := c.DigestObjectsInRange(context.Background(), "127.0.0.1:0", "C1", "S1", UUID1, UUID2, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("ServerError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		_, err := c.DigestObjectsInRange(context.Background(), server.URL[7:], "C1", "S1", UUID1, UUID2, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "status code")
	})

	t.Run("TruncatedBinaryRecord", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Response-Encoding", "binary")
			// 10 bytes — not a multiple of DigestObjectsInRangeRecordLength (24)
			w.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}) //nolint:errcheck
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		_, err := c.DigestObjectsInRange(context.Background(), server.URL[7:], "C1", "S1", UUID1, UUID2, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read digest in range record")
	})
}

func TestReplicationOverwriteObjectsCompression(t *testing.T) {
	t.Parallel()

	now := time.Now()
	input := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 UUID1,
				Class:              "C1",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(),
				Properties: map[string]interface{}{
					"stringProp": "abc",
				},
				Vector: []float32{1, 2, 3, 4, 5},
			},
			StaleUpdateTime: now.UnixMilli(),
			Version:         0,
		},
	}
	expected := []types.RepairResponse{
		{ID: UUID1.String(), UpdateTime: now.Add(time.Hour).UnixMilli()},
	}

	t.Run("SetsZstdCompressionHeader", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "zstd", r.Header.Get("X-Request-Compression"))
			assert.Equal(t, "binary", r.Header.Get("X-Request-Encoding"))
			b, _ := json.Marshal(expected)
			w.Write(b)
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		resp, err := c.OverwriteObjects(context.Background(), server.URL[7:], "C1", "S1", input)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, expected[0].ID, resp[0].ID)
	})

	t.Run("BodyIsZstdCompressed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dec, err := zstd.NewReader(r.Body)
			require.NoError(t, err)
			defer dec.Close()

			raw, err := io.ReadAll(dec)
			require.NoError(t, err)

			got, err := shared.IndicesPayloads.VersionedObjectList.UnmarshalV2(raw)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, input[0].LatestObject.ID, got[0].LatestObject.ID)

			b, _ := json.Marshal(expected)
			w.Write(b)
		}))
		defer server.Close()

		c := newReplicationClient(t, server.Client())
		_, err := c.OverwriteObjects(context.Background(), server.URL[7:], "C1", "S1", input)
		require.NoError(t, err)
	})
}

func TestReplicationOverwriteObjectsConcurrent(t *testing.T) {
	t.Parallel()

	now := time.Now()
	input := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 UUID1,
				Class:              "C1",
				LastUpdateTimeUnix: now.UnixMilli(),
			},
			StaleUpdateTime: now.UnixMilli(),
		},
	}
	expected := []types.RepairResponse{
		{ID: UUID1.String(), UpdateTime: now.UnixMilli()},
	}

	// Each request handler decompresses the body independently to verify the
	// pooled encoder produces correct output under concurrent use.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dec, err := zstd.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer dec.Close()
		if _, err := io.ReadAll(dec); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b, _ := json.Marshal(expected)
		w.Write(b)
	}))
	defer server.Close()

	// A single client is shared across all goroutines to exercise the encoder
	// pool under concurrent use. Run with -race to detect data races.
	c := newReplicationClient(t, server.Client())

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = c.OverwriteObjects(context.Background(), server.URL[7:], "C1", "S1", input)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "goroutine %d failed", i)
	}
}

func TestExpBackOff(t *testing.T) {
	N := 200
	av := time.Duration(0)
	delay := time.Nanosecond * 20
	for i := 0; i < N; i++ {
		av += backOff(delay)
	}
	av /= time.Duration(N)
	if av < time.Nanosecond*30 || av > time.Nanosecond*50 {
		t.Errorf("average time got %v", av)
	}
}

func newReplicationClient(t *testing.T, httpClient *http.Client) *replicationClient {
	t.Helper()
	c, err := NewReplicationClient(httpClient)
	require.NoError(t, err)
	c.minBackOff = time.Millisecond * 1
	c.maxBackOff = time.Millisecond * 8
	c.timeoutUnit = time.Millisecond * 20
	return c
}
