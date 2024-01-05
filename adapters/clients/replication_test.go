//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
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
	method         string
	path           string
	RequestError   replica.SimpleResponse
	RequestSuccess replica.SimpleResponse
	host           string
}

func newFakeReplicationServer(t *testing.T, method, path string) *fakeServer {
	return &fakeServer{
		method:         method,
		path:           path,
		RequestError:   replica.SimpleResponse{Errors: []replica.Error{{Msg: "error"}}},
		RequestSuccess: replica.SimpleResponse{},
	}
}

func (f *fakeServer) server(t *testing.T) *httptest.Server {
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
	f := newFakeReplicationServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects")
	ts := f.server(t)
	defer ts.Close()

	client := newReplicationClient(ts.Client())
	t.Run("EncodeRequest", func(t *testing.T) {
		obj := &storobj.Object{}
		_, err := client.PutObject(ctx, "Node1", "C1", "S1", "RID", obj)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "encode")
	})

	obj := &storobj.Object{MarshallerVersion: 1, Object: anyObject(UUID1)}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.PutObject(ctx, "", "C1", "S1", "", obj)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.PutObject(ctx, f.host, "C1", "S1", RequestError, obj)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: f.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.PutObject(ctx, f.host, "C1", "S1", RequestMalFormedResponse, obj)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.PutObject(ctx, f.host, "C1", "S1", RequestInternalError, obj)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationDeleteObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uuid := UUID1
	path := "/replicas/indices/C1/shards/S1/objects/" + uuid.String()
	fs := newFakeReplicationServer(t, http.MethodDelete, path)
	ts := fs.server(t)
	defer ts.Close()

	client := newReplicationClient(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, "", "C1", "S1", "", uuid)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObject(ctx, fs.host, "C1", "S1", RequestError, uuid)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, uuid)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, fs.host, "C1", "S1", RequestInternalError, uuid)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationPutObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := newFakeReplicationServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects")
	fs.RequestError.Errors = append(fs.RequestError.Errors, replica.Error{Msg: "error2"})
	ts := fs.server(t)
	defer ts.Close()

	client := newReplicationClient(ts.Client())
	t.Run("EncodeRequest", func(t *testing.T) {
		objs := []*storobj.Object{{}}
		_, err := client.PutObjects(ctx, "Node1", "C1", "S1", "RID", objs)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "encode")
	})

	objects := []*storobj.Object{
		{MarshallerVersion: 1, Object: anyObject(UUID1)},
		{MarshallerVersion: 1, Object: anyObject(UUID2)},
	}

	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.PutObjects(ctx, "", "C1", "S1", "", objects)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.PutObjects(ctx, fs.host, "C1", "S1", RequestError, objects)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.PutObjects(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, objects)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.PutObjects(ctx, fs.host, "C1", "S1", RequestInternalError, objects)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationMergeObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uuid := UUID1
	f := newFakeReplicationServer(t, http.MethodPatch, "/replicas/indices/C1/shards/S1/objects/"+uuid.String())
	ts := f.server(t)
	defer ts.Close()

	client := newReplicationClient(ts.Client())
	doc := &objects.MergeDocument{ID: uuid}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.MergeObject(ctx, "", "C1", "S1", "", doc)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.MergeObject(ctx, f.host, "C1", "S1", RequestError, doc)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: f.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.MergeObject(ctx, f.host, "C1", "S1", RequestMalFormedResponse, doc)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.MergeObject(ctx, f.host, "C1", "S1", RequestInternalError, doc)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationAddReferences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := newFakeReplicationServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects/references")
	fs.RequestError.Errors = append(fs.RequestError.Errors, replica.Error{Msg: "error2"})
	ts := fs.server(t)
	defer ts.Close()

	client := newReplicationClient(ts.Client())
	refs := []objects.BatchReference{{OriginalIndex: 1}, {OriginalIndex: 2}}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.AddReferences(ctx, "", "C1", "S1", "", refs)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.AddReferences(ctx, fs.host, "C1", "S1", RequestError, refs)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.AddReferences(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, refs)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.AddReferences(ctx, fs.host, "C1", "S1", RequestInternalError, refs)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationDeleteObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fs := newFakeReplicationServer(t, http.MethodDelete, "/replicas/indices/C1/shards/S1/objects")
	fs.RequestError.Errors = append(fs.RequestError.Errors, replica.Error{Msg: "error2"})
	ts := fs.server(t)
	defer ts.Close()
	client := newReplicationClient(ts.Client())

	uuids := []strfmt.UUID{strfmt.UUID("1"), strfmt.UUID("2")}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, "", "C1", "S1", "", uuids, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestError, uuids, false)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, uuids, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestInternalError, uuids, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationAbort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/replicas/indices/C1/shards/S1:abort"
	fs := newFakeReplicationServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newReplicationClient(ts.Client())

	t.Run("ConnectionError", func(t *testing.T) {
		client := newReplicationClient(ts.Client())
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
	client.timeoutUnit = client.maxBackOff * 3
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
	fs := newFakeReplicationServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	resp := replica.SimpleResponse{}
	client := newReplicationClient(ts.Client())

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

	expected := objects.Replica{
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

	c := newReplicationClient(server.Client())
	resp, err := c.FetchObject(context.Background(), server.URL[7:],
		"C1", "S1", expected.ID, nil, additional.Properties{})
	require.Nil(t, err)
	assert.Equal(t, expected.ID, resp.ID)
	assert.Equal(t, expected.Deleted, resp.Deleted)
	assert.EqualValues(t, expected.Object, resp.Object)
}

func TestReplicationFetchObjects(t *testing.T) {
	t.Parallel()
	expected := objects.Replicas{
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

	c := newReplicationClient(server.Client())
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
	expected := []replica.RepairResponse{
		{
			ID:         UUID1.String(),
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
		{
			ID:         UUID2.String(),
			UpdateTime: now.UnixMilli(),
			Version:    1,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := json.Marshal(expected)
		w.Write(b)
	}))

	c := newReplicationClient(server.Client())
	resp, err := c.DigestObjects(context.Background(), server.URL[7:], "C1", "S1", []strfmt.UUID{
		strfmt.UUID(expected[0].ID),
		strfmt.UUID(expected[1].ID),
	})
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
	expected := []replica.RepairResponse{
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

	c := newReplicationClient(server.Client())
	resp, err := c.OverwriteObjects(context.Background(), server.URL[7:], "C1", "S1", input)
	require.Nil(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, expected[0].ID, resp[0].ID)
	assert.Equal(t, expected[0].Version, resp[0].Version)
	assert.Equal(t, expected[0].UpdateTime, resp[0].UpdateTime)
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

func newReplicationClient(httpClient *http.Client) *replicationClient {
	c := NewReplicationClient(httpClient).(*replicationClient)
	c.minBackOff = time.Millisecond * 1
	c.maxBackOff = time.Millisecond * 8
	c.timeoutUnit = time.Millisecond * 20
	return c
}
