//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/replica"
	"github.com/stretchr/testify/assert"
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

func newFakeServer(t *testing.T, method, path string) *fakeServer {
	return &fakeServer{
		method:         method,
		path:           path,
		RequestError:   replica.SimpleResponse{Errors: []string{"error"}},
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
	ctx := context.Background()
	f := newFakeServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects")
	ts := f.server(t)
	defer ts.Close()

	client := NewReplicationClient(ts.Client())
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
	ctx := context.Background()
	uuid := UUID1
	path := "/replicas/indices/C1/shards/S1/objects/" + uuid.String()
	fs := newFakeServer(t, http.MethodDelete, path)
	ts := fs.server(t)
	defer ts.Close()

	client := NewReplicationClient(ts.Client())
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
	ctx := context.Background()
	fs := newFakeServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects")
	fs.RequestError.Errors = append(fs.RequestError.Errors, "error2")
	ts := fs.server(t)
	defer ts.Close()

	client := NewReplicationClient(ts.Client())
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
	ctx := context.Background()
	uuid := UUID1
	f := newFakeServer(t, http.MethodPatch, "/replicas/indices/C1/shards/S1/objects/"+uuid.String())
	ts := f.server(t)
	defer ts.Close()

	client := NewReplicationClient(ts.Client())
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
	ctx := context.Background()
	fs := newFakeServer(t, http.MethodPost, "/replicas/indices/C1/shards/S1/objects/references")
	fs.RequestError.Errors = append(fs.RequestError.Errors, "error2")
	ts := fs.server(t)
	defer ts.Close()

	client := NewReplicationClient(ts.Client())
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
	ctx := context.Background()
	fs := newFakeServer(t, http.MethodDelete, "/replicas/indices/C1/shards/S1/objects")
	fs.RequestError.Errors = append(fs.RequestError.Errors, "error2")
	ts := fs.server(t)
	defer ts.Close()
	client := NewReplicationClient(ts.Client())

	docs := []uint64{1, 2}
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, "", "C1", "S1", "", docs, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("Error", func(t *testing.T) {
		resp, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestError, docs, false)
		assert.Nil(t, err)
		assert.Equal(t, replica.SimpleResponse{Errors: fs.RequestError.Errors}, resp)
	})

	t.Run("DecodeResponse", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestMalFormedResponse, docs, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.DeleteObjects(ctx, fs.host, "C1", "S1", RequestInternalError, docs, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationAbort(t *testing.T) {
	ctx := context.Background()
	path := "/replicas/indices/C1/shards/S1:abort"
	fs := newFakeServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()

	client := NewReplicationClient(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
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

	t.Run("ServerInternalError", func(t *testing.T) {
		_, err := client.Abort(ctx, fs.host, "C1", "S1", RequestInternalError)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "status code")
	})
}

func TestReplicationCommit(t *testing.T) {
	ctx := context.Background()
	path := "/replicas/indices/C1/shards/S1:commit"
	fs := newFakeServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	resp := replica.SimpleResponse{}

	client := NewReplicationClient(ts.Client())
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
