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

//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//	 CONTACT: hello@semi.technology
package clients

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
)

func TestRemoteIndexIncreaseRF(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/replicas/indices/C1/replication-factor:increase"
	fs := newFakeRemoteIndexServer(t, http.MethodPut, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		err := client.IncreaseReplicationFactor(ctx, "", "C1", nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
		} else if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		n++
	}
	t.Run("Success", func(t *testing.T) {
		err := client.IncreaseReplicationFactor(ctx, fs.host, "C1", nil)
		assert.Nil(t, err)
	})
}

func TestRemoteIndexReInitShardIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1:reinit"
	fs := newFakeRemoteIndexServer(t, http.MethodPut, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		err := client.ReInitShard(ctx, "", "C1", "S1")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
		} else if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		n++
	}
	t.Run("Success", func(t *testing.T) {
		err := client.ReInitShard(ctx, fs.host, "C1", "S1")
		assert.Nil(t, err)
	})
}

func TestRemoteIndexCreateShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1"
	fs := newFakeRemoteIndexServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		err := client.CreateShard(ctx, "", "C1", "S1")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
		} else if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
		n++
	}
	t.Run("Success", func(t *testing.T) {
		err := client.CreateShard(ctx, fs.host, "C1", "S1")
		assert.Nil(t, err)
	})
}

func TestRemoteIndexUpdateShardStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1/status"
	fs := newFakeRemoteIndexServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		err := client.UpdateShardStatus(ctx, "", "C1", "S1", "NewStatus")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
		} else if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
		}
		n++
	}
	t.Run("Success", func(t *testing.T) {
		err := client.UpdateShardStatus(ctx, fs.host, "C1", "S1", "NewStatus")
		assert.Nil(t, err)
	})
}

func TestRemoteIndexShardStatus(t *testing.T) {
	t.Parallel()
	var (
		ctx    = context.Background()
		path   = "/indices/C1/shards/S1/status"
		fs     = newFakeRemoteIndexServer(t, http.MethodGet, path)
		Status = "READONLY"
	)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())
	t.Run("ConnectionError", func(t *testing.T) {
		_, err := client.GetShardStatus(ctx, "", "C1", "S1")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
		} else if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
		} else if n == 2 {
			w.Header().Set("content-type", "any")
		} else if n == 3 {
			clusterapi.IndicesPayloads.GetShardStatusResults.SetContentTypeHeader(w)
		} else {
			clusterapi.IndicesPayloads.GetShardStatusResults.SetContentTypeHeader(w)
			bytes, _ := clusterapi.IndicesPayloads.GetShardStatusResults.Marshal(Status)
			w.Write(bytes)
		}
		n++
	}

	t.Run("ContentType", func(t *testing.T) {
		_, err := client.GetShardStatus(ctx, fs.host, "C1", "S1")
		assert.NotNil(t, err)
	})
	t.Run("Status", func(t *testing.T) {
		_, err := client.GetShardStatus(ctx, fs.host, "C1", "S1")
		assert.NotNil(t, err)
	})
	t.Run("Success", func(t *testing.T) {
		st, err := client.GetShardStatus(ctx, fs.host, "C1", "S1")
		assert.Nil(t, err)
		assert.Equal(t, "READONLY", st)
	})
}

func TestRemoteIndexPutFile(t *testing.T) {
	t.Parallel()
	var (
		ctx  = context.Background()
		path = "/indices/C1/shards/S1/files/file1"
		fs   = newFakeRemoteIndexServer(t, http.MethodPost, path)
	)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())

	rsc := struct {
		*strings.Reader
		io.Closer
	}{
		strings.NewReader("hello, world"),
		io.NopCloser(nil),
	}
	t.Run("ConnectionError", func(t *testing.T) {
		err := client.PutFile(ctx, "", "C1", "S1", "file1", rsc)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		if n == 0 {
			w.WriteHeader(http.StatusInternalServerError)
		} else if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		n++
	}

	t.Run("Success", func(t *testing.T) {
		err := client.PutFile(ctx, fs.host, "C1", "S1", "file1", rsc)
		assert.Nil(t, err)
	})
}

func newRemoteIndex(httpClient *http.Client) *RemoteIndex {
	ri := NewRemoteIndex(httpClient)
	ri.minBackOff = time.Millisecond * 1
	ri.maxBackOff = time.Millisecond * 10
	ri.timeoutUnit = time.Millisecond * 20
	return ri
}

type fakeRemoteIndexServer struct {
	method   string
	path     string
	host     string
	doBefore func(w http.ResponseWriter, r *http.Request) error
	doAfter  func(w http.ResponseWriter, r *http.Request)
}

func newFakeRemoteIndexServer(t *testing.T, method, path string) *fakeRemoteIndexServer {
	f := &fakeRemoteIndexServer{
		method: method,
		path:   path,
	}
	f.doBefore = func(w http.ResponseWriter, r *http.Request) error {
		if r.Method != f.method {
			w.WriteHeader(http.StatusBadRequest)
			return fmt.Errorf("method want %s got %s", method, r.Method)
		}
		if f.path != r.URL.Path {
			w.WriteHeader(http.StatusBadRequest)
			return fmt.Errorf("path want %s got %s", path, r.URL.Path)
		}
		return nil
	}
	return f
}

func (f *fakeRemoteIndexServer) server(t *testing.T) *httptest.Server {
	if f.doBefore == nil {
		f.doBefore = func(w http.ResponseWriter, r *http.Request) error {
			if r.Method != f.method {
				w.WriteHeader(http.StatusBadRequest)
				return fmt.Errorf("method want %s got %s", f.method, r.Method)
			}
			if f.path != r.URL.Path {
				w.WriteHeader(http.StatusBadRequest)
				return fmt.Errorf("path want %s got %s", f.path, r.URL.Path)
			}
			return nil
		}
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		if err := f.doBefore(w, r); err != nil {
			t.Error(err)
			return
		}
		if f.doAfter != nil {
			f.doAfter(w, r)
		}
	}
	serv := httptest.NewServer(http.HandlerFunc(handler))
	f.host = serv.URL[7:]
	return serv
}
