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

	"github.com/weaviate/weaviate/entities/additional"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

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
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
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
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
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
		err := client.UpdateShardStatus(ctx, "", "C1", "S1", "NewStatus", 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})
	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			// do nothing
		}
		n++
	}
	t.Run("Success", func(t *testing.T) {
		err := client.UpdateShardStatus(ctx, fs.host, "C1", "S1", "NewStatus", 0)
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
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.Header().Set("content-type", "any")
		case 3:
			clusterapi.IndicesPayloads.GetShardStatusResults.SetContentTypeHeader(w)
		default:
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
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
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
	// maxBackOff drives MaxElapsedTime = n*maxBackOff in the retryer.
	// A small value like 10ms makes MaxElapsedTime=90ms, which a single
	// slow CI round-trip can exhaust before retries complete. 500ms gives
	// MaxElapsedTime=4.5s — still fast in practice (actual backoff starts
	// at minBackOff=1ms) but resilient to CI scheduling jitter.
	ri.maxBackOff = time.Millisecond * 500
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
	t.Helper()

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

func TestRemoteIndexAddAsyncReplicationTargetNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	indexName := "C1"
	shardName := "S1"
	endpoint := AsyncReplicationTargetNodeEndpoint(indexName, shardName)

	fs := newFakeRemoteIndexServer(t, http.MethodPost, endpoint)
	ts := fs.server(t)
	defer ts.Close()

	client := newRemoteIndex(ts.Client())
	override := additional.AsyncReplicationTargetNodeOverride{}

	t.Run("ConnectionError", func(t *testing.T) {
		err := client.AddAsyncReplicationTargetNode(ctx, "", indexName, shardName, override, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusOK)
		}
		n++
	}

	t.Run("Success", func(t *testing.T) {
		err := client.AddAsyncReplicationTargetNode(ctx, fs.host, indexName, shardName, override, 0)
		assert.Nil(t, err)
	})
}

func TestRemoteIndexRemoveAsyncReplicationTargetNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	indexName := "C1"
	shardName := "S1"
	endpoint := AsyncReplicationTargetNodeEndpoint(indexName, shardName)

	fs := newFakeRemoteIndexServer(t, http.MethodDelete, endpoint)
	ts := fs.server(t)
	defer ts.Close()

	client := newRemoteIndex(ts.Client())
	override := additional.AsyncReplicationTargetNodeOverride{}

	t.Run("ConnectionError", func(t *testing.T) {
		err := client.RemoveAsyncReplicationTargetNode(ctx, "", indexName, shardName, override)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "connect")
	})

	n := 0
	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		switch n {
		case 0:
			w.WriteHeader(http.StatusInternalServerError)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
		n++
	}

	t.Run("Success", func(t *testing.T) {
		err := client.RemoveAsyncReplicationTargetNode(ctx, fs.host, indexName, shardName, override)
		assert.Nil(t, err)
	})
}

// TestRemoteIndexSearchShardShedRehydratesOverloaded pins M2: when a remote node
// sheds every shard-search attempt with HTTP 429 and the retryer exhausts its
// attempts, SearchShard must rehydrate queryadmission.ErrOverloaded so the shed
// keeps its identity (429 / gRPC ResourceExhausted) at the coordinator ingress
// mapping instead of degrading to a generic 500.
func TestRemoteIndexSearchShardShedRehydratesOverloaded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1/objects/_search"
	fs := newFakeRemoteIndexServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())

	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("node overloaded, request shed"))
	}

	_, _, _, err := client.SearchShard(ctx, fs.host, "C1", "S1",
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, queryadmission.ErrOverloaded,
		"a cross-node admission shed (429) surviving retry exhaustion must carry ErrOverloaded, got: %v", err)
	// The underlying status detail is preserved for operators.
	require.Contains(t, err.Error(), "429")
}

// TestRemoteIndexSearchShardNon429NotOverloaded is the negative control: a
// non-429 exhaustion (500) must NOT be mislabelled as an admission shed.
func TestRemoteIndexSearchShardNon429NotOverloaded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1/objects/_search"
	fs := newFakeRemoteIndexServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())

	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}

	_, _, _, err := client.SearchShard(ctx, fs.host, "C1", "S1",
		nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, queryadmission.ErrOverloaded)
}
