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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/weaviate/weaviate/entities/additional"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterapi "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/usecases/objects"
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

// TestRemoteIndexSearchShardShedRehydratesOverloaded pins M2: retry
// exhaustion on a 429-shedding remote node must rehydrate
// queryadmission.ErrOverloaded, not collapse to a generic 500.
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

// TestRemoteIndexAggregateShedRehydratesOverloaded mirrors the SearchShard
// case: a remote aggregation shed (429) surviving retry exhaustion must carry
// ErrOverloaded so the ingress maps it, not collapse to a generic error.
func TestRemoteIndexAggregateShedRehydratesOverloaded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1/objects/_aggregations"
	fs := newFakeRemoteIndexServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())

	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("node overloaded, request shed"))
	}

	_, err := client.Aggregate(ctx, fs.host, "C1", "S1", aggregation.Params{})
	require.Error(t, err)
	require.ErrorIs(t, err, queryadmission.ErrOverloaded,
		"a cross-node aggregation shed (429) surviving retry exhaustion must carry ErrOverloaded, got: %v", err)
	require.Contains(t, err.Error(), "429")
}

func TestRemoteIndexAggregateNon429NotOverloaded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := "/indices/C1/shards/S1/objects/_aggregations"
	fs := newFakeRemoteIndexServer(t, http.MethodPost, path)
	ts := fs.server(t)
	defer ts.Close()
	client := newRemoteIndex(ts.Client())

	fs.doAfter = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}

	_, err := client.Aggregate(ctx, fs.host, "C1", "S1", aggregation.Params{})
	require.Error(t, err)
	require.NotErrorIs(t, err, queryadmission.ErrOverloaded)
}

// TestRemoteIndexDeleteObjectBatch asserts that every id sent to the remote
// shard comes back with its own result, whether the remote shard answered per
// object or the request failed as a whole.
func TestRemoteIndexDeleteObjectBatch(t *testing.T) {
	t.Parallel()

	const (
		id1 = strfmt.UUID("00000000-0000-0000-0000-000000000001")
		id2 = strfmt.UUID("00000000-0000-0000-0000-000000000002")
	)
	path := "/indices/C1/shards/S1/objects"
	writeResults := func(w http.ResponseWriter, body []byte) {
		clusterapi.IndicesPayloads.BatchDeleteResults.SetContentTypeHeader(w)
		_, _ = w.Write(body)
	}

	tests := []struct {
		name string
		// unreachable sends the request to an empty host name.
		unreachable bool
		respond     func(t *testing.T, w http.ResponseWriter)
		// wantErrs maps a position to a substring of its error, empty for a
		// deleted object.
		wantErrs []string
	}{
		{
			name: "per-object results",
			respond: func(t *testing.T, w http.ResponseWriter) {
				body, err := clusterapi.IndicesPayloads.BatchDeleteResults.Marshal(objects.BatchSimpleObjects{
					{UUID: id1},
					{UUID: id2, Err: errors.New("store is read-only")},
				})
				require.NoError(t, err)
				writeResults(w, body)
			},
			wantErrs: []string{"", "store is read-only"},
		},
		{
			name:        "connection error",
			unreachable: true,
			wantErrs:    []string{"send http request", "send http request"},
		},
		{
			name:     "unexpected status code",
			respond:  func(t *testing.T, w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) },
			wantErrs: []string{"unexpected status code", "unexpected status code"},
		},
		{
			name:     "unexpected content type",
			respond:  func(t *testing.T, w http.ResponseWriter) { _, _ = w.Write([]byte("[]")) },
			wantErrs: []string{"unexpected content type", "unexpected content type"},
		},
		{
			name:     "unparseable body",
			respond:  func(t *testing.T, w http.ResponseWriter) { writeResults(w, []byte("not json")) },
			wantErrs: []string{"unmarshal body", "unmarshal body"},
		},
		{
			name:     "one row without an id",
			respond:  func(t *testing.T, w http.ResponseWriter) { writeResults(w, []byte(`[{"UUID":"","Err":{}}]`)) },
			wantErrs: []string{"without an error message", "without an error message"},
		},
		{
			name: "rows without ids",
			respond: func(t *testing.T, w http.ResponseWriter) {
				writeResults(w, []byte(`[{"UUID":"","Err":{}},{"UUID":"","Err":null}]`))
			},
			wantErrs: []string{"without an error message", "no result for this id"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fs := newFakeRemoteIndexServer(t, http.MethodDelete, path)
			fs.doAfter = func(w http.ResponseWriter, r *http.Request) { test.respond(t, w) }
			ts := fs.server(t)
			defer ts.Close()
			host := fs.host
			if test.unreachable {
				host = ""
			}
			ids := []strfmt.UUID{id1, id2}

			results := newRemoteIndex(ts.Client()).DeleteObjectBatch(context.Background(),
				host, "C1", "S1", ids, time.Now(), false, 0)

			require.Len(t, results, len(ids))
			for pos, result := range results {
				require.Equalf(t, ids[pos], result.UUID, "position %d must keep its id", pos)
				if test.wantErrs[pos] == "" {
					require.NoErrorf(t, result.Err, "position %d was deleted", pos)
					continue
				}
				require.ErrorContainsf(t, result.Err, test.wantErrs[pos], "position %d must carry its failure", pos)
			}
		})
	}
}
