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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/klauspost/compress/zstd"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

type maintenanceRouteCase struct {
	method      string
	path        string
	allowlisted bool
}

func maintenanceRouteCases() []maintenanceRouteCase {
	const shard = "/replicas/indices/MyClass/shards/myshard"
	const checkpoint = "/replicas/indices/MyClass/async-checkpoint"
	const roots = "/replicas/indices/MyClass/objects/_compareHashTreeRoots"
	return []maintenanceRouteCase{
		{"POST", shared.CompareHashTreeRootsMultiPath, true},
		{"POST", roots, true},
		{"POST", shard + "/objects/hashtree/level/3", true},
		{"POST", shard + "/objects/compareDigests", true},
		{"POST", shard + "/objects/digestsInRange", true},
		{"PUT", shard + "/objects/_overwrite", true},
		{"GET", checkpoint, true},
		{"POST", checkpoint, true},
		{"DELETE", checkpoint, true},

		{"GET", shard + "/objects/_digest", false},
		{"DELETE", shard + "/objects/deadbeef", false},
		{"PATCH", shard + "/objects/deadbeef", false},
		{"GET", shard + "/objects/deadbeef", false},
		{"POST", shard + "/objects/references", false},
		{"GET", shard + "/objects", false},
		{"POST", shard + "/objects", false},
		{"DELETE", shard + "/objects", false},
		{"GET", shard + "/objects/_count", false},
		{"PUT", shard + "/replication-factor:increase", false},
		{"POST", shard + ":commit", false},
		{"POST", shard + ":abort", false},

		{"GET", shared.CompareHashTreeRootsMultiPath, false},
		{"PUT", shared.CompareHashTreeRootsMultiPath, false},
		{"GET", roots, false},
		{"GET", shard + "/objects/hashtree/level/3", false},
		{"GET", shard + "/objects/compareDigests", false},
		{"GET", shard + "/objects/digestsInRange", false},
		{"POST", shard + "/objects/_overwrite", false},
		{"GET", shard + "/objects/_overwrite", false},
		{"PUT", checkpoint, false},
		{"PATCH", checkpoint, false},

		{"GET", shard + "/objects/_digest" + checkpoint, false},
		{"GET", shard + "/objects/_count" + checkpoint, false},
		{"POST", shard + "/objects/deadbeef" + "/indices/MyClass/shards/myshard/objects/compareDigests", false},
		{"PUT", shard + "/objects/_overwrite/extra", false},
		{"POST", shard + "/objects/hashtree/level/3x", false},
		{"POST", "/replicas/indices/Other" + roots, false},
	}
}

func newMaintenanceTestServer(t *testing.T, maintenance, asyncInMaintenance, ready func() bool) *httptest.Server {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(newFakeReplicator(false), clusterapi.NewNoopAuthHandler(),
		maintenance, asyncInMaintenance, logger, ready)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func doStatus(t *testing.T, server *httptest.Server, method, path string) int {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, nil)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	return res.StatusCode
}

func TestMaintenanceModeReplicatedIndices(t *testing.T) {
	on := func() bool { return true }
	off := func() bool { return false }

	t.Run("strict mock: flag off returns 418 on every route", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		indices := clusterapi.NewReplicatedIndices(replicaTypes.NewMockReplicator(t), clusterapi.NewNoopAuthHandler(), on, off, logger, on)
		mux := http.NewServeMux()
		mux.Handle("/replicas/indices/", indices.Indices())
		server := httptest.NewServer(mux)
		defer server.Close()
		for _, rc := range maintenanceRouteCases() {
			assert.Equal(t, http.StatusTeapot, doStatus(t, server, rc.method, rc.path), "%s %s", rc.method, rc.path)
		}
	})

	flagStates := []struct {
		name string
		flag func() bool
	}{
		{name: "flag nil", flag: nil},
		{name: "flag off", flag: off},
		{name: "flag on", flag: on},
	}
	for _, fs := range flagStates {
		t.Run("maintenance on, "+fs.name, func(t *testing.T) {
			server := newMaintenanceTestServer(t, on, fs.flag, on)
			for _, rc := range maintenanceRouteCases() {
				status := doStatus(t, server, rc.method, rc.path)
				if fs.flag != nil && fs.flag() && rc.allowlisted {
					assert.NotEqual(t, http.StatusTeapot, status, "%s %s", rc.method, rc.path)
					assert.NotEqual(t, http.StatusServiceUnavailable, status, "%s %s", rc.method, rc.path)
					continue
				}
				assert.Equal(t, http.StatusTeapot, status, "%s %s", rc.method, rc.path)
			}
		})
	}

	t.Run("maintenance off: the flag changes nothing", func(t *testing.T) {
		flagOff := newMaintenanceTestServer(t, off, off, on)
		flagOn := newMaintenanceTestServer(t, off, on, on)
		for _, rc := range maintenanceRouteCases() {
			statusOff := doStatus(t, flagOff, rc.method, rc.path)
			assert.NotEqual(t, http.StatusTeapot, statusOff, "%s %s", rc.method, rc.path)
			assert.Equal(t, statusOff, doStatus(t, flagOn, rc.method, rc.path), "%s %s", rc.method, rc.path)
		}
	})

	t.Run("maintenance on, flag on, node not ready: allowlisted routes get 503", func(t *testing.T) {
		server := newMaintenanceTestServer(t, on, on, off)
		for _, rc := range maintenanceRouteCases() {
			want := http.StatusTeapot
			if rc.allowlisted {
				want = http.StatusServiceUnavailable
			}
			assert.Equal(t, want, doStatus(t, server, rc.method, rc.path), "%s %s", rc.method, rc.path)
		}
	})

	t.Run("flag flips take effect on the next request", func(t *testing.T) {
		var flag atomic.Bool
		server := newMaintenanceTestServer(t, on, flag.Load, on)
		path := "/replicas/indices/MyClass/shards/myshard/objects/hashtree/level/3"
		assert.Equal(t, http.StatusTeapot, doStatus(t, server, "POST", path))
		flag.Store(true)
		assert.NotEqual(t, http.StatusTeapot, doStatus(t, server, "POST", path))
		flag.Store(false)
		assert.Equal(t, http.StatusTeapot, doStatus(t, server, "POST", path))
	})
}

func newOverwriteServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		newFakeReplicator(false),
		clusterapi.NewNoopAuthHandler(),
		func() bool { return false },
		func() bool { return false },
		logger,
		func() bool { return true },
	)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	url := server.URL + "/replicas/indices/C1/shards/S1/objects/_overwrite"
	return server, url
}

func vobjectPayload(t *testing.T) []byte {
	t.Helper()
	now := time.Now()
	vobjs := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
				Class:              "C1",
				LastUpdateTimeUnix: now.UnixMilli(),
			},
			StaleUpdateTime: now.UnixMilli(),
		},
	}
	body, err := shared.IndicesPayloads.VersionedObjectList.MarshalV2(vobjs)
	require.NoError(t, err)
	return body
}

func TestPutOverwriteObjectsCompression(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		compressionHdr string
		encodingHdr    string
		compressBody   bool
		useBinaryBody  bool
		wantStatus     int
	}{
		{
			name:           "ZstdCompressedBinaryRequest",
			compressionHdr: "zstd",
			encodingHdr:    "binary",
			compressBody:   true,
			useBinaryBody:  true,
			wantStatus:     http.StatusOK,
		},

		{
			name:           "UncompressedJSONFallback",
			compressionHdr: "",
			encodingHdr:    "",
			compressBody:   false,
			useBinaryBody:  false,
			wantStatus:     http.StatusOK,
		},

		{
			name:           "UnsupportedCompressionAlgorithm",
			compressionHdr: "gzip",
			compressBody:   false,
			useBinaryBody:  false,
			wantStatus:     http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, url := newOverwriteServer(t)

			var body []byte
			if tc.useBinaryBody {
				body = vobjectPayload(t) // MarshalV2 binary format
			} else {
				now := time.Now()
				vobjs := []*objects.VObject{{
					LatestObject: &models.Object{
						ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
						Class:              "C1",
						LastUpdateTimeUnix: now.UnixMilli(),
					},
					StaleUpdateTime: now.UnixMilli(),
				}}
				var err error
				body, err = shared.IndicesPayloads.VersionedObjectList.Marshal(vobjs)
				require.NoError(t, err)
			}

			if tc.compressBody {
				enc, err := zstd.NewWriter(nil)
				require.NoError(t, err)
				body = enc.EncodeAll(body, make([]byte, 0, len(body)))
				enc.Close()
			}

			req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
			require.NoError(t, err)
			if tc.compressionHdr != "" {
				req.Header.Set("X-Request-Compression", tc.compressionHdr)
			}
			if tc.encodingHdr != "" {
				req.Header.Set("X-Request-Encoding", tc.encodingHdr)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.wantStatus, resp.StatusCode)

			if tc.wantStatus == http.StatusOK {
				var result []interface{}
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			}
		})
	}
}

func TestPutOverwriteObjectsCorruptedZstdBody(t *testing.T) {
	t.Parallel()

	_, url := newOverwriteServer(t)

	// Send a body that has the zstd compression header but is not valid zstd data.
	// This exercises the error path in readRequestBodyWithOptionalCompression where
	// io.ReadAll fails on an invalid stream, and ensures the decoder is properly
	// closed/returned before the error is returned.
	body := []byte("this is not valid zstd data")
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-Request-Compression", "zstd")
	req.Header.Set("X-Request-Encoding", "binary")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPutOverwriteObjectsZstdConcurrent(t *testing.T) {
	t.Parallel()

	_, url := newOverwriteServer(t)

	body := vobjectPayload(t)
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := enc.EncodeAll(body, make([]byte, 0, len(body)))
	enc.Close()

	const goroutines = 50
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(compressed))
			if err != nil {
				errs <- fmt.Errorf("create request: %w", err)
				return
			}
			req.Header.Set("X-Request-Compression", "zstd")
			req.Header.Set("X-Request-Encoding", "binary")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs <- fmt.Errorf("do request: %w", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("unexpected status: got %d want %d", resp.StatusCode, http.StatusOK)
				return
			}
			errs <- nil
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
