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

func TestMaintenanceModeReplicatedIndices(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := replicaTypes.NewMockReplicator(t)
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(fakeReplicator, noopAuth, func() bool { return true }, logger, func() bool { return true })
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)

	defer server.Close()

	maintenanceModeExpectedHTTPStatus := http.StatusTeapot
	requestURL := func(suffix string) string {
		return fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard%s", server.URL, suffix)
	}
	indicesTestRequests := []indicesTestRequest{
		{"GET", "/objects/_digest"},
		{"PUT", "/objects/_overwrite"},
		{"DELETE", "/objects/deadbeef"},
		{"PATCH", "/objects/deadbeef"},
		{"GET", "/objects/deadbeef"},
		{"POST", "/objects/references"},
		{"GET", "/objects"},
		{"POST", "/objects"},
		{"DELETE", "/objects"},
		{"GET", "/objects/_count"},
		{"PUT", "/replication-factor:increase"},
		{"POST", ":commit"},
		{"POST", ":abort"},
	}
	for _, testRequest := range indicesTestRequests {
		t.Run(fmt.Sprintf("%s on %s returns maintenance mode status", testRequest.method, testRequest.suffix), func(t *testing.T) {
			req, err := http.NewRequest(testRequest.method, requestURL(testRequest.suffix), nil)
			assert.Nil(t, err)
			res, err := http.DefaultClient.Do(req)
			assert.Nil(t, err)
			defer res.Body.Close()
			assert.True(t, res.StatusCode == maintenanceModeExpectedHTTPStatus, "expected %d, got %d", maintenanceModeExpectedHTTPStatus, res.StatusCode)
		})
	}
}

func newOverwriteServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		newFakeReplicator(false),
		clusterapi.NewNoopAuthHandler(),
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
