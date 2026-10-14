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

package graceful_shutdown

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/test/docker"
)

// A REST batch still vectorizing when SIGTERM arrives must get 503, and the
// node must exit well before the 15s graceful timeout.
func TestRESTRequestInFlightAtShutdown(t *testing.T) {
	ctx := context.Background()
	embeddings := newHangingEmbeddings(t)

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateHostGateway().
		WithText2VecOpenAI("test-key", "", "").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Logf("failed to terminate test containers: %v", err)
		}
	})
	weaviateURL := "http://" + compose.GetWeaviate().URI()

	postJSON(t, weaviateURL+"/v1/schema", map[string]any{
		"class":      "ShutdownTest",
		"vectorizer": "text2vec-openai",
		"moduleConfig": map[string]any{
			"text2vec-openai": map[string]any{"baseURL": embeddings.url()},
		},
		"properties": []map[string]any{{"name": "text", "dataType": []string{"text"}}},
	}, http.StatusOK)

	logger, _ := logrustest.NewNullLogger()
	batchStatus := make(chan int, 1)
	enterrors.GoWrapper(func() {
		body, _ := json.Marshal(map[string]any{"objects": []map[string]any{
			{"class": "ShutdownTest", "properties": map[string]any{"text": "hello"}},
		}})
		resp, err := http.Post(weaviateURL+"/v1/batch/objects", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Errorf("batch request: %v", err)
			batchStatus <- 0
			return
		}
		resp.Body.Close()
		batchStatus <- resp.StatusCode
	}, logger)

	select {
	case <-embeddings.called:
	case <-time.After(30 * time.Second):
		t.Fatal("the batch never reached the embeddings endpoint")
	}

	stopTimeout := 30 * time.Second
	start := time.Now()
	require.NoError(t, compose.Stop(ctx, docker.Weaviate0, &stopTimeout))
	exitedAfter := time.Since(start)
	t.Logf("node exited %s after SIGTERM", exitedAfter)

	assert.Equal(t, http.StatusServiceUnavailable, <-batchStatus)
	assert.Less(t, exitedAfter, 12*time.Second, "node must exit well before the 15s graceful timeout")

	logs, err := compose.GetWeaviate().Container().Logs(ctx)
	require.NoError(t, err)
	defer logs.Close()
	out, err := io.ReadAll(logs)
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(out), "refused 1 REST requests"), "shutdown log line missing")
}

// hangingEmbeddings is an OpenAI-compatible embeddings endpoint that holds every
// call until Weaviate cancels it, keeping the batch in flight deterministically.
type hangingEmbeddings struct {
	port   int
	called chan struct{}
}

func newHangingEmbeddings(t *testing.T) *hangingEmbeddings {
	t.Helper()
	// Bind all interfaces so the container reaches it through the host gateway.
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	h := &hangingEmbeddings{port: ln.Addr().(*net.TCPAddr).Port, called: make(chan struct{}, 1)}
	released := make(chan struct{})

	srv := &httptest.Server{
		Listener: ln,
		Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case h.called <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-released:
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		})},
	}
	srv.Start()
	t.Cleanup(srv.Close)
	// Docker Desktop's host-gateway proxy can hold the connection open after the
	// container is gone, so release the handler before Close waits for it.
	t.Cleanup(func() { close(released) })
	return h
}

func (h *hangingEmbeddings) url() string {
	return fmt.Sprintf("http://host.docker.internal:%d", h.port)
}

func postJSON(t *testing.T, url string, payload any, wantStatus int) {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, wantStatus, resp.StatusCode, "POST %s: %s", url, respBody)
}
