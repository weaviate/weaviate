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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

// asyncCheckpointHandlerTestServer spins up the same handler chain
// production uses (NewReplicatedIndices wired into a mux), giving each test
// the freedom to inject custom Replicator expectations.
func asyncCheckpointHandlerTestServer(t *testing.T, rep replicaTypes.Replicator) (*httptest.Server, func()) {
	t.Helper()
	auth := clusterapi.NewNoopAuthHandler()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		rep, auth,
		func() bool { return false }, // maintenance mode off
		logger,
		func() bool { return true }, // node ready
	)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	return server, server.Close
}

func asyncCheckpointURL(serverURL, className string) string {
	return fmt.Sprintf("%s/replicas/indices/%s/async-checkpoint", serverURL, className)
}

// readBodyOnce drains and returns the trimmed response body. The HTTP body
// is a one-shot reader, so tests that want both the status code and the
// body for diagnostic messages must capture the body here exactly once and
// then assert against the returned string.
func readBodyOnce(t *testing.T, res *http.Response) string {
	t.Helper()
	b, _ := io.ReadAll(res.Body)
	return string(bytes.TrimSpace(b))
}

func TestAsyncCheckpoint_CreatedAtSkewGuard_Rejects(t *testing.T) {
	// A createdAt 10 minutes in the future is well past the 5-minute
	// tolerance. The handler must reject *before* the Replicator sees the
	// value, so no Replicator expectation is set up — any call would
	// trigger an unexpected-mock assertion.
	rep := replicaTypes.NewMockReplicator(t)
	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	body := map[string]any{
		"shards":        []string{"shard-1"},
		"cutoff_ms":     time.Now().UnixMilli(),
		"created_at_ms": time.Now().Add(10 * time.Minute).UnixMilli(),
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	respBody := readBodyOnce(t, res)
	assert.Equal(t, http.StatusBadRequest, res.StatusCode, "body: %s", respBody)
	assert.Contains(t, respBody, "created_at_ms is too far in the future")
}

func TestAsyncCheckpoint_CreatedAtSkewGuard_AcceptsWithinTolerance(t *testing.T) {
	// 1 minute ahead is comfortably inside the 5-minute tolerance; the
	// handler must forward to the Replicator.
	rep := replicaTypes.NewMockReplicator(t)
	rep.On("CreateAsyncCheckpoint",
		mock.Anything,       // ctx
		"MyClass",           // className
		[]string{"shard-1"}, // shards
		int64(123),          // cutoffMs
		mock.AnythingOfType("time.Time"),
	).Return(nil).Once()

	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	body := map[string]any{
		"shards":        []string{"shard-1"},
		"cutoff_ms":     int64(123),
		"created_at_ms": time.Now().Add(time.Minute).UnixMilli(),
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "body: %s", readBodyOnce(t, res))
}

func TestAsyncCheckpoint_CreatedAtSkewGuard_AllowsPastDated(t *testing.T) {
	// Stale createdAt is harmless on the wire — the tie-breaker on the
	// Shard rejects with 409. The handler itself must not reject past-dated
	// values; they're legitimate (clock drift in the other direction, or a
	// retry of an older request).
	rep := replicaTypes.NewMockReplicator(t)
	rep.On("CreateAsyncCheckpoint",
		mock.Anything, "MyClass", []string{"shard-1"}, int64(123),
		mock.AnythingOfType("time.Time"),
	).Return(nil).Once()

	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	body := map[string]any{
		"shards":        []string{"shard-1"},
		"cutoff_ms":     int64(123),
		"created_at_ms": time.Now().Add(-24 * time.Hour).UnixMilli(),
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "body: %s", readBodyOnce(t, res))
}

func TestAsyncCheckpoint_PostRejectsBadBody(t *testing.T) {
	// Validation table: pre-skew-guard rejections that must come back as
	// 400 without ever reaching the Replicator. Each case asserts the
	// handler short-circuits on its own.
	cases := []struct {
		name    string
		body    map[string]any
		wantSub string
	}{
		{
			name:    "cutoff_zero",
			body:    map[string]any{"shards": []string{"s1"}, "cutoff_ms": 0, "created_at_ms": time.Now().UnixMilli()},
			wantSub: "cutoff_ms must be > 0",
		},
		{
			name:    "cutoff_negative",
			body:    map[string]any{"shards": []string{"s1"}, "cutoff_ms": -1, "created_at_ms": time.Now().UnixMilli()},
			wantSub: "cutoff_ms must be > 0",
		},
		{
			name:    "created_at_zero",
			body:    map[string]any{"shards": []string{"s1"}, "cutoff_ms": 123, "created_at_ms": 0},
			wantSub: "created_at_ms must be > 0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := replicaTypes.NewMockReplicator(t)
			server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
			defer cleanup()

			raw, _ := json.Marshal(tc.body)
			req, err := http.NewRequest(http.MethodPost, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(raw))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, http.StatusBadRequest, res.StatusCode)
			assert.Contains(t, readBodyOnce(t, res), tc.wantSub)
		})
	}
}

func TestAsyncCheckpoint_PostAcceptsEmptyShards(t *testing.T) {
	// Empty shards is a valid request: the handler forwards it as-is, and
	// the receiver layer (DB.CreateAsyncCheckpoint) is responsible for
	// expanding it to "every shard of this class loaded on this node". The
	// handler must NOT reject it pre-emptively.
	rep := replicaTypes.NewMockReplicator(t)
	rep.On("CreateAsyncCheckpoint",
		mock.Anything, "MyClass", []string{}, int64(123),
		mock.AnythingOfType("time.Time"),
	).Return(nil).Once()

	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	body := map[string]any{
		"shards":        []string{},
		"cutoff_ms":     int64(123),
		"created_at_ms": time.Now().UnixMilli(),
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode, "body: %s", readBodyOnce(t, res))
}

func TestAsyncCheckpoint_StatusMapping(t *testing.T) {
	// Verifies the asyncCheckpointHTTPStatus mapping is wired correctly:
	// stale createdAt → 409, async-rep-inactive → 412, generic → 500.
	// Each case returns the exact error string the production code emits
	// so the substring match in the mapper actually fires.
	cases := []struct {
		name       string
		repErr     error
		wantStatus int
	}{
		{
			name:       "stale_createdAt_returns_409",
			repErr:     replica.ErrAsyncCheckpointStale,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "async_rep_inactive_returns_412",
			repErr:     replica.ErrAsyncReplicationNotActive,
			wantStatus: http.StatusPreconditionFailed,
		},
		{
			name:       "other_error_returns_500",
			repErr:     fmt.Errorf("some unexpected backend failure"),
			wantStatus: http.StatusInternalServerError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := replicaTypes.NewMockReplicator(t)
			rep.On("CreateAsyncCheckpoint",
				mock.Anything, "MyClass", []string{"s1"}, int64(123),
				mock.AnythingOfType("time.Time"),
			).Return(tc.repErr).Once()

			server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
			defer cleanup()

			body := map[string]any{
				"shards":        []string{"s1"},
				"cutoff_ms":     int64(123),
				"created_at_ms": time.Now().UnixMilli(),
			}
			raw, _ := json.Marshal(body)
			req, err := http.NewRequest(http.MethodPost, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(raw))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, tc.wantStatus, res.StatusCode, "body: %s", readBodyOnce(t, res))
		})
	}
}

func TestAsyncCheckpoint_DeleteAndGet_HappyPath(t *testing.T) {
	rep := replicaTypes.NewMockReplicator(t)
	rep.On("DeleteAsyncCheckpoint", mock.Anything, "MyClass", []string{"s1", "s2"}).
		Return(nil).Once()
	rep.On("GetAsyncCheckpointStatus", mock.Anything, "MyClass", []string{"s1", "s2"}).
		Return(map[string]replica.AsyncCheckpointShardStatus{
			"s1": {CutoffMs: 100, CreatedAt: time.UnixMilli(50).UTC()},
		}, nil).Once()

	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	// DELETE
	delBody, _ := json.Marshal(map[string]any{"shards": []string{"s1", "s2"}})
	delReq, err := http.NewRequest(http.MethodDelete, asyncCheckpointURL(server.URL, "MyClass"), bytes.NewReader(delBody))
	require.NoError(t, err)
	delReq.Header.Set("Content-Type", "application/json")
	delRes, err := http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	defer delRes.Body.Close()
	assert.Equal(t, http.StatusOK, delRes.StatusCode)

	// GET with repeated ?shards= parameters mirroring tools/async_checkpoint.sh
	getURL := asyncCheckpointURL(server.URL, "MyClass") + "?shards=s1&shards=s2"
	getReq, err := http.NewRequest(http.MethodGet, getURL, nil)
	require.NoError(t, err)
	getRes, err := http.DefaultClient.Do(getReq)
	require.NoError(t, err)
	defer getRes.Body.Close()
	assert.Equal(t, http.StatusOK, getRes.StatusCode)

	var parsed map[string]struct {
		Root        []byte `json:"root"`
		CutoffMs    int64  `json:"cutoff_ms"`
		CreatedAtMs int64  `json:"created_at_ms"`
	}
	require.NoError(t, json.NewDecoder(getRes.Body).Decode(&parsed))
	require.Contains(t, parsed, "s1")
	assert.Equal(t, int64(100), parsed["s1"].CutoffMs)
	assert.Equal(t, int64(50), parsed["s1"].CreatedAtMs)
}

func TestAsyncCheckpoint_Status_InactiveShardZerosWireFields(t *testing.T) {
	// Pins the wire contract for "loaded but inactive" shards: CutoffMs is 0,
	// Root is empty, and CreatedAtMs is 0. Without the encoder's zero-out
	// step, a zero time.Time would render as ≈ -6795364578871 on the wire,
	// forcing every consumer to special-case a negative timestamp.
	rep := replicaTypes.NewMockReplicator(t)
	rep.On("GetAsyncCheckpointStatus", mock.Anything, "MyClass", []string{"inactive"}).
		Return(map[string]replica.AsyncCheckpointShardStatus{
			"inactive": {CutoffMs: 0, CreatedAt: time.Time{}},
		}, nil).Once()

	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	getURL := asyncCheckpointURL(server.URL, "MyClass") + "?shards=inactive"
	req, err := http.NewRequest(http.MethodGet, getURL, nil)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	var parsed map[string]struct {
		Root        []byte `json:"root"`
		CutoffMs    int64  `json:"cutoff_ms"`
		CreatedAtMs int64  `json:"created_at_ms"`
	}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&parsed))
	require.Contains(t, parsed, "inactive")
	assert.Equal(t, int64(0), parsed["inactive"].CutoffMs)
	assert.Empty(t, parsed["inactive"].Root, "inactive shard must encode root as empty")
	assert.Equal(t, int64(0), parsed["inactive"].CreatedAtMs,
		"inactive shard must encode created_at_ms as 0, not the negative result of time.Time{}.UnixMilli()")
}

func TestAsyncCheckpoint_MethodNotAllowed(t *testing.T) {
	// PUT/PATCH aren't part of the checkpoint contract; the handler must
	// surface 405 instead of routing to a no-op.
	rep := replicaTypes.NewMockReplicator(t)
	server, cleanup := asyncCheckpointHandlerTestServer(t, rep)
	defer cleanup()

	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, asyncCheckpointURL(server.URL, "MyClass"), nil)
			require.NoError(t, err)
			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode)
		})
	}
}

// ensure context-typed mock arg is exercised so the import isn't pruned by
// future refactors.
var _ = context.Background
