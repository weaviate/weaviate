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

package clusterapi

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestReadAllCapped(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		maxBytes int64
		wantErr  bool
	}{
		{name: "below limit", size: 10, maxBytes: 100, wantErr: false},
		{name: "at limit", size: 100, maxBytes: 100, wantErr: false},
		{name: "one over limit", size: 101, maxBytes: 100, wantErr: true},
		{name: "far over limit", size: 4096, maxBytes: 100, wantErr: true},
		{name: "empty within zero limit", size: 0, maxBytes: 0, wantErr: false},
		{name: "one over zero limit", size: 1, maxBytes: 0, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(bytes.Repeat([]byte("a"), tc.size))
			b, err := readAllCapped(r, tc.maxBytes)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "exceeds maximum allowed size")
				return
			}
			require.NoError(t, err)
			assert.Len(t, b, tc.size)
		})
	}
}

func TestReadRequestBodyWithOptionalCompressionCap(t *testing.T) {
	oversized := strings.Repeat("a", 100)
	_, err := readRequestBodyWithOptionalCompression(readCloser{strings.NewReader(oversized)}, "", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum allowed size")
}

type readCloser struct{ *strings.Reader }

func (readCloser) Close() error { return nil }

func TestAsyncCheckpointHTTPStatusMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "stale checkpoint", err: replica.ErrAsyncCheckpointStale, want: http.StatusConflict},
		{name: "not active", err: replica.ErrAsyncReplicationNotActive, want: http.StatusPreconditionFailed},
		{name: "cutoff in past", err: replica.ErrAsyncCheckpointCutoffInPast, want: http.StatusPreconditionFailed},
		{name: "loading shard unprocessable", err: enterrors.NewErrUnprocessable(fmt.Errorf("local shard is not ready")), want: http.StatusPreconditionFailed},
		{name: "wrapped unprocessable", err: fmt.Errorf("overwrite: %w", enterrors.NewErrUnprocessable(fmt.Errorf("loading"))), want: http.StatusPreconditionFailed},
		{name: "generic error", err: fmt.Errorf("boom"), want: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, asyncCheckpointHTTPStatus(tt.err))
		})
	}
}
