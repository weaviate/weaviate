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

package grpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestWithDocsLink(t *testing.T) {
	documented := fmt.Errorf("search: memory pressure: cannot load shard: %w", enterrors.ErrNotEnoughMappings)

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
		same     bool
	}{
		{name: "nil stays nil", err: nil, same: true},
		{name: "undocumented plain error is untouched", err: fmt.Errorf("boom"), same: true},
		{name: "undocumented status error is untouched", err: status.Error(codes.NotFound, "no such class"), same: true},
		{
			name:     "documented plain error becomes Unknown with the page appended",
			err:      documented,
			wantCode: codes.Unknown,
			wantMsg:  "search: memory pressure: cannot load shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name:     "documented error wrapping a status error keeps its code",
			err:      fmt.Errorf("load shard: %w: %w", status.Error(codes.ResourceExhausted, "grpc"), enterrors.ErrNotEnoughMappings),
			wantCode: codes.ResourceExhausted,
			wantMsg:  "load shard: rpc error: code = ResourceExhausted desc = grpc: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := withDocsLink(tt.err)
			if tt.same {
				assert.Equal(t, tt.err, got)
				return
			}
			st, ok := status.FromError(got)
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, st.Code())
			assert.Equal(t, tt.wantMsg, st.Message())
		})
	}
}

// A documented error wrapping a status that carries protobuf details keeps
// them: only the message changes.
func TestWithDocsLinkKeepsStatusDetails(t *testing.T) {
	st, err := status.New(codes.ResourceExhausted, "over the line").WithDetails(&errdetails.ErrorInfo{Reason: "LIMIT"})
	require.NoError(t, err)
	wrapped := fmt.Errorf("load shard: %w: %w", st.Err(), enterrors.ErrNotEnoughMappings)

	got, ok := status.FromError(withDocsLink(wrapped))
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, got.Code())
	assert.Contains(t, got.Message(), "(see https://docs.weaviate.io/e/core-mem001)")
	require.Len(t, got.Details(), 1)
	info, isInfo := got.Details()[0].(*errdetails.ErrorInfo)
	require.True(t, isInfo)
	assert.Equal(t, "LIMIT", info.Reason)
}

func TestDocsLinkInterceptorsWrapHandlerErrors(t *testing.T) {
	documented := fmt.Errorf("x: %w", enterrors.ErrNotEnoughMappings)

	unary := makeDocsLinkUnaryInterceptor()
	resp, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		return "resp", documented
	})
	assert.Equal(t, "resp", resp)
	assert.Contains(t, err.Error(), "https://docs.weaviate.io/e/core-mem001")

	stream := makeDocsLinkStreamInterceptor()
	err = stream(nil, nil, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error { return documented })
	assert.Contains(t, err.Error(), "https://docs.weaviate.io/e/core-mem001")

	err = stream(nil, nil, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error { return nil })
	assert.NoError(t, err)
}
