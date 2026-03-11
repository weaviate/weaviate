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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/replica"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicationErrorToGRPC(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		assert.Nil(t, replicationErrorToGRPC(nil))
	})

	t.Run("ErrUnprocessable returns FailedPrecondition", func(t *testing.T) {
		err := enterrors.NewErrUnprocessable(errors.New("shard loading"))
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Contains(t, st.Message(), "shard loading")
	})

	t.Run("other error returns Internal", func(t *testing.T) {
		err := errors.New("something went wrong")
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "something went wrong")
	})
}

func TestLocalIndexNotReady(t *testing.T) {
	t.Run("empty response", func(t *testing.T) {
		assert.False(t, localIndexNotReady(replica.SimpleResponse{}))
	})

	t.Run("non-StatusNotReady error", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replica.Error{
				{Code: replica.StatusConflict, Msg: "conflict"},
			},
		}
		assert.False(t, localIndexNotReady(resp))
	})

	t.Run("StatusNotReady returns true", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replica.Error{
				{Code: replica.StatusNotReady, Msg: "index loading"},
			},
		}
		assert.True(t, localIndexNotReady(resp))
	})
}
