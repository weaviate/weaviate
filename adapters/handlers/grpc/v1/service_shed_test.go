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

package v1

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/usecases/queryadmission"
)

func TestAdmissionToGRPCError(t *testing.T) {
	t.Run("shed maps to ResourceExhausted", func(t *testing.T) {
		err := admissionToGRPCError(queryadmission.ErrOverloaded)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.ResourceExhausted, st.Code())
	})

	t.Run("wrapped shed still maps to ResourceExhausted", func(t *testing.T) {
		err := admissionToGRPCError(fmt.Errorf("search shard: %w", queryadmission.ErrOverloaded))
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.ResourceExhausted, st.Code())
	})

	t.Run("unrelated error is not remapped", func(t *testing.T) {
		require.Nil(t, admissionToGRPCError(io.ErrUnexpectedEOF))
	})

	t.Run("nil error is not remapped", func(t *testing.T) {
		require.Nil(t, admissionToGRPCError(nil))
	})
}
