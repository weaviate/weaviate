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

package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/objects"
	wlnamespaces "github.com/weaviate/weaviate/wl/namespaces"
)

// Drives BatchObjects with requests that return before the batch manager is
// touched. The namespace is recorded before class resolution and authorization,
// so a rejected batch is still attributed.
// docs/metrics.md lists the residuals.
func TestHandler_BatchObjects(t *testing.T) {
	t.Run("records the caller's namespace in the slot", func(t *testing.T) {
		tests := []struct {
			name      string
			principal *models.Principal
			want      string
		}{
			{name: "nil principal yields empty label", principal: nil, want: ""},
			{
				name:      "global operator yields empty label",
				principal: &models.Principal{Username: "admin", Namespace: "ns_a", IsGlobalOperator: true},
				want:      "",
			},
			{
				name:      "namespace-less principal yields empty label",
				principal: &models.Principal{Username: "legacy"},
				want:      "",
			},
			{
				name:      "namespaced user yields its namespace",
				principal: &models.Principal{Username: "ns_a:alice", Namespace: "ns_a"},
				want:      "ns_a",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				authenticator := auth.NewHandler(false, func(string, []string) (*models.Principal, error) {
					return tc.principal, nil
				})
				h := NewHandler(nil, nil, logger, authenticator, nil, wlnamespaces.NewPrefixing())

				ctx, slot := restCtx.WithBatchNamespaceSlot(context.Background())
				reply, err := h.BatchObjects(ctx, &pb.BatchObjectsRequest{})

				require.NoError(t, err)
				require.NotNil(t, reply)
				assert.Equal(t, tc.want, slot.Namespace)
			})
		}
	})

	t.Run("failed authentication leaves the slot empty", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		authenticator := auth.NewHandler(false, func(string, []string) (*models.Principal, error) {
			return nil, errors.New("invalid token")
		})
		h := NewHandler(nil, nil, logger, authenticator, nil, wlnamespaces.NewPrefixing())

		ctx, slot := restCtx.WithBatchNamespaceSlot(context.Background())
		_, err := h.BatchObjects(ctx, &pb.BatchObjectsRequest{})

		require.Error(t, err)
		assert.Empty(t, slot.Namespace)
	})

	t.Run("every object with an empty collection fails alone", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		authenticator := auth.NewHandler(false, func(string, []string) (*models.Principal, error) {
			return &models.Principal{Username: "ns_a:alice", Namespace: "ns_a"}, nil
		})
		// The nil authorizer, batch manager and schema manager panic if the handler reaches them.
		h := NewHandler(nil, nil, logger, authenticator, nil, wlnamespaces.NewPrefixing())

		ctx, _ := restCtx.WithBatchNamespaceSlot(context.Background())
		reply, err := h.BatchObjects(ctx, &pb.BatchObjectsRequest{Objects: []*pb.BatchObject{
			{Collection: "", Uuid: "a0b55b05-bc5b-4cc9-b646-1452d1390a62"},
			{Collection: "", Uuid: "a0b55b05-bc5b-4cc9-b646-1452d1390a63"},
		}})

		require.NoError(t, err)
		require.Len(t, reply.Errors, 2)
		var indexes []int32
		for _, batchErr := range reply.Errors {
			assert.Contains(t, batchErr.Error, objects.ErrEmptyClass.Error())
			indexes = append(indexes, batchErr.Index)
		}
		assert.ElementsMatch(t, []int32{0, 1}, indexes)
	})
}
