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

package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestMapSubmitTaskError pins that an FSM conflict rejection maps to 409 (not
// 500); genuine infra failures still map to 500.
func TestMapSubmitTaskError(t *testing.T) {
	h := admissionHandler()

	t.Run("conflict sentinel -> 409, ID not leaked to caller", func(t *testing.T) {
		// Mirrors the FSM path: wrapped through executing-command, riding ErrTaskConflict.
		fsmErr := fmt.Errorf("executing command: task reindex/Other:change-tokenization:secret:ffff conflicts with existing task: %w",
			distributedtask.ErrTaskConflict)
		resp := h.mapSubmitTaskError(context.Background(), nil, "C", "C:change-tokenization:p:aaaa", fsmErr)
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusConflict, code, "FSM conflict must map to 409")
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "already in flight")
		assert.NotContains(t, body.Error[0].Message, "secret",
			"the internal conflicting task ID must not leak to the caller")
	})

	t.Run("replica-movement sentinel -> 409", func(t *testing.T) {
		fsmErr := fmt.Errorf("executing command: %w",
			distributedtask.NewBlockedByReplicaMovementError("C"))
		resp := h.mapSubmitTaskError(context.Background(), nil, "C", "C:change-tokenization:p:aaaa", fsmErr)
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusConflict, code,
			"a movement the caller can wait out must not read as a server fault")
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "replica movement in flight")
	})

	// The apply refuses these, so a forwarded submit gets them back after the gRPC hop.
	notHot := db.ErrTenantsNotHot.Error()
	for _, tc := range []struct {
		name     string
		err      error
		reader   bool
		wantCode int
		want     string
	}{
		{"tenants not HOT, tenant reader", distributedtask.NewTenantsNotHotError("C", 3), true, http.StatusBadRequest, notHot + " (target tenants that are not HOT: 3)"},
		{"tenants not HOT, collection reader only", distributedtask.NewTenantsNotHotError("C", 3), false, http.StatusBadRequest, notHot},
		{"tenants missing", distributedtask.NewTenantsMissingError("C"), true, http.StatusBadRequest, "some target tenants do not exist"},
		{"no shards", distributedtask.NewHasNoUnitsError("C"), true, http.StatusBadRequest, "collection has no shards"},
		{"collection gone", distributedtask.NewCollectionGoneError("C"), true, http.StatusConflict, `collection "C" was deleted while the reindex was submitted`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var authz authorization.Authorizer = &collectionsOnly{}
			if tc.reader {
				authz = &authorization.DummyAuthorizer{}
			}
			h := &indexesHandlers{appState: &state.State{Logger: logrus.New(), Authorizer: authz}}
			hopped := distributedtask.RehydratePermanentRejection(distributedtask.ToRPCError(tc.err))
			resp := h.mapSubmitTaskError(context.Background(), nil, "C", "C:change-tokenization:p:aaaa",
				fmt.Errorf("executing command: %w", hopped))
			code, body := statusOf(t, resp)
			require.Equal(t, tc.wantCode, code)
			require.Len(t, body.Error, 1)
			assert.Equal(t, tc.want, body.Error[0].Message)
		})
	}

	t.Run("generic infra error -> 500", func(t *testing.T) {
		resp := h.mapSubmitTaskError(context.Background(), nil, "C", "C:change-tokenization:p:aaaa",
			errors.New("raft leader lost"))
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code)
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "submitting task")
	})
}
