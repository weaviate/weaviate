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
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// TestMapSubmitTaskError pins that an FSM conflict rejection maps to 409 (not
// 500); genuine infra failures still map to 500.
func TestMapSubmitTaskError(t *testing.T) {
	h := admissionHandler()

	t.Run("conflict sentinel -> 409, ID not leaked to caller", func(t *testing.T) {
		// Mirrors the FSM path: wrapped through executing-command, riding ErrTaskConflict.
		fsmErr := fmt.Errorf("executing command: task reindex/Other:change-tokenization:secret:ffff conflicts with existing task: %w",
			distributedtask.ErrTaskConflict)
		resp := h.mapSubmitTaskError(nil, "C", "C:change-tokenization:p:aaaa", fsmErr)
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
		resp := h.mapSubmitTaskError(nil, "C", "C:change-tokenization:p:aaaa", fsmErr)
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusConflict, code,
			"a movement the caller can wait out must not read as a server fault")
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "replica movement in flight")
	})

	t.Run("replicas-changed sentinel -> 409", func(t *testing.T) {
		fsmErr := fmt.Errorf("executing command: %w",
			distributedtask.NewReplicasChangedError("C"))
		resp := h.mapSubmitTaskError(nil, "C", "C:change-tokenization:p:aaaa", fsmErr)
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusConflict, code)
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "resubmit it against the current state")
	})

	// A leader's refusal reaches a follower wrapped in gRPC text. The client
	// sees only the public message and the status of its sentinel.
	forwarded := func(err error) error {
		return fmt.Errorf("executing command: rpc error: code = FailedPrecondition desc = %w\npermanent FSM rejection", err)
	}
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{"suspended namespace", namespaces.ErrNamespaceSuspended, http.StatusUnprocessableEntity, "instance suspended"},
		{"deleting namespace", namespaces.ErrNamespaceDeleting, http.StatusUnprocessableEntity, "instance unavailable"},
		{"resuming namespace", namespaces.ErrNamespaceResuming, http.StatusServiceUnavailable, "instance resuming, retry shortly"},
	} {
		t.Run(tc.name+" -> public message, forwarded text hidden", func(t *testing.T) {
			resp := h.mapSubmitTaskError(nil, "customer1:Movies", "", forwarded(tc.err))
			code, body := statusOf(t, resp)
			require.Equal(t, tc.wantStatus, code)
			require.Len(t, body.Error, 1)
			assert.Contains(t, body.Error[0].Message, tc.wantBody)
			assert.NotContains(t, body.Error[0].Message, "permanent FSM rejection")
		})
	}

	t.Run("generic infra error -> 500", func(t *testing.T) {
		resp := h.mapSubmitTaskError(nil, "C", "C:change-tokenization:p:aaaa",
			errors.New("raft leader lost"))
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code)
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "submitting task")
	})
}
