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
)

// TestMapSubmitTaskError pins that an FSM conflict rejection maps to 409
// (not 500), and that genuine infra failures stay 500. The FSM conflict is
// the multi-node race the REST pre-flight can't cover; mapping it to 500 told
// the caller "server error" for what is actually a retryable conflict.
func TestMapSubmitTaskError(t *testing.T) {
	h := admissionHandler()

	t.Run("conflict sentinel -> 409, ID not leaked to caller", func(t *testing.T) {
		// Shape mirrors the FSM path: the conflict error names task IDs and is
		// wrapped through executing-command, riding ErrTaskConflict.
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

	t.Run("generic infra error -> 500", func(t *testing.T) {
		resp := h.mapSubmitTaskError(nil, "C", "C:change-tokenization:p:aaaa",
			errors.New("raft leader lost"))
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusInternalServerError, code)
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "submitting task")
	})
}
