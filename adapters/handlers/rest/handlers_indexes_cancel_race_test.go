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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// TestCancelReindexTask_ListToApplyRace pins: cancel apply failing with
// ErrTaskNotRunning/ErrTaskDoesNotExist (list→apply race) must be 202 NO_OP,
// not 500.
func TestCancelReindexTask_ListToApplyRace(t *testing.T) {
	h := &indexesHandlers{appState: &state.State{Logger: logrus.New()}}
	principal := &models.Principal{Username: "u1"}

	// remote mimics the wrapped+rehydrated error shape a remote-leader RPC
	// returns, so the sentinel is still errors.Is-matchable.
	remote := func(marker string) error {
		return fmt.Errorf("executing command: %w",
			distributedtask.RehydratePermanentRejection(
				status.Error(distributedtask.PermanentRejectionRPCCode, marker)))
	}

	tests := []struct {
		name     string
		err      error
		wantNoOp bool
	}{
		{
			name:     "raw ErrTaskNotRunning (local leader, unwrapped)",
			err:      distributedtask.ErrTaskNotRunning,
			wantNoOp: true,
		},
		{
			name: "local-leader wrapped permanent rejection",
			err: fmt.Errorf("executing command: %w",
				fmt.Errorf("[dtm-perm/task-not-running] task ns/t/3 is no longer running: %w",
					errors.Join(distributedtask.ErrTaskNotRunning, distributedtask.ErrPermanentRejection))),
			wantNoOp: true,
		},
		{
			name:     "remote-leader rehydrated ErrTaskNotRunning",
			err:      remote("[dtm-perm/task-not-running] task ns/t/3 is no longer running"),
			wantNoOp: true,
		},
		{
			name:     "remote-leader rehydrated ErrTaskDoesNotExist",
			err:      remote("[dtm-perm/task-not-exist] task ns/t/3 does not exist"),
			wantNoOp: true,
		},
		{
			name:     "transient RAFT error stays 500",
			err:      errors.New("raft: leadership lost while committing log"),
			wantNoOp: false,
		},
		{
			name:     "context cancelled stays 500",
			err:      context.Canceled,
			wantNoOp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := h.cancelApplyErrorResponse(tt.err, principal, "C", "title", "searchable")
			if tt.wantNoOp {
				accepted, ok := resp.(*schema.SchemaObjectsIndexesUpdateAccepted)
				require.Truef(t, ok, "want 202 Accepted NO_OP, got %T", resp)
				require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)
			} else {
				_, ok := resp.(*schema.SchemaObjectsIndexesUpdateInternalServerError)
				require.Truef(t, ok, "want 500 InternalServerError, got %T", resp)
			}
		})
	}
}
