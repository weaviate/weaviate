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

package namespaces

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// Asserting all three decisions per state in one table is what pins their
// disagreements: resuming keeps its shards open while refusing a request load,
// and suspended refuses a request load while still admitting a replication
// target.
func TestShardDecisionsByState(t *testing.T) {
	tests := []struct {
		name               string
		state              cmd.NamespaceState
		shardsShouldBeOpen bool
		loadableErr        error
		replicationErr     error
	}{
		{
			name:               "active holds shards open and allows every load",
			state:              cmd.NamespaceStateActive,
			shardsShouldBeOpen: true,
		},
		{
			name:        "suspended holds no shards open and refuses a request load, but admits a replication target",
			state:       cmd.NamespaceStateSuspended,
			loadableErr: ErrNamespaceSuspended,
		},
		{
			name:               "resuming holds shards open but refuses a request load, and admits a replication target",
			state:              cmd.NamespaceStateResuming,
			shardsShouldBeOpen: true,
			loadableErr:        ErrNamespaceResuming,
		},
		{
			name:           "deleting refuses every load and holds no shards open",
			state:          cmd.NamespaceStateDeleting,
			loadableErr:    ErrNamespaceDeleting,
			replicationErr: ErrNamespaceDeleting,
		},
		{
			name:           "the zero value refuses every load and holds no shards open",
			state:          cmd.NamespaceState(""),
			loadableErr:    ErrInvalidState,
			replicationErr: ErrInvalidState,
		},
		{
			// No write path stores an unknown state: Create, ChangeState and
			// Restore all reject one.
			name:           "a state this binary does not know refuses every load and holds no shards open",
			state:          cmd.NamespaceState("not-a-state"),
			loadableErr:    ErrInvalidState,
			replicationErr: ErrInvalidState,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.shardsShouldBeOpen, ShardsShouldBeOpen(tc.state))

			err := RequireShardLoadable(tc.state)
			if tc.loadableErr != nil {
				require.ErrorIs(t, err, tc.loadableErr)
			} else {
				require.NoError(t, err)
			}

			err = AdmitReplicationTarget(tc.state)
			if tc.replicationErr != nil {
				require.ErrorIs(t, err, tc.replicationErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
