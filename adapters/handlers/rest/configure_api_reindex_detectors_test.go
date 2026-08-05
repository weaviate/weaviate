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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestDetectorProvidersFor pins that the schema-mutation and conflict
// detectors keep the reindex namespace even when the flag kept it out of
// the scheduler's provider map.
//
// This is the wiring that made a flag-off DeleteClass mid-migration
// succeed: the detectors were built by iterating the scheduler's
// provider map, and an empty detector registry fails OPEN, so removing
// the provider silently passed the check instead of failing it.
func TestDetectorProvidersFor(t *testing.T) {
	reindexProvider := &db.ReindexProvider{}
	otherProvider := &db.ReindexProvider{} // stands in for any other namespace

	tests := []struct {
		name         string
		providers    map[string]distributedtask.Provider
		wantContains []string
	}{
		{
			name:         "flag off: reindex absent from scheduler providers",
			providers:    map[string]distributedtask.Provider{},
			wantContains: []string{db.ReindexNamespace},
		},
		{
			name: "flag off with a sibling namespace registered",
			providers: map[string]distributedtask.Provider{
				distributedtask.ShardNoopProviderNamespace: otherProvider,
			},
			wantContains: []string{db.ReindexNamespace, distributedtask.ShardNoopProviderNamespace},
		},
		{
			name: "flag on: reindex already present, still exactly once",
			providers: map[string]distributedtask.Provider{
				db.ReindexNamespace: reindexProvider,
			},
			wantContains: []string{db.ReindexNamespace},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectorProvidersFor(tt.providers, reindexProvider)

			for _, ns := range tt.wantContains {
				require.Contains(t, got, ns,
					"detector registry must cover %q; an absent entry fails open", ns)
				require.NotNil(t, got[ns])
			}
			require.Len(t, got, len(tt.wantContains))

			// The reindex entry must be a real detector, otherwise the
			// registry is populated but the guard still never runs.
			_, isMutationDetector := got[db.ReindexNamespace].(distributedtask.SchemaMutationDetector)
			require.True(t, isMutationDetector,
				"the reindex provider must satisfy SchemaMutationDetector")
			_, isConflictDetector := got[db.ReindexNamespace].(distributedtask.ConflictDetector)
			require.True(t, isConflictDetector,
				"the reindex provider must satisfy ConflictDetector")
		})
	}

	t.Run("does not invent an entry when there is no reindex provider", func(t *testing.T) {
		got := detectorProvidersFor(map[string]distributedtask.Provider{}, nil)
		require.Empty(t, got)
	})
}

// TestReindexProvider_CheckClassMutation_RefusesLiveTask pins the refusal
// the wiring above delivers: a live reindex task on the collection blocks
// DeleteClass. Paired with TestDetectorProvidersFor this covers both
// halves — the guard exists, and it is registered.
func TestReindexProvider_CheckClassMutation_RefusesLiveTask(t *testing.T) {
	provider := &db.ReindexProvider{}

	live := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{
			ID:      "Books:enable-filterable:title:abcd",
			Version: 1,
		},
		Status:  distributedtask.TaskStatusStarted,
		Payload: []byte(`{"migrationType":"enable-filterable","collection":"Books"}`),
	}}

	err := provider.CheckClassMutation("Books", live)
	require.Error(t, err, "deleting a collection with a live reindex must be refused")
	require.Contains(t, err.Error(), "cancel", "the refusal must name the operator's exit")

	require.NoError(t, provider.CheckClassMutation("Movies", live),
		"a live task on another collection must not block this one")
}
