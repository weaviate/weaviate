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
	"encoding/json"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// stuckOnProber reports a wedged teardown on exactly one collection, and records
// what it was asked, so a lookup that ignores its argument cannot pass.
type stuckOnProber struct {
	stuck string
	asked []string
}

func (p *stuckOnProber) AnyCleanupInProgress() bool { return p.stuck != "" }

func (p *stuckOnProber) BlockingHoldForCollection(collection string) bool {
	p.asked = append(p.asked, collection)
	return collection == p.stuck
}

// The wait for a wedged worker is capped in minutes, so a collection-blind
// answer refuses restores of every OTHER collection for that whole time. This
// covers the mapping itself, not just that the argument is threaded through.
func TestAnyCleanupInProgressLookupIsScopedByCollection(t *testing.T) {
	tests := []struct {
		name        string
		collections []string
		want        bool
		why         string
	}{
		{
			name:        "the wedged collection",
			collections: []string{"Stuck"},
			want:        true,
			why:         "the collection whose teardown is wedged must still be refused",
		},
		{
			name:        "an unrelated collection",
			collections: []string{"Unrelated"},
			want:        false,
			why:         "another collection's wedged teardown must not refuse this restore",
		},
		{
			name:        "a list containing the wedged one",
			collections: []string{"Unrelated", "Stuck"},
			want:        true,
			why:         "a restore covering the wedged collection is refused whatever else it covers",
		},
		{
			name:        "no class list yet",
			collections: nil,
			want:        true,
			why:         "the meta-not-found arm has no classes, so it must fall back to the blind answer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prober := &stuckOnProber{stuck: "Stuck"}
			require.Equal(t, test.want, anyCleanupInProgressLookup(prober)(test.collections), test.why)
			if len(test.collections) > 0 {
				require.NotEmpty(t, prober.asked,
					"a scoped answer has to ask about the collections; an unasked prober means the argument was ignored")
			}
		})
	}
}

// The layer above the one the fake covers: which method the wiring CALLS.
//
// Driven through a real *db.ReindexProvider on the exported lifecycle, because
// the fake pins what each method means and would survive a mechanical rename of
// the call. The discriminator is the window after a teardown finishes: the
// blocking hold is gone, the confirmation latch is still up for its fixed
// window, and only one of the two answers correctly.
func TestAnyCleanupInProgressLookupClearsWithTheTeardown(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	provider := db.NewReindexProvider(nil, nil, logger, "node1",
		func() int { return 1 }, context.Background())

	// No properties, so the teardown adopts the gate the apply parked and
	// returns without touching the DB this provider does not have.
	payload := &db.ReindexTaskPayload{
		Collection:  "Movies",
		UnitToShard: map[string]string{"u1": "shard1"},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "Movies:change-tokenization:body:ab12", Version: 1},
		Namespace:      db.ReindexNamespace,
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        raw,
		// A claimed unit: the cancel of a migration that had already started
		// writing, which is the only shape with sidecars to tear down. A cancel
		// whose units never left PENDING is the submit path's own rollback and
		// holds no gate at all, so it would refuse nothing here.
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
		},
	}

	lookup := anyCleanupInProgressLookup(provider)

	provider.OnTerminalApplied(task)
	require.True(t, lookup([]string{"Movies"}),
		"the teardown is pending, so a restore of this collection must be refused")

	require.NoError(t, provider.OnTaskCompleted(task))

	require.False(t, lookup([]string{"Movies"}),
		"the teardown is done, so the restore gate must open with it rather than wait out the confirmation window")
}
