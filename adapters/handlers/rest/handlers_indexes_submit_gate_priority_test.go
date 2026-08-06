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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/backup"
)

// The submit gate closes the backup gate on a whole collection. Two properties
// have to hold at once, and they pull in opposite directions:
//
//   - A capture already running must not be failed by a submission that is
//     going to be refused anyway.
//   - A capture admitted while the fan-out scan is still running must not have
//     its sidecars swept out from under it.
//
// The tests below pin one each, so a change that trades one for the other reds.

// gateObservingProber stands in for the nodes a backup can be running on. Every
// probe records what the collection's backup gate says at that instant, which
// is what a live capture's per-shard execution check reads.
type gateObservingProber struct {
	provider  *db.ReindexProvider
	busyNodes map[string]bool

	mu    sync.Mutex
	holds []db.ReindexHold
}

func (p *gateObservingProber) NodeActivity(_ context.Context, node string) (backup.NodeActivity, error) {
	p.mu.Lock()
	p.holds = append(p.holds, p.provider.HoldForShard("Movies", "shard1"))
	p.mu.Unlock()

	if p.busyNodes[node] {
		return backup.NodeActivity{
			Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1",
		}, nil
	}
	return backup.NodeActivity{}, nil
}

func (p *gateObservingProber) observed() []db.ReindexHold {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]db.ReindexHold, len(p.holds))
	copy(out, p.holds)
	return out
}

// localSlotProbe answers the in-process read of this node's own slots.
type localSlotProbe struct {
	activity backup.NodeActivity
	calls    atomic.Int64
}

func (p *localSlotProbe) Activity() backup.NodeActivity {
	p.calls.Add(1)
	return p.activity
}

// gatePriorityHandlers wires a real provider onto the submission fixture so the
// submit gate the handler takes is the one the assertions read.
func gatePriorityHandlers(t *testing.T, svc reindexTaskService) (*indexesHandlers, *db.ReindexProvider) {
	t.Helper()

	h := submissionHandlers(t, svc, nil)
	provider := db.NewReindexProvider(h.appState.DB, h.appState.SchemaManager,
		h.appState.Logger, fixtureNode, nil, context.Background())
	h.appState.ReindexProvider.Store(provider)
	return h, provider
}

// Pins: a submission this node's own backup slot is already going to refuse
// must never close the collection's backup gate. Closing it fails the capture
// that is running, so a caller looping on the 409's advice to retry can keep a
// collection's backups failing indefinitely.
func TestUpdateIndexRefusedByLocalBackupLeavesTheBackupGateOpen(t *testing.T) {
	svc := &raceTaskService{}
	h, provider := gatePriorityHandlers(t, svc)

	prober := &gateObservingProber{provider: provider, busyNodes: map[string]bool{fixtureNode: true}}
	h.backupActivity = prober
	local := &localSlotProbe{activity: backup.NodeActivity{
		Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1",
	}}
	h.localBackupActivity = local

	responder := submitReindex(h)

	conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a backup on this node must be refused with 409, got %T", responder)
	require.Equal(t,
		"reindex blocked: a backup is running in the cluster; retry after it finishes",
		errorMessage(t, conflict.Payload))

	for i, hold := range prober.observed() {
		require.Equalf(t, db.ReindexHoldNone, hold,
			"probe %d saw the collection's backup gate closed by the submit hold; the backup that is "+
				"already capturing fails its per-shard check while that hold is up, so this refused "+
				"submission destroyed it", i)
	}
	require.Emptyf(t, prober.observed(),
		"the local slots already settled the answer, so the fan-out must not run at all; "+
			"reaching it means the gate was taken for a submission that was never going to be admitted")
	require.EqualValues(t, 1, local.calls.Load(),
		"the local slots must be read before the gate, or there is nothing to refuse on")
	require.Zero(t, svc.adds, "a refused submission must write no task")
}

// Pins the corruption protection the gate exists for: the gate must still be
// closed across the fan-out scan. The scan answers node by node, so a backup
// that claims its slot on an already-answered node sees no submission, gets
// admitted, and would then have its sidecar dirs and .migrations tracker
// removed by the sweep the submission runs next.
func TestUpdateIndexHoldsTheBackupGateAcrossTheFanOutProbe(t *testing.T) {
	svc := &raceTaskService{}
	h, provider := gatePriorityHandlers(t, svc)

	// The backup is held where the in-process read cannot see it, so the
	// pre-check cannot settle the submission and the fan-out has to run.
	h.cluster = fixedMembership{fixtureNode, "node2"}
	prober := &gateObservingProber{provider: provider, busyNodes: map[string]bool{"node2": true}}
	h.backupActivity = prober
	h.localBackupActivity = &localSlotProbe{}

	responder := submitReindex(h)

	conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a backup on another node must be refused with 409, got %T", responder)
	require.Equal(t,
		"reindex blocked: a backup is running in the cluster; retry after it finishes",
		errorMessage(t, conflict.Payload))

	holds := prober.observed()
	require.NotEmpty(t, holds, "the fan-out must run when this node's own slots are idle")
	for i, hold := range holds {
		require.Equalf(t, db.ReindexHoldSubmit, hold,
			"probe %d ran with the submit gate open; a backup admitted at that instant is invisible to "+
				"the scan and would have its sidecars swept out from under it", i)
	}
	require.Zero(t, svc.adds, "a refused submission must write no task")
}
