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
	"net/http"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
)

// Pins the property the submit gate's ordering exists for: a capture already
// running must not be failed by a submission that will be refused anyway.

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

// localSlotProbe answers the in-process read of this node's own slots, and
// records what the collection's backup gate says at that instant. That is the
// only place the ordering is observable: once the pre-check refuses, nothing
// else in the handler runs, so no later observer exists to ask.
type localSlotProbe struct {
	activity backup.NodeActivity
	provider *db.ReindexProvider

	mu    sync.Mutex
	holds []db.ReindexHold
}

func (p *localSlotProbe) Activity() backup.NodeActivity {
	p.mu.Lock()
	p.holds = append(p.holds, p.provider.HoldForShard("Movies", "shard1"))
	p.mu.Unlock()
	return p.activity
}

func (p *localSlotProbe) observed() []db.ReindexHold {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]db.ReindexHold, len(p.holds))
	copy(out, p.holds)
	return out
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
	local := &localSlotProbe{provider: provider, activity: backup.NodeActivity{
		Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1",
	}}
	h.localBackupActivity = local

	responder := submitReindex(h)

	conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a backup on this node must be refused with 409, got %T", responder)
	require.Equal(t,
		"reindex blocked: a backup is running in the cluster; retry after it finishes",
		errorMessage(t, conflict.Payload))

	// This is the whole property, and it has to be stated as an exact slice:
	// one read, and the gate open at it. A "for range observations" loop would
	// pass with zero observations, which is what deleting the pre-check
	// produces. The hold is what the capture that is already running reads on
	// every shard it writes; anything but ReindexHoldNone fails it.
	require.Equal(t, []db.ReindexHold{db.ReindexHoldNone}, local.observed(),
		"the local slots must be read exactly once, with the collection's backup gate still open; "+
			"a submission that is refused anyway must never close the gate on a running capture")
	require.Emptyf(t, prober.observed(),
		"the local slots already settled the answer, so the fan-out must not run at all; "+
			"reaching it means the gate was taken for a submission that was never going to be admitted")
	require.Zero(t, svc.adds, "a refused submission must write no task")
}

// Every gate in handlers_indexes.go reads one of these five fields, and every
// gate test injects its own. So production dropping a field on the floor is
// invisible to all of them: the gate keeps its tests and stops running. This is
// the one place the real constructor is exercised, so it has to name each field
// and say what goes unguarded when it is nil.
func TestIndexesHandlersWireEveryGateCollaborator(t *testing.T) {
	probe := backup.NewNodeActivityProbe(nil)
	clusterState := &cluster.State{}
	tasks := &rCluster.Service{}

	h := newIndexesHandlers(&state.State{
		BackupActivity:    probe,
		Cluster:           clusterState,
		ClusterService:    tasks,
		ClusterHttpClient: &http.Client{},
		ServerConfig:      &config.WeaviateConfig{},
	})

	require.Same(t, probe, h.localBackupActivity,
		"unwired, the submit gate is taken before anything reads this node's own backup slots, "+
			"and the priority inversion is back for every real deployment")

	require.Same(t, clusterState, h.cluster,
		"unwired, there is no node list to fan out over, so the cluster-wide backup probe "+
			"scans nobody and every submission is admitted over a running capture")

	require.Same(t, tasks, h.tasks,
		"unwired, both reindex routes answer 503 and no migration can be submitted or cancelled")

	require.NotNil(t, h.backupActivity,
		"unwired, no peer is asked whether it holds a backup slot, so a submission races "+
			"a capture running anywhere else in the cluster")

	require.NotNil(t, h.reindexCleanup,
		"unwired, cancel answers without confirming that peers finished their rollback sweep")

	// A cluster service and nothing else: the shape a wiring regression leaves.
	// Every collaborator fails OPEN when nil, so the ERROR line below is the
	// only thing that would catch it.
	logger, hook := logrustest.NewNullLogger()
	h = newIndexesHandlers(&state.State{ClusterService: tasks, Logger: logger})
	require.Nil(t, h.localBackupActivity,
		"a node with no backup manager has no slots to read; the fan-out probe is the only check")

	entry := entryWithMessage(hook, "gate collaborators are missing")
	require.NotNilf(t, entry,
		"a node with a cluster service and no probes runs reindex submissions unchecked "+
			"against backups, and says nothing about it; entries were %q", entryMessages(hook))
	require.Equal(t, logrus.ErrorLevel, entry.Level,
		"a disabled safety gate is not a warning about a fixture")
	require.ElementsMatch(t,
		[]string{
			"backupActivity (no peer is asked whether it holds a backup slot)",
			"localBackupActivity (this node's own slots are never read)",
			"cluster (there is no node list to fan the backup probe out over)",
		},
		entry.Data["unwired"],
		"the line has to name which collaborator is missing, or it cannot be acted on")
}
