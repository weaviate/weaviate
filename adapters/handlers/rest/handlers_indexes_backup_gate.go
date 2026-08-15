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
	"sync"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/clients"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/reindex"
)

// A peer that accepts the connection and then stops answering must not park a
// submission for as long as its client is willing to wait.
const clusterBackupProbeTimeout = 5 * time.Second

var errProbeLeftNoAnswer = errors.New("the probe left no answer")

// Ordered by precedence: one node this gate can see backing up refuses even
// when another node's answer never arrived.
type backupActivityVerdict int

const (
	backupActivityClear backupActivityVerdict = iota
	backupActivityUnreachable
	backupActivityBusy
)

// backupActivityScan is what a rung of the gate found. Only kind reaches the
// caller; node, id and fault are for the operator log.
type backupActivityScan struct {
	verdict backupActivityVerdict
	kind    string
	id      string
	node    string
	fault   error
}

// refuseOnLocalBackupActivity reads this node's own four operation slots.
func refuseOnLocalBackupActivity(activity backup.NodeActivity) backupActivityScan {
	if !activity.Busy {
		return backupActivityScan{}
	}
	return backupActivityScan{verdict: backupActivityBusy, kind: activity.Kind, id: activity.ID}
}

// openSubmitBackupGate runs the backup rungs of the submit ladder, and the
// order is the requirement. The local slots decide first, because raising the
// hold ahead of them would refuse the very capture already running here. The
// hold then closes before the fan-out, so a capture starting anywhere while
// the probe is in flight is refused by its own gate instead of racing this one.
//
// release is always non-nil, so a refused submission cannot leak a hold.
func openSubmitBackupGate(
	localActivity func() backup.NodeActivity,
	raiseHold func() (release func()),
	scanCluster func() backupActivityScan,
) (release func(), scan backupActivityScan) {
	if local := refuseOnLocalBackupActivity(localActivity()); local.verdict != backupActivityClear {
		return func() {}, local
	}
	release = raiseHold()
	return release, scanCluster()
}

// scanBackupActivity asks every node in parallel and reports the strongest
// verdict any of them produced.
func scanBackupActivity(ctx context.Context, nodes []string,
	probe func(context.Context, string) (backup.NodeActivity, error), logger logrus.FieldLogger,
) backupActivityScan {
	results := make([]backupActivityScan, len(nodes))
	for i, node := range nodes {
		// Seeded as unreachable so a prober that panics before writing its slot
		// leaves an answer that refuses, not a zero value that admits.
		results[i] = backupActivityScan{
			verdict: backupActivityUnreachable, node: node, fault: errProbeLeftNoAnswer,
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for i, node := range nodes {
		enterrors.GoWrapper(func() {
			defer wg.Done()
			results[i] = probeBackupActivity(ctx, node, probe)
		}, logger)
	}
	wg.Wait()

	strongest := backupActivityScan{}
	for _, result := range results {
		if result.verdict > strongest.verdict {
			strongest = result
		}
	}
	return strongest
}

func probeBackupActivity(ctx context.Context, node string,
	probe func(context.Context, string) (backup.NodeActivity, error),
) backupActivityScan {
	activity, err := probe(ctx, node)
	switch {
	// A deliberate fail-open: refusing here would 503 every submission for the
	// length of a rolling upgrade. The commit-time backstop covers the hole.
	case errors.Is(err, clients.ErrNodeActivityUnsupported):
		return backupActivityScan{}
	case err != nil:
		return backupActivityScan{verdict: backupActivityUnreachable, node: node, fault: err}
	case activity.Busy:
		return backupActivityScan{
			verdict: backupActivityBusy, kind: activity.Kind, id: activity.ID, node: node,
		}
	}
	return backupActivityScan{}
}

// localBackupActivity is idle before MakeAppState installs the probe, which is
// ahead of anything that serves this handler.
func (h *indexesHandlers) localBackupActivity() backup.NodeActivity {
	if h.appState.BackupActivity == nil {
		return backup.NodeActivity{}
	}
	return h.appState.BackupActivity.Activity()
}

// scanClusterBackupActivity fans the probe out over every other node. A node
// that has not joined a cluster yet has nobody to ask, so it admits.
func (h *indexesHandlers) scanClusterBackupActivity(ctx context.Context) backupActivityScan {
	prober := h.appState.ClusterBackupActivity
	if prober == nil || h.appState.Cluster == nil {
		return backupActivityScan{}
	}
	peers := otherNodes(h.appState.Cluster.AllNames(), h.appState.Cluster.LocalName())
	if len(peers) == 0 {
		return backupActivityScan{}
	}

	probeCtx, cancel := context.WithTimeout(ctx, clusterBackupProbeTimeout)
	defer cancel()
	return scanBackupActivity(probeCtx, peers, prober.NodeActivity, h.appState.Logger)
}

// The local slots were read directly a moment ago, and asking this node over
// HTTP would answer from those same four slots one round trip later.
func otherNodes(all []string, local string) []string {
	peers := make([]string, 0, len(all))
	for _, node := range all {
		if node != local {
			peers = append(peers, node)
		}
	}
	return peers
}

// markSubmitInProgress is a no-op release before the provider exists, which is
// ahead of the handler being served.
func (h *indexesHandlers) markSubmitInProgress(collection string) (release func()) {
	if h.appState.ReindexProvider == nil {
		return func() {}
	}
	return h.appState.ReindexProvider.MarkSubmitInProgress(collection)
}

// backupActivityRefusal renders a scan, or nil when the cluster is clear.
func (h *indexesHandlers) backupActivityRefusal(principal *models.Principal,
	collection string, scan backupActivityScan,
) middleware.Responder {
	switch scan.verdict {
	case backupActivityClear:
		return nil
	case backupActivityBusy:
		h.logBackupActivityRefusal(collection, "busy", scan)
		h.appState.ReindexGateMetrics.Refused(reindex.GateSubmit, submitRefusalVerdict(scan.kind))
		return jsonResponder(http.StatusConflict,
			errorResponse(principal, backupBusyRefusal(scan.kind)))
	case backupActivityUnreachable:
		h.logBackupActivityRefusal(collection, "unreachable", scan)
		h.appState.ReindexGateMetrics.Refused(reindex.GateSubmit, reindex.VerdictUnreachable)
		return jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			"cannot confirm the cluster is free of backups: a node did not answer the "+
				"backup-activity probe; retry once every node is reachable"))
	}
	return nil
}

func (h *indexesHandlers) logBackupActivityRefusal(collection, verdict string, scan backupActivityScan) {
	if h.appState.Logger == nil {
		return
	}
	fields := logrus.Fields{
		"audit_event":  "reindex_submit_refused",
		"gate":         "submit",
		"verdict":      verdict,
		"collection":   collection,
		"node":         scan.node,
		"kind":         scan.kind,
		"operation_id": scan.id,
	}
	if scan.fault != nil {
		h.appState.Logger.WithFields(fields).Warnf(
			"submit gate: refusing this reindex; the published refusal names no node: %v", scan.fault)
		return
	}
	h.appState.Logger.WithFields(fields).
		Warn("submit gate: refusing this reindex; the published refusal names no node and no operation id")
}

// The body names the kind and nothing else: the node and the operation id are
// cluster-wide state a caller authorized for one collection must not learn.
func backupBusyRefusal(kind string) string {
	return fmt.Sprintf("reindex blocked: a %s is running in the cluster; retry after it finishes",
		publishableActivityKind(kind))
}

// The label follows the published kind, so a kind this build cannot name counts
// where its own refusal says it counts.
func submitRefusalVerdict(kind string) string {
	if publishableActivityKind(kind) == backup.NodeActivityKindRestore {
		return reindex.VerdictRestoreBusy
	}
	return reindex.VerdictBackupBusy
}

// A kind this build cannot name still refuses, but as a backup: the string came
// off the wire and must not reach an operator-facing body verbatim.
func publishableActivityKind(kind string) string {
	if kind == backup.NodeActivityKindRestore {
		return backup.NodeActivityKindRestore
	}
	return backup.NodeActivityKindBackup
}
