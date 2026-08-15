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
	"slices"
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

var (
	errProbeLeftNoAnswer = errors.New("the probe left no answer")
	// Distinct from a peer that did not answer: here there is no trustworthy
	// list of peers to ask in the first place.
	errClusterViewUnavailable = errors.New("this node is missing from its own cluster's member list")
	// Whether a peer is capturing does not depend on this node's own wiring, so
	// a missing prober or cluster view is one more way of not being able to ask.
	errClusterProbeUnwired = errors.New("this node has no wiring to ask its peers about backups")
)

// Ordered by precedence: a node seen backing up refuses even when another
// node's answer never arrived.
type backupActivityVerdict int

const (
	backupActivityClear backupActivityVerdict = iota
	backupActivityUnreachable
	backupActivityBusy
)

// Only kind reaches the caller; node, id and fault are for the operator log.
type backupActivityScan struct {
	verdict backupActivityVerdict
	kind    string
	id      string
	node    string
	fault   error
}

func refuseOnLocalBackupActivity(activity backup.NodeActivity) backupActivityScan {
	if activity.Free() {
		return backupActivityScan{}
	}
	// Absence of Busy is not an answer: a zero value clears a capturing node.
	if !activity.Answered {
		return backupActivityScan{verdict: backupActivityUnreachable, fault: errProbeLeftNoAnswer}
	}
	return backupActivityScan{verdict: backupActivityBusy, kind: activity.Kind, id: activity.ID}
}

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

// Idle before MakeAppState installs the probe, which precedes anything served.
func (h *indexesHandlers) localBackupActivity() backup.NodeActivity {
	if h.appState.BackupActivity == nil {
		// Idle-by-wiring is an answer; only a probe that ran and left nothing is not.
		return backup.NodeActivity{Answered: true}
	}
	return h.appState.BackupActivity.Activity()
}

func (h *indexesHandlers) scanClusterBackupActivity(ctx context.Context) backupActivityScan {
	prober := h.appState.ClusterBackupActivity
	if prober == nil || h.appState.Cluster == nil {
		return backupActivityScan{verdict: backupActivityUnreachable, fault: errClusterProbeUnwired}
	}

	peers, established := peersToProbe(h.appState.Cluster.AllNames(), h.appState.Cluster.LocalName())
	if !established {
		return backupActivityScan{verdict: backupActivityUnreachable, fault: errClusterViewUnavailable}
	}
	if len(peers) == 0 {
		return backupActivityScan{}
	}

	probeCtx, cancel := context.WithTimeout(ctx, clusterBackupProbeTimeout)
	defer cancel()
	return scanBackupActivity(probeCtx, peers, prober.NodeActivity, h.appState.Logger)
}

// peersToProbe reports which nodes to ask, and whether the answer can be
// trusted at all. Memberlist names this node as soon as it is up, so a list
// without it has not converged or was reaped under a partition — neither of
// which is "no peer holds a backup". Only a list naming this node and nobody
// else is a node genuinely running alone.
func peersToProbe(all []string, local string) (peers []string, established bool) {
	if local == "" || !slices.Contains(all, local) {
		return nil, false
	}

	peers = make([]string, 0, len(all))
	for _, node := range all {
		if node != local {
			peers = append(peers, node)
		}
	}
	return peers, true
}

// scanBackupActivityForSubmit answers the one question the submit gate asks:
// is a backup or restore running anywhere in the cluster. The local slots
// decide first, so a node capturing right here refuses without a fan-out, and
// on a single node they are the only rung there is.
//
// The answer is stale on arrival — it is a courtesy refusal that saves the
// caller a capture that would fail hours later, never a reservation. A capture
// admitted after this read is refused by the per-shard reindex gate or by the
// commit-time overlap check instead.
func (h *indexesHandlers) scanBackupActivityForSubmit(ctx context.Context) backupActivityScan {
	if local := refuseOnLocalBackupActivity(h.localBackupActivity()); local.verdict != backupActivityClear {
		return local
	}
	return h.scanClusterBackupActivity(ctx)
}

func (h *indexesHandlers) backupActivityRefusal(principal *models.Principal,
	collection string, scan backupActivityScan,
) middleware.Responder {
	switch scan.verdict {
	case backupActivityClear:
		return nil
	case backupActivityBusy:
		h.logBackupActivityRefusal(principal, collection, "busy", scan)
		h.appState.ReindexGateMetrics.Refused(reindex.GateSubmit, submitRefusalVerdict(scan.kind))
		return jsonResponder(http.StatusConflict,
			errorResponse(principal, backupBusyRefusal(scan.kind)))
	case backupActivityUnreachable:
		h.logBackupActivityRefusal(principal, collection, "unreachable", scan)
		h.appState.ReindexGateMetrics.Refused(reindex.GateSubmit, reindex.VerdictUnreachable)
		return jsonResponder(http.StatusServiceUnavailable, errorResponse(principal,
			"cannot confirm the cluster is free of backups: a node did not answer the "+
				"backup-activity probe; retry once every node is reachable"))
	}
	return nil
}

func (h *indexesHandlers) logBackupActivityRefusal(principal *models.Principal,
	collection, verdict string, scan backupActivityScan,
) {
	if h.appState.Logger == nil {
		return
	}
	fields := logrus.Fields{
		"audit_event":  "reindex_submit_refused",
		"gate":         "submit",
		"verdict":      verdict,
		"collection":   collection,
		"principal":    principalUsername(principal),
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

// Follows the published kind, so an unnameable one counts where it refuses.
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
