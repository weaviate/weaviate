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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/schema"
)

// dropVectorMinClusterVersion is the lowest server version that can run the
// drop-vector Phase-2 cleanup (edit-ops machinery). No cleanup task is enqueued
// until every node is at least this version (rolling-upgrade safety). Ships in 1.39.
const dropVectorMinClusterVersion = "1.39.0"

// dropVectorVersionRegex matches the leading "MAJOR.MINOR.PATCH" of a server
// version. It is deliberately tolerant of pre-release/build suffixes (e.g.
// "1.39.0-dev", "1.39.0-rc.1") which are treated as satisfying the minimum once
// the MAJOR.MINOR.PATCH triple is >= the threshold — a dev/rc build of the
// supporting release does understand the feature.
var dropVectorVersionRegex = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)`)

// dropVectorIndexEnqueuer implements schema.DropVectorIndexEnqueuer. It submits
// the Phase-2 cleanup distributed task and reports whether one is in flight,
// using the cluster DTM client + sharding state. Lives in the REST wiring layer
// so it can reuse buildUnitMaps/buildUnitSpecs.
type dropVectorIndexEnqueuer struct {
	clusterService clusterDropTaskClient
	ownership      shardOwnershipLister
	versions       clusterVersionLister
	logger         logrus.FieldLogger
}

// clusterDropTaskClient is the slice of the cluster service the enqueuer uses.
type clusterDropTaskClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
		taskPayload any, unitSpecs []distributedtask.UnitSpec) error
}

// shardOwnershipLister returns shard -> owning nodes for a collection, limited to
// shards with locally loaded data (deactivated MT tenants are excluded — their
// cleanup is deferred to activation). *db.DB satisfies it via
// ShardReplicaOwnershipActive. Narrowed so the enqueuer is testable.
type shardOwnershipLister interface {
	ShardReplicaOwnershipActive(ctx context.Context, className string) (map[string][]string, error)
}

// clusterVersionLister reports per-node software versions; *db.DB satisfies it via
// GetNodeStatus (each NodeStatus carries the build Version). "Minimal" verbosity
// keeps the cross-node call cheap — only the Version field is read.
type clusterVersionLister interface {
	GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error)
}

func newDropVectorIndexEnqueuer(clusterService clusterDropTaskClient, ownership shardOwnershipLister,
	versions clusterVersionLister, logger logrus.FieldLogger,
) *dropVectorIndexEnqueuer {
	return &dropVectorIndexEnqueuer{
		clusterService: clusterService,
		ownership:      ownership,
		versions:       versions,
		logger:         logger,
	}
}

// HasActiveDrop reports whether a non-terminal drop task already covers
// targetVector on collection.
func (e *dropVectorIndexEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return false, err
	}
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		if !task.Status.IsActive() {
			continue
		}
		var p db.DropVectorIndexTaskPayload
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		for _, t := range p.Targets {
			if t == targetVector {
				return true, nil
			}
		}
	}
	return false, nil
}

// EnqueueDropVectorIndex submits a fresh cleanup task (fresh task + op ID) with
// one unit per (shard, replica) grouped by shard, so each replica node strips
// its own objects bucket.
func (e *dropVectorIndexEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
	// Rolling-upgrade gate: an older node predates the edit-ops machinery and can't
	// run a cleanup unit, so defer enqueueing until every node is upgraded. Skip
	// (return nil), don't error: Phase 1 already set the marker and startup
	// reconciliation re-enqueues it once the cluster is fully upgraded, so the
	// user's drop isn't failed by an in-flight upgrade.
	upgraded, err := e.clusterFullyUpgraded(ctx)
	if err != nil {
		// Versions unreadable (e.g. a node unreachable mid-upgrade): defer, don't fail.
		e.logger.WithField("collection", collection).WithField("targets", targets).
			Warnf("drop-vector enqueue: cannot verify cluster version, deferring cleanup to reconciliation: %v", err)
		return nil
	}
	if !upgraded {
		e.logger.WithField("collection", collection).WithField("targets", targets).
			WithField("min_version", dropVectorMinClusterVersion).
			Info("drop-vector enqueue: cluster not fully upgraded, deferring Phase-2 cleanup to reconciliation")
		return nil
	}

	shardOwnership, err := e.ownership.ShardReplicaOwnershipActive(ctx, collection)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: shard ownership for %q: %w", collection, err)
	}
	if len(shardOwnership) == 0 {
		return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
	}

	_, unitToShard, unitToNode := buildUnitMaps(shardOwnership)
	specs := buildUnitSpecs(shardOwnership)

	// Pass the payload struct, not pre-marshaled bytes: the cluster layer
	// json.Marshals taskPayload itself (bytes would be double-encoded into a JSON
	// string and fail to decode in CheckConflict / the provider).
	payload := db.DropVectorIndexTaskPayload{
		Collection:  collection,
		Targets:     targets,
		OpID:        uuid.NewString(),
		UnitToNode:  unitToNode,
		UnitToShard: unitToShard,
	}

	// Fresh task ID per submission so a re-trigger after a FAILED run is a new
	// task version (C4). The ConflictDetector rejects a duplicate against an
	// active task, the backstop for the HasActiveDrop check race.
	taskID := uuid.NewString()
	return e.clusterService.AddDistributedTaskWithGroups(ctx, db.DropVectorIndexNamespace, taskID, payload, specs)
}

// clusterFullyUpgraded reports whether every node is at or above
// dropVectorMinClusterVersion. A nil version source (no lister wired) counts as
// upgraded, so missing wiring never permanently blocks the feature.
//
// Determinism: runs at request/reconciliation time, not in a RAFT apply, so it may
// read non-replicated, eventually-consistent per-node versions. The replicated
// marker is written separately in Phase 1; this only decides enqueue-now vs defer.
func (e *dropVectorIndexEnqueuer) clusterFullyUpgraded(ctx context.Context) (bool, error) {
	if e.versions == nil {
		return true, nil
	}
	statuses, err := e.versions.GetNodeStatus(ctx, "", "", verbosity.OutputMinimal)
	if err != nil {
		return false, fmt.Errorf("drop-vector enqueue: list node versions: %w", err)
	}
	if len(statuses) == 0 {
		return false, fmt.Errorf("drop-vector enqueue: no node statuses returned")
	}
	for _, s := range statuses {
		if s == nil {
			return false, fmt.Errorf("drop-vector enqueue: nil node status")
		}
		ok, err := versionAtLeast(s.Version, dropVectorMinClusterVersion)
		if err != nil {
			return false, fmt.Errorf("drop-vector enqueue: node %q version %q: %w", s.Name, s.Version, err)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// versionAtLeast reports whether the MAJOR.MINOR.PATCH prefix of have is >= the
// MAJOR.MINOR.PATCH of want. Pre-release/build suffixes on have are ignored (see
// dropVectorVersionRegex): a "1.39.0-dev" build of the supporting release is
// considered to satisfy "1.39.0".
func versionAtLeast(have, want string) (bool, error) {
	hMaj, hMin, hPat, err := parseSemverPrefix(have)
	if err != nil {
		return false, err
	}
	wMaj, wMin, wPat, err := parseSemverPrefix(want)
	if err != nil {
		return false, fmt.Errorf("threshold %q: %w", want, err)
	}
	if hMaj != wMaj {
		return hMaj > wMaj, nil
	}
	if hMin != wMin {
		return hMin > wMin, nil
	}
	return hPat >= wPat, nil
}

func parseSemverPrefix(v string) (major, minor, patch int, err error) {
	m := dropVectorVersionRegex.FindStringSubmatch(v)
	if m == nil {
		return 0, 0, 0, fmt.Errorf("unparseable version %q", v)
	}
	// Regex groups are \d+ so Atoi cannot fail.
	major, _ = strconv.Atoi(m[1])
	minor, _ = strconv.Atoi(m[2])
	patch, _ = strconv.Atoi(m[3])
	return major, minor, patch, nil
}

var _ schema.DropVectorIndexEnqueuer = (*dropVectorIndexEnqueuer)(nil)

// reconcileDroppedVectorIndexes enqueues cleanup for every "none" marker with no
// in-flight task — recovery for a crash between marker apply and enqueue, an
// upgrade with pre-existing markers, or a restore. Idempotent: HasActiveDrop +
// the ConflictDetector dedupe across nodes running it at startup.
func reconcileDroppedVectorIndexes(ctx context.Context, classes []*models.Class,
	enq schema.DropVectorIndexEnqueuer, logger logrus.FieldLogger,
) {
	for _, class := range classes {
		if class == nil {
			continue
		}
		for name, cfg := range class.VectorConfig {
			if !modelsext.IsVectorIndexDropped(cfg) {
				continue
			}
			active, err := enq.HasActiveDrop(ctx, class.Class, name)
			if err != nil {
				logger.WithField("collection", class.Class).WithField("vector", name).
					Warnf("drop-vector reconcile: HasActiveDrop failed: %v", err)
				continue
			}
			if active {
				continue
			}
			if err := enq.EnqueueDropVectorIndex(ctx, class.Class, []string{name}); err != nil {
				logger.WithField("collection", class.Class).WithField("vector", name).
					Warnf("drop-vector reconcile: enqueue failed: %v", err)
			}
		}
	}
}

// schemaLister returns the local schema snapshot (eventually-consistent is fine
// for an idempotent safety net); *schema.Manager satisfies it.
type schemaLister interface {
	GetSchemaSkipAuth() entschema.Schema
}

// runDropVectorIndexReconciliationAtStartup waits (bounded) for the cluster task
// store to be readable — so submits don't hit an unelected leader — then runs
// reconcileDroppedVectorIndexes once. Launch in a goroutine; cancellable via ctx.
func runDropVectorIndexReconciliationAtStartup(ctx context.Context, lister schemaLister,
	enq schema.DropVectorIndexEnqueuer, logger logrus.FieldLogger,
) {
	sch := lister.GetSchemaSkipAuth()
	if sch.Objects == nil || len(sch.Objects.Classes) == 0 {
		return
	}

	const attempts = 30
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return
		}
		// Probe the DTM read path; success means the leader is reachable.
		if _, err := enq.HasActiveDrop(ctx, "", ""); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	reconcileDroppedVectorIndexes(ctx, sch.Objects.Classes, enq, logger)
}
