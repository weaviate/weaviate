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
	"fmt"
	"regexp"
	"sort"
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
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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
// the background cleanup distributed task and reports whether one is in flight,
// using the cluster DTM client + sharding state. Lives in the REST wiring layer
// so it can reuse buildUnitMaps/buildUnitSpecs.
type dropVectorIndexEnqueuer struct {
	clusterService clusterDropTaskClient
	schemaState    schemaStateQuerier
	versions       clusterVersionLister
	logger         logrus.FieldLogger // nil-safe throughout
}

// clusterDropTaskClient is the slice of the cluster service the enqueuer uses.
type clusterDropTaskClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
		taskPayload any, unitSpecs []distributedtask.UnitSpec) error
}

// schemaStateQuerier provides leader-consistent schema reads for a collection:
// the sharding state (units must be built from the current tenant statuses, not
// this node's eventually-consistent local view — a tenant activated moments ago
// could still read COLD locally and be skipped) and the class (targets are
// re-validated as still marked dropped right before submitting the destructive
// cleanup task). cluster.Raft satisfies it.
type schemaStateQuerier interface {
	QueryShardingState(class string) (*sharding.State, uint64, error)
	QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error)
}

// clusterVersionLister reports per-node software versions; *db.DB satisfies it via
// GetNodeStatus (each NodeStatus carries the build Version). "Minimal" verbosity
// keeps the cross-node call cheap — only the Version field is read.
type clusterVersionLister interface {
	GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error)
}

func newDropVectorIndexEnqueuer(clusterService clusterDropTaskClient, schemaState schemaStateQuerier,
	versions clusterVersionLister, logger logrus.FieldLogger,
) *dropVectorIndexEnqueuer {
	return &dropVectorIndexEnqueuer{
		clusterService: clusterService,
		schemaState:    schemaState,
		versions:       versions,
		logger:         logger,
	}
}

// warnSkippedPayload surfaces an undecodable active-task payload instead of
// silently skipping it (the skip itself is deliberate fail-open behavior).
func (e *dropVectorIndexEnqueuer) warnSkippedPayload(where, taskID string, err error) {
	if e.logger != nil {
		e.logger.WithField("task", taskID).
			Warnf("drop-vector %s: skipping active task with unparseable payload: %v", where, err)
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
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			e.warnSkippedPayload("has-active-drop", task.ID, err)
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		for _, t := range p.Targets {
			// Exact match: target vector names are case-sensitive identifiers.
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
	// (return nil), don't error: Phase 1 already set the marker and the
	// reconciliation loop re-enqueues it once the cluster is fully upgraded, so the
	// user's drop isn't failed by an in-flight upgrade.
	upgraded, err := e.clusterFullyUpgraded(ctx)
	if err != nil {
		// Versions unreadable (e.g. a node unreachable mid-upgrade): defer, don't fail.
		if e.logger != nil {
			e.logger.WithField("collection", collection).WithField("targets", targets).
				Warnf("drop-vector enqueue: cannot verify cluster version, deferring cleanup to reconciliation: %v", err)
		}
		return nil
	}
	if !upgraded {
		if e.logger != nil {
			e.logger.WithField("collection", collection).WithField("targets", targets).
				WithField("min_version", dropVectorMinClusterVersion).
				Info("drop-vector enqueue: cluster not fully upgraded, deferring Phase-2 cleanup to reconciliation")
		}
		return nil
	}

	// Re-validate against the leader-consistent class: the marker commit and this
	// enqueue are not atomic, and reconciliation may run off a stale local schema
	// snapshot. A target that is no longer marked dropped (class deleted and
	// re-created, or already finalized elsewhere) must not get a cleanup task —
	// that task would strip a live vector.
	targets, err = e.stillDroppedTargets(collection, targets)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: verify targets for %q: %w", collection, err)
	}
	if len(targets) == 0 {
		return nil // nothing (still) marked dropped — no-op
	}

	state, _, err := e.schemaState.QueryShardingState(collection)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: sharding state for %q: %w", collection, err)
	}
	if state == nil {
		return fmt.Errorf("drop-vector enqueue: no sharding state for collection %q", collection)
	}
	shardOwnership := activeShardOwnership(state)
	if len(shardOwnership) == 0 {
		// All-cold MT collection: no active shard to strip now, and the marker is
		// already applied — a no-op success, not an error. Reconciliation re-enqueues
		// once tenants are active. A non-MT collection always has shards, so an empty
		// map there is a real problem.
		if state.PartitioningEnabled {
			return nil
		}
		return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
	}

	// Known limitation (matches the reindex model): a unit is emitted per
	// (shard, replica) with no liveness filter, and the DTM never reassigns a
	// claimed unit. A permanently-removed replica leaves its unit non-terminal and
	// the task STARTED until an operator removes the node; no data is at risk (the
	// marker stays and reconciliation resumes after the topology is repaired).
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
	// task version. The ConflictDetector rejects a duplicate against an active
	// task, the backstop for the HasActiveDrop check race.
	taskID := uuid.NewString()
	return e.clusterService.AddDistributedTaskWithGroups(ctx, db.DropVectorIndexNamespace, taskID, payload, specs)
}

// stillDroppedTargets filters targets to those still present and marked dropped
// in the leader-consistent class. A missing class means nothing to clean.
func (e *dropVectorIndexEnqueuer) stillDroppedTargets(collection string, targets []string) ([]string, error) {
	vclasses, err := e.schemaState.QueryReadOnlyClasses(collection)
	if err != nil {
		return nil, err
	}
	class := vclasses[collection].Class
	if class == nil {
		return nil, nil
	}
	var still []string
	for _, target := range targets {
		if cfg, ok := class.VectorConfig[target]; ok && modelsext.IsVectorIndexDropped(cfg) {
			still = append(still, target)
		}
	}
	return still, nil
}

// activeShardOwnership builds node -> shard-names from a sharding state, limited
// to shards with locally loaded data: for multi-tenant collections only HOT/ACTIVE
// tenants (a cleanup unit on a deactivated tenant's shard would load it and
// prematurely remove its files; such tenants are picked up by a later
// reconciliation once active); for non-MT collections all shards. Shard lists are
// sorted per node for determinism.
func activeShardOwnership(state *sharding.State) map[string][]string {
	result := make(map[string][]string)
	for shardName, shard := range state.Physical {
		if state.PartitioningEnabled {
			switch entschema.ActivityStatus(shard.Status) {
			case models.TenantActivityStatusHOT, models.TenantActivityStatusACTIVE:
				// Loaded — include.
			default:
				continue
			}
		}
		for _, node := range shard.BelongsToNodes {
			if node != "" {
				result[node] = append(result[node], shardName)
			}
		}
	}
	for _, shards := range result {
		sort.Strings(shards)
	}
	return result
}

// LiveOpIDs returns the op IDs of drop-vector tasks that are still active
// (non-terminal). Wired into the DB so a shard load can sweep an orphaned op — one
// whose task has finished or been removed — instead of re-arming it. Returns a
// non-nil (possibly empty) set on success; empty means "no active drop, sweep all".
func (e *dropVectorIndexEnqueuer) LiveOpIDs(ctx context.Context) (map[string]struct{}, error) {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return nil, err
	}
	live := map[string]struct{}{}
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		if !task.Status.IsActive() {
			continue
		}
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			e.warnSkippedPayload("live-op-ids", task.ID, err)
			continue
		}
		live[p.OpID] = struct{}{}
	}
	return live, nil
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

// dropVectorReconcileInterval is how often reconciliation re-checks for "none"
// markers without a live task after the startup pass — the pickup path for
// tenants that were inactive when a task finalized deferred, for markers left by
// a FAILED task, and for backup restores; without it those wait for a restart.
const dropVectorReconcileInterval = 15 * time.Minute

// runDropVectorIndexReconciliation waits (bounded) for the cluster task store to
// be readable — so submits don't hit an unelected leader — then runs
// reconcileDroppedVectorIndexes periodically until ctx is cancelled. Launch in a
// goroutine.
func runDropVectorIndexReconciliation(ctx context.Context, lister schemaLister,
	enq schema.DropVectorIndexEnqueuer, logger logrus.FieldLogger, interval time.Duration,
) {
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

	for {
		// Read the schema AFTER the readiness wait (and fresh each round): at
		// startup the local schema is restored by the same background open the
		// probe waits for, so an early read would see an empty/stale snapshot and
		// silently skip markers — and this is the sole recovery path for every
		// "reconciliation retries" deferral. Each round is panic-contained: this
		// goroutine is that sole recovery path, so one bad round must not kill the
		// loop until restart.
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("drop-vector reconcile: round panicked (loop continues): %v", r)
				}
			}()
			sch := lister.GetSchemaSkipAuth()
			if sch.Objects != nil && len(sch.Objects.Classes) > 0 {
				reconcileDroppedVectorIndexes(ctx, sch.Objects.Classes, enq, logger)
			}
		}()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
