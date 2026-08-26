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

package backup

import (
	"context"
	"sort"
	"time"

	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// ReplicaCheckpointer proves per-shard replica convergence via async-replication
// checkpoints. Implemented by *db.DB; nil on coordinators that never dedupe.
type ReplicaCheckpointer interface {
	// ShardReplicas returns shard name -> replica node names for class.
	ShardReplicas(ctx context.Context, class string) (map[string][]string, error)
	// IsAsyncReplicationEnabled reports whether the class's replicas are kept
	// consistent by async replication (true also for RF=1, where it is irrelevant).
	IsAsyncReplicationEnabled(ctx context.Context, class string) bool
	CreateAsyncCheckpoints(ctx context.Context, class string, cutoffMs int64, shards []string) error
	DeleteAsyncCheckpoints(ctx context.Context, class string, shards []string) error
	GetAsyncCheckpointNodeStatuses(ctx context.Context, class string, shards []string) (map[string][]replica.AsyncCheckpointNodeStatus, error)
}

const (
	// _DedupeCutoffLead must exceed checkpoint-create fan-out latency: shards reject a cutoff already in the past.
	_DedupeCutoffLead               = 10 * time.Second
	_DedupePollInterval             = 3 * time.Second
	_DefaultDedupeConvergenceBudget = 60 * time.Second
	_DedupeCleanupTimeout           = 10 * time.Second
)

// dedupePlan is the outcome of convergence planning for one backup.
type dedupePlan struct {
	// designations maps class -> shard -> the single node that archives it.
	designations map[string]map[string]string
	// replicas maps class -> shard -> replica nodes, for per-node projection.
	replicas map[string]map[string][]string
}

// designated counts shards assigned to a single archiving node.
func (p *dedupePlan) designated() int {
	n := 0
	for _, shards := range p.designations {
		n += len(shards)
	}
	return n
}

// planDesignatedShards proves per-shard replica convergence via async
// checkpoints and designates one archiving node per converged shard. It never
// fails the backup: every internal failure downgrades the affected shards to
// the all-replica fallback. Checkpoints are deleted before returning: proving
// convergence at the cutoff is sufficient, archiving needs no live checkpoint.
func (c *coordinator) planDesignatedShards(ctx context.Context, classes []string, budget time.Duration) *dedupePlan {
	defer func(begin time.Time) {
		monitoring.GetMetrics().BackupDedupePlanningDurations.Observe(float64(time.Since(begin).Milliseconds()))
	}(time.Now())
	if budget <= 0 {
		budget = c.dedupeConvergenceBudget
	}
	plan := &dedupePlan{
		designations: make(map[string]map[string]string),
		replicas:     make(map[string]map[string][]string),
	}

	candidates := make(map[string][]string, len(classes))
	for _, class := range classes {
		if !c.checkpointer.IsAsyncReplicationEnabled(ctx, class) {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("class_ineligible").Inc()
			c.log.WithField("action", OpCreate).WithField("class", class).
				Info("replica dedupe: class skipped, async replication not enabled")
			continue
		}
		replicasByShard, err := c.checkpointer.ShardReplicas(ctx, class)
		if err != nil {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("class_ineligible").Inc()
			c.log.WithField("action", OpCreate).WithField("class", class).
				Warnf("replica dedupe: class falls back to all-replica backup: %v", err)
			continue
		}
		var shards []string
		for shard, nodes := range replicasByShard {
			if len(uniqueNonEmpty(nodes)) >= 2 {
				shards = append(shards, shard)
				if plan.replicas[class] == nil {
					plan.replicas[class] = make(map[string][]string, len(replicasByShard))
				}
				plan.replicas[class][shard] = nodes
			}
		}
		if len(shards) > 0 {
			sort.Strings(shards)
			candidates[class] = shards
		}
	}
	if len(candidates) == 0 {
		return plan
	}

	cutoffMs := time.Now().Add(c.dedupeCutoffLead).UnixMilli()
	created := make([]string, 0, len(candidates))
	for class, shards := range candidates {
		if err := c.checkpointer.CreateAsyncCheckpoints(ctx, class, cutoffMs, shards); err != nil {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("create_failed").Inc()
			c.log.WithField("action", OpCreate).WithField("class", class).
				Warnf("replica dedupe: class falls back to all-replica backup: create checkpoints: %v", err)
			delete(candidates, class)
			continue
		}
		created = append(created, class)
	}
	defer func() {
		// Leaked checkpoints are inert, but delete regardless of how planning exits.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _DedupeCleanupTimeout)
		defer cancel()
		for _, class := range created {
			if err := c.checkpointer.DeleteAsyncCheckpoints(cleanupCtx, class, candidates[class]); err != nil {
				c.log.WithField("action", OpCreate).WithField("class", class).
					Warnf("replica dedupe: delete checkpoints: %v", err)
			}
		}
	}()
	if len(candidates) == 0 {
		return plan
	}

	if !sleepUntil(ctx, time.UnixMilli(cutoffMs)) {
		return plan
	}

	converged := c.pollConvergence(ctx, candidates, plan.replicas, cutoffMs, budget)

	loads := make(map[string]int)
	classNames := make([]string, 0, len(converged))
	for class := range converged {
		classNames = append(classNames, class)
	}
	sort.Strings(classNames)
	for _, class := range classNames {
		plan.designations[class] = assignDesignations(converged[class], loads)
		c.log.WithField("action", OpCreate).WithField("class", class).
			WithField("designated", len(plan.designations[class])).
			WithField("fallback", len(candidates[class])-len(plan.designations[class])).
			Info("replica dedupe: planning complete")
	}
	candidateShards := 0
	for _, shards := range candidates {
		candidateShards += len(shards)
	}
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("designated").Add(float64(plan.designated()))
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("fallback").Add(float64(candidateShards - plan.designated()))
	return plan
}

// pollConvergence polls checkpoint statuses until every candidate shard is
// resolved (converged or dropped) or the budget expires. It returns
// class -> shard -> replica nodes for converged shards only.
func (c *coordinator) pollConvergence(ctx context.Context, candidates map[string][]string,
	replicas map[string]map[string][]string, cutoffMs int64, budget time.Duration,
) map[string]map[string][]string {
	converged := make(map[string]map[string][]string)
	pending := make(map[string]map[string]struct{}, len(candidates))
	for class, shards := range candidates {
		pending[class] = make(map[string]struct{}, len(shards))
		for _, shard := range shards {
			pending[class][shard] = struct{}{}
		}
	}

	deadline := time.Now().Add(budget)
	for firstPoll := true; len(pending) > 0; firstPoll = false {
		for class, shards := range pending {
			shardNames := make([]string, 0, len(shards))
			for shard := range shards {
				shardNames = append(shardNames, shard)
			}
			sort.Strings(shardNames)

			statuses, err := c.checkpointer.GetAsyncCheckpointNodeStatuses(ctx, class, shardNames)
			if err != nil {
				monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("status_failed").Inc()
				c.log.WithField("action", OpCreate).WithField("class", class).
					Warnf("replica dedupe: class falls back to all-replica backup: checkpoint status: %v", err)
				delete(pending, class)
				continue
			}
			for _, shard := range shardNames {
				entries := statuses[shard]
				if convergedReplicaSet(entries, replicas[class][shard], cutoffMs) {
					if converged[class] == nil {
						converged[class] = make(map[string][]string)
					}
					converged[class][shard] = replicas[class][shard]
					delete(shards, shard)
					continue
				}
				// A create that failed everywhere leaves no entry at this cutoff; it can never converge.
				if firstPoll && !anyEntryAtCutoff(entries, cutoffMs) {
					c.log.WithField("action", OpCreate).WithField("class", class).WithField("shard", shard).
						Debug("replica dedupe: shard falls back, no checkpoint created at cutoff")
					delete(shards, shard)
				}
			}
			if len(shards) == 0 {
				delete(pending, class)
			}
		}
		if len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		if !sleepUntil(ctx, time.Now().Add(c.dedupePollInterval)) {
			break
		}
	}
	for class, shards := range pending {
		if len(shards) > 0 {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("not_converged").Add(float64(len(shards)))
			c.log.WithField("action", OpCreate).WithField("class", class).
				WithField("unconverged", len(shards)).
				Info("replica dedupe: unconverged shards fall back to all-replica backup")
		}
	}
	return converged
}

// convergedReplicaSet reports whether entries prove that every replica of the
// shard holds identical data as of the checkpoint cutoff. Absent entries mean
// a down node, an unloaded shard, or a dropped checkpoint - never agreement.
func convergedReplicaSet(entries []replica.AsyncCheckpointNodeStatus, replicas []string, wantCutoffMs int64) bool {
	replicaSet := uniqueNonEmpty(replicas)
	if len(replicaSet) < 2 {
		return false
	}
	byNode := make(map[string]replica.AsyncCheckpointNodeStatus, len(entries))
	for _, e := range entries {
		if _, ok := replicaSet[e.Node]; !ok {
			return false
		}
		if prev, ok := byNode[e.Node]; ok && prev != e {
			return false
		}
		if e.CutoffMs != wantCutoffMs {
			return false
		}
		byNode[e.Node] = e
	}
	if len(byNode) != len(replicaSet) {
		return false
	}
	var first replica.AsyncCheckpointNodeStatus
	seen := false
	for _, e := range byNode {
		if !seen {
			first = e
			seen = true
			continue
		}
		if e.Root != first.Root || !e.CreatedAt.Equal(first.CreatedAt) {
			return false
		}
	}
	return first.Root != (hashtree.Digest{})
}

func anyEntryAtCutoff(entries []replica.AsyncCheckpointNodeStatus, cutoffMs int64) bool {
	for _, e := range entries {
		if e.CutoffMs == cutoffMs {
			return true
		}
	}
	return false
}

// assignDesignations picks one archiving node per shard: least-loaded replica,
// sorted shard order, lexicographic tie-break. loads is shared across classes
// so multi-class backups balance cluster-wide.
func assignDesignations(shardReplicas map[string][]string, loads map[string]int) map[string]string {
	shards := make([]string, 0, len(shardReplicas))
	for shard := range shardReplicas {
		shards = append(shards, shard)
	}
	sort.Strings(shards)

	out := make(map[string]string, len(shards))
	for _, shard := range shards {
		nodes := make([]string, 0, len(shardReplicas[shard]))
		for node := range uniqueNonEmpty(shardReplicas[shard]) {
			nodes = append(nodes, node)
		}
		sort.Strings(nodes)
		best := nodes[0]
		for _, node := range nodes[1:] {
			if loads[node] < loads[best] {
				best = node
			}
		}
		loads[best]++
		out[shard] = best
	}
	return out
}

// projectDesignations returns the designation entries relevant to one node:
// shards it replicates. Nil when nothing applies.
func projectDesignations(plan *dedupePlan, nodeName string) map[string]map[string]string {
	if plan == nil {
		return nil
	}
	var out map[string]map[string]string
	for class, shards := range plan.designations {
		for shard, designated := range shards {
			replicated := false
			for _, node := range plan.replicas[class][shard] {
				if node == nodeName {
					replicated = true
					break
				}
			}
			if !replicated {
				continue
			}
			if out == nil {
				out = make(map[string]map[string]string)
			}
			if out[class] == nil {
				out[class] = make(map[string]string)
			}
			out[class][shard] = designated
		}
	}
	return out
}

func uniqueNonEmpty(nodes []string) map[string]struct{} {
	set := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	return set
}

// sleepUntil blocks until t or ctx cancellation; false on cancellation.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
