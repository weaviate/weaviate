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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// DedupePlanner plans a deduplicated backup; nil on coordinators and nodes that never dedupe.
type DedupePlanner interface {
	// PlanDesignatedShards designates one archiving node per convergence-proven shard, naming only members of participants.
	// cancelled reports that the backup is ending; once it is true, planning returns early and records no fallback. A nil cancelled is never cancelled.
	PlanDesignatedShards(ctx context.Context, classes []string, budget time.Duration,
		participants map[string]struct{}, preferred map[string]map[string]string, cancelled func() bool) *DedupePlan
}

// _DedupePollInterval paces readNodeMeta's retries.
const _DedupePollInterval = 3 * time.Second

// DedupePlan is the outcome of convergence planning for one backup.
type DedupePlan struct {
	Designations    map[string]map[string]string // class -> shard -> archiving node
	Replicas        map[string]map[string][]string
	Cutoffs         map[string]int64 // class -> checkpoint cutoff (epoch ms) proven by convergence
	CandidateShards int              // dedupe-eligible shards found at discovery, before any drop
}

// Designated counts shards assigned to a single archiving node.
func (p *DedupePlan) Designated() int {
	n := 0
	for _, shards := range p.Designations {
		n += len(shards)
	}
	return n
}

// Fallback counts candidate shards that ended up archived by all replicas.
func (p *DedupePlan) Fallback() int {
	return p.CandidateShards - p.Designated()
}

// stamp writes the planning outcome onto desc and returns whether the artifact is deduped.
// A nil plan stamps an artifact with no plan fields.
// An artifact with zero deduped shards is stored in the legacy layout, so pre-3.0 releases can restore it.
// The exception: if its base chain includes a deduped artifact, it gets the dedupe version.
func (p *DedupePlan) stamp(desc *backup.DistributedBackupDescriptor, baseChainDeduped bool) bool {
	dedupeEffective := p != nil && p.Designated() > 0
	desc.Version = Version
	if dedupeEffective || baseChainDeduped {
		desc.Version = VersionDedupeReplicas
	}
	desc.DedupeReplicas = dedupeEffective
	if p == nil {
		return false
	}
	desc.DedupeDesignatedShards = p.Designated()
	desc.DedupeFallbackShards = p.Fallback()
	// Maps are copied, not shared. Non-Success artifacts carry them too; that is harmless because chain validation refuses those artifacts.
	for class, shards := range p.Designations {
		if len(shards) == 0 {
			continue
		}
		if desc.DedupeCutoffsMs == nil {
			desc.DedupeCutoffsMs = make(map[string]int64, len(p.Designations))
		}
		desc.DedupeCutoffsMs[class] = p.Cutoffs[class]
		if desc.DedupeDesignations == nil {
			desc.DedupeDesignations = make(map[string]map[string]string, len(p.Designations))
		}
		desc.DedupeDesignations[class] = maps.Clone(shards)
	}
	return dedupeEffective
}

// projectDesignations returns the entries for shards the node replicates; nil when none apply.
func projectDesignations(plan *DedupePlan, nodeName string) map[string]map[string]string {
	if plan == nil {
		return nil
	}
	var out map[string]map[string]string
	for class, shards := range plan.Designations {
		for shard, designated := range shards {
			replicated := false
			for _, node := range plan.Replicas[class][shard] {
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

// verifyDesignatedCoverage confirms every designated shard is present in its designated node's uploaded descriptor.
// A miss means the shard is in nobody's archive (replica set changed mid-backup) and the backup must fail rather than report Success over silent loss.
func (c *coordinator) verifyDesignatedCoverage(ctx context.Context, req *StatusRequest, plan *DedupePlan, nodeMetas map[string]*backup.BackupDescriptor) error {
	byNode := make(map[string]map[string][]string)
	for class, shards := range plan.Designations {
		for shard, node := range shards {
			if byNode[node] == nil {
				byNode[node] = make(map[string][]string)
			}
			byNode[node][class] = append(byNode[node][class], shard)
		}
	}
	nodes := make([]string, 0, len(byNode))
	for node := range byNode {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		// commit already fetched most per-node descriptors; re-read only the ones it could not.
		meta := nodeMetas[node]
		if meta == nil {
			var err error
			meta, err = c.readNodeMeta(ctx, req, node)
			if err != nil {
				return fmt.Errorf("verify designated shards of node %q: %w", node, err)
			}
			nodeMetas[node] = meta
		}
		classNames := make([]string, 0, len(byNode[node]))
		for class := range byNode[node] {
			classNames = append(classNames, class)
		}
		sort.Strings(classNames)
		for _, class := range classNames {
			cd := meta.GetClassDescriptor(class)
			for _, shard := range byNode[node][class] {
				if cd == nil || cd.GetShardDescriptor(shard) == nil {
					return fmt.Errorf("designated shard %q of class %q missing from node %q archive; replica set likely changed during the backup, retry it", shard, class, node)
				}
			}
		}
	}
	return nil
}

// readNodeMeta reads one node's per-node descriptor, retrying backend errors on the poll cadence; a missing descriptor is deterministic and fails immediately.
func (c *coordinator) readNodeMeta(ctx context.Context, req *StatusRequest, node string) (*backup.BackupDescriptor, error) {
	backend, err := c.backends.BackupBackend(req.Backend, modulecapabilities.BackendUseCaseBackup)
	if err != nil {
		return nil, err
	}
	store := nodeStore{objectStore{
		backend:  backend,
		backupId: fmt.Sprintf("%s/%s", req.ID, node),
		bucket:   req.Bucket,
		path:     req.Path,
		node:     node,
	}}
	for attempt := 0; ; attempt++ {
		meta, err := store.Meta(ctx, req.ID, req.Bucket, req.Path)
		if err == nil {
			return meta, nil
		}
		var notFound backup.ErrNotFound
		if errors.As(err, &notFound) {
			return nil, err
		}
		if attempt >= 2 || !sleepUntil(ctx, time.Now().Add(c.dedupePollInterval)) {
			return nil, err
		}
	}
}

// attributeDedupedShardSizes lifts the global descriptor to the logical size: each designated shard's bytes are added to every replica whose descriptor lacks it; second call is a no-op.
func attributeDedupedShardSizes(log logrus.FieldLogger, desc *backup.DistributedBackupDescriptor, nodeMetas map[string]*backup.BackupDescriptor) int64 {
	if desc.DedupeSkippedBytes != 0 {
		return 0
	}
	var skipped int64
	classes := make([]string, 0, len(desc.DedupeDesignations))
	for class := range desc.DedupeDesignations {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		shards := desc.DedupeDesignations[class]
		state, err := classShardingState(desc, nodeMetas, class, shards)
		if err != nil {
			log.WithField("class", class).Warnf("dedupe size attribution skips class: %v", err)
			continue
		}
		shardNames := make([]string, 0, len(shards))
		for shard := range shards {
			shardNames = append(shardNames, shard)
		}
		sort.Strings(shardNames)
		for _, shard := range shardNames {
			archiver := shards[shard]
			am := nodeMetas[archiver]
			if am == nil {
				continue
			}
			acd := am.GetClassDescriptor(class)
			if acd == nil {
				continue
			}
			sd := acd.GetShardDescriptor(shard)
			// pre-field archiver: keep physical accounting
			if sd == nil || sd.PreCompressionSizeBytes == 0 {
				continue
			}
			for replica := range uniqueNonEmpty(state.Physical[shard].BelongsToNodes) {
				if replica == archiver {
					continue
				}
				nd := desc.Nodes[desc.ToOriginalNodeName(replica)]
				if nd == nil {
					continue
				}
				// replica archived it anyway (mid-backup churn): bytes already counted
				if rm := nodeMetas[replica]; rm != nil {
					if rcd := rm.GetClassDescriptor(class); rcd != nil && rcd.GetShardDescriptor(shard) != nil {
						continue
					}
				}
				nd.PreCompressionSizeBytes += sd.PreCompressionSizeBytes
				desc.PreCompressionSizeBytes += sd.PreCompressionSizeBytes
				skipped += sd.PreCompressionSizeBytes
			}
		}
	}
	desc.DedupeSkippedBytes = skipped
	return skipped
}

// classShardingState resolves the archived sharding state: leader, then archivers, then any holder.
func classShardingState(desc *backup.DistributedBackupDescriptor, nodeMetas map[string]*backup.BackupDescriptor, class string, shards map[string]string) (*shardingStateSubset, error) {
	archivers := make([]string, 0, len(shards))
	for _, archiver := range shards {
		archivers = append(archivers, archiver)
	}
	sort.Strings(archivers)
	holders := make([]string, 0, len(nodeMetas))
	for node := range nodeMetas {
		holders = append(holders, node)
	}
	sort.Strings(holders)
	candidates := make([]string, 0, 1+len(archivers)+len(holders))
	candidates = append(candidates, desc.Leader)
	candidates = append(candidates, archivers...)
	candidates = append(candidates, holders...)

	var firstErr error
	tried := make(map[string]struct{}, len(candidates))
	for _, node := range candidates {
		if node == "" {
			continue
		}
		if _, ok := tried[node]; ok {
			continue
		}
		tried[node] = struct{}{}
		meta := nodeMetas[node]
		if meta == nil {
			continue
		}
		cd := meta.GetClassDescriptor(class)
		if cd == nil || len(cd.ShardingState) == 0 {
			continue
		}
		var state shardingStateSubset
		if err := json.Unmarshal(cd.ShardingState, &state); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("unmarshal archived sharding state from node %q: %w", node, err)
			}
			continue
		}
		return &state, nil
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, errors.New("no node descriptor carries the archived sharding state")
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
