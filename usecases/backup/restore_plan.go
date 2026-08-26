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
	"path"
	"slices"
	"sort"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// classSource is one node prefix contributing shards to a class restore.
type classSource struct {
	node  string
	store nodeStore
	// desc is filtered to the shards this participant restores from that source.
	desc *backup.ClassDescriptor
}

type classPlan struct {
	name    string
	sources []classSource
}

// restorePlan is a participant's multi-source staging plan for a fan-out restore.
type restorePlan struct {
	classes         []classPlan
	compressionType backup.CompressionType
}

// expandParticipantsForDedupe enrolls every replica node of the archived sharding state as a restore participant; it mutates only the restore descriptor and records the pre-expansion node set on req.SourceNodes.
func (c *coordinator) expandParticipantsForDedupe(req *Request, schema []backup.ClassDescriptor) error {
	sources := make([]string, 0, len(c.descriptor.Nodes))
	for node := range c.descriptor.Nodes {
		sources = append(sources, node)
	}
	sort.Strings(sources)
	req.SourceNodes = sources
	req.DedupeReplicas = true

	// A many-to-one mapping would collide participants; shrink-topology restore is unsupported.
	oldNames := make([]string, 0, len(c.descriptor.NodeMapping))
	for oldName := range c.descriptor.NodeMapping {
		oldNames = append(oldNames, oldName)
	}
	sort.Strings(oldNames)
	byNewName := make(map[string]string, len(oldNames))
	for _, oldName := range oldNames {
		newName := c.descriptor.NodeMapping[oldName]
		if prev, ok := byNewName[newName]; ok {
			return fmt.Errorf("restore of a replica-deduped backup requires an injective node_mapping: %q and %q both map to %q", prev, oldName, newName)
		}
		byNewName[newName] = oldName
	}

	// The schema slice is the full archive; the descriptor's classes are the include/exclude/authz-filtered selection. Anything outside it must not enroll participants, pollute class lists, or gate resolvability.
	selected := make(map[string]struct{}, len(c.descriptor.Nodes))
	for _, class := range c.descriptor.Classes() {
		selected[class] = struct{}{}
	}
	for i := range schema {
		if _, ok := selected[schema[i].Name]; !ok {
			continue
		}
		var state shardingStateSubset
		if err := json.Unmarshal(schema[i].ShardingState, &state); err != nil {
			return fmt.Errorf("class %q: unmarshal archived sharding state: %w", schema[i].Name, err)
		}
		for _, phys := range state.Physical {
			for _, node := range phys.BelongsToNodes {
				if node == "" {
					continue
				}
				nd := c.descriptor.Nodes[node]
				if nd == nil {
					nd = &backup.NodeDescriptor{Status: backup.Started}
					c.descriptor.Nodes[node] = nd
				}
				if !slices.Contains(nd.Classes, schema[i].Name) {
					nd.Classes = append(nd.Classes, schema[i].Name)
				}
			}
		}
	}

	// A mapping target equal to a different unmapped participant collides just as surely as two explicit entries.
	participantNames := make([]string, 0, len(c.descriptor.Nodes))
	for node := range c.descriptor.Nodes {
		participantNames = append(participantNames, node)
	}
	sort.Strings(participantNames)
	for _, node := range participantNames {
		if _, mapped := c.descriptor.NodeMapping[node]; mapped {
			continue
		}
		if prev, ok := byNewName[node]; ok && prev != node {
			return fmt.Errorf("restore of a replica-deduped backup requires an injective node_mapping: %q maps to %q, which is itself an unmapped participant", prev, node)
		}
	}

	var unresolvable []string
	for node := range c.descriptor.Nodes {
		mapped := c.descriptor.ToMappedNodeName(node)
		if _, found := c.nodeResolver.NodeHostname(mapped); !found {
			unresolvable = append(unresolvable, fmt.Sprintf("%s (mapped to %s)", node, mapped))
		}
	}
	if len(unresolvable) > 0 {
		sort.Strings(unresolvable)
		return fmt.Errorf("replica-deduped restore needs every replica node of the archived sharding state reachable; cannot resolve %v: map them to existing nodes via node_mapping", unresolvable)
	}
	return nil
}

// shardingStateSubset decodes just what planning needs from the archived sharding state.
type shardingStateSubset struct {
	Physical map[string]struct {
		BelongsToNodes []string `json:"belongsToNodes"`
	} `json:"physical"`
}

// sourceMeta is one source node's validated descriptor and prefix-pinned store.
type sourceMeta struct {
	node  string
	store nodeStore
	meta  *backup.BackupDescriptor
	// shards indexes meta's shard descriptors (class -> shard) so plan building stays linear on large tenant counts.
	shards map[string]map[string]*backup.ShardDescriptor
}

func indexShardDescriptors(meta *backup.BackupDescriptor) map[string]map[string]*backup.ShardDescriptor {
	out := make(map[string]map[string]*backup.ShardDescriptor, len(meta.Classes))
	for i := range meta.Classes {
		cd := &meta.Classes[i]
		shards := make(map[string]*backup.ShardDescriptor, len(cd.Shards))
		for _, sd := range cd.Shards {
			shards[sd.Name] = sd
		}
		out[cd.Name] = shards
	}
	return out
}

// buildFanoutPlan derives which shards this participant (originalNode, its backup-time name) restores from which source prefix.
func (r *restorer) buildFanoutPlan(ctx context.Context, originalNode string, req *Request) (*restorePlan, error) {
	if len(req.SourceNodes) == 0 {
		return nil, fmt.Errorf("replica-deduped restore request without source nodes")
	}

	metas := make([]sourceMeta, len(req.SourceNodes))
	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(r.logger, ctx)
	eg.SetLimit(_MaxNumberConns)
	for i, src := range req.SourceNodes {
		eg.Go(func() error {
			store, err := nodeBackend(src, r.backends, req.Backend, req.ID, req.Bucket, req.Path)
			if err != nil {
				return fmt.Errorf("source %q: backend: %w", src, err)
			}
			meta, err := store.Meta(egCtx, req.ID, req.Bucket, req.Path)
			if err != nil {
				return fmt.Errorf("source %q: read descriptor: %w", src, err)
			}
			if err := validateNodeMeta(meta, store.HomeDir(req.Bucket, req.Path), req.ID); err != nil {
				return fmt.Errorf("source %q: %w", src, err)
			}
			if !meta.DedupeReplicas {
				return fmt.Errorf("source %q: descriptor is not marked replica-deduped", src)
			}
			if len(req.Classes) > 0 {
				meta.Include(req.Classes)
			}
			metas[i] = sourceMeta{node: src, store: store, meta: meta, shards: indexShardDescriptors(meta)}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var own *sourceMeta
	for i := range metas {
		if metas[i].node == originalNode {
			own = &metas[i]
			break
		}
	}

	plan := &restorePlan{compressionType: metas[0].meta.GetCompressionType()}
	for _, class := range req.Classes {
		var schemaDesc *backup.ClassDescriptor
		if own != nil {
			schemaDesc = own.meta.GetClassDescriptor(class)
		}
		if schemaDesc == nil {
			for i := range metas {
				if d := metas[i].meta.GetClassDescriptor(class); d != nil {
					schemaDesc = d
					break
				}
			}
		}
		if schemaDesc == nil {
			return nil, fmt.Errorf("class %q not found in any source descriptor", class)
		}

		var state shardingStateSubset
		if err := json.Unmarshal(schemaDesc.ShardingState, &state); err != nil {
			return nil, fmt.Errorf("class %q: unmarshal archived sharding state: %w", class, err)
		}
		var myShards []string
		for shard, phys := range state.Physical {
			if slices.Contains(phys.BelongsToNodes, originalNode) {
				myShards = append(myShards, shard)
			}
		}
		sort.Strings(myShards)

		cp := classPlan{name: class}
		perSource := make(map[string][]string)
		for _, shard := range myShards {
			src, ambiguous, err := resolveShardSource(metas, own, class, shard)
			if err != nil {
				return nil, err
			}
			if src == "" {
				monitoring.GetMetrics().BackupDedupeRestoreAnomalies.WithLabelValues("no_holder").Inc()
				r.logger.WithField("action", "restore").WithField("class", class).
					WithField("shard", shard).Info("replica-deduped restore: no source copy for this node, restoring nothing for this shard")
				continue
			}
			if ambiguous {
				monitoring.GetMetrics().BackupDedupeRestoreAnomalies.WithLabelValues("multi_holder").Inc()
				r.logger.WithField("action", "restore").WithField("class", class).WithField("shard", shard).
					Warnf("replica-deduped restore: several foreign archives hold this shard, restoring deterministically from %q", src)
			}
			perSource[src] = append(perSource[src], shard)
		}

		srcNames := make([]string, 0, len(perSource))
		for src := range perSource {
			srcNames = append(srcNames, src)
		}
		sort.Strings(srcNames)
		for _, src := range srcNames {
			for i := range metas {
				if metas[i].node != src {
					continue
				}
				full := metas[i].meta.GetClassDescriptor(class)
				cp.sources = append(cp.sources, classSource{
					node:  src,
					store: metas[i].store,
					desc:  filterClassDescriptor(full, perSource[src]),
				})
			}
		}
		plan.classes = append(plan.classes, cp)
	}
	return plan, nil
}

// resolveShardSource picks the own archive when present, else a foreign holder; empty means nothing to restore.
// Several foreign holders are duplication drift, every one a legitimate replica state: the smallest name wins deterministically and ambiguous reports it.
func resolveShardSource(metas []sourceMeta, own *sourceMeta, class, shard string) (src string, ambiguous bool, err error) {
	if own != nil {
		if sd := own.shards[class][shard]; sd != nil {
			if sd.Node != own.node {
				return "", false, fmt.Errorf("inconsistent backup: shard %q of class %q under prefix %q claims node %q", shard, class, own.node, sd.Node)
			}
			return own.node, false, nil
		}
	}
	var holders []string
	for i := range metas {
		if own != nil && metas[i].node == own.node {
			continue
		}
		sd := metas[i].shards[class][shard]
		if sd == nil {
			continue
		}
		if sd.Node != metas[i].node {
			return "", false, fmt.Errorf("inconsistent backup: shard %q of class %q under prefix %q claims node %q", shard, class, metas[i].node, sd.Node)
		}
		holders = append(holders, metas[i].node)
	}
	if len(holders) == 0 {
		return "", false, nil
	}
	sort.Strings(holders)
	return holders[0], len(holders) > 1, nil
}

// filterClassDescriptor keeps only the given shards and their chunks; chunk ids are per-source, never merge across sources.
func filterClassDescriptor(desc *backup.ClassDescriptor, shards []string) *backup.ClassDescriptor {
	keep := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		keep[s] = struct{}{}
	}
	out := *desc
	out.Shards = make([]*backup.ShardDescriptor, 0, len(shards))
	for _, sd := range desc.Shards {
		if _, ok := keep[sd.Name]; ok {
			out.Shards = append(out.Shards, sd)
		}
	}
	out.Chunks = make(map[int32][]string, len(desc.Chunks))
	for id, chunkShards := range desc.Chunks {
		// The uploader packs exactly one shard per chunk today; scanning every entry keeps this correct should a future packer span shards.
		for _, cs := range chunkShards {
			if _, ok := keep[cs]; ok {
				out.Chunks[id] = chunkShards
				break
			}
		}
	}
	return &out
}

// restoreFanout stages a multi-source restore plan with the single-source scaffolding.
func (r *restorer) restoreFanout(req *Request, plan *restorePlan, store nodeStore) (CanCommitResponse, error) {
	return r.startRestore(req, store, func(ctx context.Context) error {
		return r.restoreAllFanout(ctx, plan, req.CPUPercentage, req.Bucket, req.Path, !r.namespacesEnabled)
	})
}

func (r *restorer) restoreAllFanout(ctx context.Context, plan *restorePlan,
	cpuPercentage int, overrideBucket, overridePath string, stripNamespaces bool,
) error {
	r.lastOp.set(backup.Transferring)
	for _, cp := range plan.classes {
		if err := ctx.Err(); err != nil {
			r.lastOp.set(backup.Cancelled)
			return fmt.Errorf("restore cancelled: %w", err)
		}
		if err := r.restoreOneFanout(ctx, cp, plan.compressionType, cpuPercentage, overrideBucket, overridePath, stripNamespaces); err != nil {
			if errors.Is(err, context.Canceled) {
				r.lastOp.set(backup.Cancelled)
			}
			return fmt.Errorf("restore class %s: %w", cp.name, err)
		}
		r.logger.WithField("action", "restore").
			WithField("class", cp.name).Info("successfully restored")
	}
	return nil
}

func (r *restorer) restoreOneFanout(ctx context.Context, cp classPlan,
	compressionType backup.CompressionType, cpuPercentage int,
	overrideBucket, overridePath string, stripNamespaces bool,
) error {
	totalShards := 0
	for _, src := range cp.sources {
		totalShards += len(src.desc.Shards)
	}
	if totalShards == 0 {
		return nil
	}

	classLabel := cp.name
	if monitoring.GetMetrics().Group {
		classLabel = "n/a"
	}
	if metric, err := monitoring.GetMetrics().BackupRestoreDurations.GetMetricWithLabelValues(getType(cp.sources[0].store.backend), classLabel); err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}

	fw := newFileWriter(r.sourcer, cp.sources[0].store, r.logger).
		WithPoolPercentage(cpuPercentage)

	materializedName := cp.name
	if stripNamespaces {
		materializedName = namespacing.StripQualification(cp.name)
	}
	classTempDir := path.Join(fw.tempDir, materializedName)

	if err := fw.prepare(classTempDir); err != nil {
		return err
	}
	for _, src := range cp.sources {
		if err := fw.fetch(ctx, classTempDir, src.desc, src.store, overrideBucket, overridePath, compressionType); err != nil {
			return fmt.Errorf("get files from %q: %w", src.node, err)
		}
	}
	return fw.finalize(classTempDir, cp.name, materializedName)
}
