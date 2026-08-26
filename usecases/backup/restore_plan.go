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
	// desc is a filtered copy of the source's class descriptor: only the shards
	// this participant restores from that source, and their chunks.
	desc *backup.ClassDescriptor
}

type classPlan struct {
	name    string
	sources []classSource
}

// restorePlan is a participant's multi-source staging plan for a fan-out
// restore of a replica-deduped backup.
type restorePlan struct {
	classes         []classPlan
	compressionType backup.CompressionType
}

// expandParticipantsForDedupe enrolls every replica node of the archived
// sharding state as a restore participant, so the single archived copy of each
// deduped shard is fanned out to all its replicas. It mutates only the restore
// descriptor, never the artifact, and records the pre-expansion node set on
// req.SourceNodes.
func (c *coordinator) expandParticipantsForDedupe(req *Request, schema []backup.ClassDescriptor) error {
	sources := make([]string, 0, len(c.descriptor.Nodes))
	for node := range c.descriptor.Nodes {
		sources = append(sources, node)
	}
	sort.Strings(sources)
	req.SourceNodes = sources
	req.DedupeReplicas = true

	// A many-to-one mapping would collide participants and make the reverse
	// name lookup ambiguous; shrink-topology restore is not supported.
	byNewName := make(map[string]string, len(c.descriptor.NodeMapping))
	for oldName, newName := range c.descriptor.NodeMapping {
		if prev, ok := byNewName[newName]; ok {
			return fmt.Errorf("restore of a replica-deduped backup requires an injective node_mapping: %q and %q both map to %q", prev, oldName, newName)
		}
		byNewName[newName] = oldName
	}

	for i := range schema {
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

// shardingStateSubset decodes just what participant planning needs from the
// archived sharding state.
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
}

// buildFanoutPlan reads every source node's descriptor and derives which shards
// this participant restores from which source prefix. originalNode is the
// participant's backup-time name.
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
			metas[i] = sourceMeta{node: src, store: store, meta: meta}
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
			src, err := resolveShardSource(metas, own, class, shard)
			if err != nil {
				return nil, err
			}
			if src == "" {
				// No prefix holds it (empty at backup time), or several do
				// (non-converged, archived by each replica that had data) and
				// this node's own copy was empty: faithfully restore nothing.
				r.logger.WithField("action", "restore").WithField("class", class).
					WithField("shard", shard).Debug("replica-deduped restore: no source copy for this node, skipping shard")
				continue
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

// resolveShardSource picks the prefix a shard is restored from: the node's own
// archive when present, else the single foreign holder of a deduped shard.
// Empty result means nothing to restore for this node (see caller).
func resolveShardSource(metas []sourceMeta, own *sourceMeta, class, shard string) (string, error) {
	if own != nil {
		if d := own.meta.GetClassDescriptor(class); d != nil {
			if sd := d.GetShardDescriptor(shard); sd != nil {
				if sd.Node != own.node {
					return "", fmt.Errorf("inconsistent backup: shard %q of class %q under prefix %q claims node %q", shard, class, own.node, sd.Node)
				}
				return own.node, nil
			}
		}
	}
	var holders []string
	for i := range metas {
		if own != nil && metas[i].node == own.node {
			continue
		}
		d := metas[i].meta.GetClassDescriptor(class)
		if d == nil {
			continue
		}
		if sd := d.GetShardDescriptor(shard); sd != nil {
			if sd.Node != metas[i].node {
				return "", fmt.Errorf("inconsistent backup: shard %q of class %q under prefix %q claims node %q", shard, class, metas[i].node, sd.Node)
			}
			holders = append(holders, metas[i].node)
		}
	}
	if len(holders) == 1 {
		return holders[0], nil
	}
	return "", nil
}

// filterClassDescriptor copies desc keeping only the given shards and the
// chunks that belong to them. Chunk ids are per-source; never merge the
// resulting descriptors across sources.
func filterClassDescriptor(desc *backup.ClassDescriptor, shards []string) *backup.ClassDescriptor {
	out := *desc
	out.Shards = make([]*backup.ShardDescriptor, 0, len(shards))
	for _, sd := range desc.Shards {
		if slices.Contains(shards, sd.Name) {
			out.Shards = append(out.Shards, sd)
		}
	}
	out.Chunks = make(map[int32][]string, len(desc.Chunks))
	for id, chunkShards := range desc.Chunks {
		if len(chunkShards) > 0 && slices.Contains(shards, chunkShards[0]) {
			out.Chunks[id] = chunkShards
		}
	}
	return &out
}

// restoreFanout stages a multi-source restore plan; the scaffolding matches
// the single-source restore path.
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
