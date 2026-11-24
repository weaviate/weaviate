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

package db

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// refuseSharedVectorIndexQueues fails when two records name one queue: two
// consumers would each take the other's work, and a drop of either would
// delete the other's.
func refuseSharedVectorIndexQueues(records map[string]vectorIndexRecord) error {
	queues := make(map[string]string, len(records))
	for _, name := range slices.Sorted(maps.Keys(records)) {
		queueID := records[name].queueIDOrDefault()
		if other, ok := queues[queueID]; ok {
			return fmt.Errorf("vectors %q and %q both record queue %q", other, name, queueID)
		}
		queues[queueID] = name
	}
	return nil
}

// refuseGeneratedVectorIndexCollisions fails when two records name one file
// and either has a generated ID: both would write into it, and a drop of one
// deletes the other's. Name-derived collisions predate it and still load.
func refuseGeneratedVectorIndexCollisions(records map[string]vectorIndexRecord) error {
	fileOwners := make(map[string]string)
	for _, name := range slices.Sorted(maps.Keys(records)) {
		rec := records[name]
		for _, file := range ownerOf(rec).names() {
			other, ok := fileOwners[file]
			if !ok {
				fileOwners[file] = name
				continue
			}
			if other == name {
				continue
			}
			if isGeneratedVectorIndexID(rec.storageID()) || isGeneratedVectorIndexID(rec.queueIDOrDefault()) ||
				isGeneratedVectorIndexID(records[other].storageID()) || isGeneratedVectorIndexID(records[other].queueIDOrDefault()) {
				return fmt.Errorf("vectors %q and %q both name %q", other, name, file)
			}
		}
	}
	return nil
}

// isGeneratedVectorIndexID reports whether id was generated rather than
// derived from a vector name, which never contains a "-".
func isGeneratedVectorIndexID(id string) bool {
	return strings.Contains(id, "-")
}

// vectorIndexConfigsByStorage splits the schema's vectors, the legacy one
// under the empty name, into those that own storage and the skipped ones.
func vectorIndexConfigsByStorage(legacy schemaConfig.VectorIndexConfig,
	targets map[string]schemaConfig.VectorIndexConfig,
) (active, skipped map[string]schemaConfig.VectorIndexConfig) {
	active = make(map[string]schemaConfig.VectorIndexConfig, len(targets)+1)
	skipped = make(map[string]schemaConfig.VectorIndexConfig)
	if legacy != nil {
		targets = maps.Clone(targets)
		targets[""] = legacy
	}
	for name, cfg := range targets {
		if vectorIndexHasStorage(cfg) {
			active[name] = cfg
		} else {
			skipped[name] = cfg
		}
	}
	return active, skipped
}

// vectorIndexHasStorage is false for a skipped hnsw config: a no-op index
// owns no files, so the mapping does not record it.
func vectorIndexHasStorage(cfg schemaConfig.VectorIndexConfig) bool {
	hnswCfg, ok := cfg.(hnswent.UserConfig)
	return !ok || !hnswCfg.Skip
}

// vectorIndexRecordFor is the record of a vector created under the naming
// rule: its queue shares the index's ID, and cfg is what it is built with.
func vectorIndexRecordFor(name string, cfg schemaConfig.VectorIndexConfig, state string) vectorIndexRecord {
	id := vectorIndexID(name)
	return vectorIndexRecord{PhysicalID: id, IndexType: cfg.IndexType(), State: state, QueueID: id, Config: cfg}
}

// vectorIndexOwner is where a vector's files live: its index at physicalID,
// its queue at queueID.
type vectorIndexOwner struct {
	physicalID string
	queueID    string
}

// ownerOf is where rec's files live.
func ownerOf(rec vectorIndexRecord) vectorIndexOwner {
	return vectorIndexOwner{physicalID: rec.storageID(), queueID: rec.queueIDOrDefault()}
}

// ownerAt is a vector whose index and queue are both at id, as the naming
// rule creates them.
func ownerAt(id string) vectorIndexOwner {
	return vectorIndexOwner{physicalID: id, queueID: id}
}

// names lists the physical names o owns.
func (o vectorIndexOwner) names() []string {
	return append(helpers.VectorIndexArtifactNamesForID(o.physicalID).All(), helpers.QueueDirName(o.queueID))
}

// vectorIndexOwners maps each recorded vector to where its files live.
// Skipped vectors are never recorded, so every owner has storage.
func vectorIndexOwners(records map[string]vectorIndexRecord) map[string]vectorIndexOwner {
	owners := make(map[string]vectorIndexOwner, len(records))
	for name, rec := range records {
		owners[name] = ownerOf(rec)
	}
	return owners
}

// vectorIndexCollisionsWith lists the owners other than name that share a
// physical name with mine, as "vectors A and B share N". The legacy vector
// is the empty name.
func vectorIndexCollisionsWith(owners map[string]vectorIndexOwner, name string, mine vectorIndexOwner) []string {
	own := map[string]struct{}{}
	for _, n := range mine.names() {
		own[n] = struct{}{}
	}
	var collisions []string
	for _, other := range slices.Sorted(maps.Keys(owners)) {
		if other == name {
			continue
		}
		for _, n := range owners[other].names() {
			if _, shared := own[n]; shared {
				collisions = append(collisions, fmt.Sprintf("vectors %q and %q share %q", other, name, n))
				break
			}
		}
	}
	return collisions
}

// vectorIndexCollisions lists every pair of owners that share a physical
// name, each pair once, names in order.
func vectorIndexCollisions(owners map[string]vectorIndexOwner) []string {
	names := slices.Sorted(maps.Keys(owners))
	var collisions []string
	for i, name := range names {
		before := make(map[string]vectorIndexOwner, i)
		for _, earlier := range names[:i] {
			before[earlier] = owners[earlier]
		}
		collisions = append(collisions, vectorIndexCollisionsWith(before, name, owners[name])...)
	}
	return collisions
}

// vectorIndexCollisionError is the refusal every caller of the check returns.
func vectorIndexCollisionError(name string, collisions []string) error {
	return fmt.Errorf("vector %q cannot be created: %s", name, strings.Join(collisions, "; "))
}
