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

package vectormanifest

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// owner is where a vector's files live: its index at physicalID,
// its queue at queueID.
type owner struct {
	physicalID string
	queueID    string
}

// ownerOf is where rec's files live.
func ownerOf(rec record) owner {
	return owner{physicalID: rec.storageID(), queueID: rec.queueIDOrDefault()}
}

// ownerAt is a vector whose index and queue are both at id, as the naming
// rule creates them.
func ownerAt(id string) owner {
	return owner{physicalID: id, queueID: id}
}

// names lists the physical names o owns.
func (o owner) names() []string {
	return append(helpers.VectorIndexArtifactNamesForID(o.physicalID).All(), helpers.QueueDirName(o.queueID))
}

// ownersOf maps each recorded vector to where its files live.
// Skipped vectors are never recorded, so every owner has storage.
func ownersOf(records map[string]record) map[string]owner {
	owners := make(map[string]owner, len(records))
	for name, rec := range records {
		owners[name] = ownerOf(rec)
	}
	return owners
}

// collisionsWith lists the owners other than name that share a
// physical name with mine, as "vectors A and B share N". The legacy vector
// is the empty name.
func collisionsWith(owners map[string]owner, name string, mine owner) []string {
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

// collisions lists every pair of owners that share a physical
// name, each pair once, names in order.
func collisions(owners map[string]owner) []string {
	names := slices.Sorted(maps.Keys(owners))
	var collisions []string
	for i, name := range names {
		before := make(map[string]owner, i)
		for _, earlier := range names[:i] {
			before[earlier] = owners[earlier]
		}
		collisions = append(collisions, collisionsWith(before, name, owners[name])...)
	}
	return collisions
}

// collisionError is the refusal every caller of the check returns.
func collisionError(name string, collisions []string) error {
	return fmt.Errorf("vector %q cannot be created: %s", name, strings.Join(collisions, "; "))
}

// refuseSharedFiles fails when a drop of one vector would delete what another
// one uses.
func refuseSharedFiles(records map[string]record) error {
	err := refuseSharedQueues(records)
	if err != nil {
		return err
	}
	return refuseGeneratedCollisions(records)
}

// RefuseCollidingNames fails when a vector of added would own a file that one
// of existing, or an earlier one of added, owns. Names are enough: a vector
// rebuilt under a generated ID owns only names with a "-", which no vector
// name has.
func RefuseCollidingNames(existing, added []string) error {
	owners := make(map[string]owner, len(existing)+len(added))
	for _, name := range existing {
		owners[name] = ownerAt(helpers.VectorIndexIDForTarget(name))
	}
	for _, name := range added {
		own := ownerAt(helpers.VectorIndexIDForTarget(name))
		if shared := collisionsWith(owners, name, own); len(shared) > 0 {
			return collisionError(name, shared)
		}
		owners[name] = own
	}
	return nil
}

// refuseSharedQueues fails when two records name one queue: two
// consumers would each take the other's work, and a drop of either would
// delete the other's.
func refuseSharedQueues(records map[string]record) error {
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

// refuseGeneratedCollisions fails when two records name one file
// and either has a generated ID: both would write into it, and a drop of one
// deletes the other's. Name-derived collisions predate it and still load.
func refuseGeneratedCollisions(records map[string]record) error {
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
			if isGeneratedID(rec.storageID()) || isGeneratedID(rec.queueIDOrDefault()) ||
				isGeneratedID(records[other].storageID()) || isGeneratedID(records[other].queueIDOrDefault()) {
				return fmt.Errorf("vectors %q and %q both name %q", other, name, file)
			}
		}
	}
	return nil
}

// isGeneratedID reports whether id was generated rather than
// derived from a vector name, which never contains a "-".
func isGeneratedID(id string) bool {
	return strings.Contains(id, "-")
}
