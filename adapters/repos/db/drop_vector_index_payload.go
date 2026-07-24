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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// DropVectorIndexNamespace is the distributed-task namespace for dropping a
// named vector index. The DTM Manager routes tasks with this namespace to the
// DropVectorIndexProvider.
const DropVectorIndexNamespace = "drop-vector-index"

// DropVectorIndexTaskPayload is the RAFT-replicated payload of a drop-vector
// task: the collection, the dropped named vectors (several at once is supported),
// the edit-ops bookkeeping key (OpID), and the unit→node/unit→shard assignment.
type DropVectorIndexTaskPayload struct {
	Collection string   `json:"collection"`
	Targets    []string `json:"targets"`
	OpID       string   `json:"opId"`

	// UnitToNode maps a unit ID to the node that owns it; UnitToShard maps the
	// same unit ID to the shard it covers. One unit per (shard, node).
	UnitToNode  map[string]string `json:"unitToNode"`
	UnitToShard map[string]string `json:"unitToShard"`

	// DropEpochID scopes CleanedShards to one drop of the name: a re-created
	// then re-dropped vector must not inherit the previous drop's coverage.
	// Empty on payloads from older nodes (treated as chain-less).
	DropEpochID string `json:"dropEpochId,omitempty"`
	// CleanedShards are shards cleaned by the epoch's earlier tasks; the
	// task's own UnitToShard is not included (readers use CoveredShards).
	//
	// Single-task coverage invariant: the enqueuer writes the FULL union of the
	// epoch's completed earlier tasks into every new task (RAFT serializes
	// same-target tasks), so one completed task's CoveredShards is the epoch's
	// total coverage as of its enqueue. Finalize and the removal gate rely on
	// this and read a single task — they never union across records.
	CleanedShards []string `json:"cleanedShards,omitempty"`
}

// SameTargetSet reports whether two target lists contain the same names with
// the same multiplicities (exact case — target vector names are case-sensitive
// identifiers). Shared by the enqueuer's coverage inheritance and the
// conflict-time inheritance guard, which must agree on what "same drop" means.
func SameTargetSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, t := range a {
		counts[t]++
	}
	for _, t := range b {
		counts[t]--
		if counts[t] < 0 {
			return false
		}
	}
	return true
}

// ShardsNotCovered returns the shards absent from covered, sorted.
func ShardsNotCovered(shards []string, covered map[string]struct{}) []string {
	var missing []string
	for _, shard := range shards {
		if _, ok := covered[shard]; !ok {
			missing = append(missing, shard)
		}
	}
	sort.Strings(missing)
	return missing
}

// CoveredShards returns the shards this task accounts for: its own units plus
// the inherited cleaned set. The single reader-side union (see the
// CleanedShards invariant above).
func (p *DropVectorIndexTaskPayload) CoveredShards() map[string]struct{} {
	covered := make(map[string]struct{}, len(p.UnitToShard)+len(p.CleanedShards))
	for _, shard := range p.UnitToShard {
		covered[shard] = struct{}{}
	}
	for _, shard := range p.CleanedShards {
		covered[shard] = struct{}{}
	}
	return covered
}

func (p *DropVectorIndexTaskPayload) encode() ([]byte, error) {
	return json.Marshal(p)
}

// DecodeDropVectorIndexTaskPayload decodes and validates a drop-vector task
// payload; the single decode path for out-of-package callers (REST enqueuer).
func DecodeDropVectorIndexTaskPayload(data []byte) (*DropVectorIndexTaskPayload, error) {
	return decodeDropVectorIndexPayload(data)
}

func decodeDropVectorIndexPayload(data []byte) (*DropVectorIndexTaskPayload, error) {
	var p DropVectorIndexTaskPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal drop-vector-index payload: %w", err)
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("drop-vector-index payload missing collection")
	}
	if len(p.Targets) == 0 {
		return nil, fmt.Errorf("drop-vector-index payload missing targets")
	}
	for _, t := range p.Targets {
		// Targets are filepath.Joined and os.RemoveAll'd by removeVectorIndexFiles;
		// reject empty / separators / ".." so a target can't escape the shard dir.
		if t == "" || strings.ContainsAny(t, `/\`) || strings.Contains(t, "..") {
			return nil, fmt.Errorf("drop-vector-index payload has an invalid target name %q", t)
		}
	}
	if p.OpID == "" {
		return nil, fmt.Errorf("drop-vector-index payload missing opId")
	}
	return &p, nil
}

// ExtractDropVectorIndexTaskTargets is the target extractor registered with
// the DTM Manager so a NEW drop's marker introduction can purge the previous
// drop's task records for the same (collection, target) — stale records must
// not exist while a marker they don't belong to stands, or coverage
// inheritance could adopt them. ok is false on an unparseable payload.
func ExtractDropVectorIndexTaskTargets(payload []byte) (collection string, targets []string, ok bool) {
	p, err := decodeDropVectorIndexPayload(payload)
	if err != nil {
		return "", nil, false
	}
	return p.Collection, p.Targets, true
}

// ExtractDropVectorIndexTaskCollection is the collection extractor registered
// with the DTM Manager so the DeleteClass cascade can drop this namespace's task
// records (mirrors ExtractReindexTaskCollection). ok is false on an unparseable
// payload.
func ExtractDropVectorIndexTaskCollection(payload []byte) (collection string, ok bool) {
	p, err := decodeDropVectorIndexPayload(payload)
	if err != nil {
		return "", false
	}
	return p.Collection, true
}
