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

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/sharding"
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
	// OpID keys the per-shard edit-ops bookkeeping. OpID equals DropEpochID:
	// every round of one drop re-arms the SAME op and resumes its recorded
	// progress. Records written by older versions carry a per-round uuid
	// here instead — treat the field as opaque on decode, never assume the
	// equality.
	OpID string `json:"opId"`

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
	//
	// Size: deliberately uncapped — one name per shard, no per-replica or
	// per-node blowup, so a later round of a 100k-tenant drop carries ~2 MB
	// here (vs tens of MB the uncapped unit maps would have cost; those are
	// bounded by maxShardsPerDropRound). Capping it would break the
	// single-task invariant above and with it finalize.
	CleanedShards []string `json:"cleanedShards,omitempty"`

	// DeferredShards are shards that existed at this round's enqueue and that
	// the round did NOT cover — inactive tenants, or shards past the per-round
	// cap. It records the work the drop still OWED at that point, which is what
	// distinguishes the two ways a chain can end up spanning every current
	// shard:
	//
	//   - the round did the work, or a later round of the epoch did (nothing
	//     was owed, or what was owed got covered);
	//   - the work ceased to exist because the tenant holding it was DELETED.
	//
	// Only the second may finalize on the recorded coverage: the first is also
	// the shape of a previous drop's residue next to a re-created name's marker,
	// where the recorded coverage says nothing about the new drop's data.
	//
	// Empty on payloads from older nodes, which is indistinguishable from
	// "owed nothing" — the conservative reading, and the one that keeps the
	// closed-epoch fence.
	//
	// Size: mirrors CleanedShards (one name per shard, no per-replica blowup);
	// a drop on a 100k-tenant collection with most tenants cold carries the
	// complement of CleanedShards here.
	DeferredShards []string `json:"deferredShards,omitempty"`
}

// ResolvedByShardDeletion reports whether p's recorded coverage spans every
// shard in shards while a shard p still OWED has since disappeared — i.e. the
// drop's outstanding work did not get done, it ceased to exist along with its
// tenant. Such a chain is complete for the collection as it now stands, so the
// marker can be removed without re-stripping shards that are already clean.
//
// A payload that owed nothing returns false even with full coverage: that is
// the closed-epoch residue shape (a finalized drop's records beside a
// re-created name's marker), and it must keep re-cleaning.
//
// Deterministic over its inputs: the enqueuer and the FSM removal gate both
// call it so the enqueuer never attempts a removal the gate would refuse.
func (p *DropVectorIndexTaskPayload) ResolvedByShardDeletion(shards []string) bool {
	if len(p.DeferredShards) == 0 {
		return false
	}
	if len(ShardsNotCovered(shards, p.CoveredShards())) > 0 {
		return false
	}
	current := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		current[shard] = struct{}{}
	}
	for _, owed := range p.DeferredShards {
		if _, stillThere := current[owed]; !stillThere {
			return true
		}
	}
	return false
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

// CompletedUnitShards returns the shards for which EVERY unit of the task is
// recorded COMPLETED in the FSM. A unit completion is durable proof that ONE
// replica was drained and verified — a shard has one unit per (shard, replica)
// under RF>1, and it is covered only when all of them completed: one failed
// unit flips the task FAILED while sibling units are merely orphaned, so an
// any-unit rule would fold a half-cleaned shard into CleanedShards and no
// later round would ever revisit the dirty replica. Counting whole-shard
// completions even from FAILED rounds is still deliberate (one deactivated
// tenant fails a round; at MT scale discarding the round's finished work
// would make convergence improbable and re-pay its full re-clean I/O every
// retry).
func CompletedUnitShards(task *distributedtask.Task, payload *DropVectorIndexTaskPayload) []string {
	// Iterate the payload's unit set, not task.Units: a unit the FSM has no
	// record for counts as not completed.
	allCompleted := make(map[string]bool)
	for unitID, shard := range payload.UnitToShard {
		unit := task.Units[unitID]
		done := unit != nil && unit.Status == distributedtask.UnitStatusCompleted
		if prev, seen := allCompleted[shard]; seen {
			allCompleted[shard] = prev && done
		} else {
			allCompleted[shard] = done
		}
	}
	var shards []string
	for shard, all := range allCompleted {
		if all {
			shards = append(shards, shard)
		}
	}
	sort.Strings(shards)
	return shards
}

// FirstActiveOverlappingDrop returns the first ACTIVE task — excluding
// excludeTaskID — whose payload overlaps collection (case-insensitive) and
// any of targets (exact case), along with its decoded payload and the
// overlapping names. The shared predicate behind the AddTask conflict check
// and the finalize-time replay guard; corrupt payloads are skipped fail-open
// with a warning (erroring would block on one bad record; deterministic:
// every node sees the same records).
func FirstActiveOverlappingDrop(tasks []*distributedtask.Task, excludeTaskID, collection string, targets []string,
	logger logrus.FieldLogger,
) (*distributedtask.Task, *DropVectorIndexTaskPayload, []string) {
	for _, task := range tasks {
		if task.ID == excludeTaskID || !task.Status.IsActive() {
			continue
		}
		p, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			if logger != nil {
				logger.WithField("task", task.ID).
					Warnf("drop-vector: skipping active task with unparseable payload in overlap check: %v", err)
			}
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		if overlap := intersectTargets(p.Targets, targets); len(overlap) > 0 {
			return task, p, overlap
		}
	}
	return nil, nil, nil
}

// ActiveDropCovers reports whether an ACTIVE drop task in tasks covers
// targetVector (exact case) on collection (case-insensitive). The shared
// predicate for "is this drop still running" across the REST enqueuer and the
// reconcile loop; unparseable payloads are skipped fail-open with a warning.
func ActiveDropCovers(tasks []*distributedtask.Task, collection, targetVector string, logger logrus.FieldLogger) bool {
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}
		p, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			if logger != nil {
				logger.WithField("task", task.ID).
					Warnf("drop-vector has-active-drop: skipping active task with unparseable payload: %v", err)
			}
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		for _, t := range p.Targets {
			if t == targetVector {
				return true
			}
		}
	}
	return false
}

// EpochCoveredShards unions the shards proven cleaned for one drop epoch over
// the given records: a completed (SWAPPING/FINISHED) matching task vouches its
// full CoveredShards; a FAILED or CANCELLED one vouches only its COMPLETED
// units. THE single
// implementation of the inheritance rule — the enqueuer composes claims with it
// and the AddTask-apply guard re-proves them with it (see
// TestEnqueuerGuardConsistency). Unparseable records are skipped (fail-open,
// deterministic).
func EpochCoveredShards(tasks []*distributedtask.Task, collection string, targets []string, epoch string) map[string]struct{} {
	covered := map[string]struct{}{}
	for _, task := range tasks {
		if !task.Status.IsCompleted() && !terminalWithPartialWork(task.Status) {
			continue
		}
		p, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			continue
		}
		if !SameDrop(p, collection, targets) || p.DropEpochID != epoch {
			continue
		}
		if terminalWithPartialWork(task.Status) {
			for _, shard := range CompletedUnitShards(task, p) {
				covered[shard] = struct{}{}
			}
			continue
		}
		for shard := range p.CoveredShards() {
			covered[shard] = struct{}{}
		}
	}
	return covered
}

// SameDrop reports whether a record's payload belongs to the drop identified
// by (collection, targets): the collection matches case-insensitively, the
// targets as an exact set (case-sensitive identifiers). THE single matching
// rule shared by epoch inheritance, the coverage union, and the retainer's
// newest-record anchor — if these ever used different rules, a record one of
// them counts could be invisible to another, silently breaking resume or
// coverage.
func SameDrop(p *DropVectorIndexTaskPayload, collection string, targets []string) bool {
	return strings.EqualFold(p.Collection, collection) && SameTargetSet(p.Targets, targets)
}

// terminalWithPartialWork matches the two terminal states a round can reach
// with part of its shards durably drained: FAILED (one bad unit fails the
// round) and CANCELLED (operator CancelTask mid-round). Both credit their
// COMPLETED units — discarding an operator-cancelled round's finished work
// would re-pay its full re-clean I/O, exactly like the FAILED case.
func terminalWithPartialWork(s distributedtask.TaskStatus) bool {
	return s == distributedtask.TaskStatusFailed || s == distributedtask.TaskStatusCancelled
}

// EpochAndInheritedCoverage resolves the drop epoch and the cleaned-shard
// set accumulated by completed tasks of that epoch, for a new round on
// (collection, targets).
//
// A marker can only coexist with an INCOMPLETE chain of its own drop: the
// previous drop's marker can vanish only via finalize (which requires a
// complete chain) or class delete (which cascade-deletes the task records).
// So a complete chain — or no usable chain — next to a marker is a closed
// epoch's residue (re-created then re-dropped name, or a finalize that never
// landed): mint a fresh epoch and re-clean everything, never trust it. That
// costs one idempotent full re-clean; the alternative trusts stale coverage
// and finalizes over unstripped vectors.
//
// The inference is sound only because stale records cannot coexist with a
// marker they don't belong to: introducing a marker purges the previous
// drop's records in the same raft apply (schema FSM marker-introduction
// purge). Do not weaken that purge without revisiting this function. Lives
// next to EpochCoveredShards: the AddTask-apply guard re-proves every claim
// composed here with the same implementation.
func EpochAndInheritedCoverage(collection string, targets []string, state *sharding.State,
	tasks map[string][]*distributedtask.Task, logger logrus.FieldLogger,
) (epoch string, cleaned []string, finalizeNow bool) {
	// The newest matching task (raft-assigned Version: monotonic and
	// deterministic, unlike node wall clocks) names the candidate epoch.
	var newest *DropVectorIndexTaskPayload
	var newestVersion uint64
	for _, task := range tasks[DropVectorIndexNamespace] {
		p, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			if logger != nil {
				logger.WithField("task", task.ID).
					Warnf("drop-vector: coverage-inheritance: skipping task with unparseable payload: %v", err)
			}
			continue
		}
		if !SameDrop(p, collection, targets) {
			continue
		}
		if newest == nil || task.Version > newestVersion {
			newest, newestVersion = p, task.Version
		}
	}
	if newest == nil || newest.DropEpochID == "" {
		return uuid.NewString(), nil, false
	}
	// Completed tasks vouch their full CoveredShards; a FAILED or CANCELLED
	// round vouches only its COMPLETED units — a deactivated tenant fails a
	// whole round, and discarding its finished work would make MT-scale
	// convergence improbable.
	covered := EpochCoveredShards(tasks[DropVectorIndexNamespace], collection, targets, newest.DropEpochID)
	// Prune to current shards: deleted tenants would otherwise accumulate in
	// every subsequent payload forever.
	cleaned = make([]string, 0, len(covered))
	for shard := range covered {
		if _, ok := state.Physical[shard]; ok {
			cleaned = append(cleaned, shard)
		}
	}
	sort.Strings(cleaned)
	shardNames := make([]string, 0, len(state.Physical))
	for shard := range state.Physical {
		shardNames = append(shardNames, shard)
	}
	remaining := make(map[string]struct{}, len(cleaned))
	for _, shard := range cleaned {
		remaining[shard] = struct{}{}
	}
	if len(ShardsNotCovered(shardNames, remaining)) == 0 {
		// The chain spans every current shard while the marker still stands.
		// Either the shards it still owed were DELETED — the work is genuinely
		// done for the collection as it now stands, so finalize rather than
		// re-strip shards that are already clean — or the chain owed nothing,
		// which is closed-epoch residue (see above) and must re-clean.
		if resolvedByDeletion(tasks[DropVectorIndexNamespace], collection, targets,
			newest.DropEpochID, shardNames) {
			return newest.DropEpochID, cleaned, true
		}
		return uuid.NewString(), nil, false
	}
	return newest.DropEpochID, cleaned, false
}

// resolvedByDeletion reports whether some task of the epoch proves the drop's
// remaining work vanished with a deleted shard. Scoped to the epoch for the
// same reason coverage is: another drop's records say nothing about this one.
func resolvedByDeletion(tasks []*distributedtask.Task, collection string, targets []string,
	epoch string, shardNames []string,
) bool {
	for _, task := range tasks {
		if !task.Status.IsCompleted() && !terminalWithPartialWork(task.Status) {
			continue
		}
		p, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			continue
		}
		if !SameDrop(p, collection, targets) || p.DropEpochID != epoch {
			continue
		}
		if p.ResolvedByShardDeletion(shardNames) {
			return true
		}
	}
	return false
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
// records. Where ExtractReindexTaskCollection decodes the collection field
// alone, this validates the whole payload, so a record missing its targets or
// its opId is not attributed at all. ok is false on either.
func ExtractDropVectorIndexTaskCollection(payload []byte) (collection string, ok bool) {
	p, err := decodeDropVectorIndexPayload(payload)
	if err != nil {
		return "", false
	}
	return p.Collection, true
}
