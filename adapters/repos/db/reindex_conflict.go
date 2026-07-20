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
	"slices"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// ReindexBucketEffect classifies migration type t: which inverted-index
// buckets it writes (touchesSearchable/touchesFilterable, gating conflict
// detection and idempotency checks) and whether completing it leaves the
// searchable bucket on blockmax (producesBlockmax, feeding
// [SearchablePropertyBlockmaxFromRAFT]). One switch is the source of truth
// for all three.
//
// ok is false for an unrecognized type — the forward-compat hot path for a
// rolling upgrade observing a newer peer's task type. It MUST NOT panic (peer
// input panicking would be a cross-version DoS), so unknown types fail SAFE:
// touches=true (conflict rejects a racing submit) and producesBlockmax=true
// (post-GA the only realistic direction; under-reporting would let a
// re-submitted change-algorithm corrupt an already-blockmax bucket).
// Exhaustiveness is enforced by TestReindexBucketEffect_Exhaustive, not a
// production panic.
func ReindexBucketEffect(t ReindexMigrationType) (touchesSearchable, touchesFilterable, producesBlockmax, ok bool) {
	switch t {
	case ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable, ReindexTypeEnableSearchable:
		return true, false, true, true
	case ReindexTypeChangeTokenization:
		return true, true, false, true
	case ReindexTypeChangeTokenizationFilterable, ReindexTypeRepairFilterable, ReindexTypeEnableFilterable:
		return false, true, false, true
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return false, false, false, true
	default:
		return true, true, true, false
	}
}

// producesBlockmaxSearchable reports whether completing a migration type leaves
// the property's searchable bucket on blockmax. Thin accessor over
// [ReindexBucketEffect] (see there for the unknown-type policy).
func producesBlockmaxSearchable(t ReindexMigrationType) bool {
	_, _, producesBlockmax, _ := ReindexBucketEffect(t)
	return producesBlockmax
}

// SearchablePropertyIsBlockmax resolves whether (class, propName)'s searchable
// bucket is blockmax, from RAFT-consistent state only. Precedence: (1) the
// durable per-property stamp if set (survives task-list ageout and the
// same-tick sibling wedge), else (2) the legacy class-flag/FINISHED-task
// derivation ([SearchablePropertyBlockmaxFromRAFT]). nil reindexTasks is valid
// when the caller has none (e.g. shard init). Every read site must route
// through this resolver.
func SearchablePropertyIsBlockmax(class *models.Class, propName string, reindexTasks []*distributedtask.Task) bool {
	if blockmax, resolved := searchableStampOrClassFlag(class, propName); resolved {
		return blockmax
	}
	// classFlag is false here (else resolved would be true), so the task-list
	// fallback carries the whole decision.
	return SearchablePropertyBlockmaxFromRAFT(false, class.Class, propName, reindexTasks)
}

// searchableStampOrClassFlag applies the first two precedence tiers of
// [SearchablePropertyIsBlockmax] — the durable per-property stamp, then the
// class-wide flag. resolved is false when neither tier decides and the caller
// must consult the FINISHED reindex-task list.
func searchableStampOrClassFlag(class *models.Class, propName string) (blockmax, resolved bool) {
	for _, p := range class.Properties {
		if p.Name == propName {
			if p.SearchableBlockmax != nil {
				return *p.SearchableBlockmax, true
			}
			break
		}
	}
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
		return true, true
	}
	return false, false
}

// SearchablePropertyIsBlockmaxParsed is [SearchablePropertyIsBlockmax] using a
// pre-computed finishedBlockmaxProps set (built once by the GET-indexes
// handler) instead of a raw task list, so per-property resolution is O(1).
func SearchablePropertyIsBlockmaxParsed(class *models.Class, propName string, finishedBlockmaxProps map[string]struct{}) bool {
	if blockmax, resolved := searchableStampOrClassFlag(class, propName); resolved {
		return blockmax
	}
	_, ok := finishedBlockmaxProps[propName]
	return ok
}

// SearchablePropertyBlockmaxFromRAFT is the legacy per-property blockmax
// derivation (class flag + task list), used as [SearchablePropertyIsBlockmax]'s
// fallback when a property carries no durable stamp. Its historical hole —
// once a FINISHED task ages out, a migrated property in a permanently-partial
// class reads back as WAND — is what the stamp layer closes for post-stamp
// migrations.
func SearchablePropertyBlockmaxFromRAFT(classFlagBlockmax bool, collection, propName string, reindexTasks []*distributedtask.Task) bool {
	if classFlagBlockmax {
		return true
	}
	for _, task := range reindexTasks {
		if task.Status != distributedtask.TaskStatusFinished {
			continue
		}
		var payload ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if !producesBlockmaxSearchable(payload.MigrationType) {
			continue
		}
		if strings.EqualFold(payload.Collection, collection) && slices.Contains(payload.Properties, propName) {
			return true
		}
	}
	return false
}

// CheckConflict implements [distributedtask.ConflictDetector] for the
// reindex namespace. Called under [Manager.mu] from the RAFT-apply
// AddTask path BEFORE the new task is appended to FSM-stored state.
// Returns a non-nil error iff `newPayload` would conflict with an
// already-STARTED task in `existingTasks`.
//
// FSM-determinism: every node applies the same RAFT log entry, sees
// the same `existingTasks` snapshot, and runs this same function — so
// every node reaches the same accept/reject decision. The function
// must remain a pure transform of its arguments.
//
// Conflict rule: any two reindex migrations on overlapping properties
// of the same collection conflict, regardless of which bucket type
// they primarily write to. See [typesConflictReason] for the
// rationale.
func (p *ReindexProvider) CheckConflict(newPayload []byte, existingTasks []*distributedtask.Task) error {
	var newP ReindexTaskPayload
	if err := json.Unmarshal(newPayload, &newP); err != nil {
		return fmt.Errorf("unmarshal new reindex payload: %w", err)
	}
	if newP.Collection == "" || newP.MigrationType == "" {
		return fmt.Errorf("new reindex payload missing Collection or MigrationType")
	}

	for _, task := range existingTasks {
		// PREPARING and SWAPPING both count as in-flight (via
		// [distributedtask.TaskStatus.IsActive]): every unit has reached
		// terminal state, but the post-completion callbacks (per-node
		// PREP, cluster-wide PrepCompleteAck barrier, per-node swap,
		// cluster-wide schema flip) have not yet committed. Submitting
		// a new migration on the same property during either window
		// could land before MarkDistributedTaskFinalized commits the
		// schema flip, leaving the new task and the unfinished swap of
		// the prior one racing on the same bucket pointers.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			// Existing task has an unparseable payload. We can't prove
			// non-conflict, so reject — the alternative (silently
			// allow) would let two real migrations race on shared
			// bucket state.
			return fmt.Errorf(
				"in-flight reindex task %q has unparseable payload; cannot verify conflict",
				task.ID)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or MigrationType",
				task.ID)
		}
		if !strings.EqualFold(existP.Collection, newP.Collection) {
			continue
		}

		if reason := typesConflictReason(newP.MigrationType, newP.Properties,
			existP.MigrationType, existP.Properties); reason != "" {
			return fmt.Errorf("reindex task %q conflicts: %s", task.ID, reason)
		}
	}
	return nil
}

// typesConflictReason returns a non-empty reason string if two reindex
// migrations on the same collection target overlapping properties.
//
// Earlier versions allowed parallel migrations as long as they wrote
// to different bucket types (e.g. enable-filterable + enable-rangeable
// on the same property). That was a real Sev 1: when one of those
// migrations completed, its OnMigrationComplete fired an
// UpdateProperty RAFT command whose MergeProps preserved the
// still-false sibling flag (the other migration hasn't flipped its
// flag yet). On apply, Migrator.UpdateProperty →
// Shard.updatePropertyBuckets ran cleanStaleMigrationDirs for every
// index whose flag was now false, removing the in-flight migration's
// .migrations/<dir>/ working directory and causing the next
// markProgress to fail with "progress.mig.000000001: no such file or
// directory" → task FAILED. https://github.com/weaviate/weaviate/issues/10675 frontend repro on
// parallel enable-filterable + enable-rangeable hit this.
//
// Closing the window at submit time is correct: reject any new task
// whose property set overlaps an in-flight task's property set, so the
// caller gets a clean conflict error and can serialize the operations.
// Empty props means "all properties" (reserved for a future
// whole-collection rebuild) and overlaps with everything.
func typesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	// Conflict is property-overlap only; it doesn't consult bucket-touch
	// predicates, so an unknown (peer-written) type on an overlapping
	// property still conflicts without needing recognition here.
	if !ReindexPropsOverlap(newProps, existProps) {
		return ""
	}
	if newType == existType {
		return fmt.Sprintf("already running %s for overlapping properties", newType)
	}
	return fmt.Sprintf("already running %s for overlapping properties; "+
		"concurrent %s on the same property would race on shared on-disk "+
		"migration state — wait for the in-flight task to finish before "+
		"submitting another", existType, newType)
}

// TypesConflictReason is the package-public alias for typesConflictReason,
// used by the REST handlers' pre-flight conflict check. Inline so
// internal callers (CheckConflict, CheckPropertyUpdate) continue to use
// the lowercase symbol without indirection.
func TypesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	return typesConflictReason(newType, newProps, existType, existProps)
}

// ReindexPropsOverlap returns true if two property sets overlap. An
// empty set means "all properties", which overlaps with everything.
//
// Public so REST handlers can use the same predicate as the
// FSM-deterministic conflict check.
func ReindexPropsOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, ap := range a {
		for _, bp := range b {
			if ap == bp {
				return true
			}
		}
	}
	return false
}

// TouchesSearchable reports whether migration type t writes to the searchable
// bucket. Thin accessor over [ReindexBucketEffect] (see there for the
// unknown-type policy).
func TouchesSearchable(t ReindexMigrationType) bool {
	touchesSearchable, _, _, _ := ReindexBucketEffect(t)
	return touchesSearchable
}

// TouchesFilterable reports whether migration type t writes to the filterable
// bucket. Thin accessor over [ReindexBucketEffect].
func TouchesFilterable(t ReindexMigrationType) bool {
	_, touchesFilterable, _, _ := ReindexBucketEffect(t)
	return touchesFilterable
}

// CheckPropertyUpdate implements
// [distributedtask.SchemaMutationDetector] for the reindex namespace.
// Called from the schema FSM's UpdateProperty apply path under
// [Manager.mu] to reject external property mutations while a reindex
// migration on the same (collection, property) is in any non-terminal
// state (STARTED, PREPARING, or SWAPPING).
//
// Motivating failure mode: a `change-tokenization` migration spawns
// separate per-shard sub-tasks for the searchable and filterable
// indexes. A DELETE `/index/searchable` arriving mid-flight applies
// `cleanStaleMigrationDirs("<prop>", "searchable")`, which wipes the
// searchable sub-task's working dir under the still-running
// runtimeSwap → searchable sub-unit FAILs → sibling filterable
// sub-unit commits its local swap → per-shard ack barrier sees mixed
// acks → task FAILED → `flipSemanticMigrationSchema` skipped →
// schema stays at OLD tokenization while the filterable bucket on
// disk holds NEW-tokenized data. Bucket↔schema inversion — same
// family as the ack-barrier failure mode but triggered by an external
// schema mutation instead of a crash.
//
// Rule: blanket reject any property mutation overlapping an in-flight
// reindex task's properties on the same collection. Migration-driven
// schema flips bypass this guard via
// [api.UpdatePropertyRequest.FromInFlightMigration] = true, set by
// [Raft.UpdatePropertyFromMigration] from the scheduler's
// OnTaskCompleted dispatch. So the guard rejects external mutations
// without breaking the migration's own scheduled completion flip.
//
// FSM-determinism: pure function of (className, propertyName,
// existingTasks). Unparseable in-flight payloads are treated as a
// hard reject (same as [ConflictDetector.CheckConflict]) — the
// alternative (silently allow) would let a real bucket-level conflict
// slip through and re-open the race this guard exists to close.
func (p *ReindexProvider) CheckPropertyUpdate(className, propertyName string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			return fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; "+
					"cannot verify whether property update on %s.%s would "+
					"conflict: %w",
				task.ID, className, propertyName, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether property update "+
					"on %s.%s would conflict",
				task.ID, className, propertyName)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		if !ReindexPropsOverlap(existP.Properties, []string{propertyName}) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s.%s (status=%s); "+
				"schema mutations on this property are blocked until the "+
				"reindex completes or is cancelled — wait for the task "+
				"to reach a terminal state, or cancel it via the reindex "+
				"REST API before retrying",
			task.ID, existP.MigrationType,
			existP.Collection, propertyName, task.Status)
	}
	return nil
}

// CheckClassMutation implements
// [distributedtask.SchemaMutationDetector] for class-wide
// destructive mutations (DeleteClass). Stricter than
// CheckPropertyUpdate — any reindex task on the class (regardless of
// which property) is a conflict, because dropping the class destroys
// every property's bucket state at once including the in-flight
// migration's working dirs and canonical bucket pointers.
//
// Class-wide blast radius: DeleteClass arriving mid-reindex is the
// catastrophic extension of the per-property bucket↔schema inversion
// — it destroys every property's bucket state at once.
//
// Same FSM-determinism contract as CheckPropertyUpdate. Unparseable
// in-flight payloads are treated as a hard reject (we cannot prove
// non-conflict).
func (p *ReindexProvider) CheckClassMutation(className string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			return fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; "+
					"cannot verify whether DeleteClass on %s would "+
					"conflict: %w",
				task.ID, className, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether DeleteClass on "+
					"%s would conflict",
				task.ID, className)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s (status=%s); "+
				"deleting this class would destroy the migration's "+
				"working state and produce a bucket↔schema inversion "+
				"on every replica — cancel the reindex via the REST "+
				"API before deleting the class",
			task.ID, existP.MigrationType, existP.Collection, task.Status)
	}
	return nil
}

// CheckTenantMutation implements
// [distributedtask.SchemaMutationDetector] for tenant-level
// mutations that make tenant shards locally unavailable
// (DeleteTenants, UpdateTenants transitioning away from ACTIVE).
//
// Today's reindex task payload names a collection but not a specific
// tenant — a migration submitted on a multi-tenant collection
// applies to whatever shards exist for that collection. So the
// conservative implementation is "block every tenant mutation on a
// class with any in-flight reindex": if a reindex is running on the
// class, we cannot prove the tenant being mutated is not part of
// its working set without a more granular payload.
//
// Same FSM-determinism contract as CheckPropertyUpdate.
//
// `tenants` is informational — the rejection error names them so
// the caller knows which tenants would be affected.
func (p *ReindexProvider) CheckTenantMutation(className string, tenants []string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			return fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; "+
					"cannot verify whether tenant mutation on %s/%v "+
					"would conflict: %w",
				task.ID, className, tenants, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether tenant "+
					"mutation on %s/%v would conflict",
				task.ID, className, tenants)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s (status=%s); "+
				"mutating tenants %v would make their shards locally "+
				"unavailable and produce a bucket↔schema inversion — "+
				"cancel the reindex via the REST API before mutating "+
				"these tenants",
			task.ID, existP.MigrationType, existP.Collection,
			task.Status, tenants)
	}
	return nil
}
