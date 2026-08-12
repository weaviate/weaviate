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
)

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
		// Every non-terminal status counts as in-flight (via
		// [distributedtask.TaskStatus.IsActive]). PREPARING and SWAPPING
		// are the subtle ones: every unit has reached terminal state, but
		// the post-completion callbacks (per-node PREP, cluster-wide
		// PrepCompleteAck barrier, per-node swap, cluster-wide schema
		// flip) have not yet committed. Submitting a new migration on the
		// same property during either window could land before
		// MarkDistributedTaskFinalized commits the schema flip, leaving
		// the new task and the unfinished swap of the prior one racing on
		// the same bucket pointers.
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
//
// Fails closed on an unrecognized migration type (RAFT apply path — a panic
// would crash-loop the cluster). Overlap is checked first since it needs
// only property sets, which stay correct regardless of type. Safe only
// because every overlap currently conflicts; a future compatible-types
// exception must preserve that, or an older node would reject what a newer
// one accepts, splitting the FSM.
func typesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	if !ReindexPropsOverlap(newProps, existProps) {
		return ""
	}
	if unknown := firstUnknownMigrationType(newType, existType); unknown != "" {
		return fmt.Sprintf(
			"already running %s for overlapping properties; migration type %q "+
				"is not known to this build (most likely submitted by a newer "+
				"node), so it cannot be proven safe to run alongside — wait for "+
				"the in-flight task to finish. Submitting from a node that knows "+
				"the type does not help: every node applies the same RAFT entry, "+
				"including this one",
			existType, unknown)
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
// used by the REST handlers' pre-flight conflict check. A separate
// exported name rather than a rename, so the in-package callers keep
// using the lowercase symbol.
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

// ReindexTargetIndexes lists the inverted-index keys a migration type writes
// to, or nil for an unknown type.
//
// Single source of truth for that mapping — read by [ReindexCancelCall],
// [firstUnknownMigrationType], the REST cancel matcher, submit-time
// cleanup, and both disk-deleting cleanup paths. A mismatch between callers
// risks silent data loss. [reindexRepairBody] needs its own arm per type;
// both tables are pinned against the declared set in reindex_conflict_test.go.
func ReindexTargetIndexes(t ReindexMigrationType) []string {
	switch t {
	case ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm,
		ReindexTypeRebuildSearchable:
		return []string{"searchable"}
	case ReindexTypeEnableFilterable, ReindexTypeRepairFilterable,
		ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return []string{"rangeable"}
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}
	}
	return nil
}

// firstUnknownMigrationType returns the first of ts this build does not
// recognize, or "" when it knows them all. "Known" is defined as "named by
// [ReindexTargetIndexes]", which keeps the single-source-of-truth mapping
// the only place a new type has to be registered.
func firstUnknownMigrationType(ts ...ReindexMigrationType) ReindexMigrationType {
	for _, t := range ts {
		if len(ReindexTargetIndexes(t)) == 0 {
			return t
		}
	}
	return ""
}

// ReindexCancelCall renders a request that cancels the task described by p,
// or "" when p.Collection/p.Properties are empty or p.MigrationType maps to
// no index. Callers must not print a placeholder for "": the cancel
// endpoint 202s with NO_OP on a non-matching request, so a guessed URL
// would look like success for a still-running task.
//
// The collection keeps its namespace-qualified prefix — needed by the
// global-operator reader; a confined caller's own prefix is stripped
// elsewhere ([namespacing.StripErrorMessage]).
//
// Exported only for handlers_schema_remedy_test.go.
func ReindexCancelCall(p ReindexTaskPayload, askedProperty string) string {
	indexes := ReindexTargetIndexes(p.MigrationType)
	if p.Collection == "" || len(p.Properties) == 0 || len(indexes) == 0 {
		return ""
	}
	return fmt.Sprintf(`PUT /v1/schema/%s/indexes/%s {"%s":{"cancel":true}}`,
		p.Collection, reindexNamedProperty(p, askedProperty), indexes[0])
}

// ReindexRepairCall renders the request that repairs the property after the
// migration terminalized with buckets ahead of the schema, or "" when this
// build cannot name one. Re-submits the original migration rather than a
// bare rebuild: every rebuild verb validates against the bit the skipped
// schema flip would have set.
//
// change-algorithm on an already-promoted shard is untested (weaviate/weaviate#12575).
//
// Exported for the same reason as [ReindexCancelCall].
func ReindexRepairCall(p ReindexTaskPayload, askedProperty string) string {
	return reindexSubmitCall(p, askedProperty, reindexRepairBody(p))
}

// reindexSubmitCall renders a submit request against p's collection and
// named property with the given body, or "" when any part is missing.
func reindexSubmitCall(p ReindexTaskPayload, askedProperty, body string) string {
	if p.Collection == "" || len(p.Properties) == 0 || body == "" {
		return ""
	}
	return fmt.Sprintf("PUT /v1/schema/%s/indexes/%s %s",
		p.Collection, reindexNamedProperty(p, askedProperty), body)
}

// reindexRepairBody renders the submit body that produced p's migration
// type — one index group per body, matching every per-type REST
// precondition, not just group exclusivity (e.g. enable-searchable carries
// its tokenization since the handler reads the target from the same body).
//
// "" for an unrecognized type, for enable-rangeable (no body is valid in
// every terminal state), or when the payload carries no target
// tokenization.
//
// No cluster reaches that last one: every submit path for the three types
// that read the field rejects an empty target, and no payload version has
// ever omitted it. The guard stays because this renders a request an
// operator pastes, and a body carrying an empty tokenization is one the
// API rejects — printing nothing beats printing a command that cannot
// work, and guessing a value would retokenize to the wrong one.
func reindexRepairBody(p ReindexTaskPayload) string {
	switch p.MigrationType {
	case ReindexTypeEnableSearchable:
		if p.TargetTokenization == "" {
			return ""
		}
		return fmt.Sprintf(`{"searchable":{"enabled":true,"tokenization":%q}}`,
			p.TargetTokenization)
	case ReindexTypeRebuildSearchable:
		return `{"searchable":{"rebuild":true}}`
	case ReindexTypeChangeAlgorithm:
		// blockmax hardcoded: it is the only target algorithm
		// validateChangeAlgorithmProperty accepts, and the payload carries
		// no target field. A second algorithm must extend both places.
		return `{"searchable":{"algorithm":"blockmax"}}`
	case ReindexTypeChangeTokenization:
		if p.TargetTokenization == "" {
			return ""
		}
		return fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, p.TargetTokenization)
	case ReindexTypeChangeTokenizationFilterable:
		if p.TargetTokenization == "" {
			return ""
		}
		return fmt.Sprintf(`{"filterable":{"tokenization":%q}}`, p.TargetTokenization)
	case ReindexTypeEnableFilterable:
		return `{"filterable":{"enabled":true}}`
	case ReindexTypeRepairFilterable:
		return `{"filterable":{"rebuild":true}}`
	case ReindexTypeEnableRangeable:
		// No body the API accepts in every terminal state: the strategy
		// flips IndexRangeFilters per shard as it goes, so `enabled` 400s
		// once any shard finished and `rebuild` 400s while none has.
		return ""
	case ReindexTypeRepairRangeable:
		return `{"rangeable":{"rebuild":true}}`
	}
	return ""
}

// reindexNamedProperty picks the property a rendered call should name:
// askedProperty when the task actually carries it, the first one otherwise.
func reindexNamedProperty(p ReindexTaskPayload, askedProperty string) string {
	if slices.Contains(p.Properties, askedProperty) {
		return askedProperty
	}
	return p.Properties[0]
}

// ReindexGateRemedy is the closing sentence every reindex schema gate (RAFT
// apply path and REST pre-check) appends to its refusal, kept in one place
// so the two can't drift. Not used by the backup gate (shard-keyed, never
// sees the task — its wording is in reindexInFlightError).
//
// p is the offending task's payload; askedProperty is the property named in
// the caller's request, or "" when the refusal isn't property-scoped.
// callerDropsTheData is true only when the caller's action destroys ALL the
// shards the migration works on (today only DeleteClass): those get told to
// re-issue their request, since a follow-up repair call would 404. Tenant
// mutations pass false — the migration's on-disk state survives them.
//
// STARTED is the only status a cancel is accepted in: both
// [distributedtask.Manager.CancelTask] and the REST pre-flight key on
// [distributedtask.TaskStatus.IsCancellable], a literal `== STARTED`, and
// answer 409 Conflict for everything else. So the split below reads that
// same predicate — naming a cancel for PREPARING or SWAPPING would hand the
// operator a request the API refuses.
//
// Precondition: status is non-terminal ([distributedtask.TaskStatus.IsActive]);
// callers pre-filter on it, so a terminal status here would be misreported
// as one this build doesn't recognize.
//
// The rendered repair is a submit, so it needs RUNTIME_REINDEX_ENABLED=true;
// cancel is exempt from that flag.
//
// FSM-determinism: three of the four call sites are apply-path gates, and the
// wording branches on two local vocabularies (the status and the migration
// type). Only the wording does. The gate returns an error either way, the
// accept/reject decision never reads the remedy, and follower apply errors
// are discarded — so the apply stays deterministic across binaries.
func ReindexGateRemedy(status distributedtask.TaskStatus, p ReindexTaskPayload, askedProperty string, callerDropsTheData bool) string {
	if status.IsCancellable() {
		return cancellableGateRemedy(p, askedProperty, callerDropsTheData)
	}
	if status.IsCoordinationPhase() {
		return uncancellableGateRemedy(p, askedProperty, callerDropsTheData)
	}
	return "this build does not know that status, most likely because a " +
		"newer node reported it, so it cannot tell you whether cancel " +
		"still applies; read the task on a node that knows the status"
}

// cancellableGateRemedy is [ReindexGateRemedy] for a STARTED task — the one
// status where the cancel it names is accepted.
func cancellableGateRemedy(p ReindexTaskPayload, askedProperty string, callerDropsTheData bool) string {
	cancelCall := ReindexCancelCall(p, askedProperty)
	if cancelCall == "" {
		return "the cancel endpoint is keyed on one collection, property and " +
			"index type, and this task names none this build can fill in, " +
			"so this build can only tell you to wait it out; if a newer node " +
			"submitted this migration type, read the task there instead"
	}
	if !IsSemanticMigration(p.MigrationType) {
		partial := "cancel it via " + cancelCall + ", or wait for it to finish. " +
			"This migration has no cluster-wide cutover, so its shards commit " +
			"one by one rather than at a single point: cancelling leaves the " +
			"ones that already finished rebuilt and the rest untouched. "
		if callerDropsTheData {
			// Wording note: no literal "go " on this line —
			// tools/linter_go_routines.sh greps for it and would report this
			// file as using bare goroutines.
			return partial + "The rebuilt shards are dropped along with the " +
				"data you are removing, so there is nothing to finish " +
				"afterwards: cancel it, then re-issue this request."
		}
		if p.MigrationType == ReindexTypeEnableRangeable {
			return partial + "To finish the job later, re-submit it via " +
				reindexSubmitCall(p, askedProperty, `{"rangeable":{"enabled":true}}`) +
				" while no shard has finished yet, or via " +
				reindexSubmitCall(p, askedProperty, `{"rangeable":{"rebuild":true}}`) +
				" once one has (both need RUNTIME_REINDEX_ENABLED=true, unlike " +
				"cancel). enable-rangeable sets indexRangeFilters on the " +
				"property as soon as its first shard commits, and each of the " +
				"two verbs is rejected in the state the other one covers."
		}
		// A format-only type flips no schema, so its original submit body
		// stays accepted post-cancel — the re-submit IS the repair.
		return partial + "To finish the job later, re-submit it via " +
			ReindexRepairCall(p, askedProperty) +
			" (which needs RUNTIME_REINDEX_ENABLED=true, unlike cancel), which " +
			"re-runs every shard, the ones that already finished included."
	}
	return "cancel it via " + cancelCall + ", or wait for it to finish"
}

// uncancellableGateRemedy is [ReindexGateRemedy] for a task in a coordination
// phase (PREPARING or SWAPPING). Every cancel is refused there, so no arm may
// name one: waiting is the whole remedy, and the rest of the sentence is what
// the operator has to be ready for once the wait ends.
//
// Both phases are entered from AllUnitsTerminal, and only
// [distributedtask.Manager.CancelTask] ever writes CANCELLED — so a task that
// reached here can still end FINISHED or FAILED, but no longer CANCELLED.
//
// For a semantic migration the repair window is already open: merged.mig is
// written during PREPARING, before any shard swaps, and
// [FinalizeCompletedMigrations] promotes on it alone. A FAILED outcome
// therefore leaves promotion-eligible data behind that the next restart
// promotes into the bucket↔schema inversion [ReindexProvider.CheckClassMutation]
// treats as catastrophic (weaviate/weaviate#12575).
func uncancellableGateRemedy(p ReindexTaskPayload, askedProperty string, callerDropsTheData bool) string {
	wait := "wait for it to reach a terminal state (GET /v1/tasks reports it). " +
		"From this phase on the cancel endpoint answers 409 Conflict, because " +
		"nodes may already have merged their new buckets or renamed bucket " +
		"directories and stopping the rest would leave the cluster serving " +
		"migrated buckets under the pre-migration schema, so there is no way " +
		"to end this task early. "
	if firstUnknownMigrationType(p.MigrationType) != "" {
		return wait + "What it leaves behind is something this build cannot " +
			"name: it does not know this migration type, most likely because " +
			"a newer node submitted it, so read the task on that node instead."
	}
	if !IsSemanticMigration(p.MigrationType) {
		partial := wait + "Every unit of this migration has already completed " +
			"on every node; what is left is the cluster-wide barrier that ends " +
			"the task. "
		if callerDropsTheData {
			return partial + "Re-issue this request once the task is terminal."
		}
		return partial + "Nothing about the wait leaves its index rebuild " +
			"half-applied, so there is nothing to re-submit afterwards."
	}
	inversion := wait + "Its per-shard work may already be merged on disk, so " +
		"if the task ends in FAILED rather than FINISHED the schema change is " +
		"skipped while that data stays, and the next restart promotes it, " +
		"leaving those buckets holding the new format under the old schema. "
	if callerDropsTheData {
		return inversion + "That inversion goes with the data you are removing, " +
			"so there is nothing left to repair: re-issue this request once the " +
			"task is terminal."
	}
	repairCall := ReindexRepairCall(p, askedProperty)
	if repairCall == "" {
		return inversion + "That is repairable only by re-running the " +
			"migration, which this build cannot name from the task's payload."
	}
	return inversion + "That is repairable only by re-running the migration " +
		"via " + repairCall + " (which needs RUNTIME_REINDEX_ENABLED=true)."
}

// abortedMigrationConsequence names what killing an in-flight migration
// costs the data that survives it: a schema inversion for semantic
// types, a half-applied rebuild for format-only ones. Only
// [ReindexProvider.CheckTenantMutation] uses it — see
// [ReindexProvider.CheckClassMutation] for why DeleteClass does not.
//
// [IsSemanticMigration] is a positive allowlist: an unrecognized type would
// otherwise fall into the format-only arm and claim a cost about semantics
// this build doesn't know. Unknown types abstain instead.
func abortedMigrationConsequence(mt ReindexMigrationType) string {
	if firstUnknownMigrationType(mt) != "" {
		return "have a consequence this build cannot name (it does not know " +
			"this migration type, most likely because a newer node submitted it)"
	}
	if IsSemanticMigration(mt) {
		return "can produce a bucket↔schema inversion"
	}
	return "leave its index rebuild half-applied"
}

// CheckPropertyUpdate implements
// [distributedtask.SchemaMutationDetector] for the reindex namespace.
// Called from the schema FSM's UpdateProperty apply path under
// [Manager.mu] to reject external property mutations while a reindex
// migration on the same (collection, property) is in any non-terminal
// state — per [distributedtask.TaskStatus.IsActive], so STARTED,
// PREPARING, SWAPPING, and any status this build does not recognize
// because a newer node reported it.
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
			// Task ID withheld: an unreadable payload also hides which
			// namespace the task belongs to.
			return fmt.Errorf(
				"an in-flight reindex task has an unparseable payload; "+
					"cannot verify whether property update on %s.%s would "+
					"conflict (see GET /v1/tasks): %w",
				className, propertyName, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			// Task ID withheld for the same reason as above.
			return fmt.Errorf(
				"an in-flight reindex task has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether property update "+
					"on %s.%s would conflict (see GET /v1/tasks)",
				className, propertyName)
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
				"reindex reaches a terminal state — %s",
			task.ID, existP.MigrationType,
			existP.Collection, propertyName, task.Status,
			ReindexGateRemedy(task.Status, existP, propertyName, false))
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
// Skips [abortedMigrationConsequence]: that names a cost the surviving data
// carries, but DeleteClass leaves no data behind — whatever the migration
// half-wrote disappears with the class.
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
			// Task ID withheld: an unreadable payload also hides which
			// namespace the task belongs to.
			return fmt.Errorf(
				"an in-flight reindex task has an unparseable payload; "+
					"cannot verify whether DeleteClass on %s would "+
					"conflict (see GET /v1/tasks): %w",
				className, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			// Task ID withheld for the same reason as above.
			return fmt.Errorf(
				"an in-flight reindex task has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether DeleteClass on "+
					"%s would conflict (see GET /v1/tasks)",
				className)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s (status=%s); "+
				"deleting this class would abort the migration on every "+
				"replica, and the interrupted migration's partial state is "+
				"removed with the class, so nothing is left to repair — %s",
			task.ID, existP.MigrationType, existP.Collection, task.Status,
			ReindexGateRemedy(task.Status, existP, "", true))
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
//
// Unlike [ReindexProvider.CheckClassMutation], the remedy must not claim the
// migration's state disappears: a deactivated shard promotes its merged
// generation on reactivation (weaviate/weaviate#12575), and a delete leaves it on
// every surviving shard. The guard can't tell the two apart, so both render
// callerDropsTheData=false.
func (p *ReindexProvider) CheckTenantMutation(className string, tenants []string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			// Task ID withheld: an unreadable payload also hides which
			// namespace the task belongs to.
			return fmt.Errorf(
				"an in-flight reindex task has an unparseable payload; "+
					"cannot verify whether tenant mutation on %s/%v "+
					"would conflict (see GET /v1/tasks): %w",
				className, tenants, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			// Task ID withheld for the same reason as above.
			return fmt.Errorf(
				"an in-flight reindex task has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether tenant "+
					"mutation on %s/%v would conflict (see GET /v1/tasks)",
				className, tenants)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s (status=%s); "+
				"mutating tenants %v would make their shards locally "+
				"unavailable and %s. The migration's on-disk state is not "+
				"removed by this mutation: a deactivated shard promotes any "+
				"merged generation on reactivation, and a delete leaves every "+
				"remaining "+
				"tenant's shard carrying it — %s",
			task.ID, existP.MigrationType, existP.Collection,
			task.Status, tenants,
			abortedMigrationConsequence(existP.MigrationType),
			ReindexGateRemedy(task.Status, existP, "", false))
	}
	return nil
}
