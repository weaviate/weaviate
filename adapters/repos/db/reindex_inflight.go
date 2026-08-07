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
	"time"

	"github.com/sirupsen/logrus"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

// reindexGateSamplerBudget rate-limits each operator-facing gate WARN to one
// line per hour. The conditions they report (a gate reached before its lookup
// was installed, a hold kind this build cannot classify) persist until someone
// fixes the wiring or ships a fix, so the line has to keep reappearing for an
// operator who starts reading the logs later — but not once per shard checked.
const reindexGateSamplerBudget = time.Hour

// reindexGateSamplers holds one budget per fail-open gate.
//
// Built per [DB], not per process: the budget describes the node that is
// failing open, and a package-level one leaves every test after the first with
// an exhausted budget, which is what stopped this package from running with
// -count=2.
type reindexGateSamplers struct {
	unwiredGate        *logrusext.Sampler
	unwiredRestoreGate *logrusext.Sampler
	unwiredOverlap     *logrusext.Sampler
	unknownHold        *logrusext.Sampler
}

func newReindexGateSamplers(logger logrus.FieldLogger) *reindexGateSamplers {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	newSampler := func() *logrusext.Sampler {
		return logrusext.NewSampler(logger, 1, reindexGateSamplerBudget)
	}
	return &reindexGateSamplers{
		unwiredGate:        newSampler(),
		unwiredRestoreGate: newSampler(),
		unwiredOverlap:     newSampler(),
		unknownHold:        newSampler(),
	}
}

// gateSamplers returns this DB's samplers, building them on first use so the
// many fixtures that construct a bare &DB{} still get the production budget
// rather than a nil dereference.
func (db *DB) gateSamplers() *reindexGateSamplers {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	if db.reindexGateSamplers == nil {
		db.reindexGateSamplers = newReindexGateSamplers(db.logger)
	}
	return db.reindexGateSamplers
}

// warnUnwiredGate emits a fail-open gate WARN through sampler, which callers
// take from [DB.gateSamplers]. The sampler already carries the DB's logger.
func warnUnwiredGate(sampler *logrusext.Sampler, action, msg string) {
	sampler.WithSampling(func(l logrus.FieldLogger) {
		l.WithField("action", action).Warn(msg)
	})
}

// warnUnwiredReindexGate is [warnUnwiredGate] for the per-shard backup gate.
func (db *DB) warnUnwiredReindexGate() {
	warnUnwiredGate(db.gateSamplers().unwiredGate, "backup_reindex_gate",
		"backup-reindex gate: ShardReindexActivityLookup not yet installed; allowing backup. "+
			"Expected briefly during startup; if this persists past bootstrap, check the SetShardReindexActivityLookup wiring in configure_api.go.")
}

// warnUnknownReindexHold reports a hold kind the gate cannot classify.
func (db *DB) warnUnknownReindexHold(hold ReindexHold) {
	db.gateSamplers().unknownHold.WithSampling(func(l logrus.FieldLogger) {
		l.WithField("action", "backup_reindex_gate").
			WithField("hold", int(hold)).
			Warn("backup-reindex gate: refusing — unrecognized ReindexHold value. " +
				"A hold kind was added to the enum without teaching the backup gate about it; " +
				"add the missing case in reindexBlockReasonIn.")
	})
}

// reindexBlockReason says which gate refused, so the refusal can give advice
// that matches: a cancelled task mid-teardown must not be described as one the
// operator can still cancel.
type reindexBlockReason int

const (
	reindexNotBlocked reindexBlockReason = iota
	reindexBlockedByLiveTask
	reindexBlockedByCleanup
	reindexBlockedBySubmit
	reindexBlockedPreWire
	// reindexBlockedByUnknownHold is the fail-closed answer for a
	// [ReindexHold] the gate cannot classify; see [reindexBlockReasonIn].
	reindexBlockedByUnknownHold
)

// AnyLiveReindexForShard answers the cluster-wide question: does DTM
// have any LIVE reindex task targeting (collection, shardName)?
//
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go.
//
// Default to "no live reindex" when the lookup is unwired (with a
// rate-limited WARN). The original conservative default (refuse) was
// correct in isolation but broke every module-test fixture that
// spins up Weaviate without going through the post-bootstrap
// install path. The window is short but externally reachable: HTTP can
// answer before the install runs, so the WARN reports a backup that was
// really admitted without a check, not a broken wiring.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
	// RUNTIME_REINDEX_ENABLED=false short-circuits inside newReindexGateSnapshot,
	// which every gate consumer builds through.
	return db.reindexBlockReason(collection, shardName) != reindexNotBlocked
}

// reindexGateSnapshot is one admission pass's view of both backup-gate lookups,
// built once per pass because the activity builder issues a leader-forwarded
// RAFT query: per-shard rebuilds cost one leader round trip per shard.
//
// Shards checked late therefore miss a task that appeared mid-pass. The pass was
// never atomic anyway, and the commit-time overlap check catches those.
//
// A nil activity lookup admits the backup, per [DB.AnyLiveReindexForShard].
type reindexGateSnapshot struct {
	activity ShardReindexActivityLookup
	cleanup  CleanupInProgressLookup
}

// newReindexGateSnapshot builds the per-admission-pass snapshot. Callers
// that check more than one shard must build it once and reuse it; see
// [reindexGateSnapshot].
func (db *DB) newReindexGateSnapshot() reindexGateSnapshot {
	var snap reindexGateSnapshot
	if db.config.RuntimeReindexDisabled {
		// Returning before the builders run is the point: the activity builder
		// issues a leader-forwarded RAFT query, and the kill switch has to cost
		// nothing. A zero snapshot reads as "not blocked" downstream, without
		// the unwired warning below.
		//
		// Flag-off is "no reindex check anywhere", not "no reindex running";
		// the residual that leaves is stated in full at
		// [DB.RefuseIfReindexOverlapped].
		return snap
	}

	db.reindexAuditMu.RLock()
	activityBuilder := db.shardReindexActivityLookupBuilder
	cleanupBuilder := db.reindexCleanupInProgressLookupBldr
	db.reindexAuditMu.RUnlock()

	if activityBuilder == nil {
		db.warnUnwiredReindexGate()
	} else {
		snap.activity = activityBuilder()
	}
	// Installed independently of the activity lookup. The cleanup hold is a map
	// read on this node's own provider — it needs nothing from DTM — so letting
	// a missing activity builder suppress it makes a submission that is already
	// deleting sidecars invisible to a concurrent backup for the whole
	// post-bootstrap wait, while HTTP is already serving.
	if cleanupBuilder != nil {
		snap.cleanup = cleanupBuilder()
	}
	return snap
}

// reindexBlockReason is AnyLiveReindexForShard's answer with the branch kept,
// so the refusal can match its advice to what actually blocked. Single-shard
// callers only: it builds its own snapshot.
func (db *DB) reindexBlockReason(collection, shardName string) reindexBlockReason {
	return db.reindexBlockReasonIn(db.newReindexGateSnapshot(), collection, shardName)
}

// reindexBlockReasonIn answers for one shard against an already-built
// snapshot.
func (db *DB) reindexBlockReasonIn(snap reindexGateSnapshot, collection, shardName string) reindexBlockReason {
	if snap.activity != nil && snap.activity(collection, shardName) {
		// Debug-level so flag-on operators get visibility into which
		// side of the OR fired the gate refusal. The matching cleanup
		// branch below logs at the same level.
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "activity_lookup_live_task").
				Debug("backup-reindex gate: refusing — DTM lists a live reindex task on this shard")
		}
		return reindexBlockedByLiveTask
	}
	// Cleanup lookup is OR-d in: the DTM task may have flipped to
	// terminal while autoCleanupAfterTerminal is still tearing the
	// sidecar buckets.
	if snap.cleanup == nil {
		return reindexNotBlocked
	}
	switch hold := snap.cleanup(collection, shardName); hold {
	case ReindexHoldNone:
		return reindexNotBlocked
	case ReindexHoldCleanup:
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "cleanup_in_progress").
				Debug("backup-reindex gate: refusing — a cancelled task holds this shard, either tearing its sidecars down or waiting to start")
		}
		return reindexBlockedByCleanup
	case ReindexHoldSubmit:
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "submit_in_progress").
				Debug("backup-reindex gate: refusing — a reindex submission is sweeping stale sidecars on this shard")
		}
		return reindexBlockedBySubmit
	default:
		// Fail closed. Every arm above answers a hold this build knows how to
		// classify; anything else means the enum grew a kind the gate was never
		// taught, and guessing "not held" would admit a backup over a shard some
		// other operation is actively holding. Same direction as
		// [IsLiveReindexTaskStatus] on an unrecognized DTM status.
		db.warnUnknownReindexHold(hold)
		return reindexBlockedByUnknownHold
	}
}

// SetReindexCleanupInProgressLookup installs the builder used by
// [DB.AnyLiveReindexForShard] to detect terminal-task cleanup that has
// not yet finished tearing __reindex / __ingest sidecar dirs. Wired in
// post-bootstrap alongside [DB.SetShardReindexActivityLookup].
func (db *DB) SetReindexCleanupInProgressLookup(builder CleanupInProgressLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexCleanupInProgressLookupBldr = builder
}

// refuseIfReindexInFlight is the per-shard backup-gate check used by
// [DB.Backupable], [Index.backupInactiveShardWithHardlinks],
// [Index.backupInactiveShardWithoutHardlinks], and
// [Shard.HaltForTransfer]. Consults DTM via
// [DB.AnyLiveReindexForShard]; the filesystem-marker variant it
// replaced only saw the local node and lagged DTM's actual state.
//
// If i.db is nil the gate is conservative: it refuses the backup, on
// the assumption that wiring is in progress.
//
// Single-shard call sites only — it builds a fresh gate snapshot per
// call. Multi-shard passes must use [Index.refuseIfReindexInFlightIn];
// see [reindexGateSnapshot].
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	if i.db == nil {
		return reindexInFlightError(i.Config.ClassName.String(), reindexBlockedPreWire)
	}
	err := i.refuseIfReindexInFlightIn(i.db.newReindexGateSnapshot(), shardName)
	if err != nil {
		i.logReindexRefusal(shardName)
	}
	return err
}

// refuseIfReindexInFlightIn is [Index.refuseIfReindexInFlight] against an
// already-built snapshot, for callers that check many shards in one pass.
func (i *Index) refuseIfReindexInFlightIn(snap reindexGateSnapshot, shardName string) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexInFlightError(collection, reindexBlockedPreWire)
	}
	reason := i.db.reindexBlockReasonIn(snap, collection, shardName)
	if reason == reindexNotBlocked {
		return nil
	}
	// Deliberately silent: a multi-shard pass calls this once per shard, so
	// logging here is O(shards) per refusal. The shard is what an operator
	// needs and the body withholds it, so each caller reports at its own
	// granularity instead — one line per shard where that IS the operation
	// ([Index.refuseIfReindexInFlight]), one bounded line per collection
	// where it is not ([DB.Backupable]).
	return reindexInFlightError(collection, reason)
}

// logReindexRefusal records the shard the body withholds. Callers that check a
// single shard use this; a pass over many shards must summarise instead.
func (i *Index) logReindexRefusal(shardName string) {
	if i.db == nil || i.db.logger == nil {
		return
	}
	i.db.logger.WithField("action", "backup_reindex_gate").
		WithField("collection", i.Config.ClassName.String()).
		WithField("shard", shardName).
		WithField("node", i.db.localNodeName).
		Warn("backup-reindex gate: refused a backup; a runtime-reindex is live on this shard")
}

// reindexInFlightError formats the operator-facing rejection. reason picks the
// advice: a live task, a cancelled task still tearing down, and a lookup that
// is not yet installed each need a different next step.
//
// Names no shard and no node: this text reaches an API response body, and
// backing up a collection grants nothing on either. The caller already named
// the collection, and the shard and node reach the operator through the log in
// [Index.refuseIfReindexInFlight].
func reindexInFlightError(collection string, reason reindexBlockReason) error {
	var advice string
	switch reason {
	case reindexBlockedPreWire:
		advice = ": backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping"
	case reindexBlockedBySubmit:
		// Nothing was cancelled here, so the cleanup text below would send the
		// operator looking for a migration that does not exist.
		advice = ": a reindex submission is preparing this collection; retry in a moment"
	case reindexBlockedByUnknownHold:
		// The gate knows the shard is held but not by what, so this promises
		// nothing it cannot back: no cancelled migration, no submission, no
		// duration estimate. The diagnosis is a server-side defect, which is
		// what the log line in [reindexBlockReasonIn] carries.
		advice = " is held by a reindex operation this server build does not recognize; retry, and report this to Weaviate if it persists"
	case reindexBlockedByCleanup:
		// No cancel advice: the task this is cleaning up after is already
		// cancelled, and telling the operator to cancel it sends them looking
		// for something that is gone.
		advice = ": a cancelled migration is still removing its temporary index files; retry once the cleanup finishes (usually a few seconds)"
	default:
		// The cancel clause is stated as conditional because it is: DTM only
		// accepts a cancel while the task is still building indexes. Once it
		// has entered its commit phase (PREPARING / SWAPPING) it is still live
		// here and still refuses the backup, but cancel refuses too. Promising
		// cancel outright sent operators into a loop between a refused backup
		// and a cancel that reported nothing to do.
		advice = " has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\"). While it is still building indexes you can cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}; once it has started committing its result it can only be waited out. If every index already reports \"ready\", the task holding this gate is one this server cannot attribute to a collection — the same cancel call, on any collection, clears it"
	}
	return entitiesbackup.ReindexBlockedError{Msg: fmt.Sprintf("%s: collection %q%s",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, collection, advice)}
}

// NoSearchableIndexHint identifies which `PUT /v1/schema/{class}/indexes/{prop}`
// verb hit the "property has no searchable index" gate so the helper can
// emit the right remediation suggestion. Tokenization changes can fall
// back to the filterable side; rebuild and algorithm changes cannot.
type NoSearchableIndexHint int

const (
	// NoSearchableIndexHintTokenization is the hint for
	// `{"searchable":{"tokenization":...}}`: suggest the filterable
	// retokenization path as an alternative.
	NoSearchableIndexHintTokenization NoSearchableIndexHint = iota
	// NoSearchableIndexHintRebuildOrAlgorithm is the hint for
	// `{"searchable":{"rebuild":true}}` and
	// `{"searchable":{"algorithm":...}}`: only the enable-searchable
	// remediation makes sense (no filterable fallback).
	NoSearchableIndexHintRebuildOrAlgorithm
)

// NoSearchableIndexError formats the operator-facing 400 returned when
// a `PUT /v1/schema/{class}/indexes/{prop}` request asks the server to
// act on a searchable index that does not exist on the property. Centralised
// here so every handler call site emits identical phrasing — prior to
// unification three handlers used three slightly different strings
// ("has no searchable index; use ...", "does not have a searchable index",
// and the inline filterable hint), which made operator log triage harder
// and risked drift as new verbs were added.
//
// The canonical wording is "property %q has no searchable index" plus a
// verb-appropriate remediation tail; the inverse case ("already has a
// searchable index", emitted by enable-searchable validation) is
// deliberately not unified with this helper since it carries the
// opposite meaning.
func NoSearchableIndexError(propertyName string, hint NoSearchableIndexHint) string {
	switch hint {
	case NoSearchableIndexHintTokenization:
		return fmt.Sprintf(
			"property %q has no searchable index; use {\"filterable\":{\"tokenization\":...}} to retokenize the filterable bucket, or {\"searchable\":{\"enabled\":true,\"tokenization\":...}} to add a searchable index",
			propertyName,
		)
	default: // NoSearchableIndexHintRebuildOrAlgorithm
		return fmt.Sprintf(
			"property %q has no searchable index; use {\"searchable\":{\"enabled\":true,\"tokenization\":...}} to add one first",
			propertyName,
		)
	}
}
