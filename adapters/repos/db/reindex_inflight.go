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
	"sync"

	"github.com/sirupsen/logrus"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// unwiredGateWarnOnce ensures the operator-facing WARN for the
// "lookup-not-installed" path fires at most once per process. The
// warning is informational: production gates HTTP serving on bootstrap
// completion, so under normal startup the unwired window is unreachable
// by an external backup request. If the WARN does fire in production
// logs, it means either (a) startup ordering is broken (lookup wiring
// never fires) or (b) a non-HTTP code path called Backupable before
// the lookup installed.
var unwiredGateWarnOnce sync.Once

// reindexGate is the resolved pair of backup-gate lookups for ONE backup
// precheck. Build one gate per precheck and reuse it for every shard the
// precheck touches.
//
// The gate exists because the activity lookup is expensive to build: its
// builder issues a cluster-wide ListDistributedTasks RAFT query against
// the leader. Resolving it at the per-shard leaf turned one precheck into
// one round trip per local shard — paid even when no reindex task exists
// anywhere — which on a multi-tenant node with many shards is thousands of
// queries per backup. Holding the resolved lookups in a value the caller
// passes down also keeps the cost visible at the call site rather than
// hidden behind a closure invocation at the leaf.
//
// Resolution is lazy so a precheck that never reaches a shard (all classes
// missing, or no local shards) issues no RAFT query at all.
type reindexGate struct {
	activityBuilder ShardReindexActivityLookupBuilder
	cleanupBuilder  CleanupInProgressLookupBuilder
	logger          logrus.FieldLogger

	once     sync.Once
	activity ShardReindexActivityLookup
	cleanup  CleanupInProgressLookup
}

// newReindexGate captures the currently installed lookup builders. The
// builders are read under the audit lock; the lookups themselves are built
// on first use by [reindexGate.resolve].
func (db *DB) newReindexGate() *reindexGate {
	db.reindexAuditMu.RLock()
	defer db.reindexAuditMu.RUnlock()
	return &reindexGate{
		activityBuilder: db.shardReindexActivityLookupBuilder,
		cleanupBuilder:  db.reindexCleanupInProgressLookupBldr,
		logger:          db.logger,
	}
}

// newReindexGate builds the gate for the single-shard backup paths. An
// Index without its DB back-reference yields an empty gate: the nil-db
// branch of [Index.refuseIfReindexInFlight] refuses before the gate is
// ever consulted.
func (i *Index) newReindexGate() *reindexGate {
	if i.db == nil {
		return &reindexGate{}
	}
	return i.db.newReindexGate()
}

// resolve builds both lookups, at most once per gate.
//
// Default to "no live reindex" when the activity builder is unwired (with
// a one-time WARN). The original conservative default (refuse) was correct
// in isolation but broke every module-test fixture that spins up Weaviate
// without going through the post-bootstrap install path; production HTTP
// gates on bootstrap completion so the unwired window is unreachable by
// external traffic.
func (g *reindexGate) resolve() {
	g.once.Do(func() {
		if g.activityBuilder == nil {
			unwiredGateWarnOnce.Do(func() {
				logger := g.logger
				if logger == nil {
					logger = logrus.New()
				}
				logger.WithField("action", "backup_reindex_gate").
					Warn("backup-reindex gate: ShardReindexActivityLookup not yet installed; allowing backup. " +
						"Expected briefly during startup; if this persists past bootstrap, check the SetShardReindexActivityLookup wiring in configure_api.go.")
			})
			return
		}
		g.activity = g.activityBuilder()
		if g.activity == nil {
			// A builder that yields no lookup takes the same fail-open
			// path as an unwired one, so the cleanup lookup is not
			// consulted and need not be built.
			return
		}
		// Cleanup builder is optional — older wiring paths and test
		// fixtures that install only the activity lookup keep the prior
		// semantics.
		if g.cleanupBuilder != nil {
			g.cleanup = g.cleanupBuilder()
		}
	})
}

// anyLiveReindexForShard answers the cluster-wide question: does the
// resolved DTM snapshot have any LIVE reindex task targeting
// (collection, shardName)?
//
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go.
func (g *reindexGate) anyLiveReindexForShard(collection, shardName string) bool {
	g.resolve()
	if g.activity == nil {
		return false
	}
	if g.activity(collection, shardName) {
		// Debug-level so flag-on operators get visibility into which
		// side of the OR fired the gate refusal. The matching cleanup
		// branch below logs at the same level.
		g.logRefusal(collection, shardName, "activity_lookup_live_task",
			"backup-reindex gate: refusing — DTM lists a live reindex task on this shard")
		return true
	}
	// Cleanup lookup is OR-d in: the DTM task may have flipped to
	// terminal while autoCleanupAfterTerminal is still tearing the
	// sidecar buckets.
	if g.cleanup == nil {
		return false
	}
	if g.cleanup(collection, shardName) {
		g.logRefusal(collection, shardName, "cleanup_in_progress",
			"backup-reindex gate: refusing — autoCleanupAfterTerminal still draining sidecars on this shard")
		return true
	}
	return false
}

func (g *reindexGate) logRefusal(collection, shardName, reason, msg string) {
	if g.logger == nil {
		return
	}
	g.logger.WithField("action", "backup_reindex_gate").
		WithField("collection", collection).
		WithField("shard", shardName).
		WithField("reason", reason).
		Debug(msg)
}

// AnyLiveReindexForShard is the single-shard convenience form of
// [reindexGate.anyLiveReindexForShard]: it resolves its own gate, so it
// costs one cluster-wide DTM query. Callers checking more than one shard
// must build a gate once and reuse it instead.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
	return db.newReindexGate().anyLiveReindexForShard(collection, shardName)
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
// [Shard.HaltForTransfer]. Consults DTM via the passed gate; the
// filesystem-marker variant it replaced only saw the local node and
// lagged DTM's actual state.
//
// The gate is a parameter rather than something this function resolves
// itself so that multi-shard callers pay one cluster-wide DTM query for
// the whole precheck instead of one per shard. Single-shard callers pass
// [Index.newReindexGate] inline.
//
// If i.db is nil the gate is conservative: it refuses the backup, on
// the assumption that wiring is in progress.
func (i *Index) refuseIfReindexInFlight(shardName string, gate *reindexGate) error {
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexInFlightError(i.Config.ClassName.String(), shardName, true)
	}
	if !gate.anyLiveReindexForShard(i.Config.ClassName.String(), shardName) {
		return nil
	}
	return reindexInFlightError(i.Config.ClassName.String(), shardName, false)
}

// reindexInFlightError formats the operator-facing rejection. The
// `preWire` flag distinguishes "DTM lookup says live" from "lookup not
// yet installed" so the error body can hint at the right next step.
func reindexInFlightError(collection, shardName string, preWire bool) error {
	if preWire {
		return fmt.Errorf(
			"%w: shard %q (collection %q): backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	}
	return fmt.Errorf(
		"%w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
	)
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
