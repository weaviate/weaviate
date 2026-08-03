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

// unwiredGateWarnOnce keeps the "lookup not installed" WARN to one per
// process: the window is a startup-ordering fault, not steady state.
var unwiredGateWarnOnce sync.Once

// reindexGate amortizes one DTM snapshot (a cluster-wide RAFT query) across a
// whole shard-loop backup, built lazily so a node holding none of the
// requested shards issues no query. Safe for concurrent use: resolve runs
// under sync.Once, which orders the lookup writes before every read.
type reindexGate struct {
	// db is read at resolve time, not captured at construction, so a gate
	// built between the two setter calls in configure_api.go still sees both
	// builders.
	db     *DB
	logger logrus.FieldLogger

	once        sync.Once
	activity    ShardReindexActivityLookup
	activityErr error
	cleanup     CleanupInProgressLookup
}

// String stops fmt from reflecting over fields a concurrent resolve writes.
// Load-bearing, not decorative: the mockery-generated ShardLike mock hands the
// gate to testify's Called, and Arguments.Diff formats every argument with %v
// before consulting any matcher, so mock.Anything does not avoid it. Covers
// only the Stringer verbs — %#v, spew, and reflect.DeepEqual still reflect.
func (g *reindexGate) String() string { return "reindexGate" }

// Removing String would otherwise surface only as a data race in fmt frames.
var _ fmt.Stringer = (*reindexGate)(nil)

func (db *DB) newReindexGate() *reindexGate {
	return &reindexGate{db: db, logger: db.logger}
}

// newReindexGate builds the gate for one Index's backup pass; an Index without
// a DB back-reference yields an empty gate, which [Index.refuseIfReindexInFlight]
// refuses before use.
func (i *Index) newReindexGate() *reindexGate {
	if i.db == nil {
		return &reindexGate{}
	}
	return i.db.newReindexGate()
}

// installedBuilders reads the currently installed builders under the audit lock.
func (g *reindexGate) installedBuilders() (ShardReindexActivityLookupBuilder, CleanupInProgressLookupBuilder) {
	if g.db == nil {
		return nil, nil
	}
	g.db.reindexAuditMu.RLock()
	defer g.db.reindexAuditMu.RUnlock()
	return g.db.shardReindexActivityLookupBuilder, g.db.reindexCleanupInProgressLookupBldr
}

// resolve takes both lookups, at most once per gate. An unwired activity
// builder defaults to "no live reindex" — see
// [DB.SetShardReindexActivityLookup] for why fail-open.
func (g *reindexGate) resolve() {
	g.once.Do(func() {
		activityBuilder, cleanupBuilder := g.installedBuilders()
		if activityBuilder == nil {
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
		g.activity, g.activityErr = activityBuilder()
		if g.activityErr != nil || g.activity == nil {
			// A nil lookup fails open like an unwired builder; an error fails
			// closed in refusalReason. Either way cleanup need not be built.
			return
		}
		if cleanupBuilder != nil {
			g.cleanup = cleanupBuilder()
		}
	})
}

// reindexRefusal names why the gate refuses a backup, so the operator-facing
// error can say what to do about it.
type reindexRefusal int

const (
	reindexRefusalNone reindexRefusal = iota
	// reindexRefusalPreWire is an Index with no DB back-reference.
	reindexRefusalPreWire
	// reindexRefusalUnknown is a DTM snapshot that could not be taken.
	reindexRefusalUnknown
	reindexRefusalLiveTask
	reindexRefusalCleanup
)

// refusalReason reports why (collection, shardName) cannot be backed up now.
func (g *reindexGate) refusalReason(collection, shardName string) reindexRefusal {
	g.resolve()
	if g.activityErr != nil {
		return reindexRefusalUnknown
	}
	if g.activity == nil {
		return reindexRefusalNone
	}
	if g.activity(collection, shardName) {
		// Debug-level so operators can see which check refused.
		g.logRefusal(collection, shardName, "activity_lookup_live_task",
			"backup-reindex gate: refusing — DTM lists a live reindex task on this shard")
		return reindexRefusalLiveTask
	}
	// cleanup is OR'd in: a DTM task can flip terminal while
	// autoCleanupAfterTerminal is still tearing sidecar buckets down.
	if g.cleanup == nil {
		return reindexRefusalNone
	}
	if g.cleanup(collection, shardName) {
		g.logRefusal(collection, shardName, "cleanup_in_progress",
			"backup-reindex gate: refusing — autoCleanupAfterTerminal still draining sidecars on this shard")
		return reindexRefusalCleanup
	}
	return reindexRefusalNone
}

// anyLiveReindexForShard reports whether the gate refuses this shard, for
// callers that do not need to know why.
func (g *reindexGate) anyLiveReindexForShard(collection, shardName string) bool {
	return g.refusalReason(collection, shardName) != reindexRefusalNone
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

// SetReindexCleanupInProgressLookup installs the builder used by
// [reindexGate.refusalReason] to detect terminal-task cleanup still tearing
// down __reindex / __ingest sidecar dirs. Wired in post-bootstrap alongside
// [DB.SetShardReindexActivityLookup].
//
// Unlike the activity builder, the returned closure re-reads live state on
// every call rather than a fixed snapshot.
func (db *DB) SetReindexCleanupInProgressLookup(builder CleanupInProgressLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.reindexCleanupInProgressLookupBldr = builder
}

// refuseIfReindexInFlight is the per-shard backup-gate check shared by
// [DB.Backupable], the inactive-shard descriptor paths, and
// [Shard.HaltForTransfer]. Callers supply the gate so a multi-shard caller
// pays one DTM query for the whole set; an Index with no DB back-reference
// refuses, assuming wiring is still in progress.
func (i *Index) refuseIfReindexInFlight(shardName string, gate *reindexGate) error {
	if i.db == nil {
		return reindexInFlightError(i.Config.ClassName.String(), shardName, reindexRefusalPreWire)
	}
	if gate == nil {
		// A nil gate would panic in resolve.
		gate = i.newReindexGate()
	}
	reason := gate.refusalReason(i.Config.ClassName.String(), shardName)
	if reason == reindexRefusalNone {
		return nil
	}
	return reindexInFlightError(i.Config.ClassName.String(), shardName, reason)
}

// reindexInFlightError formats the operator-facing rejection. Each reason gets
// its own remediation, because "poll until the migration finishes" is wrong
// advice when there is no migration.
func reindexInFlightError(collection, shardName string, reason reindexRefusal) error {
	switch reason {
	case reindexRefusalPreWire:
		return fmt.Errorf(
			"%w: shard %q (collection %q): backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	case reindexRefusalUnknown:
		return fmt.Errorf(
			"%w: shard %q (collection %q): cannot read reindex state — listing distributed tasks failed, so backups are refused until the cluster's task manager is reachable again; check leader health and retry",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	case reindexRefusalCleanup:
		return fmt.Errorf(
			"%w: shard %q (collection %q): a finished runtime-reindex task is still removing its __reindex / __ingest buckets; retry in a few seconds",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	default:
		return fmt.Errorf(
			"%w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry after the migration finishes (poll GET /v1/schema/<class>/indexes until all indexes report status=\"ready\") or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, shardName, collection,
		)
	}
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
