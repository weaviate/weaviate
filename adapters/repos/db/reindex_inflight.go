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
	"errors"
	"fmt"
	"slices"
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

// AnyLiveReindexForShard answers the cluster-wide question: does DTM
// have any LIVE reindex task targeting (collection, shardName)?
//
// Replaces the prior filesystem-marker check, which only saw this node
// and lagged DTM's actual state. The lookup builder is installed by
// [DB.SetShardReindexActivityLookup] from the post-bootstrap goroutine
// in configure_api.go.
//
// Default to "no live reindex" when the lookup is unwired (with a
// one-time WARN). The original conservative default (refuse) was
// correct in isolation but broke every module-test fixture that
// spins up Weaviate without going through the post-bootstrap
// install path; production HTTP gates on bootstrap completion so the
// unwired window is unreachable by external traffic.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
	if db.config.RuntimeReindexDisabled {
		// Runtime reindex is off, so no new task can start. Return before
		// consulting the lookup so the backup path makes no reindex check
		// at all — the pre-gate behavior this restores.
		return false
	}
	db.reindexAuditMu.RLock()
	activityBuilder := db.shardReindexActivityLookupBuilder
	cleanupBuilder := db.reindexCleanupInProgressLookupBldr
	db.reindexAuditMu.RUnlock()
	if activityBuilder == nil {
		unwiredGateWarnOnce.Do(func() {
			logger := db.logger
			if logger == nil {
				logger = logrus.New()
			}
			logger.WithField("action", "backup_reindex_gate").
				Warn("backup-reindex gate: ShardReindexActivityLookup not yet installed; allowing backup. " +
					"Expected briefly during startup; if this persists past bootstrap, check the SetShardReindexActivityLookup wiring in configure_api.go.")
		})
		return false
	}
	lookup := activityBuilder()
	if lookup == nil {
		return false
	}
	if lookup(collection, shardName) {
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
		return true
	}
	// Cleanup lookup is OR-d in: the DTM task may have flipped to
	// terminal while autoCleanupAfterTerminal is still tearing the
	// sidecar buckets. The cleanup builder is optional — older
	// wiring paths and test fixtures that install only the activity
	// lookup keep the prior semantics.
	if cleanupBuilder == nil {
		return false
	}
	cleanupLookup := cleanupBuilder()
	if cleanupLookup == nil {
		return false
	}
	if cleanupLookup(collection, shardName) {
		if db.logger != nil {
			db.logger.WithField("action", "backup_reindex_gate").
				WithField("collection", collection).
				WithField("shard", shardName).
				WithField("reason", "cleanup_in_progress").
				Debug("backup-reindex gate: refusing — autoCleanupAfterTerminal still draining sidecars on this shard")
		}
		return true
	}
	return false
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
// It never logs — the whole-collection callers reach it once per shard, so a
// line here would repeat. The caller logs instead: one line per pass via
// [DB.logReindexRefusals] or [Index.logReindexRefusalSummary], and one line per
// shard via [Index.logReindexRefusal] for the single-shard callers
// ([Shard.HaltForTransfer] reached from IncomingCreateReplicaSnapshot).
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		// Index was constructed without a back-reference (test
		// fixtures, partial init). Be conservative.
		return reindexInFlightError(collection, true)
	}
	if !i.db.AnyLiveReindexForShard(collection, shardName) {
		return nil
	}
	return reindexInFlightError(collection, false)
}

// reindexRefusalShardSample caps the shard names carried in one refusal log
// line. The count beside it is exact; this only bounds the sample.
const reindexRefusalShardSample = 10

// logReindexRefusal records the shard and node the refusal body withholds. It
// is a no-op unless err is a gate refusal. Single-shard call sites only; a pass
// over many shards must use [Index.logReindexRefusalSummary].
func (i *Index) logReindexRefusal(shardName string, err error) {
	if err == nil || !errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex) {
		return
	}
	if i.logger == nil {
		return
	}
	i.logger.WithField("action", "backup_reindex_gate").
		WithField("collection", i.Config.ClassName.String()).
		WithField("shard", shardName).
		WithField("node", i.localNodeName()).
		Warn("backup-reindex gate: refused a replica shard copy; a runtime-reindex is live on this shard")
}

// logReindexRefusalSummary is [Index.logReindexRefusal] for a pass over many
// shards: one line for the whole pass, with an exact count and a capped sample
// of the names.
func (i *Index) logReindexRefusalSummary(shardNames []string) {
	logReindexRefusalPass(i.logger, "backup descriptor", i.localNodeName(),
		i.Config.ClassName.String(), shardNames)
}

// logReindexRefusalPass logs one line for a pass over many shards. Shared by
// [DB.logReindexRefusals] and [Index.logReindexRefusalSummary] so the two can't drift apart.
func logReindexRefusalPass(logger logrus.FieldLogger, stage, node, collection string, shardNames []string) {
	if len(shardNames) == 0 || logger == nil {
		return
	}
	// Sorted on a copy so repeated refusals diff cleanly without
	// mutating the caller's slice.
	sorted := slices.Sorted(slices.Values(shardNames))
	sample := sorted
	if len(sample) > reindexRefusalShardSample {
		sample = sample[:reindexRefusalShardSample]
	}
	logger.WithField("action", "backup_reindex_gate").
		WithField("collection", collection).
		WithField("node", node).
		WithField("blocked_shards", sample).
		WithField("blocked_shard_count", len(shardNames)).
		Warnf("%s refused: %d shard(s) of %q are held by the reindex gate; "+
			"blocked_shards lists the first %d", stage, len(shardNames), collection, len(sample))
}

// localNodeName is empty when the Index was built without its DB
// back-reference (test fixtures, partial init).
func (i *Index) localNodeName() string {
	if i.db == nil {
		return ""
	}
	return i.db.localNodeName
}

// reindexInFlightError formats the operator-facing rejection. `preWire`
// distinguishes "DTM lookup says live" from "lookup not yet installed" so
// the body can hint at the right next step.
//
// Unlike the schema gates, it has no task to key a cancel call on — only
// that a shard is live — so it points at the GET poll instead of guessing a
// property/index-type pair that could 202 NO_OP. For the same reason it
// cannot drop the cancel advice the way [ReindexGateRemedy] does once the
// task is past STARTED, so it states the STARTED-only restriction instead:
// TaskStatus.IsCancellable is a literal `== STARTED`, and every other status
// answers 409 Conflict.
//
// Names no shard and no node — this reaches an API response body. Those
// reach the operator via [Index.logReindexRefusal],
// [Index.logReindexRefusalSummary] and [DB.logReindexRefusals].
//
// `collection` ([Index.Config.ClassName]) is kept namespace-qualified as
// stored; canCommit runs synchronously inside coordinator.Backup, so the REST
// error path strips it before returning. The async backup-status field is
// not stripped.
func reindexInFlightError(collection string, preWire bool) error {
	if preWire {
		return entitiesbackup.ReindexBlockedError{Msg: fmt.Sprintf(
			"%s: collection %q: backup-gate lookup not yet installed (startup window); retry once the node has finished bootstrapping",
			entitiesbackup.ErrBackupBlockedByInFlightReindex, collection,
		)}
	}
	return entitiesbackup.ReindexBlockedError{Msg: fmt.Sprintf(
		"%s: collection %q has an active runtime-reindex task in DTM; retry after the migration finishes. GET /v1/schema/%s/indexes names the property and index type that are still migrating, and PUT /v1/schema/%s/indexes/{that property} with {\"{that index type}\":{\"cancel\":true}} ends the task early — but only while it is still in status STARTED, which GET /v1/tasks reports; from PREPARING or SWAPPING on that cancel is refused with 409 Conflict and waiting is the only option",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, collection, collection, collection,
	)}
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
