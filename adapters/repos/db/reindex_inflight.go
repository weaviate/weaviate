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
	"time"

	"github.com/sirupsen/logrus"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/reindex"
)

const reindexGateWarnInterval = time.Hour

var (
	shardGateWarnBudget   reindexGateWarnBudget
	restoreGateWarnBudget reindexGateWarnBudget
)

type reindexGateWarnBudget struct {
	mu   sync.Mutex
	last time.Time
}

func (b *reindexGateWarnBudget) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.last.IsZero() && now.Sub(b.last) < reindexGateWarnInterval {
		return false
	}
	b.last = now
	return true
}

func (db *DB) warnUnwiredGate(budget *reindexGateWarnBudget, action, gate, detail string) {
	if !budget.allow(time.Now()) {
		return
	}
	logger := db.logger
	if logger == nil {
		logger = logrus.New()
	}
	logger.WithField("action", action).
		WithField("gate", gate).
		Warnf("%s gate: lookup not yet installed, so this check refuses nothing; "+
			"another check may still refuse the operation. %s", gate, detail)
}

const reindexRefusalSampleLimit = 10

// Copied, not sliced: the sample outlives the call as a log field, and
// authorization.Backups uppercases class lists in place.
func cappedSample(items []string) []string {
	return append([]string(nil), items[:min(len(items), reindexRefusalSampleLimit)]...)
}

// Unbudgeted on purpose, unlike warnUnwiredGate above: an unwired gate is one
// standing condition, while every refusal is a distinct event the operator is
// owed. One line per gate call, so per collection per node, never per shard
// or tenant.
func (db *DB) warnRefusal(action, reason, message string, fields logrus.Fields) {
	if db.logger == nil {
		return
	}
	fields["action"] = action
	fields["node"] = db.localNodeName
	fields["reason"] = reason
	db.logger.WithFields(fields).Warn(message)
}

const (
	reindexReasonLiveTask           = "activity_lookup_live_task"
	reindexReasonTaskListUnreadable = "task_list_unreadable"
)

// Unwired admits: refusing breaks every fixture that skips the install path.
func (db *DB) AnyLiveReindexForShard(collection, shardName string) bool {
	if db.config.RuntimeReindexDisabled {
		return false
	}
	db.reindexAuditMu.RLock()
	activityBuilder := db.shardReindexActivityLookupBuilder
	db.reindexAuditMu.RUnlock()
	if activityBuilder == nil {
		db.warnUnwiredGate(&shardGateWarnBudget, "backup_reindex_gate", "backup",
			"Check the SetShardReindexActivityLookup wiring in configure_api.go.")
		return false
	}
	lookup := activityBuilder()
	if lookup == nil {
		return false
	}
	return lookup(collection, shardName)
}

func (i *Index) refuseIfReindexInFlight(shardName string) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		return reindexStartupWindowRefusal(collection)
	}
	if i.db.AnyLiveReindexForShard(collection, shardName) {
		return reindexLiveTaskRefusal(collection)
	}
	// After, never as an argument to the call that asks: a hold raised during
	// the round-trip is invisible to a value read before it.
	if hold := i.db.ReindexHoldFor(collection); hold != ReindexHoldNone {
		return reindexHoldRefusal(collection, hold)
	}
	return nil
}

func (i *Index) refuseIfAnyShardReindexInFlight(shards []string) error {
	collection := i.Config.ClassName.String()
	if i.db == nil {
		return reindexStartupWindowRefusal(collection)
	}

	var (
		refusal error
		reason  string
		blocked int
		sample  []string
	)
	for _, shardName := range shards {
		if !i.db.AnyLiveReindexForShard(collection, shardName) {
			continue
		}
		blocked++
		if len(sample) < reindexRefusalSampleLimit {
			sample = append(sample, shardName)
		}
		reason, refusal = reindexReasonLiveTask, reindexLiveTaskRefusal(collection)
	}
	// One read for the whole loop, and after it. A hold is a property of the
	// collection, so reading it per shard costs one read per tenant and lets
	// the loop answer one way for the shards it saw before a hold was taken
	// and another for the rest. After the loop, because the round-trip inside
	// it is the window a teardown raises the hold in. A hold blocks every
	// shard, and never outranks a live task, the arm an operator can act on.
	if hold := i.db.ReindexHoldFor(collection); hold != ReindexHoldNone && len(shards) > 0 {
		blocked, sample = len(shards), cappedSample(shards)
		if refusal == nil {
			reason, refusal = hold.String(), reindexHoldRefusal(collection, hold)
		}
	}
	if refusal == nil {
		return nil
	}
	i.db.warnReindexRefusal(collection, reason, sample, blocked)
	return refusal
}

func (db *DB) warnReindexRefusal(collection, reason string, blockedShards []string, blockedCount int) {
	fields := logrus.Fields{
		"collection":          collection,
		"blocked_shard_count": blockedCount,
		"blocked_shards":      blockedShards,
	}
	if blockedCount == 1 && len(blockedShards) == 1 {
		fields["shard"] = blockedShards[0]
	}
	db.warnRefusal("backup_reindex_gate", reason,
		"backup-reindex gate: refusing this backup; the published refusal names the collection only", fields)
}

func blockedRefusal(detail string) error {
	return entitiesbackup.ReindexBlockedError{Msg: fmt.Sprintf("%s: %s",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, detail)}
}

func reindexLiveTaskRefusal(collection string) error {
	return blockedRefusal(fmt.Sprintf(
		"collection %q has an active runtime-reindex task in DTM; retry after the migration finishes, "+
			"that is, retry once that task reaches a terminal state. %s",
		collection, reindex.MigrationRemedy(collection)))
}

func reindexHoldRefusal(collection string, hold ReindexHold) error {
	return blockedRefusal(reindexHoldDetail(fmt.Sprintf("collection %q", collection), hold))
}

// An unrecognized kind refuses: admitting is the one answer nothing can undo.
func reindexHoldDetail(subject string, hold ReindexHold) string {
	switch hold {
	case ReindexHoldCleanup:
		return fmt.Sprintf("runtime-reindex cleanup is still removing its temporary "+
			"index files from %s; retry once the cleanup finishes", subject)
	default:
		return fmt.Sprintf("%s is held by runtime-reindex work this build does not recognize; "+
			"retry once every migration on it has finished", subject)
	}
}

func reindexStartupWindowRefusal(collection string) error {
	return blockedRefusal(fmt.Sprintf(
		"collection %q: backup-gate lookup not yet installed (startup window); "+
			"retry once the node has finished bootstrapping", collection))
}

// NoSearchableIndexError formats the 400 for a searchable-index operation
// (rebuild/algorithm change) on a property with no searchable index.
// Centralised for identical phrasing across call sites; not used for the
// inverse case (already has one), which carries the opposite meaning.
func NoSearchableIndexError(propertyName string) string {
	return fmt.Sprintf(
		"property %q has no searchable index; PUT /v1/schema/{className}/properties/%s/index/searchable with a tokenization to add one first",
		propertyName, propertyName,
	)
}
