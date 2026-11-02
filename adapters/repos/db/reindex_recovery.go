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
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/schema"
)

// MigrationRecoveryReindexer builds, as each shard loads, the tasks whose mirrors
// the shard re-arms, and stamps Unmirrored the records it cannot build a task
// from. A shard that never loads costs nothing.
type MigrationRecoveryReindexer struct {
	enabled       bool
	logger        logrus.FieldLogger
	schemaManager *schema.Manager
	// Seeded so OnGroupCompleted swaps on the armed instances instead of rehydrating.
	provider *ReindexProvider

	// One fault, such as a disk failing under every tenant, would otherwise log
	// once per shard load. Per kind, so one flooding kind cannot hide another.
	unreadableFaults, unbuildableFaults, unarmedFaults, armFaults, stampFaults recoveryFaultLog

	mu sync.Mutex
	// A reloaded shard re-arms the instances the provider already holds, so its
	// swap still finds the callbacks it registered.
	recovered map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric
}

func NewMigrationRecoveryReindexer(runtimeReindexEnabled bool, logger logrus.FieldLogger,
	schemaManager *schema.Manager, provider *ReindexProvider,
) *MigrationRecoveryReindexer {
	return &MigrationRecoveryReindexer{
		enabled:       runtimeReindexEnabled,
		logger:        logger,
		schemaManager: schemaManager,
		provider:      provider,
		recovered:     map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{},
	}
}

// RecoverMigrations must run before the load reconciler, which needs to see
// the unmirrored stamp before it promotes anything.
func (r *MigrationRecoveryReindexer) RecoverMigrations(shard *Shard) []*ShardReindexTaskGeneric {
	return r.remember(r.recoverFromStore(shard.migrationRecords, shard.Name()))
}

// AbandonUnarmed stamps a migration whose mirror failed to arm, so a later swap of
// now-stale data is blocked; resubmitting is the way out. A failed stamp write is
// returned so the shard load fails instead of leaving that swap unguarded.
func (r *MigrationRecoveryReindexer) AbandonUnarmed(shard *Shard, unarmed []*ShardReindexTaskGeneric, cause error) error {
	shardName := shard.Name()
	r.armFaults.report(r.logger.WithField("shard", shardName), func(l logrus.FieldLogger) {
		l.Errorf("reindex recovery: %d migration(s) could not re-arm their double-write mirror, so they will "+
			"not be swapped in: %v. %s", len(unarmed), cause, migrationUnmirroredRemedy)
	})
	var records []MigrationRecord
	for _, t := range unarmed {
		if rec, ok := shard.migrationRecords.Get(t.migrationRecordKey()); ok {
			records = append(records, rec)
		}
	}
	r.forget(unarmed)
	if _, err := stampUnmirroredRecords(shard.migrationRecords, records, nil); err != nil {
		return fmt.Errorf("record that a migration's mirror stayed unarmed: %w", err)
	}
	return nil
}

// forget drops every task of each unarmed task's unit: a cached sibling alone
// would swap one half of the migration and report the unit done.
func (r *MigrationRecoveryReindexer) forget(unarmed []*ShardReindexTaskGeneric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range unarmed {
		desc := distributedtask.TaskDescriptor{ID: t.taskID, Version: t.taskVersion}
		delete(r.recovered[desc], t.unitID)
		r.provider.evictReindexTasks(desc, t.unitID)
	}
}

func (r *MigrationRecoveryReindexer) recoverFromStore(store *MigrationRecordStore,
	shardName string,
) []*ShardReindexTaskGeneric {
	if !r.enabled {
		return nil
	}
	logger := r.logger.WithField("shard", shardName)
	someRecordsUnreadable := len(store.Unreadable()) > 0
	if someRecordsUnreadable {
		r.unreadableFaults.report(logger, func(l logrus.FieldLogger) {
			l.Warn("reindex recovery: some migration records could not be read; " +
				"recovering only the migrations the readable records name")
		})
	}

	var tasks []*ShardReindexTaskGeneric
	records := store.Records()
	armable := map[MigrationRecordKey]struct{}{}
	unbuildable := errorcompounder.New()
	unbuildableCount := 0
	for _, rec := range records {
		if !rec.IterationComplete() || rec.State() == MigrationStatePromoted {
			continue
		}
		subject := rec.Subject()
		built, err := buildRecoveryTasks(subject, shardName, logger, r.schemaManager)
		if err != nil {
			unbuildableCount++
			unbuildable.AddWrapf(err, "%s", subject.Key)
			continue
		}
		armable[subject.Key] = struct{}{}
		tasks = append(tasks, built...)
	}
	if unbuildableCount > 0 {
		r.unbuildableFaults.report(logger, func(l logrus.FieldLogger) {
			l.Warnf("reindex recovery: the record of %d migration(s) builds no reindex task, so those "+
				"migrations' mirrors stay unarmed: %v", unbuildableCount, unbuildable.ToErrorLimited(maxReportedErrors))
		})
	}
	if someRecordsUnreadable {
		// An unreadable record freezes the store and the reconciler, so nothing the stamp guards can run.
		return tasks
	}

	stamped, stampErr := stampUnmirroredRecords(store, records, armable)
	if len(stamped) > 0 {
		r.unarmedFaults.report(logger, func(l logrus.FieldLogger) {
			l.Errorf("reindex recovery: %d migration(s) awaiting their flip could not be armed with a double-write "+
				"mirror, so writes this shard takes now reach the pre-migration bucket only. "+
				"Their staged data is stale and will not be promoted over it. %s",
				len(stamped), migrationUnmirroredRemedy)
		})
	}
	if stampErr != nil {
		r.reportStampFault(shardName, stampErr)
	}
	return tasks
}

func (r *MigrationRecoveryReindexer) reportStampFault(shardName string, err error) {
	r.stampFaults.report(r.logger.WithField("shard", shardName), func(l logrus.FieldLogger) {
		l.Errorf("reindex recovery: could not record that a migration's mirror stayed unarmed, "+
			"so a later promotion may still rename its stale staged data over the live bucket: %v", err)
	})
}

// remember replaces each built task with the instance an earlier load of the
// same shard recovered, and seeds the provider with new ones.
func (r *MigrationRecoveryReindexer) remember(built []*ShardReindexTaskGeneric) []*ShardReindexTaskGeneric {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*ShardReindexTaskGeneric, 0, len(built))
	fresh := map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{}
	for _, t := range built {
		desc := distributedtask.TaskDescriptor{ID: t.taskID, Version: t.taskVersion}
		if known := r.knownTask(desc, t); known != nil {
			out = append(out, known)
			continue
		}
		if r.recovered[desc] == nil {
			r.recovered[desc] = map[string][]*ShardReindexTaskGeneric{}
		}
		r.recovered[desc][t.unitID] = append(r.recovered[desc][t.unitID], t)
		if fresh[desc] == nil {
			fresh[desc] = map[string][]*ShardReindexTaskGeneric{}
		}
		fresh[desc][t.unitID] = append(fresh[desc][t.unitID], t)
		out = append(out, t)
	}
	r.provider.SeedReindexTaskCache(fresh)
	return out
}

func (r *MigrationRecoveryReindexer) knownTask(desc distributedtask.TaskDescriptor,
	t *ShardReindexTaskGeneric,
) *ShardReindexTaskGeneric {
	for _, known := range r.recovered[desc][t.unitID] {
		if known.migrationRecordKey() == t.migrationRecordKey() {
			return known
		}
	}
	return nil
}

// recoveryFaultLog caps one fault kind at maxReportedErrors lines a minute
// across shard loads; the next line it lets through counts the ones it dropped.
type recoveryFaultLog struct {
	mu          sync.Mutex
	windowStart time.Time
	logged      int
	dropped     int
}

func (f *recoveryFaultLog) report(l logrus.FieldLogger, emit func(logrus.FieldLogger)) {
	dropped, ok := f.admit(time.Now())
	if !ok {
		return
	}
	if dropped > 0 {
		l = l.WithField("dropped_lines", dropped)
	}
	emit(l)
}

func (f *recoveryFaultLog) admit(now time.Time) (dropped int, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if now.Sub(f.windowStart) > time.Minute {
		f.windowStart, f.logged = now, 0
	}
	if f.logged >= maxReportedErrors {
		f.dropped++
		return 0, false
	}
	f.logged++
	dropped, f.dropped = f.dropped, 0
	return dropped, true
}

const migrationUnmirroredRemedy = "The migration fails at its next prepare or swap step; once it has failed and " +
	"the cause is cleared, submit a new migration covering the same properties."

// stampUnmirroredRecords stops a later promotion from renaming stale staged data over the live bucket.
func stampUnmirroredRecords(store *MigrationRecordStore, records []MigrationRecord,
	armable map[MigrationRecordKey]struct{},
) ([]string, error) {
	var stamped []string
	errs := errorcompounder.New()
	for _, rec := range records {
		subject := rec.Subject()
		if subject.Unmirrored {
			continue
		}
		if _, ok := armable[subject.Key]; ok {
			continue
		}
		next, stampable := migrationRecordStampedUnmirrored(rec)
		if !stampable {
			continue
		}
		if err := store.Put(next); err != nil {
			errs.AddWrapf(err, "%s", subject.Key)
			continue
		}
		stamped = append(stamped, subject.Key.String())
	}
	return stamped, errs.ToErrorLimited(maxReportedErrors)
}

func buildRecoveryTasks(
	subject MigrationSubject,
	shardName string,
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) ([]*ShardReindexTaskGeneric, error) {
	payload := subject.reindexPayload()
	if payload.Collection == "" {
		return nil, fmt.Errorf("record names no collection")
	}
	// A record for a filterable half exists only because the live path built that half.
	hasFilterableBucket := func(string, string) bool { return true }
	built, err := buildMigrationTasks(logger, schemaManager, subject.Key.TaskVersion, &payload, hasFilterableBucket)
	if err != nil {
		return nil, err
	}
	desc := distributedtask.TaskDescriptor{ID: subject.TaskID, Version: subject.Key.TaskVersion}
	for _, t := range built {
		if t.strategy.StrategyCode() != subject.Key.StrategyCode {
			continue
		}
		t.constrainToShard(payload.Collection, shardName)
		t.setMigrationIdentity(desc, subject.Key.UnitID, &payload)
		return []*ShardReindexTaskGeneric{t}, nil
	}
	return nil, fmt.Errorf("strategy %q names no task of a %s migration", subject.Key.StrategyCode, payload.MigrationType)
}

// constrainToShard narrows a task's shard selection to exactly the
// named shard of the named collection. After recovery this is called
// on every reconstructed task so the per-shard runtimeSwap / disable
// flow doesn't accidentally touch other shards' callbacks.
func (t *ShardReindexTaskGeneric) constrainToShard(collection, shardName string) {
	t.config.selectionEnabled = true
	if t.config.selectedShardsByCollection == nil {
		t.config.selectedShardsByCollection = map[string]map[string]struct{}{}
	}
	t.config.selectedShardsByCollection[collection] = map[string]struct{}{
		shardName: {},
	}
	// Give each per-shard task a unique name so log lines and error
	// messages stay distinguishable when several shards of the same
	// collection are recovered.
	if !strings.Contains(t.name, "[recovery:") {
		t.name = fmt.Sprintf("%s[recovery:%s/%s]", t.name, collection, shardName)
		t.logger = t.logger.WithField("task", t.name)
	}
}
