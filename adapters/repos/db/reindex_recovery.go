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
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/schema"
)

// MigrationRecoveryReindexer builds, as each shard loads, the tasks whose mirrors the shard re-arms,
// and stamps Unmirrored the records it cannot build a task from.
type MigrationRecoveryReindexer struct {
	enabled       bool
	logger        logrus.FieldLogger
	schemaManager *schema.Manager
	provider      *ReindexProvider

	// One sampler per fault kind, so a fault on every tenant logs a few lines and hides no other kind.
	unreadableFaults, unbuildableFaults, unarmedFaults, armFaults, stampFaults *logrusext.Sampler
}

func NewMigrationRecoveryReindexer(runtimeReindexEnabled bool, logger logrus.FieldLogger,
	schemaManager *schema.Manager, provider *ReindexProvider,
) *MigrationRecoveryReindexer {
	faults := func() *logrusext.Sampler { return logrusext.NewSampler(logger, maxReportedErrors, time.Minute) }
	return &MigrationRecoveryReindexer{
		enabled:       runtimeReindexEnabled,
		logger:        logger,
		schemaManager: schemaManager,
		provider:      provider,

		unreadableFaults: faults(), unbuildableFaults: faults(), unarmedFaults: faults(),
		armFaults: faults(), stampFaults: faults(),
	}
}

// RecoverMigrations runs before the load reconciler, which must see the stamp before it promotes.
func (r *MigrationRecoveryReindexer) RecoverMigrations(shard *Shard) []*ShardReindexTaskGeneric {
	return r.reuseCachedTasks(r.recoverFromStore(shard.migrationRecords, shard.Name()))
}

// AbandonUnarmed stamps Unmirrored each migration whose mirror failed to arm, so its stale
// staged data is never swapped in. On error the tasks stay cached for a later arm.
func (r *MigrationRecoveryReindexer) AbandonUnarmed(shard *Shard, unarmed []*ShardReindexTaskGeneric, cause error) error {
	var records []MigrationRecord
	for _, t := range unarmed {
		rec, ok := shard.migrationRecords.Get(t.migrationRecordKey())
		if !ok {
			continue
		}
		// Past the swap, a stamp would block the promotion that serves the new index.
		if rec.FlipDecided() {
			return fmt.Errorf("migration %s is already swapped in, so only its mirror can finish it: %w",
				t.migrationRecordKey(), cause)
		}
		records = append(records, rec)
	}
	if _, err := stampUnmirroredRecords(shard.migrationRecords, records, nil); err != nil {
		return fmt.Errorf("stamp Unmirrored the migrations whose mirror failed to arm (%w): %w", cause, err)
	}
	r.evictUnits(unarmed)
	r.armFaults.WithSampling(func(logrus.FieldLogger) {
		r.logger.WithField("shard", shard.Name()).Errorf("reindex recovery: %d migration(s) could not re-arm "+
			"their double-write mirror, so they will not be swapped in: %v. %s", len(unarmed), cause,
			migrationUnmirroredRemedy)
	})
	return nil
}

// evictUnits drops whole units, because a cached sibling alone would swap half a migration.
func (r *MigrationRecoveryReindexer) evictUnits(unarmed []*ShardReindexTaskGeneric) {
	for _, t := range unarmed {
		r.provider.evictReindexTasks(distributedtask.TaskDescriptor{ID: t.taskID, Version: t.taskVersion}, t.unitID)
	}
}

func (r *MigrationRecoveryReindexer) recoverFromStore(store *MigrationRecordStore,
	shardName string,
) []*ShardReindexTaskGeneric {
	// The flag keeps recovery off, so a load with the feature disabled behaves as it always has.
	if !r.enabled {
		return nil
	}
	logger := r.logger.WithField("shard", shardName)
	someRecordsUnreadable := len(store.Unreadable()) > 0
	if someRecordsUnreadable {
		r.unreadableFaults.WithSampling(func(logrus.FieldLogger) {
			logger.Warn("reindex recovery: some migration records could not be read; " +
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
		r.unbuildableFaults.WithSampling(func(logrus.FieldLogger) {
			logger.Warnf("reindex recovery: the record of %d migration(s) builds no reindex task, so those "+
				"migrations' mirrors stay unarmed: %v", unbuildableCount, unbuildable.ToErrorLimited(maxReportedErrors))
		})
	}
	if someRecordsUnreadable {
		// An unreadable record freezes the store and the reconciler, so nothing the stamp guards can run.
		return tasks
	}

	stamped, stampErr := stampUnmirroredRecords(store, records, armable)
	if len(stamped) > 0 {
		r.unarmedFaults.WithSampling(func(logrus.FieldLogger) {
			logger.Errorf("reindex recovery: %d migration(s) awaiting their flip could not be armed with a double-write "+
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
	r.stampFaults.WithSampling(func(logrus.FieldLogger) {
		r.logger.WithField("shard", shardName).Errorf("reindex recovery: could not record that a migration's mirror stayed unarmed, "+
			"so a later promotion may still rename its stale staged data over the live bucket: %v", err)
	})
}

// reuseCachedTasks reuses the provider's cached instances, so a reload re-arms the callbacks the swap uses.
func (r *MigrationRecoveryReindexer) reuseCachedTasks(built []*ShardReindexTaskGeneric) []*ShardReindexTaskGeneric {
	out := make([]*ShardReindexTaskGeneric, 0, len(built))
	fresh := map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{}
	for _, t := range built {
		desc := distributedtask.TaskDescriptor{ID: t.taskID, Version: t.taskVersion}
		cached := r.provider.cachedReindexTasks(desc, t.unitID)
		if i := slices.IndexFunc(cached, func(c *ShardReindexTaskGeneric) bool {
			return c.migrationRecordKey() == t.migrationRecordKey()
		}); i >= 0 {
			out = append(out, cached[i])
			continue
		}
		if fresh[desc] == nil {
			fresh[desc] = map[string][]*ShardReindexTaskGeneric{}
		}
		fresh[desc][t.unitID] = append(fresh[desc][t.unitID], t)
		out = append(out, t)
	}
	r.provider.SeedReindexTaskCache(fresh)
	return out
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
