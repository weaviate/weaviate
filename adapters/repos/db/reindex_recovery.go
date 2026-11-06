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
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/schema"
)

// ReindexRecoveryHook arms, at every shard load (a COLD tenant's reactivation
// too), the mirror of each migration its records still owe a swap, before any write.
type ReindexRecoveryHook struct {
	enabled       bool
	logger        logrus.FieldLogger
	schemaManager *schema.Manager
	faults        *logrusext.Sampler

	mu       sync.Mutex
	provider *ReindexProvider
	pending  map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric
}

// NewReindexRecoveryHook arms nothing while runtime reindex is disabled.
func NewReindexRecoveryHook(enabled bool, logger logrus.FieldLogger, schemaManager *schema.Manager) *ReindexRecoveryHook {
	return &ReindexRecoveryHook{
		enabled: enabled, logger: logger, schemaManager: schemaManager,
		faults:  logrusext.NewSampler(logger, maxReportedErrors, migrationClusterReconcileInterval),
		pending: map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{},
	}
}

// AttachProvider lets PREP and the swap reuse armed instances instead of arming twice.
func (r *ReindexRecoveryHook) AttachProvider(provider *ReindexProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.provider = provider
	provider.SeedReindexTaskCache(r.pending)
	r.pending = nil
}

func (r *ReindexRecoveryHook) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	store := shard.migrationRecordStore()
	if !r.enabled || store == nil {
		return nil
	}
	tasks := r.recoverableTasks(store, shard.Name())
	armed := map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{}
	for _, t := range tasks {
		if err := t.OnAfterLsmInit(ctx, shard); err != nil {
			r.faults.WithSampling(func(l logrus.FieldLogger) {
				l.WithField("task", t.Name()).WithField("shard", shard.Name()).
					Errorf("reindex recovery: after-LSM-init failed: %v", err)
			})
		}
		desc := distributedtask.TaskDescriptor{ID: t.taskID, Version: t.taskVersion}
		if armed[desc] == nil {
			armed[desc] = map[string][]*ShardReindexTaskGeneric{}
		}
		armed[desc][t.unitID] = append(armed[desc][t.unitID], t)
	}
	if len(armed) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.provider != nil {
		r.provider.SeedReindexTaskCache(armed)
		return nil
	}
	for desc, byUnit := range armed {
		if r.pending[desc] == nil {
			r.pending[desc] = map[string][]*ShardReindexTaskGeneric{}
		}
		for unitID, ts := range byUnit {
			r.pending[desc][unitID] = append(r.pending[desc][unitID], ts...)
		}
	}
	return nil
}

// recoverableTasks builds the tasks of each migration whose iteration finished and
// is not promoted, and stamps a record it cannot build unmirrored, so its stale
// staged data is never promoted.
func (r *ReindexRecoveryHook) recoverableTasks(store *MigrationRecordStore, shardName string) []*ShardReindexTaskGeneric {
	records := store.Records()
	armable := map[MigrationRecordKey]struct{}{}
	var tasks []*ShardReindexTaskGeneric
	unbuildable := errorcompounder.New()
	for _, rec := range records {
		if !rec.IterationComplete() || rec.State() == MigrationStatePromoted {
			continue
		}
		subject := rec.Subject()
		built, err := buildRecoveryTasks(subject, shardName, r.logger, r.schemaManager)
		if err != nil {
			unbuildable.AddWrapf(err, "%s", subject.Key)
			continue
		}
		armable[subject.Key] = struct{}{}
		tasks = append(tasks, built...)
	}
	if err := unbuildable.ToErrorLimited(maxReportedErrors); err != nil {
		r.faults.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("shard", shardName).Warnf("reindex recovery: a record builds no reindex task, so "+
				"that migration's mirror stays unarmed: %v", err)
		})
	}
	if len(store.Unreadable()) > 0 {
		// An unreadable record freezes the store and the reconciler, so nothing the stamp guards can run.
		r.faults.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("shard", shardName).Warn("reindex recovery: some migration records could not be read; " +
				"arming only the migrations the readable records name")
		})
		return tasks
	}
	stamped, stampErr := stampUnmirroredRecords(store, records, armable)
	if len(stamped) > 0 {
		r.faults.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("shard", shardName).WithField("records", stamped).
				Errorf("reindex recovery: %d migration(s) awaiting their flip could not be armed with a double-write "+
					"mirror, so writes this node takes now reach the pre-migration bucket only. "+
					"Their staged data is stale and will not be promoted over it. %s", len(stamped), migrationUnmirroredRemedy)
		})
	}
	if stampErr != nil {
		r.faults.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("shard", shardName).Errorf("reindex recovery: could not record that a migration's mirror "+
				"stayed unarmed, so a later promotion may still rename its stale staged data over the live bucket: %v", stampErr)
		})
	}
	return tasks
}

const migrationUnmirroredRemedy = "Submit a new migration covering the same properties once the cause is cleared."

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
