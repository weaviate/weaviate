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
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/schema"
)

// MigrationRecoveryReindexer re-arms an unpromoted, iteration-complete
// migration's double-write mirrors as its shard loads, so a shard that never
// loads costs nothing. A change-tokenization's two halves are separate tasks.
type MigrationRecoveryReindexer struct {
	enabled       bool
	logger        logrus.FieldLogger
	schemaManager *schema.Manager
	// One fault, such as a disk failing under every tenant, would otherwise log once per shard load.
	faults *logrusext.Sampler

	mu       sync.Mutex
	provider *ReindexProvider
	// A reloaded shard re-arms the instances the provider already holds, so its
	// swap still finds the callbacks it registered.
	recovered map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric
}

func NewMigrationRecoveryReindexer(runtimeReindexEnabled bool, logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) *MigrationRecoveryReindexer {
	return &MigrationRecoveryReindexer{
		enabled:       runtimeReindexEnabled,
		logger:        logger,
		schemaManager: schemaManager,
		faults:        logrusext.NewSampler(logger, maxReportedErrors, time.Minute),
		recovered:     map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{},
	}
}

// AttachProvider seeds the provider with every task recovered so far, and
// later ones as they arrive, so OnGroupCompleted swaps on the already-armed
// instances instead of rehydrating. Shards may load before the provider exists.
func (r *MigrationRecoveryReindexer) AttachProvider(provider *ReindexProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.provider = provider
	// Copies, so a later recovery appending here never writes into a slice the provider holds.
	seed := make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric, len(r.recovered))
	for desc, byUnit := range r.recovered {
		seed[desc] = make(map[string][]*ShardReindexTaskGeneric, len(byUnit))
		for unitID, tasks := range byUnit {
			seed[desc][unitID] = slices.Clone(tasks)
		}
	}
	provider.SeedReindexTaskCache(seed)
}

// RecoverMigrations runs before the load reconciler, which must see the
// unmirrored stamp before it promotes anything.
func (r *MigrationRecoveryReindexer) RecoverMigrations(shard *Shard) []*ShardReindexTaskGeneric {
	if shard.migrationRecords == nil {
		return nil
	}
	return r.remember(r.recoverFromStore(shard.migrationRecords, shard.Name()))
}

func (r *MigrationRecoveryReindexer) recoverFromStore(store *MigrationRecordStore,
	shardName string,
) []*ShardReindexTaskGeneric {
	if !r.enabled {
		return nil
	}
	logger := r.logger.WithField("shard", shardName)
	fault := func(log func(l logrus.FieldLogger)) {
		r.faults.WithSampling(func(l logrus.FieldLogger) { log(l.WithField("shard", shardName)) })
	}
	if err := store.Load(); err != nil {
		fault(func(l logrus.FieldLogger) {
			l.Warnf("reindex recovery: the migration records could not be read; recovering nothing: %v", err)
		})
		return nil
	}
	someRecordsUnreadable := len(store.Unreadable()) > 0
	if someRecordsUnreadable {
		fault(func(l logrus.FieldLogger) {
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
		fault(func(l logrus.FieldLogger) {
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
		fault(func(l logrus.FieldLogger) {
			l.Errorf("reindex recovery: %d migration(s) awaiting their flip could not be armed with a double-write "+
				"mirror, so writes this shard takes now reach the pre-migration bucket only. "+
				"Their staged data is stale and will not be promoted over it. %s",
				len(stamped), migrationUnmirroredRemedy)
		})
	}
	if stampErr != nil {
		fault(func(l logrus.FieldLogger) {
			l.Errorf("reindex recovery: could not record that a migration's mirror stayed unarmed, "+
				"so a later promotion may still rename its stale staged data over the live bucket: %v", stampErr)
		})
	}
	return tasks
}

// remember swaps each built task for the instance an earlier load of the same
// shard recovered, and hands new ones to the provider.
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
	if r.provider != nil {
		r.provider.SeedReindexTaskCache(fresh)
	}
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
