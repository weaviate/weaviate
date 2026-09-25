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
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/schema"
)

type RecoveredReindex struct {
	Descriptor distributedtask.TaskDescriptor
	UnitID     string
	Collection string
	ShardName  string
	Tasks      []*ShardReindexTaskGeneric
}

// DiscoverInFlightReindexTasks rebuilds, from every shard's migration records,
// the tasks of each migration whose iteration finished but whose flip is not
// promoted, so shard load re-arms their double-write mirrors before writes
// arrive. It runs before Raft opens, so the records are its only source. The
// two halves of a change-tokenization are separate entries.
func DiscoverInFlightReindexTasks(
	rootPath string,
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) ([]RecoveredReindex, error) {
	if rootPath == "" {
		return nil, nil
	}
	indices, err := os.ReadDir(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read root %q: %w", rootPath, err)
	}

	var recovered []RecoveredReindex
	// Faults are accumulated, not logged per shard: a per-shard line would follow the tenant count.
	var (
		unreadable   = map[string]struct{}{}
		unreadErrs   = errorcompounder.New()
		partlyUnread = map[string]struct{}{}
		shardsWalked int
		recordReads  int

		unbuildable     = map[string]struct{}{}
		unbuildableErrs = errorcompounder.New()

		unmirroredRecords = map[string]struct{}{}
		unmirroredNames   []string
		unmirroredErrs    = errorcompounder.New()
	)
	for _, indexEntry := range indices {
		if !indexEntry.IsDir() {
			continue
		}
		indexPath := filepath.Join(rootPath, indexEntry.Name())
		shards, err := os.ReadDir(indexPath)
		if err != nil {
			continue
		}
		for _, shardEntry := range shards {
			if !shardEntry.IsDir() {
				continue
			}
			shardsWalked++
			shardName := shardEntry.Name()
			shardKey := indexEntry.Name() + "/" + shardName
			lsmPath := filepath.Join(indexPath, shardName, "lsm")
			recordReads++
			store, someRecordsUnreadable, recordSetErr := migrationRecordStoreAt(lsmPath, logger)
			if recordSetErr != nil {
				unreadable[shardKey] = struct{}{}
				unreadErrs.AddWrapf(recordSetErr, "%s", shardKey)
				continue
			}
			if someRecordsUnreadable {
				partlyUnread[shardKey] = struct{}{}
			}
			records := store.Records()
			armable := map[MigrationRecordKey]struct{}{}
			for _, rec := range records {
				if !rec.IterationComplete() || rec.State() == MigrationStatePromoted {
					continue
				}
				subject := rec.Subject()
				tasks, err := buildRecoveryTasks(subject, shardName, logger, schemaManager)
				if err != nil {
					recordKey := shardKey + "/" + subject.Key.String()
					unbuildable[recordKey] = struct{}{}
					unbuildableErrs.AddWrapf(err, "%s", recordKey)
					continue
				}
				armable[subject.Key] = struct{}{}
				recovered = append(recovered, RecoveredReindex{
					Descriptor: distributedtask.TaskDescriptor{
						ID:      subject.TaskID,
						Version: subject.Key.TaskVersion,
					},
					UnitID:     subject.Key.UnitID,
					Collection: subject.Collection,
					ShardName:  shardName,
					Tasks:      tasks,
				})
			}
			if someRecordsUnreadable {
				// An unreadable record freezes the store and the reconciler, so nothing the stamp guards can run.
				continue
			}
			stamped, stampErr := stampUnmirroredRecords(store, records, armable)
			if len(stamped) > 0 {
				unmirroredRecords[shardKey] = struct{}{}
				unmirroredNames = append(unmirroredNames, stamped...)
			}
			if stampErr != nil {
				unmirroredErrs.AddWrapf(stampErr, "%s", shardKey)
			}
		}
	}

	logger.WithField("shards", shardsWalked).WithField("record_set_reads", recordReads).
		Debug("reindex recovery: read migration records")

	if len(unreadable) > 0 {
		logger.WithField("shards", reportedShardNames(unreadable)).
			Warnf("reindex recovery: the migration records of %d shard(s) could not be read; "+
				"recovering nothing on them: %v", len(unreadable), unreadErrs.ToErrorLimited(maxReportedErrors))
	}
	if len(partlyUnread) > 0 {
		logger.WithField("shards", reportedShardNames(partlyUnread)).
			Warnf("reindex recovery: some migration records of %d shard(s) could not be read; "+
				"recovering only the migrations the readable records name", len(partlyUnread))
	}
	if len(unbuildable) > 0 {
		logger.WithField("records", reportedShardNames(unbuildable)).
			Warnf("reindex recovery: the record of %d migration(s) builds no reindex task, so those "+
				"migrations' mirrors stay unarmed: %v",
				len(unbuildable), unbuildableErrs.ToErrorLimited(maxReportedErrors))
	}
	if len(unmirroredRecords) > 0 {
		logger.WithField("shards", reportedShardNames(unmirroredRecords)).
			WithField("record_count", len(unmirroredNames)).
			Errorf("reindex recovery: %d migration(s) awaiting their flip could not be armed with a double-write "+
				"mirror on %d shard(s), so writes this node takes now reach the pre-migration bucket only. "+
				"Their staged data is stale and will not be promoted over it. %s",
				len(unmirroredNames), len(unmirroredRecords), migrationUnmirroredRemedy)
	}
	if err := unmirroredErrs.ToErrorLimited(maxReportedErrors); err != nil {
		logger.Errorf("reindex recovery: could not record that a migration's mirror stayed unarmed, "+
			"so a later promotion may still rename its stale staged data over the live bucket: %v", err)
	}
	return recovered, nil
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
	version := subject.Key.TaskVersion
	if version < 1 || version > math.MaxInt {
		return nil, fmt.Errorf("task version %d cannot name a migration generation (must be 1..%d)",
			version, math.MaxInt)
	}
	generation := int(version)
	var raw []*ShardReindexTaskGeneric
	switch payload.MigrationType {
	case ReindexTypeChangeAlgorithm:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(logger, schemaManager, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeRepairFilterable:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeRoaringSetRefreshTask(logger, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeFilterableToRangeableTask(logger, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeEnableFilterable:
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeEnableFilterableTask(logger, payload.Properties, payload.Collection, generation),
		}
	case ReindexTypeEnableSearchable:
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("%s requires targetTokenization", payload.MigrationType)
		}
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeEnableSearchableTask(logger, payload.Properties, payload.Collection, payload.TargetTokenization, generation),
		}
	case ReindexTypeChangeTokenization:
		if len(payload.Properties) != 1 {
			return nil, fmt.Errorf("change-tokenization requires exactly one property")
		}
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("change-tokenization requires targetTokenization")
		}
		if payload.BucketStrategy == "" {
			return nil, fmt.Errorf("change-tokenization requires bucketStrategy")
		}
		propName := payload.Properties[0]
		switch subject.Key.StrategyCode {
		case StrategyCodeSearchableRetokenize:
			raw = []*ShardReindexTaskGeneric{
				NewRuntimeSearchableRetokenizeTask(
					logger, propName, payload.TargetTokenization,
					payload.Collection, payload.BucketStrategy, payload.Collection,
					generation,
				),
			}
		case StrategyCodeFilterableRetokenize:
			raw = []*ShardReindexTaskGeneric{
				NewRuntimeFilterableRetokenizeTask(
					logger,
					propName, payload.TargetTokenization,
					payload.Collection, payload.Collection,
					generation,
				),
			}
		default:
			return nil, fmt.Errorf(
				"strategy %q names neither half of a change-tokenization migration", subject.Key.StrategyCode)
		}
	case ReindexTypeChangeTokenizationFilterable:
		if len(payload.Properties) != 1 {
			return nil, fmt.Errorf("change-tokenization-filterable requires exactly one property")
		}
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("change-tokenization-filterable requires targetTokenization")
		}
		propName := payload.Properties[0]
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeFilterableRetokenizeTask(
				logger,
				propName, payload.TargetTokenization,
				payload.Collection, payload.Collection,
				generation,
			),
		}
	default:
		return nil, fmt.Errorf("unknown migration type %q", payload.MigrationType)
	}

	desc := distributedtask.TaskDescriptor{ID: subject.TaskID, Version: version}
	for _, t := range raw {
		t.constrainToShard(payload.Collection, shardName)
		t.setMigrationIdentity(desc, subject.Key.UnitID, &payload)
	}
	return raw, nil
}

// NewShardReindexerV3FromRecovered wires recovered tasks into a
// recovery-only [ShardReindexerV3] that only fires [OnAfterLsmInit];
// the DTM's OnGroupCompleted owns the swap step, keeping recovery's
// job narrow: re-install double-write callbacks before writes arrive.
func NewShardReindexerV3FromRecovered(
	recovered []RecoveredReindex,
	logger logrus.FieldLogger,
) ShardReindexerV3 {
	r := newShardReindexerV3RecoveryOnly(logger)
	for _, rr := range recovered {
		for _, t := range rr.Tasks {
			r.registerTask(t)
		}
	}
	return r
}

// SeedReindexProviderFromRecovery pre-populates the provider's
// per-descriptor task cache with instances reconstructed during startup
// recovery. The purpose is to make [ReindexProvider.OnGroupCompleted]
// reuse the recovered instances — whose double-write callbacks were
// re-registered during shard init — rather than fall through to the
// rehydrate branch and call [OnAfterLsmInit] a second time (which would
// attempt to load already-loaded ingest buckets).
//
// Pass the same slice as was given to [NewShardReindexerV3FromRecovered]
// so the in-memory instances stay in sync between the two consumers.
func SeedReindexProviderFromRecovery(provider *ReindexProvider, recovered []RecoveredReindex) {
	if provider == nil || len(recovered) == 0 {
		return
	}
	perDescUnit := map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{}
	for _, rr := range recovered {
		if rr.UnitID == "" {
			continue
		}
		if perDescUnit[rr.Descriptor] == nil {
			perDescUnit[rr.Descriptor] = map[string][]*ShardReindexTaskGeneric{}
		}
		perDescUnit[rr.Descriptor][rr.UnitID] = append(
			perDescUnit[rr.Descriptor][rr.UnitID], rr.Tasks...)
	}
	provider.SeedReindexTaskCache(perDescUnit)
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

// shardReindexerV3RecoveryOnly is a stripped-down [ShardReindexerV3]
// used during startup recovery. It only fires [OnAfterLsmInit] for each
// registered task on each shard load; the heavier iteration / scheduler
// path is left to the distributed task provider so we don't bring up a
// second scheduling loop just for recovery. See
// [NewShardReindexerV3FromRecovered] for the rationale.
type shardReindexerV3RecoveryOnly struct {
	logger logrus.FieldLogger
	tasks  []*ShardReindexTaskGeneric
}

func newShardReindexerV3RecoveryOnly(logger logrus.FieldLogger) *shardReindexerV3RecoveryOnly {
	return &shardReindexerV3RecoveryOnly{
		logger: logger,
	}
}

func (r *shardReindexerV3RecoveryOnly) registerTask(t *ShardReindexTaskGeneric) {
	r.tasks = append(r.tasks, t)
}

func (r *shardReindexerV3RecoveryOnly) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	if len(r.tasks) == 0 {
		return nil
	}
	for _, t := range r.tasks {
		if err := t.OnAfterLsmInit(ctx, shard); err != nil {
			r.logger.WithField("task", t.Name()).WithField("shard", shard.Name()).
				Errorf("reindex recovery: after-LSM-init failed: %v", err)
		}
	}
	return nil
}
