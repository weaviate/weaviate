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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/schema"
)

// RecoveredReindex describes one in-flight reindex task discovered on disk at
// startup, with the [ShardReindexTaskGeneric] instances rebuilt from its
// payload — one per migration directory, and two instances per unit for a
// change-tokenization, which fans into a searchable and a filterable strategy.
//
// Callers register the Tasks with the static [ShardReindexerV3] before
// [DB.WaitForStartup], so [OnAfterLsmInit] re-installs the double-write
// callbacks before any post-restart write reaches the shard; without that,
// writes between shard init and the swap go only to the old main bucket and
// are lost. They also seed [ReindexProvider.reindexTasks] with the same
// instances, so the swap phase does not build fresh ones and re-run
// [OnAfterLsmInit] against already-loaded ingest buckets.
type RecoveredReindex struct {
	Descriptor distributedtask.TaskDescriptor
	UnitID     string
	Collection string
	ShardName  string
	Tasks      []*ShardReindexTaskGeneric
}

// DiscoverInFlightReindexTasks walks every shard's
// .migrations/<migrationDir>/ at startup and reconstructs
// [ShardReindexTaskGeneric] instances for the recovery window where the
// reindex iteration is terminal but the swap has not yet completed.
//
// Reads payload.mig (the typed task payload persisted by
// persistRecoveryRecord before the iteration ran) and consults the shard's
// migration records for the state each of those directories is in.
//
// Returns a flat slice; deduplication across sibling migration dirs
// belonging to the same task is the caller's job.
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
			shardName := shardEntry.Name()
			lsmPath := filepath.Join(indexPath, shardName, "lsm")
			migrationsDir := filepath.Join(lsmPath, ".migrations")
			migs, err := os.ReadDir(migrationsDir)
			if err != nil {
				if !os.IsNotExist(err) {
					// Absent is the normal path — most shards never ran a
					// migration. A directory that is there and cannot be
					// listed is the opposite, and it reads the same here: no
					// mirror is armed, so every write between this shard's
					// load and promotion goes to the bucket promotion
					// replaces. Same reason the record arm below withholds.
					logger.WithField("path", migrationsDir).
						Warnf("reindex recovery: the migration directory could not be listed, so a migration "+
							"awaiting its flip would recover unmirrored; recovering nothing on this shard: %v", err)
				}
				continue
			}
			records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
			if recordSetUnreadable {
				// Every tracker here resolves through the records, so an
				// unreadable set would otherwise read as "no migration is in
				// the recovery window" and leave the double-write mirror
				// unarmed for one that is.
				logger.WithField("shard", shardName).Warn(
					"reindex recovery: migration records could not be read; recovering nothing on this shard")
				continue
			}
			if someRecordsUnreadable {
				logger.WithField("shard", shardName).Warn(
					"reindex recovery: some migration records could not be read; recovering only the trackers the rest name")
			}
			for _, migEntry := range migs {
				if !migEntry.IsDir() {
					continue
				}
				migDir := filepath.Join(migrationsDir, migEntry.Name())
				rec, ok := loadReindexRecoveryRecord(migDir, records, logger)
				if !ok {
					continue
				}

				// Parse the per-node generation suffix from the migration dir
				// name. With per-migration generation, the strategy instances
				// reconstructed here MUST use the same gen as the in-flight
				// state on disk, otherwise their SourceBucketName / Reindex
				// SuffixName paths won't match the on-disk dirs.
				_, generation, parseOk := parseMigrationDirName(migEntry.Name())
				if !parseOk {
					logger.WithField("migrationDir", migDir).
						Warn("reindex recovery: migration dir name missing _<gen> suffix; skipping")
					continue
				}

				tasks, err := buildRecoveryTasks(rec, shardName, generation, logger, schemaManager)
				if err != nil {
					logger.WithField("migrationDir", migDir).
						Warnf("reindex recovery: skipping migration; cannot build tasks: %v", err)
					continue
				}
				recovered = append(recovered, RecoveredReindex{
					Descriptor: distributedtask.TaskDescriptor{
						ID:      rec.TaskID,
						Version: rec.TaskVersion,
					},
					UnitID:     rec.UnitID,
					Collection: rec.Payload.Collection,
					ShardName:  shardName,
					Tasks:      tasks,
				})
			}
		}
	}
	return recovered, nil
}

// loadReindexRecoveryRecord reads payload.mig from a migration directory and
// returns the decoded record, but only for a migration whose rebuild is
// complete and whose flip is not yet decided. Returns ok=false otherwise, and
// when payload.mig is missing, unreadable, or names a property this build
// would not turn into a directory.
//
// The window is where the unit is terminal in RAFT — so the scheduler will not
// call StartTask after a restart — while the swap on the next scheduler tick
// has not run. Every write arriving in between has to reach the ingest bucket
// through a double-write callback, and only shard init is early enough to
// register one.
//
// A recorded flip stays inside it until promotion actually runs: the flip
// lives only in the process that made it, so after a restart the property is
// served from the canonical directory again, and promotion removes that
// directory before renaming the staged one over it.
//
// Either side is wrong for its own reason. Before, the scheduler restarts the
// unit and arms the callbacks itself, so arming here leaves the write path
// carrying two. After promotion the staged copy is the canonical one.
func loadReindexRecoveryRecord(migDir string, records []MigrationRecord,
	logger logrus.FieldLogger,
) (reindexRecoveryRecord, bool) {
	var rec reindexRecoveryRecord
	state, ok := migrationRecordForTracker(records, filepath.Base(migDir))
	if !ok || !state.IterationComplete() || state.State() == MigrationStatePromoted {
		return rec, false
	}
	payloadPath := filepath.Join(migDir, reindexRecoveryPayloadFile)
	// [maxRecoveryWalkPayloadBytes], not the apply-path bound: refusing here
	// arms no mirror, so the flip that follows takes the canonical directory
	// away with every write since the restart, and the payload embeds the
	// cluster-wide tenant and unit maps, which clear a megabyte on a few
	// thousand tenants. Only the few migrations the record above already
	// placed in the flip window are read at all.
	if err := refuseOversizedRecoveryPayload(payloadPath, maxRecoveryWalkPayloadBytes); err != nil {
		logger.WithField("path", payloadPath).
			Warnf("reindex recovery: payload.mig is beyond any size a migration can produce, "+
				"so it is not read and this migration's mirror stays unarmed: %v", err)
		return rec, false
	}
	data, err := os.ReadFile(payloadPath)
	if err != nil {
		// The record already placed this tracker in the window, so the
		// payload is not optional here: without it no mirror is armed, and
		// every write taken before promotion goes with the directory
		// promotion removes. Absent says that as loudly as unreadable does,
		// because at this point neither is the ordinary state.
		logger.WithField("path", payloadPath).
			Warnf("reindex recovery: a migration awaiting its flip has no readable payload.mig, "+
				"so its double-write mirror stays unarmed: %v", err)
		return rec, false
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		logger.WithField("path", payloadPath).
			Warnf("reindex recovery: malformed payload.mig; skipping: %v", err)
		return rec, false
	}
	// The same per-element check [readTaskProps] applies to this field of this
	// file, for the reason its godoc gives: the strategies built from these
	// names compose bucket and sidecar directories out of them and then create
	// and remove those. A record's names passed [validateMigrationHandles] on
	// the way in; a payload's never did, and a restored archive is free to
	// carry any bytes here.
	for _, prop := range rec.Payload.Properties {
		if !migrationHandleIsOneElement(prop) {
			logger.WithField("path", payloadPath).
				Warnf("reindex recovery: payload.mig names property %q, which is not a single directory "+
					"inside the shard; leaving this migration's mirror unarmed", prop)
			return reindexRecoveryRecord{}, false
		}
	}
	return rec, true
}

// buildRecoveryTasks reconstructs the [ShardReindexTaskGeneric]
// instances that processOneUnit would have created for this migration
// type, but scoped to exactly the named shard. The scope is what makes
// per-instance callbackDisableFuncs safe to share with [runtimeSwap]
// later: the static reindexer iterates all registered tasks on every
// shard init, but each task's isShardSelected filter drops everything
// except the one shard the record came from.
func buildRecoveryTasks(
	rec reindexRecoveryRecord,
	shardName string,
	generation int,
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) ([]*ShardReindexTaskGeneric, error) {
	payload := rec.Payload
	if payload.Collection == "" {
		return nil, fmt.Errorf("payload missing collection")
	}
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
			NewRuntimeFilterableToRangeableTask(logger, schemaManager, payload.Properties, payload.Collection, generation),
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
		raw = []*ShardReindexTaskGeneric{
			NewRuntimeSearchableRetokenizeTask(
				logger, propName, payload.TargetTokenization,
				payload.Collection, payload.BucketStrategy, payload.Collection,
				generation,
			),
			NewRuntimeFilterableRetokenizeTask(
				logger,
				propName, payload.TargetTokenization,
				payload.Collection, payload.Collection,
				generation,
			),
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

	// Constrain each task to exactly this shard so multiple recovered
	// instances (one per shard) don't fight over the same
	// callbackDisableFuncs slice when [runtimeSwap] runs per-shard.
	//
	// The identity is stamped here rather than by the caller for the same
	// reason createReindexTasks stamps its own: a recovered task that
	// reached a shard unstamped could not key a record, so the flip it
	// completes after the restart would record nothing.
	desc := distributedtask.TaskDescriptor{ID: rec.TaskID, Version: rec.TaskVersion}
	for _, t := range raw {
		t.constrainToShard(payload.Collection, shardName)
		t.setMigrationIdentity(desc, rec.UnitID, &payload)
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
