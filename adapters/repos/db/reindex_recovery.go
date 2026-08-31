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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/schema"
)

// RecoveredReindex describes one in-flight reindex task discovered on disk at
// startup, with its [ShardReindexTaskGeneric] instances rebuilt from its payload.
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
					logger.WithField("path", migrationsDir).
						Warnf("reindex recovery: the migration directory could not be listed, so a migration "+
							"awaiting its flip would recover unmirrored; recovering nothing on this shard: %v", err)
				}
				continue
			}
			records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
			if recordSetUnreadable {
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
				trackerPrefix, generation, parseOk := parseMigrationDirName(migEntry.Name())
				if !parseOk {
					logger.WithField("migrationDir", migDir).
						Warn("reindex recovery: migration dir name missing _<gen> suffix; skipping")
					continue
				}

				tasks, err := buildRecoveryTasks(rec, shardName, trackerPrefix, generation, logger, schemaManager)
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

func loadReindexRecoveryRecord(migDir string, records []MigrationRecord,
	logger logrus.FieldLogger,
) (reindexRecoveryRecord, bool) {
	var rec reindexRecoveryRecord
	state, ok := migrationRecordForTracker(records, filepath.Base(migDir))
	if !ok || !state.IterationComplete() || state.State() == MigrationStatePromoted {
		return rec, false
	}
	payloadPath := filepath.Join(migDir, reindexRecoveryPayloadFile)
	// Only an oversized payload takes this arm. The bound is checked with a
	// stat, so every other stat failure — a missing payload.mig above all — would
	// otherwise be reported as a file too large to read.
	if err := refuseOversizedRecoveryPayload(payloadPath, maxRecoveryWalkPayloadBytes); errors.Is(err, errRecoveryPayloadTooLarge) {
		logger.WithField("path", payloadPath).
			Warnf("reindex recovery: payload.mig is beyond any size a migration can produce, "+
				"so it is not read and this migration's mirror stays unarmed: %v", err)
		return rec, false
	}
	data, err := os.ReadFile(payloadPath)
	if err != nil {
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

func buildRecoveryTasks(
	rec reindexRecoveryRecord,
	shardName string,
	trackerPrefix string,
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
		switch {
		case strings.HasPrefix(trackerPrefix, MigrationDirPrefixSearchableRetokenize):
			raw = []*ShardReindexTaskGeneric{
				NewRuntimeSearchableRetokenizeTask(
					logger, propName, payload.TargetTokenization,
					payload.Collection, payload.BucketStrategy, payload.Collection,
					generation,
				),
			}
		case strings.HasPrefix(trackerPrefix, MigrationDirPrefixFilterableRetokenize):
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
				"tracker directory %q names neither half of a change-tokenization migration", trackerPrefix)
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
