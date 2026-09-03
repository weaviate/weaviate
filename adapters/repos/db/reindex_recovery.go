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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
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
	// Accumulated across the whole walk, not logged per shard: every one of
	// these faults is systemic (a permission, a build that cannot read its own
	// records), so on a many-tenant node the per-shard line count follows the
	// tenant count at every boot.
	var (
		unlistable   = map[string]struct{}{}
		unlistErrs   = errorcompounder.New()
		unreadable   = map[string]struct{}{}
		partlyUnread = map[string]struct{}{}
		shardsWalked int
		recordReads  int

		// Payload faults are per tracker, so they key on the tracker dir
		// rather than the shard. The compounders carry what each fault was:
		// a missing payload.mig and an unreadable one need different action.
		oversizedPayloads  = map[string]struct{}{}
		oversizedErrs      = errorcompounder.New()
		unreadablePayloads = map[string]struct{}{}
		unreadableErrs     = errorcompounder.New()
		malformedPayloads  = map[string]struct{}{}
		malformedErrs      = errorcompounder.New()

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
			migrationsDir := filepath.Join(lsmPath, ".migrations")
			migs, err := os.ReadDir(migrationsDir)
			if err != nil {
				if !os.IsNotExist(err) {
					unlistable[shardKey] = struct{}{}
					unlistErrs.AddWrapf(err, "%s", shardKey)
				}
				continue
			}
			recordReads++
			store, someRecordsUnreadable, recordSetUnreadable := migrationRecordStoreAt(lsmPath, logger)
			if recordSetUnreadable {
				unreadable[shardKey] = struct{}{}
				continue
			}
			if someRecordsUnreadable {
				partlyUnread[shardKey] = struct{}{}
			}
			records := store.Records()
			armable := map[string]struct{}{}
			for _, migEntry := range migs {
				if !migEntry.IsDir() {
					continue
				}
				migDir := filepath.Join(migrationsDir, migEntry.Name())
				rec, fault, faultErr := loadReindexRecoveryRecord(migDir, records)
				trackerKey := shardKey + "/" + migEntry.Name()
				switch fault {
				case recoveryPayloadOK, recoveryPayloadNotApplicable:
				case recoveryPayloadOversized:
					oversizedPayloads[trackerKey] = struct{}{}
					oversizedErrs.AddWrapf(faultErr, "%s", trackerKey)
				case recoveryPayloadUnreadable:
					unreadablePayloads[trackerKey] = struct{}{}
					unreadableErrs.AddWrapf(faultErr, "%s", trackerKey)
				case recoveryPayloadMalformed:
					malformedPayloads[trackerKey] = struct{}{}
					malformedErrs.AddWrapf(faultErr, "%s", trackerKey)
				}
				if fault != recoveryPayloadOK {
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
				armable[migEntry.Name()] = struct{}{}
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
			if someRecordsUnreadable {
				// The store refuses every write while any record is unreadable,
				// and the same fault freezes the reconciler, so nothing this
				// stamp would stop can run on this shard anyway.
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

	if len(unlistable) > 0 {
		logger.WithField("shards", reportedShardNames(unlistable)).
			Warnf("reindex recovery: the migration directory of %d shard(s) could not be listed, so a migration "+
				"awaiting its flip recovers unmirrored; recovering nothing on them: %v",
				len(unlistable), unlistErrs.ToErrorLimited(maxReportedErrors))
	}
	if len(unreadable) > 0 {
		logger.WithField("shards", reportedShardNames(unreadable)).
			Warnf("reindex recovery: the migration records of %d shard(s) could not be read; "+
				"recovering nothing on them", len(unreadable))
	}
	if len(partlyUnread) > 0 {
		logger.WithField("shards", reportedShardNames(partlyUnread)).
			Warnf("reindex recovery: some migration records of %d shard(s) could not be read; "+
				"recovering only the trackers the rest name", len(partlyUnread))
	}
	if len(oversizedPayloads) > 0 {
		logger.WithField("trackers", reportedShardNames(oversizedPayloads)).
			Warnf("reindex recovery: the payload.mig of %d tracker(s) is beyond any size a migration can "+
				"produce, so it is not read and those migrations' mirrors stay unarmed: %v",
				len(oversizedPayloads), oversizedErrs.ToErrorLimited(maxReportedErrors))
	}
	if len(unreadablePayloads) > 0 {
		logger.WithField("trackers", reportedShardNames(unreadablePayloads)).
			Warnf("reindex recovery: %d migration(s) awaiting their flip have no readable payload.mig, "+
				"so their double-write mirrors stay unarmed: %v",
				len(unreadablePayloads), unreadableErrs.ToErrorLimited(maxReportedErrors))
	}
	if len(malformedPayloads) > 0 {
		logger.WithField("trackers", reportedShardNames(malformedPayloads)).
			Warnf("reindex recovery: the payload.mig of %d tracker(s) is malformed or names a property that "+
				"is not a single directory inside the shard; those mirrors stay unarmed: %v",
				len(malformedPayloads), malformedErrs.ToErrorLimited(maxReportedErrors))
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

// stampUnmirroredRecords marks every record awaiting its flip that this walk
// could not build a task for. Nothing else arms those mirrors, so the writes
// this boot takes are the ones that make the staged copy stale, and the stamp
// is what stops a later promotion from renaming it over the live bucket.
func stampUnmirroredRecords(store *MigrationRecordStore, records []MigrationRecord,
	armable map[string]struct{},
) ([]string, error) {
	var stamped []string
	errs := errorcompounder.New()
	for _, rec := range records {
		subject := rec.Subject()
		if subject.Unmirrored {
			continue
		}
		if _, ok := armable[subject.TrackerDir]; ok {
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

// recoveryPayloadFault names why one tracker's payload.mig could not be turned
// into a recovery record. The walk accumulates these rather than logging them
// here: a fault is per tracker per shard, so on a many-tenant node reporting it
// at the point of failure follows the tenant count at every boot.
type recoveryPayloadFault int

const (
	recoveryPayloadOK recoveryPayloadFault = iota
	// The tracker names no record, or one this walk has no work for.
	recoveryPayloadNotApplicable
	recoveryPayloadOversized
	recoveryPayloadUnreadable
	recoveryPayloadMalformed
)

func loadReindexRecoveryRecord(migDir string, records []MigrationRecord,
) (reindexRecoveryRecord, recoveryPayloadFault, error) {
	var rec reindexRecoveryRecord
	state, ok := migrationRecordForTracker(records, filepath.Base(migDir))
	if !ok || !state.IterationComplete() || state.State() == MigrationStatePromoted {
		return rec, recoveryPayloadNotApplicable, nil
	}
	return readRecoveryPayload(migDir)
}

// readRecoveryPayload decodes one tracker's payload.mig, which names the task,
// the unit and the migration the tracker belongs to.
func readRecoveryPayload(migDir string) (reindexRecoveryRecord, recoveryPayloadFault, error) {
	var rec reindexRecoveryRecord
	payloadPath := filepath.Join(migDir, reindexRecoveryPayloadFile)
	// Only an oversized payload takes this arm. The bound is checked with a
	// stat, so every other stat failure — a missing payload.mig above all — would
	// otherwise be reported as a file too large to read.
	if err := refuseOversizedRecoveryPayload(payloadPath, maxRecoveryWalkPayloadBytes); errors.Is(err, errRecoveryPayloadTooLarge) {
		return rec, recoveryPayloadOversized, err
	}
	data, err := os.ReadFile(payloadPath)
	if err != nil {
		return rec, recoveryPayloadUnreadable, err
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return rec, recoveryPayloadMalformed, err
	}
	for _, prop := range rec.Payload.Properties {
		if !migrationHandleIsOneElement(prop) {
			return reindexRecoveryRecord{}, recoveryPayloadMalformed,
				fmt.Errorf("property %q is not a single directory inside the shard", prop)
		}
	}
	return rec, recoveryPayloadOK, nil
}

// migrationHalvesMissingFromCache names the tracker directories this unit
// created on the shard that the recovery-seeded task set does not cover.
//
// A tracker directory exists for every task the unit generated, written before
// the iteration ran, while a recovered task exists only where the tracker also
// carries a migration record. So a change-tokenization unit that restarted
// between its two halves recovers one task and no more, and the consumer reads
// a non-empty set as the whole unit.
//
// Generations are compared away: a retried unit's older tracker names the same
// half as the running one.
func migrationHalvesMissingFromCache(lsmPath string, desc distributedtask.TaskDescriptor,
	unitID string, tasks []*ShardReindexTaskGeneric,
) []string {
	covered := map[string]struct{}{}
	for _, t := range tasks {
		if base, _, ok := parseMigrationDirName(t.strategy.MigrationDirName()); ok {
			covered[base] = struct{}{}
		}
	}

	entries, err := os.ReadDir(filepath.Join(lsmPath, migrationsDir))
	if err != nil {
		// Nothing to compare against. The walk that seeded the cache read the
		// same directory, so a fault here is one it already reported.
		return nil
	}

	missing := map[string]struct{}{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, _, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		if _, there := covered[base]; there {
			continue
		}
		// A tracker whose payload cannot be read names no unit, so it cannot be
		// claimed for this one.
		rec, fault, _ := readRecoveryPayload(filepath.Join(lsmPath, migrationsDir, entry.Name()))
		if fault != recoveryPayloadOK {
			continue
		}
		if rec.TaskID != desc.ID || rec.TaskVersion != desc.Version || rec.UnitID != unitID {
			continue
		}
		missing[base] = struct{}{}
	}
	return slices.Sorted(maps.Keys(missing))
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

// rebuildNeverStartedHalves reconstructs the task of each tracker directory
// this unit created whose half never wrote a migration record. The payload the
// unit persisted before either half started carries everything a fresh start
// needs, so a restart that landed between a unit's halves resumes it instead
// of failing it. Only the newest generation of each half that names this unit
// is rebuilt: a retried unit's older tracker names the same half as the
// running one.
func rebuildNeverStartedHalves(lsmPath, shardName string, desc distributedtask.TaskDescriptor,
	unitID string, missing []string, logger logrus.FieldLogger, schemaManager *schema.Manager,
) ([]*ShardReindexTaskGeneric, error) {
	wanted := make(map[string]struct{}, len(missing))
	for _, base := range missing {
		wanted[base] = struct{}{}
	}
	entries, err := os.ReadDir(filepath.Join(lsmPath, migrationsDir))
	if err != nil {
		return nil, fmt.Errorf("list migration trackers: %w", err)
	}

	type neverStartedHalf struct {
		rec        reindexRecoveryRecord
		generation int
	}
	newest := map[string]neverStartedHalf{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, generation, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		if _, want := wanted[base]; !want {
			continue
		}
		rec, fault, _ := readRecoveryPayload(filepath.Join(lsmPath, migrationsDir, entry.Name()))
		if fault != recoveryPayloadOK {
			continue
		}
		if rec.TaskID != desc.ID || rec.TaskVersion != desc.Version || rec.UnitID != unitID {
			continue
		}
		if have, there := newest[base]; there && have.generation >= generation {
			continue
		}
		newest[base] = neverStartedHalf{rec: rec, generation: generation}
	}
	if len(newest) != len(wanted) {
		return nil, fmt.Errorf("rebuilt %d of %d missing half(s); the rest have no readable payload claiming this unit",
			len(newest), len(wanted))
	}

	var out []*ShardReindexTaskGeneric
	for _, base := range slices.Sorted(maps.Keys(newest)) {
		half := newest[base]
		tasks, err := buildRecoveryTasks(half.rec, shardName, base, half.generation, logger, schemaManager)
		if err != nil {
			return nil, fmt.Errorf("tracker %q: %w", base, err)
		}
		out = append(out, tasks...)
	}
	return out, nil
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
