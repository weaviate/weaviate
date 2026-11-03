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

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (s *Shard) reconcileMigrationRecords(ctx context.Context, class *models.Class) []*ShardReindexTaskGeneric {
	s.migrationRecords = NewMigrationRecordStoreForUnit(s.pathLSM(), s.migrationUnit(), s.index.logger)
	s.migrationRecords.SweepTempFiles()

	reconciler := s.migrationReconciler(func() *models.Class { return class })
	var recovered []*ShardReindexTaskGeneric
	if err := reconciler.Load(); err != nil {
		s.index.logger.WithField("shard", s.ID()).Errorf("reconcile migration records: %v", err)
	} else {
		recovered = s.reindexer.RecoverMigrations(s)
		reconciler.Reconcile(ctx)
	}
	monitoring.GetMetrics().AddMigrationRecordsWedged(
		reconciler.WedgedCount(), len(s.migrationRecords.Unreadable()))
	return recovered
}

// armRecoveredMigrations returns the tasks it could neither arm nor record as
// unarmed, along with why.
func (s *Shard) armRecoveredMigrations(ctx context.Context, recovered []*ShardReindexTaskGeneric,
) ([]*ShardReindexTaskGeneric, error) {
	var unarmed []*ShardReindexTaskGeneric
	causes := errorcompounder.New()
	for _, t := range recovered {
		if err := t.OnAfterLsmInit(ctx, s); err != nil {
			unarmed = append(unarmed, t)
			causes.AddWrapf(err, "%s", t.Name())
		}
	}
	if len(unarmed) == 0 {
		return nil, nil
	}
	if err := s.reindexer.AbandonUnarmed(s, unarmed, causes.ToErrorLimited(maxReportedErrors)); err != nil {
		return unarmed, err
	}
	return nil, nil
}

// holdReadOnlyForUnarmed serves reads but no writes: a write would miss the
// staged copy, and nothing on disk would stop that copy being swapped in.
func (s *Shard) holdReadOnlyForUnarmed(unarmed []*ShardReindexTaskGeneric, cause error) {
	s.migrationMirrors.holdUnarmed(unarmed)
	s.index.logger.WithField("shard", s.ID()).Errorf("shard is %s until the disk accepts writes again and its "+
		"status is set back to %s, and its reindex migrations wait until then: %v",
		storagestate.StatusReadOnly, storagestate.StatusReady, cause)
	if err := s.SetStatusReadonly(statusReasonMirrorUnarmed); err != nil {
		s.index.logger.WithField("shard", s.ID()).Errorf("mark shard %s: %v", storagestate.StatusReadOnly, err)
	}
}

func (s *Shard) migrationUnit() string { return migrationUnitOf(s.index, s.name) }

// Empty where the node name is not wired: such a shard must not set records aside.
func migrationUnitOf(index *Index, shardName string) string {
	if index == nil || index.getSchema == nil {
		return ""
	}
	return MigrationUnitID(shardName, index.getSchema.NodeName())
}

func (s *Shard) migrationReconciler(class func() *models.Class) *migrationReconciler {
	return newMigrationReconciler(s.migrationRecords, s.pathLSM(),
		s.index.logger.WithField("shard", s.ID()),
		migrationReconcileDeps{
			LocalTasks: s.migrations().LocalTasks,
			SealUnit:   s.migrations().SealUnit,
			Class:      class,
			Mirror:     s,
			Buckets:    s,
		})
}

// Resolves the record's own copy of the property and closes that. The
// reconciler names directories instead, because it has to decide before it
// closes; both land on ShutdownStagedBucketsAt.
func (s *Shard) ShutdownStagedBuckets(ctx context.Context, key MigrationRecordKey, prop string) error {
	if s.migrationRecords == nil {
		return nil
	}
	rec, ok := s.migrationRecords.Get(key)
	if !ok {
		return nil
	}
	return s.ShutdownStagedBucketsAt(ctx, migrationOwnCopyDirs(rec.Subject(), prop))
}

func (s *Shard) ShutdownStagedBucketsAt(ctx context.Context, dirs []string) error {
	if s.store == nil {
		return nil
	}
	for _, dir := range dirs {
		if s.store.Bucket(dir) == nil {
			continue
		}
		if err := s.store.ShutdownBucket(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) migrationRecordStore() *MigrationRecordStore { return s.migrationRecords }

// Reports no store rather than loading a cold shard: a shard with no records
// on disk has nothing to reconcile, and loading one to find that out would
// resurrect a shard that was deliberately unloaded.
func (l *LazyLoadShard) migrationRecordStore() *MigrationRecordStore {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.loaded {
		return nil
	}
	return l.shard.migrationRecordStore()
}

func (s *Shard) migrationMirrorRegistry() *migrationMirrorRegistry { return &s.migrationMirrors }

func (l *LazyLoadShard) migrationMirrorRegistry() *migrationMirrorRegistry {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.loaded {
		return nil
	}
	return l.shard.migrationMirrorRegistry()
}
