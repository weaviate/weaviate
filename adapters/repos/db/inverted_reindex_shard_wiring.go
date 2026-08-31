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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (s *Shard) reconcileMigrationRecords(ctx context.Context, class *models.Class) {
	s.migrationRecords = NewMigrationRecordStoreForUnit(s.pathLSM(), s.migrationUnit(), s.index.logger)
	s.migrationRecords.SweepTempFiles()

	reconciler := s.migrationReconciler(func() *models.Class { return class })
	if err := reconciler.Reconcile(ctx); err != nil {
		s.index.logger.WithField("shard", s.ID()).Errorf("reconcile migration records: %v", err)
	}
	monitoring.GetMetrics().AddMigrationRecordsWedged(
		reconciler.WedgedCount(), len(s.migrationRecords.Unreadable()))
}

// Empty where the node name is not wired: reading every record as local is what
// this store did before it could tell them apart, and a shard that cannot name
// its own unit must not set its own records aside.
func (s *Shard) migrationUnit() string {
	if s.index == nil || s.index.getSchema == nil {
		return ""
	}
	return MigrationUnitID(s.name, s.index.getSchema.NodeName())
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
// closes; both land on ShutdownStagedBucketsAt. Called by the cutover PR,
// where a worker closes the copy it is done writing.
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
// resurrect a shard that was deliberately unloaded. It forwards rather than
// computing a path, because the store is only meaningful once reconciliation
// has populated it at load.
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
	if l.shard == nil {
		return nil
	}
	return l.shard.migrationMirrorRegistry()
}
