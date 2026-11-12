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
	"slices"

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

// armRecoveredMigrations returns the tasks that failed to arm and could not be stamped Unmirrored, and why.
func (s *Shard) armRecoveredMigrations(ctx context.Context, recovered []*ShardReindexTaskGeneric,
) ([]*ShardReindexTaskGeneric, error) {
	var order []taskMigration
	groups := map[taskMigration][]*ShardReindexTaskGeneric{}
	for _, t := range recovered {
		if groups[t.migration()] == nil {
			order = append(order, t.migration())
		}
		groups[t.migration()] = append(groups[t.migration()], t)
	}
	var unarmed []*ShardReindexTaskGeneric
	causes := errorcompounder.New()
	for _, m := range order {
		if s.hasStampedHalf(groups[m]) {
			unarmed = append(unarmed, groups[m]...)
			causes.Add(fmt.Errorf("%s: stamped Unmirrored by an earlier load", m.desc.ID))
			continue
		}
		failed := false
		for _, t := range groups[m] {
			if err := t.OnAfterLsmInit(ctx, s); err != nil {
				failed = true
				causes.AddWrapf(err, "%s", t.Name())
			}
		}
		if failed {
			unarmed = append(unarmed, groups[m]...)
		}
	}
	if len(unarmed) == 0 {
		return nil, nil
	}
	return s.reindexer.AbandonUnarmed(s, unarmed, causes.ToErrorLimited(maxReportedErrors))
}

// A stamped half fails the swap ahead for the whole unit; past the flip, the mirror keeps the live index current.
func (s *Shard) hasStampedHalf(tasks []*ShardReindexTaskGeneric) bool {
	return slices.ContainsFunc(tasks, func(t *ShardReindexTaskGeneric) bool {
		rec, ok := s.migrationRecords.Get(t.migrationRecordKey())
		return ok && rec.Subject().Unmirrored && !rec.FlipDecided()
	})
}

// holdReadOnlyForUnarmed sets the shard READONLY, because a write would miss the unguarded staged copy.
func (s *Shard) holdReadOnlyForUnarmed(unarmed []*ShardReindexTaskGeneric, cause error) {
	s.migrationMirrors.holdUnarmed(unarmed)
	s.index.logger.WithField("shard", s.ID()).Errorf("shard is %s and its reindex migrations wait: "+
		"%v. Clear that cause, then set the shard to %s, which starts a retry, or restart the node (a repaired unreadable record needs the restart); if a staged bucket cannot "+
		"open because a segment file is damaged, repair that file in place first. A migration not yet "+
		"swapped in also leaves the hold at that retry once its Unmirrored stamp can be written, unless a migration already swapped in is "+
		"held on the same shard; then both wait until the shard is repaired and set READY",
		storagestate.StatusReadOnly, cause, storagestate.StatusReady)
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
