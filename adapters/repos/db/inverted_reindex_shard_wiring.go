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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// MigrationLocalTaskSource reads this node's own applied view of the reindex
// task namespace — not the leader's, so it needs no round-trip and cannot
// block a shard load. The second result reports whether the map could be read
// at all, which reconciliation needs because an absent task is terminal and an
// unreadable map must not be mistaken for one.
type MigrationLocalTaskSource func() ([]*distributedtask.Task, bool)

// SetMigrationLocalTaskSource installs the source reconciliation consults.
// It arrives post-bootstrap, alongside the other reindex audit dependencies,
// because the cluster service does not exist when the DB is built.
func (db *DB) SetMigrationLocalTaskSource(source MigrationLocalTaskSource) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.migrationLocalTaskSource = source
}

func (db *DB) migrationLocalTasks() ([]*distributedtask.Task, bool) {
	db.reindexAuditMu.RLock()
	source := db.migrationLocalTaskSource
	db.reindexAuditMu.RUnlock()

	if source == nil {
		return nil, false
	}
	return source()
}

// reconcileMigrationRecords runs the state machine's load-time pass and leaves
// the shard holding its records. It has to stay ahead of bucket loading: it
// renames directories, and a bucket opened at a name it is about to move would
// be serving the wrong data.
func (s *Shard) reconcileMigrationRecords(ctx context.Context, class *models.Class) {
	s.migrationRecords = NewMigrationRecordStore(s.pathLSM(), s.index.logger)

	reconciler := newMigrationReconciler(s.migrationRecords, s.pathLSM(), s.index.logger,
		migrationReconcileDeps{
			LocalTasks: s.index.db.migrationLocalTasks,
			Class:      func() *models.Class { return class },
			Mirror:     s,
			Buckets:    s,
		})

	if err := reconciler.Reconcile(ctx); err != nil {
		// A shard whose records cannot be read still has to load; every
		// individual disposition already fails toward doing nothing.
		s.index.logger.WithField("shard", s.ID()).Errorf("reconcile migration records: %v", err)
	}
}

// ShutdownStagedBuckets closes a record's open buckets for one property so
// their directories can be removed. At shard load nothing is open yet and this
// is a no-op; it earns its keep when a successor retires a predecessor inside
// one process, where removing an open bucket's directory would leave mmaps, an
// in-flight compaction and a registry entry behind.
func (s *Shard) ShutdownStagedBuckets(ctx context.Context, key MigrationRecordKey, prop string) error {
	if s.store == nil || s.migrationRecords == nil {
		return nil
	}
	rec, ok := s.migrationRecords.Get(key)
	if !ok {
		return nil
	}

	subject := rec.Subject()
	for _, dir := range append([]string{subject.StagedDirs[prop]}, subject.SidecarDirs...) {
		if dir == "" || s.store.Bucket(dir) == nil {
			continue
		}
		if err := s.store.ShutdownBucket(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) migrationRecordStore() *MigrationRecordStore { return s.migrationRecords }

// A lazy shard forwards rather than computing a path, because the store is
// only meaningful once reconciliation has populated it at load.
func (l *LazyLoadShard) migrationRecordStore() *MigrationRecordStore {
	if l.shard == nil {
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
