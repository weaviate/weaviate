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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// MigrationLocalTaskSource reads this node's own applied view of the reindex
// task namespace — not the leader's, so it needs no round-trip and cannot
// block a shard load. The second result reports whether the map could be read
// at all, which reconciliation needs because an absent task is terminal and an
// unreadable map must not be mistaken for one.
type MigrationLocalTaskSource func() ([]*distributedtask.Task, bool)

// MigrationClusterTaskSource reads the reindex task namespace from the leader.
// It costs a round-trip and can fail, so reconciliation consults it only where
// a local answer is about to be acted on destructively.
type MigrationClusterTaskSource func(context.Context) ([]*distributedtask.Task, error)

// SetMigrationLocalTaskSource installs the source reconciliation consults and
// immediately re-runs the edge that needs it on every shard already loaded.
// The two are one act: the source arrives post-bootstrap, because the cluster
// service does not exist when the DB is built, and every shard loaded during
// RAFT catch-up reconciled with the map unavailable.
// The cluster source is installed in the same act, because reconciliation
// refuses to act destructively without it — the local map is what can be
// stale, and the confirmation is what makes acting on it safe.
func (db *DB) SetMigrationLocalTaskSource(ctx context.Context, source MigrationLocalTaskSource,
	cluster MigrationClusterTaskSource,
) {
	db.reindexAuditMu.Lock()
	db.migrationLocalTaskSource = source
	db.migrationClusterTaskSource = cluster
	db.reindexAuditMu.Unlock()

	db.reconcileLoadedMigrationsAfterTaskMap(ctx)
}

// reconcileLoadedMigrationsAfterTaskMap walks the shards that are already
// loaded. An unloaded one needs nothing: its own load runs the full pass with
// the map readable.
func (db *DB) reconcileLoadedMigrationsAfterTaskMap(ctx context.Context) {
	db.indexLock.RLock()
	indices := make([]*Index, 0, len(db.indices))
	for _, idx := range db.indices {
		indices = append(indices, idx)
	}
	db.indexLock.RUnlock()

	for _, idx := range indices {
		// A tenant deactivation, a tenant delete and a collection delete each
		// take a shard out of the map and tear it down while this walk holds
		// the pointer. The pass removes directories, so a teardown racing it
		// fails a memtable flush after the shard is already marked shut, and
		// that failure latches: every later reactivation of the tenant
		// returns the teardown error.
		func() {
			idx.dropIndex.RLock()
			defer idx.dropIndex.RUnlock()

			idx.ForEachLoadedShard(func(_ string, shard ShardLike) error {
				concrete, err := unwrapShard(ctx, shard)
				if err != nil {
					idx.logger.WithField("shard", shard.Name()).Errorf(
						"reconcile migration records once the task map became readable: %v", err)
					return nil
				}
				release, err := concrete.preventShutdown()
				if err != nil {
					// Multi-tenant by construction — nothing else deactivates
					// a shard — so its next activation runs the full pass with
					// the map readable.
					idx.logger.WithField("shard", shard.Name()).Infof(
						"skipping migration reconciliation on a shard that is shutting down: %v", err)
					return nil
				}
				defer release()

				concrete.reconcileMigrationRecordsAfterTaskMap(ctx)
				return nil
			})
		}()
	}
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

func (db *DB) migrationClusterTasks(ctx context.Context) ([]*distributedtask.Task, error) {
	db.reindexAuditMu.RLock()
	source := db.migrationClusterTaskSource
	db.reindexAuditMu.RUnlock()

	if source == nil {
		return nil, fmt.Errorf("no cluster-wide reindex task source is installed on this node")
	}
	return source(ctx)
}

// reconcileMigrationRecords runs the state machine's load-time pass and leaves
// the shard holding its records. It has to stay ahead of bucket loading: it
// renames directories, and a bucket opened at a name it is about to move would
// be serving the wrong data.
func (s *Shard) reconcileMigrationRecords(ctx context.Context, class *models.Class) {
	s.migrationRecords = NewMigrationRecordStore(s.pathLSM(), s.index.logger)
	// The owning store is the only one that may sweep: the same directory is
	// read by throwaway stores while this shard writes to it.
	s.migrationRecords.SweepTempFiles()

	if err := s.migrationReconciler(func() *models.Class { return class }).Reconcile(ctx); err != nil {
		// A shard whose records cannot be read still has to load; every
		// individual disposition already fails toward doing nothing.
		s.index.logger.WithField("shard", s.ID()).Errorf("reconcile migration records: %v", err)
	}
	s.warnAboutLegacyMarkerMigrations()
}

// warnAboutLegacyMarkerMigrations reports migrations that completed on the
// release before the migration records. This build preserves their data but
// cannot promote it, and the schema flip they belong to already committed
// cluster-wide, so the property answers queries from an empty bucket until the
// operator restores it or downgrades. Nothing else says so.
func (s *Shard) warnAboutLegacyMarkerMigrations() {
	if s.migrationRecords == nil || len(s.migrationRecords.Unreadable()) > 0 {
		// A record this build cannot read may be the one naming that tracker,
		// which would make the marker a leftover rather than the live claim.
		return
	}
	for _, legacy := range migrationLegacyMarkerTrackersAt(s.pathLSM(), s.migrationRecords.Records()) {
		props := legacy.servesEmpty(s.pathLSM())
		if len(props) == 0 {
			continue
		}
		s.index.logger.WithField("shard", s.ID()).
			WithField("tracker", legacy.dirName).
			WithField("marker", legacy.marker).
			WithField("properties", props).
			Warn("a migration completed on an older release holds these properties' only copy under its staged " +
				"directory; this build preserves it but cannot promote it, so they serve empty until the data is " +
				"restored or the node is downgraded")
	}
}

// reconcileMigrationRecordsAfterTaskMap is the second pass, run once this
// node's applied task map becomes readable.
func (s *Shard) reconcileMigrationRecordsAfterTaskMap(ctx context.Context) {
	if s.migrationRecords == nil {
		return
	}
	s.liveMigrationReconciler().ReconcileAfterTaskMap(ctx)
}

// retireSupersededMigrations runs the supersession relation in the process
// that just flipped, which is the only place a predecessor's mirror is armed
// and its staged buckets are open. Reconciliation re-derives the same outcome
// at any later load, where there is nothing armed and nothing open.
func (s *Shard) retireSupersededMigrations(ctx context.Context) {
	if s.migrationRecords == nil {
		return
	}
	s.liveMigrationReconciler().RetireSuperseded(ctx)
}

// liveMigrationReconciler reads the class as it is now rather than as it was
// at load, because the effect predicate answers about the current schema.
func (s *Shard) liveMigrationReconciler() *migrationReconciler {
	className := s.index.Config.ClassName.String()
	return s.migrationReconciler(func() *models.Class {
		return s.index.getSchema.ReadOnlyClass(className)
	})
}

func (s *Shard) migrationReconciler(class func() *models.Class) *migrationReconciler {
	return newMigrationReconciler(s.migrationRecords, s.pathLSM(), s.index.logger,
		migrationReconcileDeps{
			LocalTasks:   s.migrationLocalTasks,
			ClusterTasks: s.migrationClusterTasks,
			Class:        class,
			Mirror:       s,
			Buckets:      s,
		})
}

// migrationLocalTasks reads the handle at call time rather than at wiring time.
// The index is given its database handle only after NewIndex returns, and an
// eagerly loaded shard reconciles inside that call — so at wiring there may be
// no handle to bind. No handle reads as an unreadable task map, which withholds
// every disposition until the second pass runs it with the map installed.
func (s *Shard) migrationLocalTasks() ([]*distributedtask.Task, bool) {
	if s.index == nil || s.index.db == nil {
		return nil, false
	}
	return s.index.db.migrationLocalTasks()
}

func (s *Shard) migrationClusterTasks(ctx context.Context) ([]*distributedtask.Task, error) {
	if s.index == nil || s.index.db == nil {
		return nil, fmt.Errorf("shard %q has no database handle to reach the leader through", s.Name())
	}
	return s.index.db.migrationClusterTasks(ctx)
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
