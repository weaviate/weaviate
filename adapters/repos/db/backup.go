//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/schema"
)

type BackupState struct {
	BackupID   string
	InProgress bool
}

// Backupable returns whether all given class can be backed up.
func (db *DB) Backupable(ctx context.Context, classes []string) error {
	for _, c := range classes {
		className := schema.ClassName(c)
		idx := db.GetIndex(className)
		if idx == nil || idx.Config.ClassName != className {
			return fmt.Errorf("class %v doesn't exist", c)
		}
	}
	return nil
}

// ListBackupable returns a list of all classes which can be backed up.
func (db *DB) ListBackupable() []string {
	cs := make([]string, 0, len(db.indices))
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()
	for _, idx := range db.indices {
		cls := string(idx.Config.ClassName)
		cs = append(cs, cls)
	}
	return cs
}

// BackupDescriptors returns a channel of class descriptors.
// Class descriptor records everything needed to restore a class
// If an error happens a descriptor with an error will be written to the channel just before closing it.
func (db *DB) BackupDescriptors(ctx context.Context, bakid string, classes []string,
) <-chan backup.ClassDescriptor {
	ds := make(chan backup.ClassDescriptor, len(classes))
	go func() {
		for _, c := range classes {
			desc := backup.ClassDescriptor{Name: c}
			idx := db.GetIndex(schema.ClassName(c))
			if idx == nil {
				desc.Error = fmt.Errorf("class %v doesn't exist any more", c)
			} else if err := idx.descriptor(ctx, bakid, &desc); err != nil {
				desc.Error = fmt.Errorf("backup class %v descriptor: %w", c, err)
			} else {
				desc.Error = ctx.Err()
			}

			ds <- desc
			if desc.Error != nil {
				break
			}
		}
		close(ds)
	}()
	return ds
}

func (db *DB) ShardsBackup(
	ctx context.Context, bakID, class string, shards []string,
) (_ backup.ClassDescriptor, err error) {
	cd := backup.ClassDescriptor{Name: class}
	idx := db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return cd, fmt.Errorf("no index for class %q", class)
	}

	if err := idx.initBackup(bakID); err != nil {
		return cd, fmt.Errorf("init backup state for class %q: %w", class, err)
	}

	defer func() {
		if err != nil {
			go idx.ReleaseBackup(ctx, bakID)
		}
	}()

	sm := make(map[string]ShardLike, len(shards))
	for _, shardName := range shards {
		shard := idx.shards.Load(shardName)
		if shard == nil {
			return cd, fmt.Errorf("no shard %q for class %q", shardName, class)
		}
		sm[shardName] = shard
	}

	// prevent writing into the index during collection of metadata
	idx.backupMutex.Lock()
	defer idx.backupMutex.Unlock()
	for shardName, shard := range sm {
		if err := shard.BeginBackup(ctx); err != nil {
			return cd, fmt.Errorf("class %q: shard %q: begin backup: %w", class, shardName, err)
		}

		sd := backup.ShardDescriptor{Name: shardName}
		if err := shard.ListBackupFiles(ctx, &sd); err != nil {
			return cd, fmt.Errorf("class %q: shard %q: list backup files: %w", class, shardName, err)
		}

		cd.Shards = append(cd.Shards, &sd)
	}

	return cd, nil
}

// ReleaseBackup release resources acquired by the index during backup
func (db *DB) ReleaseBackup(ctx context.Context, bakID, class string) (err error) {
	fields := logrus.Fields{
		"op":    "release_backup",
		"class": class,
		"id":    bakID,
	}
	db.logger.WithFields(fields).Debug("starting")
	begin := time.Now()
	defer func() {
		l := db.logger.WithFields(fields).WithField("took", time.Since(begin))
		if err != nil {
			l.Error(err)
			return
		}
		l.Debug("finish")
	}()

	idx := db.GetIndex(schema.ClassName(class))
	if idx != nil {
		return idx.ReleaseBackup(ctx, bakID)
	}
	return nil
}

func (db *DB) ClassExists(name string) bool {
	return db.IndexExists(schema.ClassName(name))
}

// Returns the list of nodes where shards of class are contained.
// If there are no shards for the class, returns an empty list
// If there are shards for the class but no nodes are found, return an error
func (db *DB) Shards(ctx context.Context, class string) ([]string, error) {
	unique := make(map[string]struct{})

	ss := db.schemaGetter.CopyShardingState(class)
	if len(ss.Physical) == 0 {
		return []string{}, nil
	}

	for _, shard := range ss.Physical {
		for _, node := range shard.BelongsToNodes {
			unique[node] = struct{}{}
		}
	}

	var (
		nodes   = make([]string, len(unique))
		counter = 0
	)

	for node := range unique {
		nodes[counter] = node
		counter++
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("found %v shards, but has 0 nodes", len(ss.Physical))
	}

	return nodes, nil
}

func (db *DB) ListClasses(ctx context.Context) []string {
	classes := db.schemaGetter.GetSchemaSkipAuth().Objects.Classes
	classNames := make([]string, len(classes))

	for i, class := range classes {
		classNames[i] = class.Class
	}

	return classNames
}

// descriptor record everything needed to restore a class
func (i *Index) descriptor(ctx context.Context, backupID string, desc *backup.ClassDescriptor) (err error) {
	if err := i.initBackup(backupID); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			go i.ReleaseBackup(ctx, backupID)
		}
	}()

	// prevent writing into the index during collection of metadata
	i.backupMutex.Lock()
	defer i.backupMutex.Unlock()

	if err = i.ForEachShard(func(name string, s ShardLike) error {
		if err = s.BeginBackup(ctx); err != nil {
			return fmt.Errorf("pause compaction and flush: %w", err)
		}
		var sd backup.ShardDescriptor
		if err := s.ListBackupFiles(ctx, &sd); err != nil {
			return fmt.Errorf("list shard %v files: %w", s.Name(), err)
		}

		desc.Shards = append(desc.Shards, &sd)
		return nil
	}); err != nil {
		return err
	}

	if desc.ShardingState, err = i.marshalShardingState(); err != nil {
		return fmt.Errorf("marshal sharding state %w", err)
	}
	if desc.Schema, err = i.marshalSchema(); err != nil {
		return fmt.Errorf("marshal schema %w", err)
	}
	return nil
}

// ReleaseBackup marks the specified backup as inactive and restarts all
// async background and maintenance processes. It errors if the backup does not exist
// or is already inactive.
func (i *Index) ReleaseBackup(ctx context.Context, id string) error {
	defer i.resetBackupState()
	if err := i.resumeMaintenanceCycles(ctx); err != nil {
		return err
	}
	return nil
}

func (i *Index) initBackup(id string) error {
	new := &BackupState{
		BackupID:   id,
		InProgress: true,
	}
	if !i.lastBackup.CompareAndSwap(nil, new) {
		bid := ""
		if x := i.lastBackup.Load(); x != nil {
			bid = x.BackupID
		}
		return errors.Errorf(
			"cannot create new backup, backup ‘%s’ is not yet released, this "+
				"means its contents have not yet been fully copied to its destination, "+
				"try again later", bid)
	}

	return nil
}

func (i *Index) resetBackupState() {
	i.lastBackup.Store(nil)
}

func (i *Index) resumeMaintenanceCycles(ctx context.Context) (lastErr error) {
	i.ForEachShard(func(name string, shard ShardLike) error {
		if err := shard.resumeMaintenanceCycles(ctx); err != nil {
			lastErr = err
			i.logger.WithField("shard", name).WithField("op", "resume_maintenance").Error(err)
		}
		time.Sleep(time.Millisecond * 10)
		return nil
	})
	return lastErr
}

func (i *Index) marshalShardingState() ([]byte, error) {
	b, err := i.getSchema.CopyShardingState(i.Config.ClassName.String()).JSON()
	if err != nil {
		return nil, errors.Wrap(err, "marshal sharding state")
	}

	return b, nil
}

func (i *Index) marshalSchema() ([]byte, error) {
	schema := i.getSchema.GetSchemaSkipAuth()

	b, err := schema.GetClass(i.Config.ClassName).MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "marshal schema")
	}

	return b, err
}

const (
	mutexRetryDuration  = time.Millisecond * 500
	mutexNotifyDuration = 20 * time.Second
)

// backupMutex is an adapter built around rwmutex that facilitates cooperative blocking between write and read locks
type backupMutex struct {
	sync.RWMutex
	log            logrus.FieldLogger
	retryDuration  time.Duration
	notifyDuration time.Duration
}

// LockWithContext attempts to acquire a write lock while respecting the provided context.
// It reports whether the lock acquisition was successful or if the context has been cancelled.
func (m *backupMutex) LockWithContext(ctx context.Context) error {
	return m.lock(ctx, m.TryLock)
}

func (m *backupMutex) lock(ctx context.Context, tryLock func() bool) error {
	if tryLock() {
		return nil
	}
	curTime := time.Now()
	t := time.NewTicker(m.retryDuration)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if tryLock() {
				return nil
			}
			if time.Since(curTime) > m.notifyDuration {
				curTime = time.Now()
				m.log.Info("backup process waiting for ongoing writes to finish")
			}
		}
	}
}

func (s *backupMutex) RLockGuard(reader func() error) error {
	s.RLock()
	defer s.RUnlock()
	return reader()
}
