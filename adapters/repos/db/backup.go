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
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"

	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"

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

// BackupDescriptors returns a channel of class descriptors.
// Class descriptor records everything needed to restore a class
// If an error happens a descriptor with an error will be written to the channel just before closing it.
func (db *DB) BackupDescriptors(ctx context.Context, bakid string, classes []string, baseDescr *backup.BackupDescriptor,
) <-chan backup.ClassDescriptor {
	ds := make(chan backup.ClassDescriptor, len(classes))
	f := func() {
		for _, c := range classes {
			desc := backup.ClassDescriptor{Name: c, BackupId: bakid}
			func() {
				idx := db.GetIndex(schema.ClassName(c))
				if idx == nil {
					desc.Error = fmt.Errorf("class %v doesn't exist any more", c)
					return
				}
				idx.closeLock.RLock()
				defer idx.closeLock.RUnlock()
				if idx.closed {
					desc.Error = fmt.Errorf("index for class %v is closed", c)
					return
				}
				var classBaseDescr *backup.ClassDescriptor
				if baseDescr != nil {
					classBaseDescr = baseDescr.GetClassDescriptor(c)
				}
				if err := idx.descriptor(ctx, bakid, &desc, classBaseDescr); err != nil {
					desc.Error = fmt.Errorf("backup class %v descriptor: %w", c, err)
				}
			}()

			ds <- desc
			if desc.Error != nil {
				break
			}
		}
		close(ds)
	}
	enterrors.GoWrapper(f, db.logger)
	return ds
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
		idx.closeLock.RLock()
		defer idx.closeLock.RUnlock()
		return idx.ReleaseBackup(ctx, bakID)
	} else {
		// index has been deleted in the meantime. Cleanup files that were kept to complete backup
		path := filepath.Join(db.config.RootPath, backup.DeleteMarkerAdd(indexID(schema.ClassName(class))))
		exists, err := diskio.DirExists(path)
		if err != nil {
			return err
		}
		if exists {
			return os.RemoveAll(path)
		}
	}
	return nil
}

// Shards returns the list of nodes where shards of class are contained.
// If there are no shards for the class, returns an empty list
// If there are shards for the class but no nodes are found, return an error
func (db *DB) Shards(ctx context.Context, class string) ([]string, error) {
	var nodes []string
	var shardCount int

	err := db.schemaReader.Read(class, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", class)
		}
		shardCount = len(state.Physical)
		if shardCount == 0 {
			nodes = []string{}
			return nil
		}

		unique := make(map[string]struct{})
		for _, shard := range state.Physical {
			for _, node := range shard.BelongsToNodes {
				unique[node] = struct{}{}
			}
		}

		nodes = make([]string, 0, len(unique))
		for node := range unique {
			nodes = append(nodes, node)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", class, err)
	}

	if shardCount > 0 && len(nodes) == 0 {
		return nil, fmt.Errorf("found %d shards but no nodes for class %s", shardCount, class)
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
func (i *Index) descriptor(ctx context.Context, backupID string, desc, classBaseDescr *backup.ClassDescriptor) (err error) {
	if err := i.initBackup(backupID); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// closelock is hold by the caller
			enterrors.GoWrapper(func() { i.ReleaseBackup(ctx, backupID) }, i.logger)
		}
	}()

	if err = i.ForEachShard(func(name string, s ShardLike) error {
		if err = s.HaltForTransfer(ctx, false, 0); err != nil {
			return fmt.Errorf("pause compaction and flush: %w", err)
		}

		var shardBaseDescr *backup.ShardDescriptor
		if classBaseDescr != nil {
			shardBaseDescr = classBaseDescr.GetShardDescriptor(name)
		}
		// prevent writing into the index during collection of metadata
		i.backupLock.Lock(name)
		defer i.backupLock.Unlock(name)
		var sd backup.ShardDescriptor

		files, err := s.ListBackupFiles(ctx, &sd)
		if err != nil {
			return fmt.Errorf("list shard %v files: %w", s.Name(), err)
		}

		sd.Files = make([]string, 0, len(files))
		for _, file := range files {
			if shardBaseDescr != nil {
				if info, ok := shardBaseDescr.BigFilesChunk[file]; ok {
					absPath, err := diskio.SanitizeFilePathJoin(i.Config.RootPath, file)
					if err != nil {
						return fmt.Errorf("sanitize file path %v: %w", file, err)
					}
					infoNew, err := os.Stat(absPath)
					if err != nil {
						return fmt.Errorf("stat big file %v: %w", file, err)
					}
					if info.Size == infoNew.Size() || info.ModifiedAt.Equal(infoNew.ModTime()) || info.Size == infoNew.Size() {
						if sd.IncrementalBackupInfo == nil {
							sd.IncrementalBackupInfo = make(map[string][]backup.IncrementalBackupInfo)
						}
						// files that are skipped due to being unchanged from base backup
						sd.IncrementalBackupInfo[classBaseDescr.BackupId] = append(
							sd.IncrementalBackupInfo[file],
							backup.IncrementalBackupInfo{File: file, ChunkKeys: info.ChunkKeys},
						)
						continue
					}
				}
			}

			// files to backup
			sd.Files = append(sd.Files, file)
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
	if desc.Aliases, err = i.marshalAliases(); err != nil {
		return fmt.Errorf("marshal aliases %w", err)
	}
	// this has to be set true, even if aliases list is empty.
	// because eventhen JSON key `aliases` will be present in
	// newer backups. To avoid failing to backup old backups that doesn't
	// understand `aliases` key in the ClassDescriptor.
	desc.AliasesIncluded = true
	return ctx.Err()
}

// ReleaseBackup marks the specified backup as inactive and restarts all
// async background and maintenance processes. It errors if the backup does not exist
// or is already inactive.
func (i *Index) ReleaseBackup(ctx context.Context, id string) error {
	i.logger.WithField("backup_id", id).WithField("class", i.Config.ClassName).Info("release backup")
	i.resetBackupState()
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
	var jsonBytes []byte
	err := i.schemaReader.Read(i.Config.ClassName.String(), true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", i.Config.ClassName.String())
		}
		bytes, jsonErr := state.JSON()
		if jsonErr != nil {
			return jsonErr
		}

		jsonBytes = bytes
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal sharding state")
	}

	return jsonBytes, nil
}

func (i *Index) marshalSchema() ([]byte, error) {
	b, err := i.getSchema.ReadOnlyClass(i.Config.ClassName.String()).MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "marshal schema")
	}

	return b, err
}

func (i *Index) marshalAliases() ([]byte, error) {
	aliases := i.getSchema.GetAliasesForClass(i.Config.ClassName.String())
	b, err := json.Marshal(aliases)
	if err != nil {
		return nil, errors.Wrap(err, "marshal aliases failed to get aliases for collection")
	}
	return b, err
}

// shardTransfer is an adapter built around rwmutex that facilitates cooperative blocking between write and read locks
type shardTransfer struct {
	sync.RWMutex
	log            logrus.FieldLogger
	retryDuration  time.Duration
	notifyDuration time.Duration
}

// LockWithContext attempts to acquire a write lock while respecting the provided context.
// It reports whether the lock acquisition was successful or if the context has been cancelled.
func (m *shardTransfer) LockWithContext(ctx context.Context) error {
	return m.lock(ctx, m.TryLock)
}

func (m *shardTransfer) lock(ctx context.Context, tryLock func() bool) error {
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

func (s *shardTransfer) RLockGuard(reader func() error) error {
	s.RLock()
	defer s.RUnlock()
	return reader()
}
