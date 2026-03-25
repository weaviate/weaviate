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
	"io"
	"os"
	"path/filepath"
	"strings"
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

var errFrozenShard = errors.New("shard is frozen")

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
func (db *DB) BackupDescriptors(ctx context.Context, bakid string, classes []string, baseDescrs []*backup.BackupDescriptor,
) <-chan backup.ClassDescriptor {
	ds := make(chan backup.ClassDescriptor, len(classes))
	f := func() {
		for _, c := range classes {
			desc := backup.ClassDescriptor{Name: c, BackupID: bakid}
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
				var classBaseDescr []*backup.ClassDescriptor
				for _, b := range baseDescrs {
					classbaseDescrTmp := b.GetClassDescriptor(c)
					if classbaseDescrTmp == nil {
						continue
					}
					classBaseDescr = append(classBaseDescr, classbaseDescrTmp)
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
			if err := os.RemoveAll(path); err != nil {
				return err
			}
		}

		// Clean up staging directory that may have been created by CreateBackupSnapshot
		stagingDir := backupStagingDir(db.config.RootPath, bakID, schema.ClassName(class))
		if err := os.RemoveAll(stagingDir); err != nil {
			db.logger.WithField("staging_dir", stagingDir).WithError(err).Warn("failed to remove backup staging dir")
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

// probeHardlinkSupport tests whether the filesystem backing RootPath supports hardlinks.
func (i *Index) probeHardlinkSupport() bool {
	f, err := os.CreateTemp(i.Config.RootPath, ".hardlink-probe-*")
	if err != nil {
		return false
	}
	src := f.Name()
	f.Close()
	defer os.Remove(src)

	dst := src + ".link"
	defer os.Remove(dst)

	return os.Link(src, dst) == nil
}

// descriptor record everything needed to restore a class
func (i *Index) descriptor(ctx context.Context, backupID string, desc *backup.ClassDescriptor, classBaseDescrs []*backup.ClassDescriptor) (err error) {
	if err := i.initBackup(backupID); err != nil {
		return err
	}

	useHardlinks := i.probeHardlinkSupport()
	i.logger.WithField("hardlinks_supported", useHardlinks).Info("backup: probed filesystem hardlink support")

	if useHardlinks {
		return i.descriptorWithHardlinks(ctx, backupID, desc, classBaseDescrs)
	}
	return i.descriptorWithoutHardlinks(ctx, backupID, desc, classBaseDescrs)
}

// descriptorWithHardlinks creates hard-linked snapshots per shard, allowing compaction
// to resume immediately after the snapshot is taken (~2-5s pause per shard).
//
// It iterates the sharding state (single source of truth) to discover all local shards,
// then uses the shardMap to determine the backup method per shard under backupLock.Lock.
func (i *Index) descriptorWithHardlinks(ctx context.Context, backupID string, desc *backup.ClassDescriptor, classBaseDescrs []*backup.ClassDescriptor) (err error) {
	stagingRoot := backupStagingDir(i.Config.RootPath, backupID, i.Config.ClassName)
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return fmt.Errorf("create backup staging dir: %w", err)
	}

	defer func() {
		if err != nil {
			os.RemoveAll(stagingRoot)
			// closelock is hold by the caller
			enterrors.GoWrapper(func() { i.ReleaseBackup(ctx, backupID) }, i.logger)
		}
	}()

	desc.StagingDir = stagingRoot

	shardNames, err := i.localShardNames()
	if err != nil {
		return fmt.Errorf("list local shards: %w", err)
	}

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(i.logger, ctx)
	eg.SetLimit(_NUMCPU)
	mu := sync.Mutex{}
	shards := map[string]*backup.ShardDescriptor{}

	for _, name := range shardNames {
		eg.Go(func() error {
			if i.closingCtx.Err() != nil {
				return nil
			}
			sd, err := i.backupShardWithHardlinks(ctx, name, classBaseDescrs, stagingRoot)
			if err != nil {
				if errors.Is(err, errFrozenShard) {
					return nil
				}
				return err
			}
			if sd != nil {
				mu.Lock()
				shards[name] = sd
				mu.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("backup shards with hardlinks: %w", err)
	}

	// Preserve original shard order from sharding state.
	for _, name := range shardNames {
		if sd, ok := shards[name]; ok {
			desc.Shards = append(desc.Shards, sd)
		}
	}

	return i.marshalBackupMetadata(desc)
}

// backupShardWithHardlinks backs up a single shard using hardlinks. Under backupLock.Lock,
// it checks the shardMap to determine whether the shard is active (loaded in memory) or
// inactive (on disk only), and uses the appropriate backup path.
func (i *Index) backupShardWithHardlinks(ctx context.Context, name string, classBaseDescrs []*backup.ClassDescriptor, stagingRoot string) (*backup.ShardDescriptor, error) {
	shardBaseDescr := i.collectShardBaseDescrs(name, classBaseDescrs)

	i.backupLock.Lock(name)
	defer i.backupLock.Unlock(name)

	var sd backup.ShardDescriptor

	shard := i.shards.Load(name)

	if shard == nil {
		// Not in shardMap => back up from disk if directory exists.
		if err := i.backupInactiveShardWithHardlinks(name, &sd, shardBaseDescr, stagingRoot); err != nil {
			return nil, err
		}
		return &sd, nil
	}

	// For unloaded LazyLoadShards, block concurrent loading so we can safely
	// read files from disk without the LSM store being opened underneath us.
	// Read paths don't use backupLock.RLock, so backupLock.Lock alone is not
	// sufficient to prevent concurrent lazy loading.
	if lazyShard, ok := shard.(*LazyLoadShard); ok {
		releaseBlock := lazyShard.blockLoading()
		if !lazyShard.loaded {
			// Shard is in the map but not loaded; read from disk.
			defer releaseBlock()
			if err := i.backupInactiveShardWithHardlinks(name, &sd, shardBaseDescr, stagingRoot); err != nil {
				return nil, err
			}
			return &sd, nil
		}
		// Shard is already loaded => release immediately and use the active path.
		releaseBlock()
	}

	// Active path => shard is loaded in memory.
	files, err := shard.CreateBackupSnapshot(ctx, &sd, stagingRoot)
	if err != nil {
		return nil, fmt.Errorf("snapshot shard %v: %w", name, err)
	}

	if err := sd.FillFileInfo(files, shardBaseDescr, stagingRoot); err != nil {
		return nil, fmt.Errorf("gather shard %v file info: %w", name, err)
	}

	return &sd, nil
}

// backupInactiveShardWithHardlinks backs up an inactive (unloaded) shard by reading
// its files from disk and hardlinking them into the staging directory.
func (i *Index) backupInactiveShardWithHardlinks(name string, sd *backup.ShardDescriptor, shardBaseDescr []backup.ShardAndID, stagingRoot string) error {
	shardDir := shardPath(i.path(), name)
	if _, err := os.Stat(shardDir); err != nil {
		if os.IsNotExist(err) {
			// FROZEN/OFFLOADED — no local data. Status is preserved in the
			// sharding state; omit from desc.Shards.
			return errFrozenShard
		}
		return fmt.Errorf("stat shard dir: %w", err)
	}

	files, err := i.listInactiveShardFiles(name, sd)
	if err != nil {
		return fmt.Errorf("list inactive shard %s files: %w", name, err)
	}

	for _, relPath := range files {
		src := filepath.Join(i.Config.RootPath, relPath)
		dst := filepath.Join(stagingRoot, relPath)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("create staging subdir for inactive shard %s file %s: %w", name, relPath, err)
		}
		if isMutableFile(relPath) {
			if err := copyFile(src, dst); err != nil {
				return fmt.Errorf("copy mutable inactive shard %s file %s to staging: %w", name, relPath, err)
			}
			continue
		}
		if err := os.Link(src, dst); err != nil {
			return fmt.Errorf("hardlink inactive shard %s file %s to staging: %w", name, relPath, err)
		}

	}

	if err := sd.FillFileInfo(files, shardBaseDescr, stagingRoot); err != nil {
		return fmt.Errorf("gather inactive shard %s file info: %w", name, err)
	}

	return nil
}

// isMutableFile reports whether a backup file (relative path) can be modified
// in place after a COLD/INACTIVE shard is activated. Such files must be copied
// rather than hard-linked during backup to avoid post-snapshot corruption.
func isMutableFile(relPath string) bool {
	base := filepath.Base(relPath)
	ext := filepath.Ext(base)

	// LSM WAL files — reopened with O_APPEND on activation (commitlogger.go:248)
	if ext == ".wal" {
		return true
	}
	// Flat index BoltDB metadata — mmap in-place writes (metadata.go:108)
	if strings.HasPrefix(base, "meta") && ext == ".db" {
		return true
	}
	// HNSW commitlog files (non-condensed) — latest is reopened with O_APPEND
	// on activation (commit_logger.go:162). Condensed files are immutable.
	if strings.Contains(relPath, ".hnsw.commitlog.d") && ext != ".condensed" {
		return true
	}
	return false
}

// copyFile creates an independent copy of src at dst, fsyncing the destination.
// Used instead of os.Link for mutable files where a shared inode would allow
// post-snapshot writes to corrupt the backup copy.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy data: %w", err)
	}

	return out.Sync()
}

// descriptorWithoutHardlinks is the fallback path for filesystems that don't support
// hardlinks. Compaction remains paused for the entire backup upload duration.
func (i *Index) descriptorWithoutHardlinks(ctx context.Context, backupID string, desc *backup.ClassDescriptor, classBaseDescrs []*backup.ClassDescriptor) (err error) {
	defer func() {
		if err != nil {
			// closelock is hold by the caller
			enterrors.GoWrapper(func() { i.ReleaseBackup(ctx, backupID) }, i.logger)
		}
	}()

	shardNames, err := i.localShardNames()
	if err != nil {
		return fmt.Errorf("list local shards: %w", err)
	}

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(i.logger, ctx)
	eg.SetLimit(_NUMCPU)
	mu := sync.Mutex{}
	shards := map[string]*backup.ShardDescriptor{}

	for _, name := range shardNames {
		eg.Go(func() error {
			if i.closingCtx.Err() != nil {
				return nil
			}
			sd, err := i.backupShardWithoutHardlinks(ctx, name, classBaseDescrs)
			if err != nil {
				if errors.Is(err, errFrozenShard) {
					return nil
				}
				return err
			}
			if sd != nil {
				mu.Lock()
				shards[name] = sd
				mu.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("backup shards without hardlinks: %w", err)
	}

	// Preserve original shard order from sharding state.
	for _, name := range shardNames {
		if sd, ok := shards[name]; ok {
			desc.Shards = append(desc.Shards, sd)
		}
	}

	return i.marshalBackupMetadata(desc)
}

// backupShardWithoutHardlinks backs up a single shard without hardlinks. Compaction
// for active shards is paused and stays paused until ReleaseBackup is called.
//
// For inactive shards, backupProtectedShards is set and backupLock.Lock is held
// until ReleaseBackup to block both activation and FREEZE/FROZEN file operations.
func (i *Index) backupShardWithoutHardlinks(ctx context.Context, name string, classBaseDescrs []*backup.ClassDescriptor) (*backup.ShardDescriptor, error) {
	shardBaseDescr := i.collectShardBaseDescrs(name, classBaseDescrs)

	i.backupLock.Lock(name)
	unlockOnReturn := true
	defer func() {
		if unlockOnReturn {
			i.backupLock.Unlock(name)
		}
	}()

	var shard ShardLike
	var sd backup.ShardDescriptor
	var err error
	if err := func() error {
		// Acquire shardCreateLocks to atomically check shard state and protect
		// inactive shards. This prevents concurrent activation from racing.
		i.shardCreateLocks.Lock(name)
		defer i.shardCreateLocks.Unlock(name)
		shard = i.shards.Load(name)

		if shard == nil {
			// Not in shardMap => back up from disk if directory exists.
			// Mark as protected and keep backupLock.Lock held until ReleaseBackup.
			i.backupProtectedShards.Store(name, struct{}{})
			unlockOnReturn = false
			return i.backupInactiveShardWithoutHardlinks(name, &sd, shardBaseDescr)
		}

		// For unloaded LazyLoadShards, block concurrent loading so we can safely
		// read files from disk. See backupShardWithHardlinks for details.
		if lazyShard, ok := shard.(*LazyLoadShard); ok {
			releaseBlock := lazyShard.blockLoading()
			defer releaseBlock()
			if !lazyShard.loaded {
				// Shard is in the map but not loaded; protect and keep lock held.
				i.backupProtectedShards.Store(name, struct{}{})
				unlockOnReturn = false
				return i.backupInactiveShardWithoutHardlinks(name, &sd, shardBaseDescr)
			}
		}

		return nil
	}(); err != nil {
		return nil, err
	}

	// Active path => halt compaction (stays paused until ReleaseBackup).
	// backupLock.Lock is released on return (unlockOnReturn=true).
	if err := shard.HaltForTransfer(ctx, false, 0); err != nil {
		return nil, fmt.Errorf("halt shard %v for backup: %w", name, err)
	}

	files, err := shard.ListBackupFiles(ctx, &sd)
	if err != nil {
		return nil, fmt.Errorf("list backup files shard %v: %w", name, err)
	}

	if err := sd.FillFileInfo(files, shardBaseDescr, i.Config.RootPath); err != nil {
		return nil, fmt.Errorf("gather shard %v file info: %w", name, err)
	}

	return &sd, nil
}

// backupInactiveShardWithoutHardlinks backs up an inactive (unloaded) shard by reading
// its files directly from disk.
func (i *Index) backupInactiveShardWithoutHardlinks(name string, sd *backup.ShardDescriptor, shardBaseDescr []backup.ShardAndID) error {
	shardDir := shardPath(i.path(), name)
	if _, err := os.Stat(shardDir); err != nil {
		if os.IsNotExist(err) {
			// FROZEN/OFFLOADED — no local data. Status is preserved in the
			// sharding state; omit from desc.Shards.
			return errFrozenShard
		}
		return fmt.Errorf("stat shard dir: %w", err)
	}

	files, err := i.listInactiveShardFiles(name, sd)
	if err != nil {
		return fmt.Errorf("list inactive shard %s files: %w", name, err)
	}

	if err := sd.FillFileInfo(files, shardBaseDescr, i.Config.RootPath); err != nil {
		return fmt.Errorf("gather inactive shard %s file info: %w", name, err)
	}

	return nil
}

// collectShardBaseDescrs gathers base descriptors for incremental backups of a given shard.
func (i *Index) collectShardBaseDescrs(shardName string, classBaseDescrs []*backup.ClassDescriptor) []backup.ShardAndID {
	var result []backup.ShardAndID
	for _, classBaseDescr := range classBaseDescrs {
		shardBaseDescrTmp := classBaseDescr.GetShardDescriptor(shardName)
		if shardBaseDescrTmp == nil {
			continue
		}
		result = append(result, backup.ShardAndID{
			ShardDesc: shardBaseDescrTmp,
			BackupID:  classBaseDescr.BackupID,
		})
	}
	return result
}

// marshalBackupMetadata marshals sharding state, schema, and aliases into the class descriptor.
func (i *Index) marshalBackupMetadata(desc *backup.ClassDescriptor) error {
	var err error
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
	return nil
}

func backupStagingDir(rootPath, backupID string, className schema.ClassName) string {
	return filepath.Join(rootPath, backup.BackupStagingPrefix+backupID+"-"+indexID(className))
}

// ReleaseBackup marks the specified backup as inactive and restarts all
// async background and maintenance processes. It errors if the backup does not exist
// or is already inactive.
func (i *Index) ReleaseBackup(ctx context.Context, id string) error {
	i.logger.WithField("backup_id", id).WithField("class", i.Config.ClassName).Info("release backup")

	// Clean up staging directory (idempotent — RemoveAll on non-existent dir is no-op)
	stagingDir := backupStagingDir(i.Config.RootPath, id, i.Config.ClassName)
	if err := os.RemoveAll(stagingDir); err != nil {
		i.logger.WithField("staging_dir", stagingDir).WithError(err).Warn("failed to remove backup staging dir")
	}

	// Release non-hardlink backup protections: clear the protection flag and
	// release the held backupLock.Lock for each protected shard.
	i.backupProtectedShards.Range(func(key, _ any) bool {
		name := key.(string)
		i.backupLock.Unlock(name)
		i.backupProtectedShards.Delete(key)
		return true
	})

	i.resetBackupState()
	// resumeMaintenanceCycles is still called for safety, but is a no-op since
	// CreateBackupSnapshot already resumed compaction. Handles edge cases where
	// a snapshot creation failed mid-way.
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

// localShardNames reads the sharding state and returns the names of all shards
// that belong to this node, regardless of tenant status. This is used as the single
// source of truth for which shards to back up, avoiding the race condition of
// iterating two separate data structures.
func (i *Index) localShardNames() ([]string, error) {
	nodeName := i.getSchema.NodeName()
	var names []string
	err := i.schemaReader.Read(i.Config.ClassName.String(), true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return nil
		}
		for shardName, phys := range state.Physical {
			if phys.IsLocalShard(nodeName) {
				names = append(names, shardName)
			}
		}
		return nil
	})
	return names, err
}

// listInactiveShardFiles reads an INACTIVE (unloaded) shard's data directly from the
// filesystem and populates sd with metadata. Returns file paths relative to
// i.Config.RootPath. INACTIVE shards are fully quiesced (Shutdown flushes and
// closes the LSM store), so files are stable and safe to read without locking
// the store.
func (i *Index) listInactiveShardFiles(shardName string, sd *backup.ShardDescriptor) ([]string, error) {
	shardDir := shardPath(i.path(), shardName)
	rootPath := i.Config.RootPath

	sd.Name = shardName
	sd.Node = i.getSchema.NodeName()

	// Read metadata files (same data as readBackupMetadata in shard_backup.go).
	counterPath := filepath.Join(shardDir, "indexcount")
	data, err := os.ReadFile(counterPath)
	if err != nil {
		return nil, fmt.Errorf("read counter: %w", err)
	}
	sd.DocIDCounter = data
	if sd.DocIDCounterPath, err = filepath.Rel(rootPath, counterPath); err != nil {
		return nil, fmt.Errorf("counter rel path: %w", err)
	}

	plPath := filepath.Join(shardDir, "proplengths")
	data, err = os.ReadFile(plPath)
	if err != nil {
		return nil, fmt.Errorf("read proplengths: %w", err)
	}
	sd.PropLengthTracker = data
	if sd.PropLengthTrackerPath, err = filepath.Rel(rootPath, plPath); err != nil {
		return nil, fmt.Errorf("proplengths rel path: %w", err)
	}

	versionPath := filepath.Join(shardDir, "version")
	data, err = os.ReadFile(versionPath)
	if err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	sd.Version = data
	if sd.ShardVersionPath, err = filepath.Rel(rootPath, versionPath); err != nil {
		return nil, fmt.Errorf("version rel path: %w", err)
	}

	var files []string

	// List LSM store files. Unlike the ACTIVE path (Bucket.listFiles), we include
	// .wal files because Bucket.Shutdown may use flushWAL() instead of flush()
	// for small memtables (shouldReuseWAL), making the WAL the only data copy.
	lsmDir := filepath.Join(shardDir, "lsm")
	lsmFiles, err := listInactiveLSMFiles(lsmDir, rootPath)
	if err != nil {
		return nil, fmt.Errorf("list lsm files: %w", err)
	}
	files = append(files, lsmFiles...)

	// List vector index files (all non-lsm subdirectories of the shard).
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, fmt.Errorf("read shard dir: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "lsm" {
			continue
		}
		vectorDir := filepath.Join(shardDir, entry.Name())
		if err := filepath.WalkDir(vectorDir, func(fpath string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if filepath.Ext(d.Name()) == ".tmp" {
				return nil
			}
			relPath, relErr := filepath.Rel(rootPath, fpath)
			if relErr != nil {
				return relErr
			}
			files = append(files, relPath)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("list vector index %s files: %w", entry.Name(), err)
		}
	}

	return files, nil
}

// listInactiveLSMFiles walks the LSM directory of an INACTIVE shard, collecting all
// stable files. Unlike the ACTIVE path, .wal files are included because an INACTIVE
// shard's Bucket.Shutdown may have used flushWAL rather than flush, leaving
// data only in the WAL.
func listInactiveLSMFiles(lsmDir, rootPath string) ([]string, error) {
	entries, err := os.ReadDir(lsmDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string

	for _, entry := range entries {
		entryPath := filepath.Join(lsmDir, entry.Name())

		if entry.Name() == ".migrations" {
			// Walk migrations recursively, same as Store.listMigrationFiles.
			if err := filepath.WalkDir(entryPath, func(fpath string, d os.DirEntry, err error) error {
				if err != nil || d == nil || d.IsDir() {
					return nil
				}
				relPath, relErr := filepath.Rel(rootPath, fpath)
				if relErr != nil {
					return relErr
				}
				files = append(files, relPath)
				return nil
			}); err != nil {
				return nil, fmt.Errorf("list migration files: %w", err)
			}
			continue
		}

		if !entry.IsDir() {
			continue
		}

		// Bucket directory: list root-level files, skip subdirectories (scratch spaces).
		bucketEntries, err := os.ReadDir(entryPath)
		if err != nil {
			return nil, fmt.Errorf("read bucket dir %s: %w", entry.Name(), err)
		}

		basePath, err := filepath.Rel(rootPath, entryPath)
		if err != nil {
			return nil, err
		}

		for _, be := range bucketEntries {
			if be.IsDir() {
				continue
			}
			if filepath.Ext(be.Name()) == ".tmp" {
				continue
			}
			files = append(files, filepath.Join(basePath, be.Name()))
		}
	}

	return files, nil
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
