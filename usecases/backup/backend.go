//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

// TODO adjust or make configurable
const (
	storeTimeout = 2 * time.Hour
	metaTimeout  = 20 * time.Minute

	// createTimeout  = 5 * time.Minute
	// releaseTimeout = 30 * time.Second
)

const (
	// BackupFile used by a node to store its metadata
	BackupFile = "backup.json"
	// GlobalBackupFile used by coordinator to store its metadata
	GlobalBackupFile = "global_backup.json"
	_TempDirectory   = ".backup.tmp"
)

type objStore struct {
	b        modulecapabilities.BackupBackend
	BasePath string
}

// Meta gets a node's metadata from object store
func (s *objStore) HomeDir() string {
	return s.b.HomeDir(s.BasePath)
}

func (s *objStore) WriteToFile(ctx context.Context, key, destPath string) error {
	return s.b.WriteToFile(ctx, s.BasePath, key, destPath)
}

// SourceDataPath is data path of all source files
func (s *objStore) SourceDataPath() string {
	return s.b.SourceDataPath()
}

func (s *objStore) PutFile(ctx context.Context, key, srcPath string) error {
	return s.b.PutFile(ctx, s.BasePath, key, srcPath)
}

func (s *objStore) Initialize(ctx context.Context) error {
	return s.b.Initialize(ctx, s.BasePath)
}

// meta marshals and uploads metadata
func (s *objStore) putMeta(ctx context.Context, key string, desc interface{}) error {
	bytes, err := json.Marshal(desc)
	if err != nil {
		return fmt.Errorf("marshal meta file %q: %w", key, err)
	}
	ctx, cancel := context.WithTimeout(ctx, metaTimeout)
	defer cancel()
	if err := s.b.PutObject(ctx, s.BasePath, key, bytes); err != nil {
		return fmt.Errorf("upload meta file %q: %w", key, err)
	}
	return nil
}

func (s *objStore) meta(ctx context.Context, key string, dest interface{}) error {
	bytes, err := s.b.GetObject(ctx, s.BasePath, key)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, dest)
	if err != nil {
		return fmt.Errorf("marshal meta file %q: %w", key, err)
	}
	return nil
}

type nodeStore struct {
	objStore
}

func (s *nodeStore) Meta(ctx context.Context) (*backup.BackupDescriptor, error) {
	var backup backup.BackupDescriptor
	err := s.meta(ctx, BackupFile, &backup)
	return &backup, err
}

// meta marshals and uploads metadata
func (s *nodeStore) PutMeta(ctx context.Context, desc *backup.BackupDescriptor) error {
	return s.putMeta(ctx, BackupFile, desc)
}

type coordStore struct {
	objStore
}

// PutGlobalMeta puts coordinator's global metadata into object store
func (s *coordStore) PutGlobalMeta(ctx context.Context, desc *backup.DistributedBackupDescriptor) error {
	return s.putMeta(ctx, GlobalBackupFile, desc)
}

// Meta gets coordinator's global metadata from object store
func (s *coordStore) Meta(ctx context.Context, backupID string) (*backup.DistributedBackupDescriptor, error) {
	var backup backup.DistributedBackupDescriptor
	err := s.meta(ctx, GlobalBackupFile, &backup)
	return &backup, err
}

// uploader uploads backup artifacts. This includes db files and metadata
type uploader struct {
	sourcer   Sourcer
	backend   nodeStore
	backupID  string
	setStatus func(st backup.Status)
}

func newUploader(sourcer Sourcer, backend nodeStore,
	backupID string, setstaus func(st backup.Status),
) *uploader {
	return &uploader{sourcer, backend, backupID, setstaus}
}

// all uploads all files in addition to the metadata file
func (u *uploader) all(ctx context.Context, classes []string, desc *backup.BackupDescriptor) (err error) {
	u.setStatus(backup.Transferring)
	desc.Status = string(backup.Transferring)
	ch := u.sourcer.BackupDescriptors(ctx, desc.ID, classes)
	defer func() {
		if err != nil {
			desc.Error = err.Error()
			err = fmt.Errorf("upload %w: %v", err, u.backend.PutMeta(ctx, desc))
		} else {
			if err = u.backend.PutMeta(ctx, desc); err != nil {
				desc.Status = string(backup.Transferred)
			}
			u.setStatus(backup.Success)
		}
	}()
Loop:
	for {
		select {
		case cdesc, ok := <-ch:
			if !ok {
				break Loop // we are done
			}
			if cdesc.Error != nil {
				return cdesc.Error
			}
			if err := u.class(ctx, desc.ID, cdesc); err != nil {
				return err
			}
			desc.Classes = append(desc.Classes, cdesc)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
	u.setStatus(backup.Transferred)
	desc.Status = string(backup.Success)
	return nil
}

// class uploads one class
func (u *uploader) class(ctx context.Context, id string, desc backup.ClassDescriptor) (err error) {
	metric, err := monitoring.GetMetrics().BackupStoreDurations.GetMetricWithLabelValues(getType(u.backend.b), desc.Name)
	if err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}
	defer func() {
		// backups need to be released anyway
		go u.sourcer.ReleaseBackup(context.Background(), id, desc.Name)
	}()
	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	for _, shard := range desc.Shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, fpath := range shard.Files {
			if err := u.backend.PutFile(ctx, fpath, fpath); err != nil {
				return err
			}
		}
	}
	return nil
}

// fileWriter downloads files from object store and writes files to the destintion folder destDir
type fileWriter struct {
	sourcer    Sourcer
	backend    nodeStore
	tempDir    string
	destDir    string
	movedFiles []string // files successfully moved to destination folder
}

func newFileWriter(sourcer Sourcer, backend nodeStore,
	backupID string,
) *fileWriter {
	destDir := backend.SourceDataPath()
	return &fileWriter{
		sourcer:    sourcer,
		backend:    backend,
		destDir:    destDir,
		tempDir:    path.Join(destDir, _TempDirectory),
		movedFiles: make([]string, 0, 64),
	}
}

// Write downloads files and put them in the destination directory
func (fw *fileWriter) Write(ctx context.Context, desc *backup.ClassDescriptor) (rollback func() error, err error) {
	if len(desc.Shards) == 0 { // nothing to copy
		return func() error { return nil }, nil
	}
	classTempDir := path.Join(fw.tempDir, desc.Name)
	defer func() {
		if err != nil {
			if rerr := fw.rollBack(classTempDir); rerr != nil {
				err = fmt.Errorf("%w: %v", err, rerr)
			}
		}
		os.RemoveAll(classTempDir)
	}()

	if err := fw.writeTempFiles(ctx, classTempDir, desc); err != nil {
		return nil, fmt.Errorf("get files: %w", err)
	}
	if err := fw.moveAll(classTempDir); err != nil {
		return nil, fmt.Errorf("move files to destination: %w", err)
	}
	return func() error { return fw.rollBack(classTempDir) }, nil
}

// writeTempFiles writes class files into a temporary directory
// temporary directory path = d.tempDir/className
// Function makes sure that created files will be removed in case of an erro
func (fw *fileWriter) writeTempFiles(ctx context.Context, classTempDir string, desc *backup.ClassDescriptor) (err error) {
	if err := os.RemoveAll(classTempDir); err != nil {
		return fmt.Errorf("remove %s: %w", classTempDir, err)
	}
	for _, part := range desc.Shards {
		for _, key := range part.Files {
			destPath := path.Join(classTempDir, key)
			destDir := path.Dir(destPath)
			if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
				return fmt.Errorf("create folder %s: %w", destDir, err)
			}
			if err := fw.backend.WriteToFile(ctx, key, destPath); err != nil {
				return fmt.Errorf("write file %s: %w", destPath, err)
			}
		}
		destPath := path.Join(classTempDir, part.DocIDCounterPath)
		if err := os.WriteFile(destPath, part.DocIDCounter, os.ModePerm); err != nil {
			return fmt.Errorf("write file %s: %w", destPath, err)
		}
		destPath = path.Join(classTempDir, part.PropLengthTrackerPath)
		if err := os.WriteFile(destPath, part.PropLengthTracker, os.ModePerm); err != nil {
			return fmt.Errorf("write file %s: %w", destPath, err)
		}
		destPath = path.Join(classTempDir, part.ShardVersionPath)
		if err := os.WriteFile(destPath, part.Version, os.ModePerm); err != nil {
			return fmt.Errorf("write file %s: %w", destPath, err)
		}
	}
	return nil
}

// moveAll moves all files to the destination
func (fw *fileWriter) moveAll(classTempDir string) (err error) {
	files, err := os.ReadDir(classTempDir)
	if err != nil {
		return fmt.Errorf("read %s", classTempDir)
	}
	destDir := fw.destDir
	for _, key := range files {
		from := path.Join(classTempDir, key.Name())
		to := path.Join(destDir, key.Name())
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("move %s %s: %w", from, to, err)
		}
		fw.movedFiles = append(fw.movedFiles, to)
	}

	return nil
}

// rollBack successfully written files
func (fw *fileWriter) rollBack(classTempDir string) (err error) {
	// rollback successfully moved files
	for _, fpath := range fw.movedFiles {
		if rerr := os.RemoveAll(fpath); rerr != nil && err == nil {
			err = fmt.Errorf("rollback %s: %w", fpath, rerr)
		}
	}
	return err
}
