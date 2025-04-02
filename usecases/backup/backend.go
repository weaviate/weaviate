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

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TODO adjust or make configurable
const (
	storeTimeout = 24 * time.Hour
	metaTimeout  = 20 * time.Minute

	// DefaultChunkSize if size is not specified
	DefaultChunkSize = 1 << 27 // 128MB

	// maxChunkSize is the upper bound on the chunk size
	maxChunkSize = 1 << 29 // 512MB

	// minChunkSize is the lower bound on the chunk size
	minChunkSize = 1 << 21 // 2MB

	// maxCPUPercentage max CPU percentage can be consumed by the file writer
	maxCPUPercentage = 80

	// DefaultCPUPercentage default CPU percentage can be consumed by the file writer
	DefaultCPUPercentage = 50
)

const (
	// BackupFile used by a node to store its metadata
	BackupFile = "backup.json"
	// GlobalBackupFile used by coordinator to store its metadata
	GlobalBackupFile  = "backup_config.json"
	GlobalRestoreFile = "restore_config.json"
	TempDirectory     = ".backup.tmp"
)

var _NUMCPU = runtime.NumCPU()

type objectStore struct {
	backend modulecapabilities.BackupBackend

	backupId string // use supplied backup id
	bucket   string // Override bucket for one call
	path     string // Override path for one call
}

func (s *objectStore) HomeDir(overrideBucket, overridePath string) string {
	return s.backend.HomeDir(s.backupId, overrideBucket, overridePath)
}

func (s *objectStore) WriteToFile(ctx context.Context, key, destPath, overrideBucket, overridePath string) error {
	return s.backend.WriteToFile(ctx, s.backupId, key, destPath, overrideBucket, overridePath)
}

// SourceDataPath is data path of all source files
func (s *objectStore) SourceDataPath() string {
	return s.backend.SourceDataPath()
}

func (s *objectStore) Write(ctx context.Context, key, overrideBucket, overridePath string, r io.ReadCloser) (int64, error) {
	return s.backend.Write(ctx, s.backupId, key, overrideBucket, overridePath, r)
}

func (s *objectStore) Read(ctx context.Context, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	return s.backend.Read(ctx, s.backupId, key, overrideBucket, overridePath, w)
}

func (s *objectStore) Initialize(ctx context.Context, overrideBucket, overridePath string) error {
	return s.backend.Initialize(ctx, s.backupId, overrideBucket, overridePath)
}

// meta marshals and uploads metadata
func (s *objectStore) putMeta(ctx context.Context, key, overrideBucket, overridePath string, desc interface{}) error {
	bytes, err := json.Marshal(desc)
	if err != nil {
		return fmt.Errorf("putMeta: marshal meta file %q: %w", key, err)
	}
	ctx, cancel := context.WithTimeout(ctx, metaTimeout)
	defer cancel()
	if err := s.backend.PutObject(ctx, s.backupId, key, overrideBucket, overridePath, bytes); err != nil {
		return fmt.Errorf("putMeta: upload meta file %q into bucket %v, path %v: %w", key, overrideBucket, overridePath, err)
	}
	return nil
}

func (s *objectStore) meta(ctx context.Context, key, overrideBucket, overridePath string, dest interface{}) error {
	bytes, err := s.backend.GetObject(ctx, s.backupId, key, overrideBucket, overridePath)
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
	objectStore
}

func NewNodeStore(backend modulecapabilities.BackupBackend, backupId, bucket, path string) *nodeStore {
	return &nodeStore{objectStore: objectStore{backend, backupId, bucket, path}}
}

// Meta gets meta data using standard path or deprecated old path
//
// adjustBasePath: sets the base path to the old path if the backup has been created prior to v1.17.
func (s *nodeStore) Meta(ctx context.Context, backupID, overrideBucket, overridePath string, adjustBasePath bool) (*backup.BackupDescriptor, error) {
	var result backup.BackupDescriptor
	err := s.meta(ctx, BackupFile, overrideBucket, overridePath, &result)
	if err != nil {
		cs := &objectStore{s.backend, backupID, overrideBucket, overridePath} // for backward compatibility
		if err := cs.meta(ctx, BackupFile, overrideBucket, overridePath, &result); err == nil {
			if adjustBasePath {
				s.objectStore.backupId = backupID
			}
			return &result, nil
		}
	}

	return &result, err
}

// meta marshals and uploads metadata
func (s *nodeStore) PutMeta(ctx context.Context, desc *backup.BackupDescriptor, overrideBucket, overridePath string) error {
	return s.putMeta(ctx, BackupFile, overrideBucket, overridePath, desc)
}

type coordStore struct {
	objectStore
}

// PutMeta puts coordinator's global metadata into object store
func (s *coordStore) PutMeta(ctx context.Context, filename string, desc *backup.DistributedBackupDescriptor, overrideBucket, overridePath string) error {
	return s.putMeta(ctx, filename, overrideBucket, overridePath, desc)
}

// Meta gets coordinator's global metadata from object store
func (s *coordStore) Meta(ctx context.Context, filename, overrideBucket, overridePath string) (*backup.DistributedBackupDescriptor, error) {
	var result backup.DistributedBackupDescriptor
	err := s.meta(ctx, filename, overrideBucket, overridePath, &result)
	if err != nil && filename == GlobalBackupFile {
		var oldBackup backup.BackupDescriptor
		if err := s.meta(ctx, BackupFile, overrideBucket, overridePath, &oldBackup); err == nil {
			return oldBackup.ToDistributed(), nil
		}
	}
	return &result, err
}

// uploader uploads backup artifacts. This includes db files and metadata
type uploader struct {
	sourcer        Sourcer
	rbacSourcer    SourcerNonClass
	dynUserSourcer SourcerNonClass
	backend        nodeStore
	backupID       string
	zipConfig
	setStatus func(st backup.Status)
	log       logrus.FieldLogger
}

func newUploader(sourcer Sourcer, rbacSourcer SourcerNonClass, dynUserSourcer SourcerNonClass, backend nodeStore,
	backupID string, setstatus func(st backup.Status), l logrus.FieldLogger,
) *uploader {
	return &uploader{
		sourcer, rbacSourcer, dynUserSourcer, backend,
		backupID,
		newZipConfig(Compression{
			Level:         DefaultCompression,
			CPUPercentage: DefaultCPUPercentage,
			ChunkSize:     DefaultChunkSize,
		}),
		setstatus,
		l,
	}
}

func (u *uploader) withCompression(cfg zipConfig) *uploader {
	u.zipConfig = cfg
	return u
}

// all uploads all files in addition to the metadata file
func (u *uploader) all(ctx context.Context, classes []string, desc *backup.BackupDescriptor, overrideBucket, overridePath string) (err error) {
	u.setStatus(backup.Transferring)
	desc.Status = string(backup.Transferring)
	ch := u.sourcer.BackupDescriptors(ctx, desc.ID, classes)
	defer func() {
		//  make sure context is not cancelled when uploading metadata
		ctx := context.Background()
		if err != nil {
			desc.Error = err.Error()
			if errors.Is(err, context.Canceled) {
				u.setStatus(backup.Cancelled)
				desc.Status = string(backup.Cancelled)
				u.releaseIndexes(classes, desc.ID)
			}
			err = fmt.Errorf("upload %w: %w", err, u.backend.PutMeta(ctx, desc, overrideBucket, overridePath))
		} else {
			u.log.Info("start uploading meta data")
			if err = u.backend.PutMeta(ctx, desc, overrideBucket, overridePath); err != nil {
				desc.Status = string(backup.Transferred)
			}
			u.setStatus(backup.Success)
			u.log.Info("finish uploading meta data")
		}
	}()

	contextChecker := func(ctx context.Context) error {
		ctxerr := ctx.Err()
		if ctxerr != nil {
			u.setStatus(backup.Cancelled)
			desc.Status = string(backup.Cancelled)
			u.releaseIndexes(classes, desc.ID)
		}
		return ctxerr
	}

Loop:
	for {
		select {
		case cdesc, ok := <-ch:
			if !ok {
				u.releaseIndexes(classes, desc.ID)
				break Loop // we are done
			}
			if cdesc.Error != nil {
				return cdesc.Error
			}
			u.log.WithField("class", cdesc.Name).Info("start uploading files")
			if err := u.class(ctx, desc.ID, &cdesc, overrideBucket, overridePath); err != nil {
				return err
			}
			desc.Classes = append(desc.Classes, cdesc)
			u.log.WithField("class", cdesc.Name).Info("finish uploading files")

		case <-ctx.Done():
			return contextChecker(ctx)
		}
	}

	if err := ctx.Err(); err != nil {
		return contextChecker(ctx)
	} else if u.rbacSourcer != nil {
		descrp, err := u.rbacSourcer.GetDescriptors(ctx)
		if err != nil {
			return err
		}
		desc.RbacBackups = descrp
	}

	if err := ctx.Err(); err != nil {
		return contextChecker(ctx)
	} else if u.dynUserSourcer != nil {
		descrp, err := u.dynUserSourcer.GetDescriptors(ctx)
		if err != nil {
			return err
		}
		desc.DynUserBackups = descrp
	}

	u.setStatus(backup.Transferred)
	desc.Status = string(backup.Success)
	return nil
}

func (u *uploader) releaseIndexes(classes []string, ID string) {
	for _, class := range classes {
		className := class
		enterrors.GoWrapper(func() {
			if err := u.sourcer.ReleaseBackup(context.Background(), ID, className); err != nil {
				u.log.WithFields(logrus.Fields{
					"class":    className,
					"backupID": ID,
				}).Error("failed to release backup")
			}
		}, u.log)
	}
}

// class uploads one class
func (u *uploader) class(ctx context.Context, id string, desc *backup.ClassDescriptor, overrideBucket, overridePath string) (err error) {
	classLabel := desc.Name
	if monitoring.GetMetrics().Group {
		classLabel = "n/a"
	}
	metric, err := monitoring.GetMetrics().BackupStoreDurations.GetMetricWithLabelValues(getType(u.backend.backend), classLabel)
	if err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}
	defer func() {
		// backups need to be released anyway
		enterrors.GoWrapper(func() {
			if err := u.sourcer.ReleaseBackup(context.Background(), id, desc.Name); err != nil {
				u.log.WithFields(logrus.Fields{
					"class":    id,
					"backupID": desc.Name,
				}).Error("failed to release backup")
			}
		}, u.log)
	}()
	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()

	u.log.WithFields(logrus.Fields{
		"action":   "upload_class",
		"duration": storeTimeout,
	}).Debug("context.WithTimeout")

	nShards := len(desc.Shards)
	if nShards == 0 {
		return nil
	}

	desc.Chunks = make(map[int32][]string, 1+nShards/2)
	var (
		hasJobs   atomic.Bool
		lastChunk = int32(0)
		nWorker   = u.GoPoolSize
	)
	if nWorker > nShards {
		nWorker = nShards
	}
	hasJobs.Store(nShards > 0)

	// jobs produces work for the processor
	jobs := func(xs []*backup.ShardDescriptor) <-chan *backup.ShardDescriptor {
		sendCh := make(chan *backup.ShardDescriptor)
		f := func() {
			defer close(sendCh)
			defer hasJobs.Store(false)

			for _, shard := range xs {
				select {
				case sendCh <- shard:
				// cancellation will happen for two reasons:
				//  - 1. if the whole operation has been aborted,
				//  - 2. or if the processor routine returns an error
				case <-ctx.Done():
					return
				}
			}
		}
		enterrors.GoWrapper(f, u.log)
		return sendCh
	}

	// processor
	processor := func(nWorker int, sender <-chan *backup.ShardDescriptor) <-chan chuckShards {
		eg, ctx := enterrors.NewErrorGroupWithContextWrapper(u.log, ctx)
		eg.SetLimit(nWorker)
		recvCh := make(chan chuckShards, nWorker)
		f := func() {
			defer close(recvCh)
			for i := 0; i < nWorker; i++ {
				eg.Go(func() error {
					// operation might have been aborted see comment above
					if err := ctx.Err(); err != nil {
						return err
					}
					for hasJobs.Load() {
						if err := ctx.Err(); err != nil {
							return err
						}
						chunk := atomic.AddInt32(&lastChunk, 1)
						shards, err := u.compress(ctx, desc.Name, chunk, sender, overrideBucket, overridePath)
						if err != nil {
							return err
						}
						if m := int32(len(shards)); m > 0 {
							recvCh <- chuckShards{chunk, shards}
						}
					}
					return err
				})
			}
			err = eg.Wait()
		}
		enterrors.GoWrapper(f, u.log)
		return recvCh
	}

	for x := range processor(nWorker, jobs(desc.Shards)) {
		desc.Chunks[x.chunk] = x.shards
	}
	return
}

type chuckShards struct {
	chunk  int32
	shards []string
}

func (u *uploader) compress(ctx context.Context,
	class string, // class name
	chunk int32, // chunk index
	ch <-chan *backup.ShardDescriptor, // chan of shards
	overrideBucket, overridePath string, // bucket name and path
) ([]string, error) {
	var (
		chunkKey = chunkKey(class, chunk)
		shards   = make([]string, 0, 10)
		// add tolerance to enable better optimization of the chunk size
		maxSize = int64(u.ChunkSize + u.ChunkSize/20) // size + 5%
	)
	zip, reader := NewZip(u.backend.SourceDataPath(), u.Level)
	producer := func() error {
		defer zip.Close()
		lastShardSize := int64(0)
		for shard := range ch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := zip.WriteShard(ctx, shard); err != nil {
				return err
			}
			shard.Chunk = chunk
			shards = append(shards, shard.Name)
			shard.ClearTemporary()
			zip.gzw.Flush() // flush new shard
			lastShardSize = zip.lastWritten() - lastShardSize
			if zip.lastWritten()+lastShardSize > maxSize {
				break
			}
		}
		return nil
	}

	// consumer
	eg := enterrors.NewErrorGroupWrapper(u.log)
	eg.Go(func() error {
		if _, err := u.backend.Write(ctx, chunkKey, overrideBucket, overridePath, reader); err != nil {
			return err
		}
		return nil
	})

	if err := producer(); err != nil {
		return shards, err
	}
	// wait for the consumer to finish
	return shards, eg.Wait()
}

// fileWriter downloads files from object store and writes files to the destination folder destDir
type fileWriter struct {
	sourcer    Sourcer
	backend    nodeStore
	tempDir    string
	destDir    string
	movedFiles []string // files successfully moved to destination folder
	compressed bool
	GoPoolSize int
	migrator   func(classPath string) error
	logger     logrus.FieldLogger
}

func newFileWriter(sourcer Sourcer, backend nodeStore,
	compressed bool, logger logrus.FieldLogger,
) *fileWriter {
	destDir := backend.SourceDataPath()
	return &fileWriter{
		sourcer:    sourcer,
		backend:    backend,
		destDir:    destDir,
		tempDir:    path.Join(destDir, TempDirectory),
		movedFiles: make([]string, 0, 64),
		compressed: compressed,
		GoPoolSize: routinePoolSize(50),
		logger:     logger,
	}
}

func (fw *fileWriter) WithPoolPercentage(p int) *fileWriter {
	fw.GoPoolSize = routinePoolSize(p)
	return fw
}

func (fw *fileWriter) setMigrator(m func(classPath string) error) { fw.migrator = m }

// Write downloads files and put them in the destination directory
func (fw *fileWriter) Write(ctx context.Context, desc *backup.ClassDescriptor, overrideBucket, overridePath string) (err error) {
	if len(desc.Shards) == 0 { // nothing to copy
		return nil
	}
	classTempDir := path.Join(fw.tempDir, desc.Name)

	if err := fw.writeTempFiles(ctx, classTempDir, overrideBucket, overridePath, desc); err != nil {
		return fmt.Errorf("get files: %w", err)
	}

	if fw.migrator != nil {
		if err := fw.migrator(classTempDir); err != nil {
			return fmt.Errorf("migrate from pre 1.23: %w", err)
		}
	}

	return nil
}

// writeTempFiles writes class files into a temporary directory
// temporary directory path = d.tempDir/className
// Function makes sure that created files will be removed in case of an error
func (fw *fileWriter) writeTempFiles(ctx context.Context, classTempDir, overrideBucket, overridePath string, desc *backup.ClassDescriptor) (err error) {
	if err := os.RemoveAll(classTempDir); err != nil {
		return fmt.Errorf("remove %s: %w", classTempDir, err)
	}
	if err := os.MkdirAll(classTempDir, os.ModePerm); err != nil {
		return fmt.Errorf("create temp class folder %s: %w", classTempDir, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// no compression processed as before
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(fw.logger, ctx)
	if !fw.compressed {
		eg.SetLimit(2 * _NUMCPU)
		for _, shard := range desc.Shards {
			shard := shard
			eg.Go(func() error { return fw.writeTempShard(ctx, shard, classTempDir, overrideBucket, overridePath) }, shard.Name)
		}
		return eg.Wait()
	}

	// source files are compressed

	eg.SetLimit(fw.GoPoolSize)
	for k := range desc.Chunks {
		chunk := chunkKey(desc.Name, k)
		eg.Go(func() error {
			uz, w := NewUnzip(classTempDir)
			enterrors.GoWrapper(func() {
				fw.backend.Read(ctx, chunk, overrideBucket, overridePath, w)
			}, fw.logger)
			_, err := uz.ReadChunk()
			return err
		})
	}
	return eg.Wait()
}

func (fw *fileWriter) writeTempShard(ctx context.Context, sd *backup.ShardDescriptor, classTempDir, overrideBucket, overridePath string) error {
	for _, key := range sd.Files {
		destPath := path.Join(classTempDir, key)
		destDir := path.Dir(destPath)
		if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
			return fmt.Errorf("create folder %s: %w", destDir, err)
		}
		if err := fw.backend.WriteToFile(ctx, key, destPath, overrideBucket, overridePath); err != nil {
			return fmt.Errorf("write file %s: %w", destPath, err)
		}
	}
	destPath := path.Join(classTempDir, sd.DocIDCounterPath)
	if err := os.WriteFile(destPath, sd.DocIDCounter, os.ModePerm); err != nil {
		return fmt.Errorf("write counter file %s: %w", destPath, err)
	}
	destPath = path.Join(classTempDir, sd.PropLengthTrackerPath)
	if err := os.WriteFile(destPath, sd.PropLengthTracker, os.ModePerm); err != nil {
		return fmt.Errorf("write prop file %s: %w", destPath, err)
	}
	destPath = path.Join(classTempDir, sd.ShardVersionPath)
	if err := os.WriteFile(destPath, sd.Version, os.ModePerm); err != nil {
		return fmt.Errorf("write version file %s: %w", destPath, err)
	}
	return nil
}

func chunkKey(class string, id int32) string {
	return fmt.Sprintf("%s/chunk-%d", class, id)
}

func routinePoolSize(percentage int) int {
	if percentage == 0 { // default value
		percentage = DefaultCPUPercentage
	} else if percentage > maxCPUPercentage {
		percentage = maxCPUPercentage
	}
	if x := (_NUMCPU * percentage) / 100; x > 0 {
		return x
	}
	return 1
}

// RestoreClassDir returns a func that restores classes on the filesystem directly from the temporary class backup stored on disk.
// This function is invoked by the Raft store when a restoration request is sent by the backup coordinator.
func RestoreClassDir(dataPath string) func(class string) error {
	return func(class string) error {
		classTempDir := filepath.Join(dataPath, TempDirectory, class)
		// nothing to restore
		if _, err := os.Stat(classTempDir); err != nil {
			return nil
		}
		defer os.RemoveAll(classTempDir)
		files, err := os.ReadDir(classTempDir)
		if err != nil {
			return fmt.Errorf("read %s", classTempDir)
		}
		destDir := dataPath

		for _, key := range files {
			from := path.Join(classTempDir, key.Name())
			to := path.Join(destDir, key.Name())
			if err := os.Rename(from, to); err != nil {
				return fmt.Errorf("move %s %s: %w", from, to, err)
			}
		}

		return nil
	}
}
