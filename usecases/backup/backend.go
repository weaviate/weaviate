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
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"golang.org/x/sync/errgroup"
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
	_TempDirectory    = ".backup.tmp"
)

var _NUMCPU = runtime.NumCPU()

type objStore struct {
	b        modulecapabilities.BackupBackend
	BasePath string
}

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

func (s *objStore) Write(ctx context.Context, key string, r io.ReadCloser) (int64, error) {
	return s.b.Write(ctx, s.BasePath, key, r)
}

func (s *objStore) Read(ctx context.Context, key string, w io.WriteCloser) (int64, error) {
	return s.b.Read(ctx, s.BasePath, key, w)
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

// Meta gets meta data using standard path or deprecated old path
//
// adjustBasePath: sets the base path to the old path if the backup has been created prior to v1.17.
func (s *nodeStore) Meta(ctx context.Context, backupID string, adjustBasePath bool) (*backup.BackupDescriptor, error) {
	var result backup.BackupDescriptor
	err := s.meta(ctx, BackupFile, &result)
	if err != nil {
		cs := &objStore{s.b, backupID} // for backward compatibility
		if err := cs.meta(ctx, BackupFile, &result); err == nil {
			if adjustBasePath {
				s.objStore.BasePath = backupID
			}
			return &result, nil
		}
	}

	return &result, err
}

// meta marshals and uploads metadata
func (s *nodeStore) PutMeta(ctx context.Context, desc *backup.BackupDescriptor) error {
	return s.putMeta(ctx, BackupFile, desc)
}

type coordStore struct {
	objStore
}

// PutMeta puts coordinator's global metadata into object store
func (s *coordStore) PutMeta(ctx context.Context, filename string, desc *backup.DistributedBackupDescriptor) error {
	return s.putMeta(ctx, filename, desc)
}

// Meta gets coordinator's global metadata from object store
func (s *coordStore) Meta(ctx context.Context, filename string) (*backup.DistributedBackupDescriptor, error) {
	var result backup.DistributedBackupDescriptor
	err := s.meta(ctx, filename, &result)
	if err != nil && filename == GlobalBackupFile {
		var oldBackup backup.BackupDescriptor
		if err := s.meta(ctx, BackupFile, &oldBackup); err == nil {
			return oldBackup.ToDistributed(), nil
		}
	}
	return &result, err
}

// uploader uploads backup artifacts. This includes db files and metadata
type uploader struct {
	sourcer  Sourcer
	backend  nodeStore
	backupID string
	zipConfig
	setStatus func(st backup.Status)
	log       logrus.FieldLogger
}

func newUploader(sourcer Sourcer, backend nodeStore,
	backupID string, setstatus func(st backup.Status), l logrus.FieldLogger,
) *uploader {
	return &uploader{
		sourcer, backend,
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
func (u *uploader) all(ctx context.Context, classes []string, desc *backup.BackupDescriptor) (err error) {
	u.setStatus(backup.Transferring)
	desc.Status = string(backup.Transferring)
	ch := u.sourcer.BackupDescriptors(ctx, desc.ID, classes)
	defer func() {
		//  make sure context is not cancelled when uploading metadata
		ctx := context.Background()
		if err != nil {
			desc.Error = err.Error()
			err = fmt.Errorf("upload %w: %v", err, u.backend.PutMeta(ctx, desc))
		} else {
			u.log.Info("start uploading meta data")
			if err = u.backend.PutMeta(ctx, desc); err != nil {
				desc.Status = string(backup.Transferred)
			}
			u.setStatus(backup.Success)
			u.log.Info("finish uploading meta data")
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
			u.log.WithField("class", cdesc.Name).Info("start uploading files")
			if err := u.class(ctx, desc.ID, &cdesc); err != nil {
				return err
			}
			desc.Classes = append(desc.Classes, cdesc)
			u.log.WithField("class", cdesc.Name).Info("finish uploading files")

		case <-ctx.Done():
			return ctx.Err()
		}
	}
	u.setStatus(backup.Transferred)
	desc.Status = string(backup.Success)
	return nil
}

// class uploads one class
func (u *uploader) class(ctx context.Context, id string, desc *backup.ClassDescriptor) (err error) {
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
		go func() {
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
		}()
		return sendCh
	}

	// processor
	processor := func(nWorker int, sender <-chan *backup.ShardDescriptor) <-chan chuckShards {
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(nWorker)
		recvCh := make(chan chuckShards, nWorker)
		go func() {
			defer close(recvCh)
			for i := 0; i < nWorker; i++ {
				eg.Go(func() error {
					// operation might have been aborted see comment above
					if err := ctx.Err(); err != nil {
						return err
					}
					for hasJobs.Load() {
						chunk := atomic.AddInt32(&lastChunk, 1)
						shards, err := u.compress(ctx, desc.Name, chunk, sender)
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
		}()
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
	var eg errgroup.Group
	eg.Go(func() error {
		if _, err := u.backend.Write(ctx, chunkKey, reader); err != nil {
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
}

func newFileWriter(sourcer Sourcer, backend nodeStore,
	backupID string, compressed bool,
) *fileWriter {
	destDir := backend.SourceDataPath()
	return &fileWriter{
		sourcer:    sourcer,
		backend:    backend,
		destDir:    destDir,
		tempDir:    path.Join(destDir, _TempDirectory),
		movedFiles: make([]string, 0, 64),
		compressed: compressed,
		GoPoolSize: routinePoolSize(50),
	}
}

func (fw *fileWriter) WithPoolPercentage(p int) *fileWriter {
	fw.GoPoolSize = routinePoolSize(p)
	return fw
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
// Function makes sure that created files will be removed in case of an error
func (fw *fileWriter) writeTempFiles(ctx context.Context, classTempDir string, desc *backup.ClassDescriptor) (err error) {
	if err := os.RemoveAll(classTempDir); err != nil {
		return fmt.Errorf("remove %s: %w", classTempDir, err)
	}
	if err := os.MkdirAll(classTempDir, os.ModePerm); err != nil {
		return fmt.Errorf("create temp class folder %s: %w", classTempDir, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// no compression processed as before
	eg, ctx := errgroup.WithContext(ctx)
	if !fw.compressed {
		eg.SetLimit(2 * _NUMCPU)
		for _, shard := range desc.Shards {
			shard := shard
			eg.Go(func() error { return fw.writeTempShard(ctx, shard, classTempDir) })
		}
		return eg.Wait()
	}

	// source files are compressed

	eg.SetLimit(fw.GoPoolSize)
	for k := range desc.Chunks {
		chunk := chunkKey(desc.Name, k)
		eg.Go(func() error {
			uz, w := NewUnzip(classTempDir)
			go func() {
				fw.backend.Read(ctx, chunk, w)
			}()
			_, err := uz.ReadChunk()
			return err
		})
	}
	return eg.Wait()
}

func (fw *fileWriter) writeTempShard(ctx context.Context, sd *backup.ShardDescriptor, classTempDir string) error {
	for _, key := range sd.Files {
		destPath := path.Join(classTempDir, key)
		destDir := path.Dir(destPath)
		if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
			return fmt.Errorf("create folder %s: %w", destDir, err)
		}
		if err := fw.backend.WriteToFile(ctx, key, destPath); err != nil {
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
