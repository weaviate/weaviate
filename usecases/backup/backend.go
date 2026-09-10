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

package backup

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TODO adjust or make configurable
const (
	storeTimeout = 24 * time.Hour
	metaTimeout  = 20 * time.Minute

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

// numCPU sizes worker pools. GOMAXPROCS (not NumCPU) may reflect the cgroup CPU
// limit, and is read at the call site so a GOMAXPROCS change during startup
// (see limitResources) is honored rather than captured at package init.
func numCPU() int {
	return runtime.GOMAXPROCS(0)
}

type objectStore struct {
	backend modulecapabilities.BackupBackend

	backupId string // use supplied backup id
	bucket   string // Override bucket for one call
	path     string // Override path for one call
	node     string
}

func (s *objectStore) HomeDir(overrideBucket, overridePath string) string {
	return s.backend.HomeDir(s.backupId, overrideBucket, overridePath)
}

// SourceDataPath is data path of all source files
func (s *objectStore) SourceDataPath() string {
	return s.backend.SourceDataPath()
}

func (s *objectStore) Write(ctx context.Context, key, overrideBucket, overridePath string, r backup.ReadCloserWithError) (int64, error) {
	return s.backend.Write(ctx, s.backupId, key, overrideBucket, overridePath, r)
}

func (s *objectStore) Read(ctx context.Context, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	return s.backend.Read(ctx, s.backupId, key, overrideBucket, overridePath, w)
}

func (s *objectStore) ReadFromOtherBackup(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	return s.backend.Read(ctx, fmt.Sprintf("%s/%s", backupID, s.node), key, overrideBucket, overridePath, w)
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

// hasMeta reports whether a parseable metadata file exists at key.
func (s *objectStore) hasMeta(ctx context.Context, key, overrideBucket, overridePath string) bool {
	var desc backup.BackupDescriptor
	return s.meta(ctx, key, overrideBucket, overridePath, &desc) == nil
}

type nodeStore struct {
	objectStore
}

// Meta gets the node's metadata. A backup carrying metadata only at the top-level base path
// is refused as errLegacySingleNode.
func (s *nodeStore) Meta(ctx context.Context, backupID, overrideBucket, overridePath string) (*backup.BackupDescriptor, error) {
	var result backup.BackupDescriptor
	err := s.meta(ctx, BackupFile, overrideBucket, overridePath, &result)
	if err != nil {
		base := &objectStore{s.backend, backupID, overrideBucket, overridePath, ""}
		if base.hasMeta(ctx, BackupFile, overrideBucket, overridePath) {
			return &result, errLegacySingleNode
		}
	}

	return &result, err
}

func (s *nodeStore) MetaForBackupID(ctx context.Context, backupID, overrideBucket, overridePath string) (*backup.BackupDescriptor, error) {
	var result *backup.BackupDescriptor

	cs := &objectStore{s.backend, fmt.Sprintf("%s/%s", backupID, s.node), overrideBucket, overridePath, ""} // for backward compatibility
	if err := cs.meta(ctx, BackupFile, overrideBucket, overridePath, &result); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("no backup descriptor found in %s", backupID)
	}
	return result, nil
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

// Meta gets coordinator's global metadata from object store. A backup carrying only the
// top-level per-node metadata is refused as errLegacySingleNode.
func (s *coordStore) Meta(ctx context.Context, filename, overrideBucket, overridePath string) (*backup.DistributedBackupDescriptor, error) {
	var result backup.DistributedBackupDescriptor
	err := s.meta(ctx, filename, overrideBucket, overridePath, &result)
	if err != nil && filename == GlobalBackupFile &&
		s.hasMeta(ctx, BackupFile, overrideBucket, overridePath) {
		return &result, errLegacySingleNode
	}
	return &result, err
}

func (s *coordStore) MetaForBackupID(ctx context.Context, backupID, overrideBucket, overridePath string) (*backup.BackupDescriptor, error) {
	var result *backup.BackupDescriptor

	cs := &coordStore{objectStore{s.backend, backupID, overrideBucket, overridePath, ""}}
	if err := cs.meta(ctx, GlobalBackupFile, overrideBucket, overridePath, &result); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("no global backup descriptor found in %s", backupID)
	}
	return result, nil
}

// uploader uploads backup artifacts. This includes db files and metadata
type uploader struct {
	cfg            config.Backup
	sourcer        Sourcer
	rbacSourcer    RBACSnapshotter
	dynUserSourcer dynUserSnapshotter
	// Resolved includeUsers ids; empty → whole-cluster snapshot.
	users []string
	// Resolved includeRoles names; empty → whole-cluster RBAC snapshot.
	roles    []string
	backend  nodeStore
	backupID string
	zipConfig
	// slot is the node's own operation slot, which is what a status poll reads
	// until the descriptor is written to the backend.
	slot statusPublisher
	log  logrus.FieldLogger
}

// statusPublisher is the observable half of a node's operation slot. Failing
// goes through its own method so a failure can never be published without the
// reason that belongs to it; see [backupStat.setFailed].
type statusPublisher interface {
	set(st backup.Status)
	setFailed(reason string)
}

func newUploader(cfg config.Backup, sourcer Sourcer, rbacSourcer RBACSnapshotter, dynUserSourcer dynUserSnapshotter, users, roles []string, backend nodeStore,
	backupID string, slot statusPublisher, l logrus.FieldLogger,
) *uploader {
	return &uploader{
		cfg:            cfg,
		sourcer:        sourcer,
		rbacSourcer:    rbacSourcer,
		dynUserSourcer: dynUserSourcer,
		users:          users,
		roles:          roles,
		backend:        backend,
		backupID:       backupID,
		zipConfig: newZipConfig(Compression{
			Level:         GzipDefaultCompression,
			CPUPercentage: DefaultCPUPercentage,
		}),
		slot: slot,
		log:  l,
	}
}

func (u *uploader) withCompression(cfg zipConfig) *uploader {
	u.zipConfig = cfg
	return u
}

// all uploads all files in addition to the metadata file
func (u *uploader) all(ctx context.Context, classes []string, desc *backup.BackupDescriptor, baseDescr []*backup.BackupDescriptor, overrideBucket, overridePath string) (err error) {
	u.slot.set(backup.Transferring)
	desc.Status = backup.Transferring
	// all owns the producer's context so it can be stopped before any index is
	// released. Without that the wait covers every class the backup never reached.
	producerCtx, stopProducer := context.WithCancel(ctx)
	ch := u.sourcer.BackupDescriptors(producerCtx, desc.ID, classes, baseDescr)
	// A class the producer snapshots after the release below stays marked in
	// progress, and the next backup of that class fails. Draining to close is what
	// proves it stopped. Draining twice costs nothing, so the normal path calls
	// this directly and the defer covers a panic or an early return.
	stopAndDrainProducer := func() {
		stopProducer()
		for range ch {
		}
	}
	var totalPreCompressionSize int64 // Track total pre-compression bytes
	// completed is set on the one path that runs the backup to the end. The defer
	// below reads it to tell that path from a panic, which leaves err nil on a
	// backup that stopped somewhere in the middle.
	var completed bool

	defer monitoring.GetBackgroundProcessMetrics().Started(monitoring.ProcessBackup)()

	defer func() {
		//  release indexes under all conditions
		u.releaseIndexes(classes, desc.ID)

		//  make sure context is not cancelled when uploading metadata. Its own
		//  name, so the cancellation check below still reads the operation's
		//  context rather than this one, which can never be cancelled.
		metaCtx := context.Background()

		// A panic unwinds through here with err nil, and the success branch would
		// publish the node as done on a backup that never reached the end.
		if err == nil && !completed {
			err = errors.New("backup did not run to completion")
		}

		// Handle success case first
		if err == nil {
			u.log.Info("start uploading metadata")
			if err = u.backend.PutMeta(metaCtx, desc, overrideBucket, overridePath); err != nil {
				// Nothing to restore from without the descriptor, so this ends
				// as a failure. Publishing SUCCESS here would have the
				// coordinator count the node done and report a backup that
				// cannot be restored as good.
				desc.Status = backup.Transferred
				u.slot.setFailed(err.Error())
			} else {
				u.slot.set(backup.Success)
			}
			u.log.Info("finish uploading metadata")
			return
		}

		desc.Error = nonEmptyErrMsg(err)

		// Handle error cases
		cancelled := errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled)
		if cancelled {
			u.slot.set(backup.Cancelled)
			desc.Status = backup.Cancelled
		} else {
			desc.Status = backup.Failed
			monitoring.GetBackgroundProcessMetrics().Failed(monitoring.ProcessBackup)
		}

		u.log.Info("start uploading metadata for cancelled or failed backup")
		if metaErr := u.backend.PutMeta(metaCtx, desc, overrideBucket, overridePath); metaErr != nil {
			// combine errors for shadowing the original error in case
			// of putMeta failure
			err = fmt.Errorf("upload %w: %w", err, metaErr)
		}
		// After the meta write, which has to carry the reason by the time a
		// poll can see FAILED. err is published rather than desc.Error, which
		// was fixed before the write and so says nothing when the write is
		// what failed.
		if !cancelled {
			u.slot.setFailed(err.Error())
		}
		u.log.Info("finish uploading metadata for cancelled or failed backup")
	}()

	// Registered after the release above, so reverse order runs it first and no
	// index is released while the producer can still snapshot its class.
	defer stopAndDrainProducer()

	contextChecker := func(ctx context.Context) error {
		ctxerr := ctx.Err()
		if ctxerr != nil {
			u.slot.set(backup.Cancelled)
			desc.Status = backup.Cancelled
			u.releaseIndexes(classes, desc.ID)
		}
		return ctxerr
	}

	// One pool for the whole backup, shared by the shards of every class. A pool
	// per class could never run wider than that class's shard count, so a node
	// holding thousands of single-shard collections would upload one shard at a time.
	poolCtx, cancelPool := context.WithCancel(ctx)
	eg, poolCtx := enterrors.NewErrorGroupWithContextWrapper(u.log, poolCtx)
	eg.SetLimit(max(u.GoPoolSize, 1))
	// Releasing an index deletes the class's staging dir, which its shard jobs read
	// from. Defers run in reverse order, so this one drains the pool before the
	// release in the defer above it. Cancelling first is what bounds the drain. A
	// shard job runs under storeTimeout, so waiting on jobs nothing has cancelled
	// parks the backup for a day. The normal path already waits below. This defer
	// is for a panic or an early return.
	defer func() {
		cancelPool()
		_ = eg.Wait()
	}()

	var (
		uploads []*classUpload
		descErr error
	)

Loop:
	for {
		select {
		case cdesc, ok := <-ch:
			if !ok {
				break Loop // we are done
			}
			if cdesc.Error != nil {
				descErr = cdesc.Error
				cancelPool()
				break Loop
			}
			uploads = append(uploads, u.submitClass(poolCtx, eg, desc.ID, cdesc, overrideBucket, overridePath))

		// cancelled when the backup is aborted and when the first shard job fails.
		// Either way there is no point taking more class descriptors.
		case <-poolCtx.Done():
			break Loop
		}
	}

	// Wait before releasing. A class still in the pool is reading its staging dir,
	// and the release deletes it. Every class the loop submitted has already
	// released itself in finish, so this only covers the ones it never reached.
	poolErr := eg.Wait()

	stopAndDrainProducer()
	u.releaseIndexes(classes, desc.ID)

	// uploads is in the order the descriptors arrived, so classes finishing out of
	// order does not reorder desc.Classes.
	for _, cu := range uploads {
		if !cu.complete() {
			continue
		}
		desc.Classes = append(desc.Classes, cu.desc)
		totalPreCompressionSize += cu.desc.PreCompressionSizeBytes
	}

	// descErr is returned first, because it is the real cause. cancelPool has
	// already failed the in-flight shards with context.Canceled, and the defer
	// above publishes that as a cancelled backup instead of a failed one. Those
	// shard errors are not lost, since finish logs every class that did not
	// upload in full.
	if descErr != nil {
		return descErr
	}
	if poolErr != nil {
		return poolErr
	}
	// The producer stopped without saying why, so publishing here would report a
	// backup that omits every class it never described.
	if ctx.Err() == nil && len(uploads) != len(classes) {
		return fmt.Errorf("backup describes %d of %d classes", len(uploads), len(classes))
	}

	if err := ctx.Err(); err != nil {
		return contextChecker(ctx)
	} else if u.rbacSourcer != nil {
		u.log.Info("start uploading RBAC backups")
		descrp, err := u.rbacSourcer.Snapshot(u.roles...)
		if err != nil {
			return err
		}
		desc.RbacBackups = descrp
	} else if len(u.roles) > 0 {
		return fmt.Errorf("includeRoles requested but RBAC is not enabled")
	}

	if err := ctx.Err(); err != nil {
		return contextChecker(ctx)
	} else if u.dynUserSourcer != nil {
		u.log.Info("start uploading dynamic user backups")
		descrp, err := u.dynUserSourcer.Snapshot(u.users...)
		if err != nil {
			return err
		}
		desc.UserBackups = descrp
	} else if len(u.users) > 0 {
		return fmt.Errorf("includeUsers requested but DB Users are not enabled")
	}

	u.slot.set(backup.Transferred)
	desc.Status = backup.Success
	// After all classes, set desc.PreCompressionSizeBytes as the sum of all class sizes
	desc.PreCompressionSizeBytes = totalPreCompressionSize
	completed = true
	return nil
}

// nonEmptyErrMsg is err's text, or a stand-in when it has none. The failure
// text is served verbatim from the status API, backend messages and all.
func nonEmptyErrMsg(err error) string {
	if msg := err.Error(); msg != "" {
		return msg
	}
	return failureWithoutReason
}

// labelErr names the step err came from, and reports nil for a step that
// succeeded. Labelling a nil error with %w instead renders "%!w(<nil>)" for the
// step that worked, in text the status API serves verbatim.
func labelErr(label string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", label, err)
}

func (u *uploader) releaseIndexes(classes []string, bakID string) {
	for _, class := range classes {
		className := class
		enterrors.GoWrapper(func() {
			if err := u.sourcer.ReleaseBackup(context.Background(), bakID, className); err != nil {
				u.log.WithFields(logrus.Fields{
					"class":    className,
					"backupID": bakID,
				}).Error("failed to release backup")
			}
		}, u.log)
	}
}

// classUpload is the state one class's shard jobs share. It holds the
// descriptor they fill in, and the counters that decide when the class is done
// and whether it uploaded in full.
type classUpload struct {
	desc      backup.ClassDescriptor
	lastChunk atomic.Int32

	// mu guards the descriptor fields the shard jobs write.
	mu sync.Mutex

	// pending counts shard jobs that have not returned yet. The job that brings it
	// to zero runs finish.
	pending atomic.Int32
	// shardsDone counts the shards that uploaded. A job bumps it only on its way
	// out, so one that fails or panics leaves its class incomplete.
	shardsDone atomic.Int32

	// finish runs once, on whichever shard job returns last.
	finish func()
}

// complete reports whether every shard of the class uploaded. Only a complete
// class may join the backup descriptor. A partial one still lists all its
// shards but holds chunks for only some. The restore then skips the rest
// without reporting anything.
func (c *classUpload) complete() bool {
	return int(c.shardsDone.Load()) == len(c.desc.Shards)
}

// submitClass queues every shard of one class on the shared pool and returns
// without waiting for them, so the caller can move on to the next class. The
// returned classUpload is what those jobs write their result into. Read it only
// once the pool has drained.
func (u *uploader) submitClass(ctx context.Context, eg *enterrors.ErrorGroupWrapper,
	id string, cdesc backup.ClassDescriptor, overrideBucket, overridePath string,
) *classUpload {
	cu := &classUpload{desc: cdesc}

	classLabel := cdesc.Name
	if monitoring.GetMetrics().Group {
		classLabel = "n/a"
	}
	observe := func() {}
	if metric, err := monitoring.GetMetrics().BackupStoreDurations.
		GetMetricWithLabelValues(getType(u.backend.backend), classLabel); err == nil {
		timer := prometheus.NewTimer(metric)
		observe = func() { timer.ObserveDuration() }
	}

	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	u.log.WithFields(logrus.Fields{
		"action":   "upload_class",
		"duration": storeTimeout,
	}).Debug("context.WithTimeout")

	// finish runs when the last shard job of the class has returned. That is the
	// earliest the index may be released, since releasing it deletes the staging
	// dir the jobs read from. It is also late enough for complete to know whether
	// the class made it into the backup.
	cu.finish = func() {
		cancel()
		observe()
		u.releaseIndexes([]string{cu.desc.Name}, id)
		if cu.complete() {
			u.log.WithField("class", cu.desc.Name).Info("finish uploading files")
			return
		}
		u.log.WithFields(logrus.Fields{
			"class":       cu.desc.Name,
			"shards_done": cu.shardsDone.Load(),
			"shards":      len(cu.desc.Shards),
		}).Warn("class left out of the backup, not all of its shards uploaded")
	}

	// Determine source path: use staging dir (hard-linked snapshot) if available,
	// otherwise fall back to live data path for backward compatibility.
	sourcePath := u.backend.SourceDataPath()
	if cdesc.StagingDir != "" {
		sourcePath = cdesc.StagingDir
	}

	u.log.WithField("class", cu.desc.Name).Info("start uploading files")

	nShards := len(cu.desc.Shards)
	if nShards == 0 {
		// there are no jobs to queue, so no job will call finish
		cu.finish()
		return cu
	}

	cu.desc.Chunks = make(map[int32][]string, 1+nShards/2)
	cu.pending.Store(int32(nShards))

	for _, shard := range cu.desc.Shards {
		eg.Go(func() error {
			defer func() {
				if cu.pending.Add(-1) == 0 {
					cu.finish()
				}
			}()
			return u.uploadShard(ctx, cu, shard, overrideBucket, overridePath, sourcePath)
		})
	}
	return cu
}

// uploadShard compresses one shard into chunks and records them on its class.
func (u *uploader) uploadShard(ctx context.Context, cu *classUpload, shard *backup.ShardDescriptor,
	overrideBucket, overridePath, sourcePath string,
) error {
	// a failing job cancels ctx, but jobs already queued behind it still start
	if err := ctx.Err(); err != nil {
		return err
	}

	chunks, err := u.processShard(ctx, shard, cu.desc.Name, &cu.lastChunk, overrideBucket, overridePath, sourcePath)
	if err != nil {
		return err
	}

	cu.mu.Lock()
	defer cu.mu.Unlock()
	for _, c := range chunks {
		cu.desc.Chunks[c.chunk] = c.shards
		cu.desc.PreCompressionSizeBytes += c.preCompressionSize
	}
	cu.desc.PreCompressionSizeBytes += shard.IncrementalBackupInfo.TotalSize
	cu.shardsDone.Add(1)
	return nil
}

type chunkShards struct {
	chunk              int32
	shards             []string
	preCompressionSize int64
}

// processShard compresses a single shard into one or more chunks, handling split files
// that span multiple chunks. It returns the produced chunks and any error.
func (u *uploader) processShard(
	ctx context.Context,
	shard *backup.ShardDescriptor,
	className string,
	lastChunk *atomic.Int32,
	overrideBucket, overridePath, sourcePath string,
) ([]chunkShards, error) {
	filesInShard, err := u.createFileList(shard, sourcePath)
	if err != nil {
		return nil, fmt.Errorf("create file list for shard %q: %w", shard.Name, err)
	}
	var results []chunkShards
	var fileSizeExceeded *SplitFile
	firstChunk := true
	for {
		chunk := lastChunk.Add(1)
		fileSizeExceededTmp, preCompressionSize, err := u.compress(ctx, className, chunk, shard, filesInShard, firstChunk, fileSizeExceeded, overrideBucket, overridePath, sourcePath)
		if err != nil {
			return results, err
		}
		fileSizeExceeded = fileSizeExceededTmp
		results = append(results, chunkShards{chunk, []string{shard.Name}, preCompressionSize})
		firstChunk = false
		if filesInShard.Len() == 0 && fileSizeExceeded == nil {
			break
		}
	}
	return results, nil
}

func (u *uploader) compress(ctx context.Context,
	class string, // class name
	chunk int32, // chunk index
	shard *backup.ShardDescriptor, // shard to be backed up
	filesInShard *backup.FileList,
	firstChunkForShard bool, // is this the first chunk for the shard, which means that the metadata needs to be included
	fileSizeExceededWrite *SplitFile, // if not nil, continue from previous split
	overrideBucket, overridePath string, // bucket name and path
	sourcePath string, // root path for reading source files (staging dir or live data path)
) (*SplitFile, int64, error) {
	var (
		chunkKey           = chunkKey(class, chunk)
		preCompressionSize atomic.Int64
		eg                 = enterrors.NewErrorGroupWrapper(u.log)
	)

	// bigFilesThreshold: files >= this size are "big" and get their own chunk (tracked for incremental dedup).
	// chunkTargetSize controls the max size when packing small files together; it must be at least bigFilesThreshold.
	bigFilesThreshold := max(u.cfg.MinChunkSize, filesInShard.BigFilesThreshold)
	chunkTargetSize := max(u.cfg.ChunkTargetSize, bigFilesThreshold)
	zip, reader, err := NewZip(sourcePath, u.Level, chunkTargetSize, bigFilesThreshold, u.cfg.SplitFileSize)
	if err != nil {
		return nil, preCompressionSize.Load(), err
	}
	producer := func() (_ *SplitFile, err error) {
		defer func() {
			// Capture close error and join with any existing error.
			// Close writes tar/gzip trailers and could fail if the pipe is closed.
			// Use CloseWithError to signal any producer error to the consumer,
			// so the consumer's read fails instead of seeing EOF.
			closeErr := zip.CloseWithError(err)
			err = errors.Join(err, labelErr("close", closeErr))
		}()

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var fileSizeExceededInfo *SplitFile
		if fileSizeExceededWrite != nil {
			// Only write the split file part in this chunk; remaining space is intentionally
			// left unused to keep the logic simple and avoid mixing split file parts with
			// regular files in the same chunk.
			fileSizeExceededInfo, err = zip.WriteSplitFile(ctx, shard, fileSizeExceededWrite, &preCompressionSize, chunkKey)
			if err != nil {
				return nil, fmt.Errorf("write split file for shard %q: %w", shard.Name, err)
			}
		} else {
			_, fileSizeExceededInfo, err = zip.WriteShard(ctx, shard, filesInShard, firstChunkForShard, &preCompressionSize, chunkKey)
			if err != nil {
				return nil, fmt.Errorf("write files for shard %q: %w", shard.Name, err)
			}
		}
		shard.ClearTemporary()

		if zip.compressorWriter != nil {
			if err := zip.compressorWriter.Flush(); err != nil {
				return nil, fmt.Errorf("flush compressor: %w", err)
			}
		}

		return fileSizeExceededInfo, nil
	}

	// consumer
	eg.Go(func() error {
		if _, err := u.backend.Write(ctx, chunkKey, overrideBucket, overridePath, reader); err != nil {
			u.log.WithFields(logrus.Fields{
				"chunkKey": chunkKey,
			}).Errorf("failed to write chunk to backend: %v", err)
			return err
		}
		return nil
	})

	fileSizeExceededInfo, producerErr := producer()
	// Always wait for the consumer to finish to capture its error.
	// If the consumer fails (e.g., network error), it closes the pipe, causing
	// the producer to fail with "closed pipe". We need both errors to show
	// the actual cause (consumer error), not just the symptom (closed pipe).
	consumerErr := eg.Wait()
	return fileSizeExceededInfo, preCompressionSize.Load(),
		errors.Join(labelErr("producer", producerErr), labelErr("consumer", consumerErr))
}

// calculateShardPreCompressionSize calculates the total size of a shard before compression
// Since shards are paused and memtables are flushed during backup, we only need to calculate
// the size of files on disk, not in-memory data.
func (u *uploader) calculateShardPreCompressionSize(shard *backup.ShardDescriptor) int64 {
	var totalSize int64
	sourceDataPath := u.backend.SourceDataPath()
	// Add size of files on disk (in-memory data is flushed to disk during backup preparation)
	for _, filePath := range shard.Files {
		fullPath := filepath.Join(sourceDataPath, filePath)
		if info, err := os.Stat(fullPath); err == nil {
			totalSize += info.Size()
		}
	}

	u.log.WithFields(logrus.Fields{
		"shard":          shard.Name,
		"filesCount":     len(shard.Files),
		"totalSize":      totalSize,
		"sourceDataPath": sourceDataPath,
	}).Debug("calculated pre-compression size for shard")

	return totalSize
}

// createFileList creates a FileList from a ShardDescriptor with Files copied,
// FileSizes map populated, and BigFilesThreshold calculated.
// This allows file sizes to be collected once at the start of processing rather than repeatedly during compression.
// Returns an error if any file in the shard doesn't exist at either the normal path or delete marker path.
func (u *uploader) createFileList(shard *backup.ShardDescriptor, sourcePath string) (*backup.FileList, error) {
	sourceDataPath := sourcePath
	files := shard.Files
	fileSizes := make(map[string]int64, len(files))

	for _, relPath := range files {
		fullPath := filepath.Join(sourceDataPath, relPath)
		if info, err := os.Stat(fullPath); err == nil {
			fileSizes[relPath] = info.Size()
		} else if os.IsNotExist(err) {
			// Check if the file exists with the delete marker prefix
			deletedPath := filepath.Join(sourceDataPath, backup.DeleteMarkerAdd(relPath))
			if info, err := os.Stat(deletedPath); err == nil {
				fileSizes[relPath] = info.Size()
			} else {
				return nil, fmt.Errorf("file %q not found at %q or %q: %w", relPath, fullPath, deletedPath, err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat file %q: %w", fullPath, err)
		}
	}

	// Copy files slice
	filesCopy := make([]string, len(files))
	copy(filesCopy, files)

	// A config not built by FromEnv leaves this nil, so Get yields 0.
	maxIndividualFiles := u.cfg.MaxIndividualFiles.Get()
	if maxIndividualFiles <= 0 {
		maxIndividualFiles = config.DefaultBackupMaxIndividualFiles
	}

	return &backup.FileList{
		Files:     filesCopy,
		FileSizes: fileSizes,
		BigFilesThreshold: calculateBigFilesThreshold(fileSizes, shard.IncrementalBackupInfo.NumFilesSkipped,
			maxIndividualFiles, u.cfg.MinChunkSize),
	}, nil
}

// calculateBigFilesThreshold returns the size of the k-th biggest file, clamped to minSize,
// where k is maxIndividualFiles reduced by numSkippedFiles and at least 1. Returns the
// smallest file's size if there are fewer than k files.
// Uses a min-heap for O(n) time and O(min(k, n)) space.
func calculateBigFilesThreshold(fileSizes map[string]int64, numSkippedFiles, maxIndividualFiles int, minSize int64) int64 {
	k := max(maxIndividualFiles-numSkippedFiles, 1) // take into account that this might be an incremental backup with skipped files

	if len(fileSizes) == 0 {
		return minSize
	}

	// Use a min-heap to track the k largest file sizes
	h := &int64MinHeap{}
	heap.Init(h)

	for _, size := range fileSizes {
		if h.Len() < k {
			heap.Push(h, size)
		} else if size > (*h)[0] {
			heap.Pop(h)
			heap.Push(h, size)
		}
	}

	// The root of the min-heap is the k-th largest (or smallest if < k files)
	result := (*h)[0]
	if result < minSize {
		return minSize
	}
	return result
}

// int64MinHeap implements heap.Interface for a min-heap of int64 values.
type int64MinHeap []int64

func (h int64MinHeap) Len() int           { return len(h) }
func (h int64MinHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h int64MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *int64MinHeap) Push(x interface{}) { *h = append(*h, x.(int64)) }

func (h *int64MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// fileWriter downloads files from object store and writes files to the destination folder destDir
type fileWriter struct {
	sourcer    Sourcer
	backend    nodeStore
	tempDir    string
	destDir    string
	movedFiles []string // files successfully moved to destination folder
	GoPoolSize int
	migrator   func(classPath string) error
	logger     logrus.FieldLogger
}

func newFileWriter(sourcer Sourcer, backend nodeStore,
	logger logrus.FieldLogger,
) *fileWriter {
	destDir := backend.SourceDataPath()
	return &fileWriter{
		sourcer:    sourcer,
		backend:    backend,
		destDir:    destDir,
		tempDir:    path.Join(destDir, TempDirectory),
		movedFiles: make([]string, 0, 64),
		GoPoolSize: routinePoolSize(50),
		logger:     logger,
	}
}

func (fw *fileWriter) WithPoolPercentage(p int) *fileWriter {
	fw.GoPoolSize = routinePoolSize(p)
	return fw
}

func (fw *fileWriter) setMigrator(m func(classPath string) error) { fw.migrator = m }

// Write downloads files into the staging directory. materializedName keys the
// staging dir; it differs from desc.Name only under namespace-graduation
// restore, where it must match the RAFT-applied RestoreClassDir lookup. Chunk
// keys keep desc.Name — object-storage paths are immutable from upload.
func (fw *fileWriter) Write(ctx context.Context, desc *backup.ClassDescriptor, materializedName, overrideBucket, overridePath string, compressionType backup.CompressionType) (err error) {
	if len(desc.Shards) == 0 { // nothing to copy
		return nil
	}
	classTempDir := path.Join(fw.tempDir, materializedName)

	if err := fw.writeTempFiles(ctx, classTempDir, overrideBucket, overridePath, desc, compressionType); err != nil {
		return fmt.Errorf("get files: %w", err)
	}

	if materializedName != desc.Name {
		oldIndexDir := filepath.Join(classTempDir, strings.ToLower(desc.Name))
		newIndexDir := filepath.Join(classTempDir, strings.ToLower(materializedName))
		if _, err := os.Stat(oldIndexDir); err == nil {
			if err := os.Rename(oldIndexDir, newIndexDir); err != nil {
				return fmt.Errorf("rename strip index dir %s -> %s: %w", oldIndexDir, newIndexDir, err)
			}
		}

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
func (fw *fileWriter) writeTempFiles(ctx context.Context, classTempDir, overrideBucket, overridePath string, desc *backup.ClassDescriptor, compressionType backup.CompressionType) (err error) {
	if err := os.RemoveAll(classTempDir); err != nil {
		return fmt.Errorf("remove %s: %w", classTempDir, err)
	}
	if err := os.MkdirAll(classTempDir, os.ModePerm); err != nil {
		return fmt.Errorf("create temp class folder %s: %w", classTempDir, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(fw.logger, ctx)
	eg.SetLimit(fw.GoPoolSize)
	for k := range desc.Chunks {
		// Check for cancellation before processing each chunk
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := chunkKey(desc.Name, k)
		eg.Go(func() error {
			return fw.readAndUnzipChunk(classTempDir, compressionType, chunk,
				func(w io.WriteCloser) error {
					_, err := fw.backend.Read(ctx, chunk, overrideBucket, overridePath, w)
					return err
				})
		})
	}

	// fetch files from base backup(s)
	for _, shard := range desc.Shards {
		for backupId, incrementalBackupInfos := range shard.IncrementalBackupInfo.FilesPerBackup { // can be multiple incremental backups
			for _, incrementalBackupInfo := range incrementalBackupInfos { // files per base backup
				for _, chunkId := range incrementalBackupInfo.ChunkKeys { // chunks for file
					eg.Go(func() error {
						return fw.readAndUnzipChunk(classTempDir, compressionType, chunkId,
							func(w io.WriteCloser) error {
								_, err := fw.backend.ReadFromOtherBackup(ctx, backupId, chunkId, overrideBucket, overridePath, w)
								return err
							})
					})
				}
			}
		}
	}
	return eg.Wait()
}

// readAndUnzipChunk downloads a chunk via readFn and unzips it into classTempDir.
// It propagates errors from both the download and the unzip so that partial
// downloads are never silently accepted.
func (fw *fileWriter) readAndUnzipChunk(classTempDir string, compressionType backup.CompressionType, chunkName string, readFn func(w io.WriteCloser) error) error {
	uz, w := NewUnzip(classTempDir, compressionType)

	readErrCh := make(chan error, 1)
	enterrors.GoWrapper(func() {
		var err error
		// Ensure readErrCh is always signaled even if readFn panics,
		// otherwise the receiver on readErrCh will hang forever.
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in readFn: %v", r)
				readErrCh <- err
				panic(r) // re-panic so GoWrapper still logs the full stack
			}
			readErrCh <- err
		}()
		err = readFn(w)
		if err != nil {
			fw.logger.WithField("chunk", chunkName).Errorf("failed to read chunk from backend: %v", err)
		}
	}, fw.logger)

	_, unzipErr := uz.ReadChunk()
	// Close the pipe reader so any in-progress pw.Write() in readFn unblocks
	// with ErrClosedPipe. Without this, readFn can hang forever if the
	// decompressor detected end-of-stream before io.Copy finished writing all
	// bytes from the backend.
	uz.Close()
	// Always drain readErrCh to prevent leaking the readFn goroutine.
	readErr := <-readErrCh
	if unzipErr != nil {
		return fmt.Errorf("unzip chunk %s: %w", chunkName, unzipErr)
	}
	if readErr != nil && !errors.Is(readErr, io.ErrClosedPipe) {
		return fmt.Errorf("read chunk %s from backend: %w", chunkName, readErr)
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
	if x := (numCPU() * percentage) / 100; x > 0 {
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
