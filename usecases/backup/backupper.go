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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

type backupper struct {
	node           string
	logger         logrus.FieldLogger
	cfg            config.Backup
	sourcer        Sourcer
	rbacSourcer    fsm.Snapshotter
	dynUserSourcer fsm.Snapshotter
	backends       BackupBackendProvider
	// shardCoordinationChan is sync and coordinate operations
	shardSyncChan
}

func newBackupper(node string, logger logrus.FieldLogger, cfg config.Backup, sourcer Sourcer, rbacSourcer fsm.Snapshotter, dynUserSourcer fsm.Snapshotter, backends BackupBackendProvider,
) *backupper {
	return &backupper{
		node:           node,
		logger:         logger,
		cfg:            cfg,
		sourcer:        sourcer,
		rbacSourcer:    rbacSourcer,
		dynUserSourcer: dynUserSourcer,
		backends:       backends,
		shardSyncChan:  shardSyncChan{coordChan: make(chan interface{}, 5)},
	}
}

func (b *backupper) OnStatus(ctx context.Context, req *StatusRequest) (reqState, error) {
	// check if backup is still active
	st := b.lastOp.get()
	if st.ID == req.ID {
		return st, nil // st contains path, which is the homedir, a combination of bucket and path
	}

	// The backup might have been already created.
	store, err := nodeBackend(b.node, b.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		return reqState{}, fmt.Errorf("no backup provider %q, did you enable the right module?", req.Backend)
	}

	meta, err := store.Meta(ctx, req.ID, store.bucket, store.path, false)
	if err != nil {
		path := fmt.Sprintf("%s/%s", req.ID, BackupFile)
		return reqState{}, fmt.Errorf("cannot get status while backing up: %w: %q: %w", errMetaNotFound, path, err)
	}
	if meta.Error != "" {
		return reqState{}, errors.New(meta.Error)
	}

	return reqState{
		Starttime: meta.StartedAt,
		ID:        req.ID,
		Path:      store.HomeDir(store.bucket, store.path),
		Status:    backup.Status(meta.Status),
	}, nil
}

// backup checks if the node is ready to back up (can commit phase)
//
// Moreover it starts a goroutine in the background which waits for the
// next instruction from the coordinator (second phase).
// It will start the backup as soon as it receives an ack, or abort otherwise
func (b *backupper) backup(store nodeStore, req *Request) (CanCommitResponse, error) {
	id := req.ID
	expiration := req.Duration
	if expiration > _TimeoutShardCommit {
		expiration = _TimeoutShardCommit
	}
	ret := CanCommitResponse{
		Method:  OpCreate,
		ID:      req.ID,
		Timeout: expiration,
	}

	// make sure there is no active backup
	if prevID := b.lastOp.renew(id, store.HomeDir(req.Bucket, req.Path), req.Bucket, req.Path); prevID != "" {
		return ret, fmt.Errorf("backup %s already in progress", prevID)
	}

	b.waitingForCoordinatorToCommit.Store(true) // is set to false by wait()
	// waits for ack from coordinator in order to processed with the backup
	//
	// DEADLOCK NOTE — participant-side hang candidates between this
	// async-goroutine start and the first u.setStatus(Transferring)
	// inside uploader.all (which is what advances the coordinator-
	// visible Status off Started):
	//
	//   1. waitForCoordinator (shard.go:104) reads c.coordChan with a
	//      timeout — if OnCommit never lands the timer fires; that's
	//      a coordinator-side regression, not a hang here.
	//
	//   2. resolveBaseBackupChain (below) does backend I/O. With
	//      baseBackupID == "" it should be a no-op, but a slow
	//      s3/azure HEAD can block here on a real chain. The phase
	//      log "participant_resolve_base_start"/"_ok" brackets it.
	//
	//   3. The big one: provider.all → sourcer.BackupDescriptors →
	//      per-shard backupShardWithHardlinks acquires:
	//        a) i.backupLock.Lock(name) — per-shard mutex.
	//        b) i.shardCreateLocks.Lock(name) — contention with
	//           class create / lazy shard init.
	//        c) shard.HaltForTransfer → s.haltForTransferMux.Lock,
	//           then s.store.PauseCompaction(ctx). PauseCompaction's
	//           compactionCallbacksCtrl.Deactivate(ctx) waits for the
	//           currently-running compaction cycle to drain, which
	//           can be slow under cancel-cleanup that just finished
	//           tearing sidecar buckets. PauseCompaction also
	//           re-acquires s.bucketAccessLock.RLock — if a parallel
	//           ShutdownBucket is mid-flight (Write-locked) it blocks.
	//
	//   The participant phase trace below isolates (2) vs (3). If the
	//   stuck-state lands AT "participant_provider_all_start" without
	//   reaching the uploader's "start uploading files" line, the hang
	//   is inside (3) — sourcer.BackupDescriptors / HaltForTransfer.
	phaseLog := b.logger.WithFields(logrus.Fields{
		"action":    "backup_phase",
		"backup_id": req.ID,
		"node":      b.node,
		"backup_op": string(OpCreate),
	})
	phaseLog.WithField("backup_phase", "participant_async_start").Info("backup-participant: async goroutine starting; waiting for OnCommit")
	f := func() {
		defer b.lastOp.reset()
		waitStart := time.Now()
		if err := b.waitForCoordinator(expiration, id); err != nil {
			phaseLog.WithField("backup_phase", "participant_wait_coordinator_failed").
				WithField("duration_ms", time.Since(waitStart).Milliseconds()).
				Errorf("backup-participant: waitForCoordinator returned error: %v", err)
			b.logger.WithField("action", "create_backup").
				Error(err)
			b.lastAsyncError = err
			return
		}
		phaseLog.WithField("backup_phase", "participant_wait_coordinator_ok").
			WithField("duration_ms", time.Since(waitStart).Milliseconds()).
			Info("backup-participant: received OnCommit from coordinator")

		provider := newUploader(b.cfg, b.sourcer, b.rbacSourcer, b.dynUserSourcer, store, req.ID, b.lastOp.set, b.logger).
			withCompression(newZipConfig(req.Compression))

		compressionType, err := CompressionTypeFromLevel(req.Level)
		if err != nil {
			phaseLog.WithField("backup_phase", "participant_compression_failed").
				Errorf("backup-participant: CompressionTypeFromLevel failed: %v", err)
			b.logger.WithField("action", "create_backup").Error(err)
			b.lastAsyncError = err
			return
		}

		// the coordinator might want to abort the backup
		done := make(chan struct{})
		ctx := b.withCancellation(context.Background(), id, done, b.logger)
		defer close(done)
		logFields := logrus.Fields{"action": "create_backup", "backup_id": req.ID, "override_bucket": req.Bucket, "override_path": req.Path}

		baseBackupID := req.BaseBackupID
		phaseLog.WithField("backup_phase", "participant_resolve_base_start").
			WithField("base_backup_id", baseBackupID).
			Info("backup-participant: resolving base-backup chain")
		baseResolveStart := time.Now()
		baseDescrs, err := resolveBaseBackupChain(ctx, baseBackupID, store.bucket, store.path, compressionType, store.MetaForBackupID)
		if err != nil {
			phaseLog.WithField("backup_phase", "participant_resolve_base_failed").
				WithField("duration_ms", time.Since(baseResolveStart).Milliseconds()).
				Errorf("backup-participant: resolveBaseBackupChain failed: %v", err)
			b.logger.WithFields(logFields).Error(err)
			b.lastAsyncError = err
			return
		}
		phaseLog.WithField("backup_phase", "participant_resolve_base_ok").
			WithField("duration_ms", time.Since(baseResolveStart).Milliseconds()).
			WithField("base_descriptor_count", len(baseDescrs)).
			Info("backup-participant: base-backup chain resolved")

		result := backup.BackupDescriptor{
			StartedAt:       time.Now().UTC(),
			ID:              id,
			Classes:         make([]backup.ClassDescriptor, 0, len(req.Classes)),
			Version:         Version,
			ServerVersion:   config.ServerVersion,
			CompressionType: &compressionType,
			BaseBackupID:    baseBackupID,
		}

		phaseLog.WithField("backup_phase", "participant_provider_all_start").
			WithField("class_count", len(req.Classes)).
			Info("backup-participant: handing off to uploader.all (will set Transferring)")
		providerStart := time.Now()
		if err := provider.all(ctx, req.Classes, &result, baseDescrs, req.Bucket, req.Path); err != nil {
			phaseLog.WithField("backup_phase", "participant_provider_all_failed").
				WithField("duration_ms", time.Since(providerStart).Milliseconds()).
				Errorf("backup-participant: uploader.all returned error: %v", err)
			b.logger.WithFields(logFields).Error(err)
			b.lastAsyncError = err
		} else {
			phaseLog.WithField("backup_phase", "participant_provider_all_ok").
				WithField("duration_ms", time.Since(providerStart).Milliseconds()).
				Info("backup-participant: uploader.all complete")
			b.logger.WithFields(logFields).Info("backup completed successfully")
		}
		result.CompletedAt = time.Now().UTC()
	}
	enterrors.GoWrapper(f, b.logger)

	return ret, nil
}

type ChainDescriptor interface {
	GetBaseBackupID() string
	GetCompressionType() backup.CompressionType
	GetStatus() backup.Status
}

// resolveBaseBackupChain follows the chain of base backups and validates them.
// It returns all base backup descriptors in the chain, ordered from the most recent
// (the requested baseBackupID) to the oldest (the full backup).
// It validates:
// - No circular references in the backup chain
// - All backups in the chain exist
// - All backups have compression type set
// - All backups have the same compression type as requested
func resolveBaseBackupChain[T ChainDescriptor](
	ctx context.Context,
	baseBackupID string,
	bucket, path string,
	compression backup.CompressionType,
	fetchMeta func(ctx context.Context, backupID, bucket, path string) (T, error),
) ([]T, error) {
	if baseBackupID == "" {
		return nil, nil
	}

	var baseDescrs []T
	visitedIDs := make(map[string]struct{})
	nextID := baseBackupID

	for {
		// Check for circular references
		if _, ok := visitedIDs[nextID]; ok {
			return nil, fmt.Errorf("circular references in backup ids detected, all visited IDs: %v, circular ID %v", visitedIDs, nextID)
		}
		visitedIDs[nextID] = struct{}{}

		// Fetch the backup descriptor
		baseDescr, err := fetchMeta(ctx, nextID, bucket, path)
		if err != nil {
			return nil, fmt.Errorf("could not fetch base backup: %w", err)
		}

		if baseDescr.GetCompressionType() != compression {
			return nil, fmt.Errorf("backup %q has compression type %q, expected %q", nextID, baseDescr.GetCompressionType(), compression)
		}

		if baseDescr.GetStatus() != backup.Success {
			return nil, fmt.Errorf("backup %q has status %q, expected %q", nextID, baseDescr.GetStatus(), backup.Success)
		}

		baseDescrs = append(baseDescrs, baseDescr)

		// Check if we've reached the end of the chain
		if baseDescr.GetBaseBackupID() == "" {
			break
		}
		nextID = baseDescr.GetBaseBackupID()
	}

	return baseDescrs, nil
}
