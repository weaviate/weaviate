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
	"reflect"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type restorer struct {
	node           string // node name
	logger         logrus.FieldLogger
	sourcer        Sourcer
	rbacSourcer    fsm.Snapshotter
	dynUserSourcer fsm.Snapshotter
	backends       BackupBackendProvider
	shardSyncChan

	// TODO: keeping status in memory after restore has been done
	// is not a proper solution for communicating status to the user.
	// On app crash or restart this data will be lost
	// This should be regarded as workaround and should be fixed asap
	restoreStatusMap sync.Map
}

func newRestorer(node string, logger logrus.FieldLogger,
	sourcer Sourcer, rbacSourcer fsm.Snapshotter, dynUserSourcer fsm.Snapshotter,
	backends BackupBackendProvider,
) *restorer {
	return &restorer{
		node:           node,
		logger:         logger,
		sourcer:        sourcer,
		rbacSourcer:    rbacSourcer,
		dynUserSourcer: dynUserSourcer,
		backends:       backends,
		shardSyncChan:  shardSyncChan{coordChan: make(chan interface{}, 5)},
	}
}

func (r *restorer) restore(
	req *Request,
	desc *backup.BackupDescriptor,
	store nodeStore,
) (CanCommitResponse, error) {
	expiration := req.Duration
	if expiration > _TimeoutShardCommit {
		expiration = _TimeoutShardCommit
	}
	ret := CanCommitResponse{
		Method:  OpCreate,
		ID:      req.ID,
		Timeout: expiration,
	}

	destPath := store.HomeDir(req.Bucket, req.Path)

	if lastOp := r.lastOp.get(); lastOp.ID == req.ID &&
		(lastOp.Status == backup.Cancelling || lastOp.Status == backup.Cancelled) {
		err := fmt.Errorf("restore %s cancellation in progress, please wait for it to complete", req.ID)
		return ret, err
	}

	// make sure there is no active restore
	if prevID := r.lastOp.renew(req.ID, destPath, req.Bucket, req.Path); prevID != "" {
		err := fmt.Errorf("restore %s already in progress", prevID)
		return ret, err
	}
	r.waitingForCoordinatorToCommit.Store(true) // is set to false by wait()

	f := func() {
		var err error
		status := Status{
			Path:      destPath,
			StartedAt: time.Now().UTC(),
			Status:    backup.Transferring,
		}
		backgroundDone := monitoring.GetBackgroundProcessMetrics().Started(monitoring.ProcessRestore)
		defer func() {
			backgroundDone()
			status.CompletedAt = time.Now().UTC()
			if err == nil {
				status.Status = backup.Success
			} else {
				status.Err = err.Error()
				// Check if error is due to cancellation
				if errors.Is(err, context.Canceled) {
					status.Status = backup.Cancelled
				} else {
					status.Status = backup.Failed
					monitoring.GetBackgroundProcessMetrics().Failed(monitoring.ProcessRestore)
				}
			}
			r.restoreStatusMap.Store(basePath(req.Backend, req.ID), status)
			r.lastOp.reset()
		}()

		if err = r.waitForCoordinator(expiration, req.ID); err != nil {
			r.logger.WithField("action", "restore_backup").
				Error(err)
			r.lastAsyncError = err
			return
		}

		// the coordinator might want to abort the restore
		done := make(chan struct{})
		ctx := r.withCancellation(context.Background(), req.ID, done, r.logger)
		defer close(done)

		overrideBucket := req.Bucket
		overridePath := req.Path

		err = r.restoreAll(ctx, desc, req.CPUPercentage, store, overrideBucket, overridePath, req.RbacRestoreOption, req.UserRestoreOption)
		logFields := logrus.Fields{"action": "restore", "backup_id": req.ID}
		if err != nil {
			r.logger.WithFields(logFields).Error(err)
		} else {
			r.logger.WithFields(logFields).Info("backup restored successfully")
		}
	}
	enterrors.GoWrapper(f, r.logger)

	return ret, nil
}

// restoreAll restores classes in temporary directories on the filesystem.
// The final backup restoration is orchestrated by the raft store.
func (r *restorer) restoreAll(ctx context.Context,
	desc *backup.BackupDescriptor, cpuPercentage int,
	store nodeStore, overrideBucket, overridePath, rbacRestoreOption, usersRestoreOption string,
) error {
	compressionType := desc.GetCompressionType()
	r.lastOp.set(backup.Transferring)

	// Check for cancellation before starting restore operations
	if err := ctx.Err(); err != nil {
		r.lastOp.set(backup.Cancelled)
		return fmt.Errorf("restore cancelled: %w", err)
	}

	if r.dynUserSourcer != nil && len(desc.UserBackups) > 0 && usersRestoreOption != models.RestoreConfigUsersOptionsNoRestore {
		if err := r.dynUserSourcer.Restore(desc.UserBackups); err != nil {
			return fmt.Errorf("restore rbac: %w", err)
		}
		// Check for cancellation after User restore
		if err := ctx.Err(); err != nil {
			r.lastOp.set(backup.Cancelled)
			return fmt.Errorf("restore cancelled: %w", err)
		}
	}

	if r.rbacSourcer != nil && len(desc.RbacBackups) > 0 && rbacRestoreOption != models.RestoreConfigRolesOptionsNoRestore {
		if err := r.rbacSourcer.Restore(desc.RbacBackups); err != nil {
			return fmt.Errorf("restore rbac: %w", err)
		}
		// Check for cancellation after RBAC restore
		if err := ctx.Err(); err != nil {
			r.lastOp.set(backup.Cancelled)
			return fmt.Errorf("restore cancelled: %w", err)
		}
	}

	for _, cdesc := range desc.Classes {
		// Check for cancellation before each class restore
		if err := ctx.Err(); err != nil {
			r.lastOp.set(backup.Cancelled)
			return fmt.Errorf("restore cancelled: %w", err)
		}
		if err := r.restoreOne(ctx, &cdesc, compressionType, cpuPercentage, store, overrideBucket, overridePath); err != nil {
			if errors.Is(err, context.Canceled) {
				r.lastOp.set(backup.Cancelled)
				return fmt.Errorf("restore cancelled: %w", err)
			}
			return fmt.Errorf("restore class %s: %w", cdesc.Name, err)
		}
		r.logger.WithField("action", "restore").
			WithField("backup_id", desc.ID).
			WithField("class", cdesc.Name).Info("successfully restored")
	}

	return nil
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Pointer {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (r *restorer) restoreOne(ctx context.Context,
	desc *backup.ClassDescriptor, compressionType backup.CompressionType,
	cpuPercentage int, store nodeStore,
	overrideBucket, overridePath string,
) (err error) {
	classLabel := desc.Name
	if monitoring.GetMetrics().Group {
		classLabel = "n/a"
	}
	metric, err := monitoring.GetMetrics().BackupRestoreDurations.GetMetricWithLabelValues(getType(store.backend), classLabel)
	if err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}

	fw := newFileWriter(r.sourcer, store, r.logger).
		WithPoolPercentage(cpuPercentage)

	if err := fw.Write(ctx, desc, overrideBucket, overridePath, compressionType); err != nil {
		return fmt.Errorf("write files: %w", err)
	}

	return nil
}

func (r *restorer) status(backend, ID string) (Status, error) {
	if st := r.lastOp.get(); st.ID == ID {
		return Status{
			Path:      st.Path,
			StartedAt: st.Starttime,
			Status:    st.Status,
		}, nil
	}
	ref := basePath(backend, ID)
	istatus, ok := r.restoreStatusMap.Load(ref)
	if !ok {
		err := fmt.Errorf("status not found: %s", ref)
		return Status{}, backup.NewErrNotFound(err)
	}
	return istatus.(Status), nil
}

func (r *restorer) validate(ctx context.Context, store *nodeStore, req *Request) (*backup.BackupDescriptor, []string, error) {
	destPath := store.HomeDir(req.Bucket, req.Path)
	meta, err := store.Meta(ctx, req.ID, req.Bucket, req.Path)
	if err != nil {
		nerr := backup.ErrNotFound{}
		if errors.As(err, &nerr) {
			return nil, nil, fmt.Errorf("restorer cannot validate: %w: %q (%w)", errMetaNotFound, destPath, err)
		}
		return nil, nil, fmt.Errorf("find backup %s: %w", destPath, err)
	}
	if meta.ID != req.ID {
		return nil, nil, fmt.Errorf("wrong backup file: expected %q got %q", req.ID, meta.ID)
	}
	if meta.Status != backup.Success {
		err = fmt.Errorf("invalid backup in restorer %s status: %s", destPath, meta.Status)
		return nil, nil, err
	}
	if err := checkRestorableVersion(meta.Version, meta.ServerVersion); err != nil {
		return nil, nil, err
	}
	if err := meta.Validate(); err != nil {
		return nil, nil, fmt.Errorf("corrupted backup file: %w", err)
	}
	cs := meta.List()
	if len(req.Classes) > 0 {
		if first := meta.AllExist(req.Classes); first != "" {
			err = fmt.Errorf("class %s doesn't exist in the backup, but does have %v: ", first, cs)
			return nil, cs, err
		}
		meta.Include(req.Classes)
	}

	return meta, cs, nil
}
