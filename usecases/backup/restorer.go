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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	migratefs "github.com/weaviate/weaviate/usecases/schema/migrate/fs"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type restorer struct {
	node              string // node name
	logger            logrus.FieldLogger
	sourcer           Sourcer
	backends          BackupBackendProvider
	namespacesEnabled bool
	shardSyncChan

	// TODO: keeping status in memory after restore has been done
	// is not a proper solution for communicating status to the user.
	// On app crash or restart this data will be lost
	// This should be regarded as workaround and should be fixed asap
	restoreStatusMap sync.Map
}

func newRestorer(node string, logger logrus.FieldLogger,
	sourcer Sourcer, backends BackupBackendProvider, namespacesEnabled bool,
) *restorer {
	return &restorer{
		node:              node,
		logger:            logger,
		sourcer:           sourcer,
		backends:          backends,
		namespacesEnabled: namespacesEnabled,
		shardSyncChan:     shardSyncChan{coordChan: make(chan interface{}, 5), logger: logger},
	}
}

// stagedDirs records the staging dirs one restore attempt created; cleanup removes exactly those, never a sibling attempt's.
type stagedDirs struct {
	mu   sync.Mutex
	dirs []string
}

func (s *stagedDirs) record(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirs = append(s.dirs, dir)
}

func (s *stagedDirs) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.dirs)
}

func (r *restorer) restore(
	req *Request,
	desc *backup.BackupDescriptor,
	store nodeStore,
) (CanCommitResponse, error) {
	return r.startRestore(req, store, func(ctx context.Context, staged *stagedDirs) error {
		return r.restoreAll(ctx, desc, req.CPUPercentage, store, req.Bucket, req.Path, !r.namespacesEnabled, staged)
	})
}

// startRestore reserves the restore slot and runs work in a coordinator-gated goroutine; RAFT applies staged files after Finalizing.
func (r *restorer) startRestore(req *Request, store nodeStore, work func(ctx context.Context, staged *stagedDirs) error) (CanCommitResponse, error) {
	limit := _TimeoutShardCommit
	if req.DedupeReplicas {
		limit = _TimeoutDedupeRestoreCanCommit + _BookingPeriod
	}
	expiration := min(req.Duration, limit)
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
	if prevID := r.lastOp.renew(req.ID, req.AttemptID, destPath, req.Bucket, req.Path); prevID != "" {
		err := fmt.Errorf("restore %s already in progress", prevID)
		return ret, err
	}
	r.waitingForCoordinatorToCommit.Store(true) // is set to false by wait()

	staged := &stagedDirs{}
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
				// A later RAFT-applied RestoreClassDir would adopt leftovers; remove only what this attempt staged.
				for _, dir := range staged.list() {
					if rerr := os.RemoveAll(dir); rerr != nil {
						r.logger.WithField("backup_id", req.ID).Warnf("remove restore staging dir %s: %v", dir, rerr)
					}
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

		err = work(ctx, staged)
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
// The final backup restoration is orchestrated by the raft store. Roles and
// dynamic users are not restored here: the coordinator applies them
// cluster-wide through one RAFT entry once staging has committed.
func (r *restorer) restoreAll(ctx context.Context,
	desc *backup.BackupDescriptor, cpuPercentage int,
	store nodeStore, overrideBucket, overridePath string,
	stripNamespaces bool, staged *stagedDirs,
) error {
	compressionType := desc.GetCompressionType()
	r.lastOp.set(backup.Transferring)

	// Check for cancellation before starting restore operations
	if err := ctx.Err(); err != nil {
		r.lastOp.set(backup.Cancelled)
		return fmt.Errorf("restore cancelled: %w", err)
	}

	for _, cdesc := range desc.Classes {
		// Check for cancellation before each class restore
		if err := ctx.Err(); err != nil {
			r.lastOp.set(backup.Cancelled)
			return fmt.Errorf("restore cancelled: %w", err)
		}
		if err := r.restoreOne(ctx, &cdesc, desc.ServerVersion, compressionType, cpuPercentage, store, overrideBucket, overridePath, stripNamespaces, staged); err != nil {
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
	desc *backup.ClassDescriptor, serverVersion string, compressionType backup.CompressionType,
	cpuPercentage int, store nodeStore,
	overrideBucket, overridePath string,
	stripNamespaces bool, staged *stagedDirs,
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
		WithPoolPercentage(cpuPercentage).
		withStagedRecorder(staged.record)

	// Pre-v1.23 versions store files in a flat format
	if serverVersion < "1.23" {
		f, err := hfsMigrator(desc, r.node, serverVersion)
		if err != nil {
			return fmt.Errorf("migrate to pre 1.23: %w", err)
		}
		fw.setMigrator(f)
	}

	// Local staging dir uses the post-strip name so the RAFT-applied
	// RestoreClassDir (sees the stripped class.Class) finds the files.
	// Object-storage chunk paths keep desc.Name — see fileWriter.Write.
	materializedName := desc.Name
	if stripNamespaces {
		materializedName = namespacing.StripQualification(desc.Name)
	}

	if err := fw.Write(ctx, desc, materializedName, overrideBucket, overridePath, compressionType); err != nil {
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
	if err := validateNodeMeta(meta, destPath, req.ID); err != nil {
		return nil, nil, err
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

// validateNodeMeta checks a per-node descriptor is the requested, successful, restorable backup.
func validateNodeMeta(meta *backup.BackupDescriptor, destPath, reqID string) error {
	if meta.ID != reqID {
		return fmt.Errorf("wrong backup file: restore request asked for %q but the per-node descriptor at %q reports backup ID %q (this happens when metadata from a different backup was placed into this slot, or a prior aborted restore wrote stale state; remove %s/ on the backend and retry with the original backup ID)",
			reqID, path.Join(destPath, BackupFile), meta.ID, destPath)
	}
	if meta.Status != backup.Success {
		return fmt.Errorf("invalid backup in restorer %s status: %s", destPath, meta.Status)
	}
	if err := checkRestorableVersion(meta.Version, meta.ServerVersion); err != nil {
		return err
	}
	// Mirrors the scheduler's global gate: a 3.x per-node descriptor without the flag (or the reverse) is tampered or corrupt, and the legacy path must never restore a deduped node descriptor thin.
	if major, ok := parseMajor(meta.Version); ok && (major >= 3) != meta.DedupeReplicas {
		return fmt.Errorf("corrupted backup file: version %s inconsistent with dedupeReplicas=%v", meta.Version, meta.DedupeReplicas)
	}
	if err := meta.Validate(); err != nil {
		return fmt.Errorf("corrupted backup file: %w", err)
	}
	return nil
}

// oneClassSchema allows for creating schema with one class
// This is required when migrating to hierarchical file structure from pre-v1.23
type oneClassSchema struct {
	cls *models.Class
	ss  *sharding.State
}

func (s oneClassSchema) Read(_ string, reader func(*models.Class, *sharding.State) error) error {
	return reader(s.cls, s.ss)
}

func (s oneClassSchema) Shards(_ string) ([]string, error) {
	return s.ss.AllPhysicalShards(), nil
}

func (s oneClassSchema) LocalShards() ([]string, error) {
	return s.ss.AllLocalPhysicalShards(), nil
}

func (s oneClassSchema) ReadOnlySchema() models.Schema {
	return models.Schema{
		Classes: []*models.Class{s.cls},
	}
}

// hfsMigrator builds and return a class migrator ready for use
func hfsMigrator(desc *backup.ClassDescriptor, nodeName string, serverVersion string) (func(classDir string) error, error) {
	if serverVersion >= "1.23" {
		return func(string) error { return nil }, nil
	}
	var ss sharding.State
	if desc.ShardingState != nil {
		err := json.Unmarshal(desc.ShardingState, &ss)
		if err != nil {
			return nil, fmt.Errorf("marshal sharding state: %w", err)
		}
	}
	ss.SetLocalName(nodeName)

	// get schema and sharding state
	class := &models.Class{}
	if err := json.Unmarshal(desc.Schema, &class); err != nil {
		return nil, fmt.Errorf("marshal class schema: %w", err)
	}

	return func(classDir string) error {
		return migratefs.MigrateToHierarchicalFS(classDir, oneClassSchema{class, &ss})
	}, nil
}
