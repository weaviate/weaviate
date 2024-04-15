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
	"reflect"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/monitoring"
	migratefs "github.com/weaviate/weaviate/usecases/schema/migrate/fs"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type restorer struct {
	node     string // node name
	logger   logrus.FieldLogger
	sourcer  Sourcer
	backends BackupBackendProvider
	schema   schemaManger
	shardSyncChan

	// TODO: keeping status in memory after restore has been done
	// is not a proper solution for communicating status to the user.
	// On app crash or restart this data will be lost
	// This should be regarded as workaround and should be fixed asap
	restoreStatusMap sync.Map
}

func newRestorer(node string, logger logrus.FieldLogger,
	sourcer Sourcer,
	backends BackupBackendProvider,
	schema schemaManger,
) *restorer {
	return &restorer{
		node:          node,
		logger:        logger,
		sourcer:       sourcer,
		backends:      backends,
		schema:        schema,
		shardSyncChan: shardSyncChan{coordChan: make(chan interface{}, 5)},
	}
}

func (r *restorer) restore(ctx context.Context,
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

	destPath := store.HomeDir()

	// make sure there is no active restore
	if prevID := r.lastOp.renew(req.ID, destPath); prevID != "" {
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
		defer func() {
			status.CompletedAt = time.Now().UTC()
			if err == nil {
				status.Status = backup.Success
			} else {
				status.Err = err.Error()
				status.Status = backup.Failed
			}
			r.restoreStatusMap.Store(basePath(req.Backend, req.ID), status)
			r.lastOp.reset()
		}()

		if err = r.waitForCoordinator(expiration, req.ID); err != nil {
			r.logger.WithField("action", "create_backup").
				Error(err)
			r.lastAsyncError = err
			return
		}

		err = r.restoreAll(context.Background(), desc, req.CPUPercentage, store, req.NodeMapping)
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

func (r *restorer) restoreAll(ctx context.Context,
	desc *backup.BackupDescriptor, cpuPercentage int,
	store nodeStore, nodeMapping map[string]string,
) (err error) {
	compressed := desc.Version > version1
	r.lastOp.set(backup.Transferring)
	for _, cdesc := range desc.Classes {
		if err := r.restoreOne(ctx, &cdesc, desc.ServerVersion, compressed, cpuPercentage, store, nodeMapping); err != nil {
			return fmt.Errorf("restore class %s: %w", cdesc.Name, err)
		}
		r.logger.WithField("action", "restore").
			WithField("backup_id", desc.ID).
			WithField("class", cdesc.Name).Info("successfully restored")
	}
	return nil
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (r *restorer) restoreOne(ctx context.Context,
	desc *backup.ClassDescriptor, serverVersion string,
	compressed bool, cpuPercentage int, store nodeStore, nodeMapping map[string]string,
) (err error) {
	classLabel := desc.Name
	if monitoring.GetMetrics().Group {
		classLabel = "n/a"
	}
	metric, err := monitoring.GetMetrics().BackupRestoreDurations.GetMetricWithLabelValues(getType(store.b), classLabel)
	if err != nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}

	if r.sourcer.ClassExists(desc.Name) {
		return fmt.Errorf("already exists")
	}
	fw := newFileWriter(r.sourcer, store, compressed, r.logger).
		WithPoolPercentage(cpuPercentage)

	// Pre-v1.23 versions store files in a flat format
	if serverVersion < "1.23" {
		f, err := hfsMigrator(desc, r.node, serverVersion)
		if err != nil {
			return fmt.Errorf("migrate to pre 1.23: %w", err)
		}
		fw.setMigrator(f)
	}

	rollback, err := fw.Write(ctx, desc)
	if err != nil {
		return fmt.Errorf("write files: %w", err)
	}
	if err := r.schema.RestoreClass(ctx, desc, nodeMapping); err != nil {
		if rerr := rollback(); rerr != nil {
			r.logger.WithField("className", desc.Name).WithField("action", "rollback").Error(rerr)
		}
		return fmt.Errorf("restore schema: %w", err)
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
	destPath := store.HomeDir()
	meta, err := store.Meta(ctx, req.ID, true)
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
	if meta.Status != string(backup.Success) {
		err = fmt.Errorf("invalid backup %s status: %s", destPath, meta.Status)
		return nil, nil, err
	}
	if err := meta.Validate(meta.Version > version1); err != nil {
		return nil, nil, fmt.Errorf("corrupted backup file: %w", err)
	}
	if v := meta.Version; v > Version {
		return nil, nil, fmt.Errorf("%s: %s > %s", errMsgHigherVersion, v, Version)
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

// oneClassSchema allows for creating schema with one class
// This is required when migrating to hierarchical file structure from pre-v1.23
type oneClassSchema struct {
	cls *models.Class
	ss  *sharding.State
}

func (s oneClassSchema) CopyShardingState(class string) *sharding.State {
	return s.ss
}

func (s oneClassSchema) GetSchemaSkipAuth() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{s.cls},
		},
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
