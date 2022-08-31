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
	"regexp"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/sirupsen/logrus"
)

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type schemaManger interface {
	RestoreClass(context.Context, *models.Principal,
		*models.Class, *snapshots.Snapshot,
	) error
}

type Manager struct {
	logger        logrus.FieldLogger
	authorizer    authorizer
	schema        schemaManger
	backups       *backupManager
	RestoreStatus sync.Map
	RestoreError  sync.Map
	sync.Mutex
}

func NewManager(
	logger logrus.FieldLogger,
	authorizer authorizer,
	schema schemaManger,
	snapshotters SnapshotterProvider,
	storages BackupStorageProvider,
	shardingStateFunc shardingStateFunc,
) *Manager {
	m := &Manager{
		logger:     logger,
		authorizer: authorizer,
		schema:     schema,
		backups:    NewBackupManager(logger, snapshotters, storages, shardingStateFunc),
	}
	return m
}

func (m *Manager) CreateSnapshot(ctx context.Context, principal *models.Principal,
	className, storageName, ID string,
) (*models.SnapshotMeta, error) {
	path := fmt.Sprintf("schema/%s/snapshots/%s/%s", className, storageName, ID)
	if err := m.authorizer.Authorize(principal, "add", path); err != nil {
		return nil, err
	}

	if err := validateSnapshotID(ID); err != nil {
		return nil, err
	}

	if meta, err := m.backups.CreateBackup(ctx, className, storageName, ID); err != nil {
		return nil, err
	} else {
		status := string(meta.Status)
		return &models.SnapshotMeta{
			ID:          ID,
			StorageName: storageName,
			Status:      &status,
			Path:        meta.Path,
		}, nil
	}
}

func (m *Manager) CreateSnapshotStatus(ctx context.Context, principal *models.Principal,
	className, storageName, snapshotID string,
) (*models.SnapshotMeta, error) {
	err := m.authorizer.Authorize(principal, "get", fmt.Sprintf(
		"schema/%s/snapshots/%s/%s", className, storageName, snapshotID))
	if err != nil {
		return nil, err
	}

	return m.backups.CreateBackupStatus(ctx, className, storageName, snapshotID)
}

func (m *Manager) RestoreSnapshotStatus(ctx context.Context, principal *models.Principal,
	className, storageName, ID string,
) (status string, errorString string, path string, err error) {
	snapshotUID := storageName + "-" + className + "-" + ID
	path = fmt.Sprintf("schema/%s/snapshots/%s/%s/restore", className, storageName, ID)
	if err := m.authorizer.Authorize(principal, "get", path); err != nil {
		return "", "", "", err
	}

	statusInterface, ok := m.RestoreStatus.Load(snapshotUID)
	if !ok {
		return "", "", "", errors.Errorf("snapshot status not found for %s", snapshotUID)
	}
	status = statusInterface.(string)
	errInterface, ok := m.RestoreError.Load(snapshotUID)
	if !ok {
		return "", "", "", errors.Errorf("snapshot status not found for %s", snapshotUID)
	}

	if errInterface != nil {
		restoreError := errInterface.(error)
		errorString = restoreError.Error()
	}

	path, err = m.backups.DestinationPath(storageName, className, ID)
	if err != nil {
		return "", "", "", err
	}

	return status, errorString, path, nil
}

func (m *Manager) RestoreSnapshot(ctx context.Context, principal *models.Principal,
	className, storageName, ID string,
) (*models.SnapshotRestoreMeta, error) {
	snapshotUID := fmt.Sprintf("%s-%s-%s", storageName, className, ID)
	m.RestoreStatus.Store(snapshotUID, models.SnapshotRestoreMetaStatusSTARTED)
	m.RestoreError.Store(snapshotUID, nil)
	path := fmt.Sprintf("schema/%s/snapshots/%s/%s/restore", className, storageName, ID)
	if err := m.authorizer.Authorize(principal, "restore", path); err != nil {
		return nil, err
	}

	go func(ctx context.Context, className, snapshotId string) {
		timer := prometheus.NewTimer(monitoring.GetMetrics().SnapshotRestoreDurations.WithLabelValues(storageName, className))
		defer timer.ObserveDuration()
		m.RestoreStatus.Store(snapshotUID, models.SnapshotRestoreMetaStatusTRANSFERRING)
		if meta, snapshot, err := m.backups.RestoreBackup(context.Background(), className, storageName, ID); err != nil {
			if meta != nil {
				m.RestoreStatus.Store(snapshotUID, string(meta.Status))
			} else {
				m.RestoreStatus.Store(snapshotUID, models.SnapshotRestoreMetaStatusFAILED)
			}
			m.RestoreError.Store(snapshotUID, err)
			return
		} else {
			classM := models.Class{}
			if err := json.Unmarshal([]byte(snapshot.Schema), &classM); err != nil {
				m.RestoreStatus.Store(snapshotUID, models.SnapshotRestoreMetaStatusFAILED)
				m.RestoreError.Store(snapshotUID, err)
				return
			}

			err := m.schema.RestoreClass(ctx, principal, &classM, snapshot)
			if err != nil {
				m.RestoreStatus.Store(snapshotUID, models.SnapshotRestoreMetaStatusFAILED)
				m.RestoreError.Store(snapshotUID, err)
				return
			}

			m.RestoreStatus.Store(snapshotUID, models.SnapshotRestoreMetaStatusSUCCESS)

		}
	}(context.Background(), className, ID)

	statusInterface, ok := m.RestoreStatus.Load(snapshotUID)
	if !ok {
		return nil, errors.Errorf("snapshot  not found for %s", snapshotUID)
	}
	status := statusInterface.(string)

	path, err := m.backups.DestinationPath(storageName, className, ID)
	if err != nil {
		return nil, err
	}
	returnData := &models.SnapshotRestoreMeta{
		ID:          ID,
		StorageName: storageName,
		Status:      &status,
		Path:        path,
	}
	return returnData, nil
}

func validateSnapshotID(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("missing snapshotID value")
	}

	exp := regexp.MustCompile("^[a-z0-9_-]+$")
	if !exp.MatchString(snapshotID) {
		return fmt.Errorf("invalid characters for snapshotID. Allowed are lowercase, numbers, underscore, minus")
	}

	return nil
}
