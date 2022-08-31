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
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
)

// TODO adjust or make configurable
const (
	createTimeout  = 5 * time.Minute
	storeTimeout   = 12 * time.Hour
	releaseTimeout = 30 * time.Second
	metaTimeout    = 5 * time.Second
)

type backupProvider struct {
	snapshotter Sourcer
	storage     modulecapabilities.SnapshotStorage
	className   string
	snapshotID  string
}

func newBackupProvider(snapshotter Sourcer, storage modulecapabilities.SnapshotStorage,
	className, snapshotID string,
) *backupProvider {
	return &backupProvider{snapshotter, storage, className, snapshotID}
}

func (sp *backupProvider) start(ctx context.Context) (*backup.Snapshot, error) {
	snapshot, err := sp.init()
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (sp *backupProvider) backup(ctx context.Context, snapshot *backup.Snapshot) error {
	var ctxCreate, ctxStore, ctxRelease context.Context
	var cancelCreate, cancelStore, cancelRelease context.CancelFunc

	ctxCreate, cancelCreate = context.WithTimeout(context.Background(), createTimeout)
	defer cancelCreate()
	snapshot, err := sp.snapshotter.CreateBackup(ctxCreate, snapshot)
	if err != nil {
		return sp.setMetaFailed(errors.Wrap(err, "create snapshot"))
	}

	if err := sp.setMetaStatus(backup.CreateTransferring); err != nil {
		return err
	}

	ctxStore, cancelStore = context.WithTimeout(context.Background(), storeTimeout)
	defer cancelStore()
	if err := sp.storage.StoreSnapshot(ctxStore, snapshot); err != nil {
		return sp.setMetaFailed(errors.Wrap(err, "store snapshot"))
	}

	if err := sp.setMetaStatus(backup.CreateTransferred); err != nil {
		return err
	}

	ctxRelease, cancelRelease = context.WithTimeout(context.Background(), releaseTimeout)
	defer cancelRelease()
	if err := sp.snapshotter.ReleaseBackup(ctxRelease, sp.snapshotID); err != nil {
		return sp.setMetaFailed(errors.Wrap(err, "release snapshot"))
	}

	if err := sp.setMetaStatus(backup.CreateSuccess); err != nil {
		return err
	}

	return nil
}

func (sp *backupProvider) setMetaFailed(err error) error {
	ctx, cancel := context.WithTimeout(context.Background(), metaTimeout)
	defer cancel()

	if errMeta := sp.storage.SetMetaError(ctx, sp.className, sp.snapshotID, err); errMeta != nil {
		ec := &errorcompounder.ErrorCompounder{}
		ec.Add(errMeta)
		ec.Add(err)
		return ec.ToError()
	}
	return err
}

func (sp *backupProvider) setMetaStatus(status backup.CreateStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), metaTimeout)
	defer cancel()

	if err := sp.storage.SetMetaStatus(ctx, sp.className, sp.snapshotID, string(status)); err != nil {
		return errors.Wrapf(err, "update snapshot meta to %s", status)
	}
	return nil
}

func (sp *backupProvider) init() (*backup.Snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaTimeout)
	defer cancel()

	snapshot, err := sp.storage.InitSnapshot(ctx, sp.className, sp.snapshotID)
	if err != nil {
		return nil, errors.Wrap(err, "init snapshot meta")
	}
	return snapshot, nil
}
