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
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

// TODO adjust or make configurable
const (
	createTimeout  = time.Minute
	storeTimeout   = time.Minute
	releaseTimeout = time.Minute
	metaTimeout    = 5 * time.Second
)

type snapshotProvider struct {
	snapshotter Snapshotter
	storage     modulecapabilities.SnapshotStorage
	className   string
	snapshotID  string
}

func newSnapshotProvider(snapshotter Snapshotter, storage modulecapabilities.SnapshotStorage,
	className, snapshotID string,
) *snapshotProvider {
	return &snapshotProvider{snapshotter, storage, className, snapshotID}
}

func (sp *snapshotProvider) start(ctx context.Context) (*snapshots.Snapshot, error) {
	snapshot, err := sp.initSnapshot()
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (sp *snapshotProvider) backup(ctx context.Context, snapshot *snapshots.Snapshot) error {
	var ctxCreate, ctxStore, ctxRelease context.Context
	var cancelCreate, cancelStore, cancelRelease context.CancelFunc

	ctxCreate, cancelCreate = context.WithTimeout(context.Background(), createTimeout)
	defer cancelCreate()
	snapshot, err := sp.snapshotter.CreateSnapshot(ctxCreate, snapshot)
	if err != nil {
		return sp.setMetaFailed(errors.Wrap(err, "create snapshot"))
	}

	if err := sp.setMetaStatus(snapshots.CreateTransferring); err != nil {
		return err
	}

	ctxStore, cancelStore = context.WithTimeout(context.Background(), storeTimeout)
	defer cancelStore()
	if err := sp.storage.StoreSnapshot(ctxStore, snapshot); err != nil {
		return sp.setMetaFailed(errors.Wrap(err, "store snapshot"))
	}

	if err := sp.setMetaStatus(snapshots.CreateTransferred); err != nil {
		return err
	}

	ctxRelease, cancelRelease = context.WithTimeout(context.Background(), releaseTimeout)
	defer cancelRelease()
	if err := sp.snapshotter.ReleaseSnapshot(ctxRelease, sp.snapshotID); err != nil {
		return sp.setMetaFailed(errors.Wrap(err, "release snapshot"))
	}

	if err := sp.setMetaStatus(snapshots.CreateSuccess); err != nil {
		return err
	}

	return nil
}

func (sp *snapshotProvider) setMetaFailed(err error) error {
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

func (sp *snapshotProvider) setMetaStatus(status snapshots.CreateStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), metaTimeout)
	defer cancel()

	if err := sp.storage.SetMetaStatus(ctx, sp.className, sp.snapshotID, string(status)); err != nil {
		return errors.Wrapf(err, "update snapshot meta to %s", status)
	}
	return nil
}

func (sp *snapshotProvider) initSnapshot() (*snapshots.Snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaTimeout)
	defer cancel()

	snapshot, err := sp.storage.InitSnapshot(ctx, sp.className, sp.snapshotID)
	if err != nil {
		return nil, errors.Wrap(err, "init snapshot meta")
	}
	return snapshot, nil
}
