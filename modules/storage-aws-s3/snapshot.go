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

package modstgs3

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/snapshots"
)

func (m *StorageAWSS3Module) StoreSnapshot(ctx context.Context, snapshot snapshots.Snapshot) error {
	// TODO implement
	m.logger.Errorf("StoreSnapshot of StorageAWSS3Module not yet implemented")
	return nil
}

func (m *StorageAWSS3Module) RestoreSnapshot(ctx context.Context, snapshotId string) error {
	// TODO implement
	m.logger.Errorf("RestoreSnapshot of StorageAWSS3Module not yet implemented")
	return nil
}

func (m *StorageAWSS3Module) initSnapshotStorage(ctx context.Context) error {
	// TODO implement
	m.logger.Errorf("initSnapshotStorage of StorageAWSS3Module not yet implemented")
	return nil
}
