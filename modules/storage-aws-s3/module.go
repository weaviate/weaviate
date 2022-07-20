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
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus"
)

const Name = "storage-aws-s3"

type StorageAWSS3Module struct {
	logger logrus.FieldLogger
}

func New() *StorageAWSS3Module {
	return &StorageAWSS3Module{}
}

func (m *StorageAWSS3Module) Name() string {
	return Name
}

func (m *StorageAWSS3Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Storage
}

func (m *StorageAWSS3Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()

	if err := m.initSnapshotStorage(ctx); err != nil {
		return errors.Wrap(err, "init snapshot storage")
	}

	return nil
}

func (m *StorageAWSS3Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}
