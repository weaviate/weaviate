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

package modstggcs

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
	"github.com/sirupsen/logrus"
)

const (
	Name      = "storage-gcs"
	gcsBucket = "STORAGE_GCS_BUCKET"
)

type StorageGCSModule struct {
	logger          logrus.FieldLogger
	storageProvider modulecapabilities.SnapshotStorage
	config          gcs.Config
	dataPath        string
}

func New() *StorageGCSModule {
	return &StorageGCSModule{}
}

func (m *StorageGCSModule) Name() string {
	return Name
}

func (m *StorageGCSModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Storage
}

func (m *StorageGCSModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()

	if err := m.initSnapshotStorage(ctx); err != nil {
		return errors.Wrap(err, "init snapshot storage")
	}

	return nil
}

func (m *StorageGCSModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *StorageGCSModule) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["bucketName"] = m.config.BucketName()
	return metaInfo, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.SnapshotStorage(New())
	_ = modulecapabilities.MetaProvider(New())
)
