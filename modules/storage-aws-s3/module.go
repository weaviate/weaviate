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
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
	"github.com/sirupsen/logrus"
)

const (
	Name       = "storage-aws-s3"
	AltName1   = "aws-s3"
	AltName2   = "s3"
	s3Endpoint = "STORAGE_S3_ENDPOINT"
	s3Bucket   = "STORAGE_S3_BUCKET"
	s3UseSSL   = "STORAGE_S3_USE_SSL"
)

type StorageS3Module struct {
	logger          logrus.FieldLogger
	storageProvider modulecapabilities.SnapshotStorage
	config          s3.Config
	dataPath        string
}

func New() *StorageS3Module {
	return &StorageS3Module{}
}

func (m *StorageS3Module) Name() string {
	return Name
}

func (m *StorageS3Module) AltNames() []string {
	return []string{AltName1, AltName2}
}

func (m *StorageS3Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Storage
}

func (m *StorageS3Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()

	if err := m.initSnapshotStorage(ctx); err != nil {
		return errors.Wrap(err, "init snapshot storage")
	}

	return nil
}

func (m *StorageS3Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *StorageS3Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{})
	metaInfo["endpoint"] = m.config.Endpoint()
	metaInfo["bucketName"] = m.config.BucketName()
	metaInfo["useSSL"] = m.config.UseSSL()
	return metaInfo, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.SnapshotStorage(New())
	_ = modulecapabilities.MetaProvider(New())
)
