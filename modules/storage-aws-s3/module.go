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
