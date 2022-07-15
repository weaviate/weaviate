package modstgfs

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus"
)

const (
	Name              = "storage-filesystem"
	snapshotsPathName = "STG_FS_SNAPSHOTS_PATH"
)

type StorageFileSystemModule struct {
	logger        logrus.FieldLogger
	snapshotsPath string
}

func New() *StorageFileSystemModule {
	return &StorageFileSystemModule{}
}

func (m *StorageFileSystemModule) Name() string {
	return Name
}

func (m *StorageFileSystemModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Storage
}

func (m *StorageFileSystemModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()

	snapshotsPath := os.Getenv(snapshotsPathName)
	if err := m.initSnapshotStorage(ctx, snapshotsPath); err != nil {
		return errors.Wrap(err, "init snapshot storage")
	}

	return nil
}

func (m *StorageFileSystemModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}
