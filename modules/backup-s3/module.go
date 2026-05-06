//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modstgs3

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	Name       = "backup-s3"
	AltName1   = "s3"
	s3Endpoint = "BACKUP_S3_ENDPOINT"
	s3Bucket   = "BACKUP_S3_BUCKET"
	s3UseSSL   = "BACKUP_S3_USE_SSL"

	// this is an optional value, allowing for
	// the backup to be stored in a specific
	// directory inside the provided bucket.
	//
	// if left unset, the backup files will
	// be stored directly in the root of the
	// bucket.
	s3Path = "BACKUP_S3_PATH"

	// STS AssumeRole options for cross-account export access.
	// When EXPORT_S3_ROLE_ARN is set, the export path will use
	// STS AssumeRole to obtain temporary credentials.
	// Backup operations are not affected.
	exportS3RoleARN         = "EXPORT_S3_ROLE_ARN"
	exportS3ExternalID      = "EXPORT_S3_EXTERNAL_ID"
	exportS3STSEndpoint     = "EXPORT_S3_STS_ENDPOINT"
	exportS3RoleSessionName = "EXPORT_S3_ROLE_SESSION_NAME"
)

type Module struct {
	*s3Client              // embedded — handles backup (all BackupBackend methods auto-promoted)
	exportClient *s3Client // export-only client: no default bucket or path; the scheduler supplies both
	logger       logrus.FieldLogger
	dataPath     string
}

func New() *Module {
	return &Module{}
}

func (m *Module) Name() string {
	return Name
}

func (m *Module) IsExternal() bool {
	return true
}

func (m *Module) AltNames() []string {
	return []string{AltName1}
}

func (m *Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Backup
}

func (m *Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	m.dataPath = params.GetStorageProvider().DataPath()

	bucket := os.Getenv(s3Bucket)
	if bucket == "" {
		return errors.Errorf("backup init: '%s' must be set", s3Bucket)
	}
	// SSL on by default
	useSSL := strings.ToLower(os.Getenv(s3UseSSL)) != "false"
	config := newConfig(os.Getenv(s3Endpoint), bucket, os.Getenv(s3Path), useSSL)
	client, err := newClient(config, m.logger, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "initialize S3 backup module")
	}
	m.s3Client = client

	// Create a separate export client with no default bucket or path.
	// The export scheduler supplies both via EXPORT_DEFAULT_BUCKET and
	// EXPORT_DEFAULT_PATH. When EXPORT_S3_ROLE_ARN is set the export
	// client additionally uses STS AssumeRole for cross-account access.
	exportCfg := newConfig(os.Getenv(s3Endpoint), "", "", useSSL)
	if exportRoleARN := os.Getenv(exportS3RoleARN); exportRoleARN != "" {
		exportCfg.RoleARN = exportRoleARN
		exportCfg.ExternalID = os.Getenv(exportS3ExternalID)
		exportCfg.STSEndpoint = os.Getenv(exportS3STSEndpoint)
		exportCfg.RoleSessionName = os.Getenv(exportS3RoleSessionName)
		if exportCfg.RoleSessionName == "" {
			exportCfg.RoleSessionName = "weaviate-export-s3"
		}
	}
	exportClient, err := newClient(exportCfg, m.logger, m.dataPath)
	if err != nil {
		return errors.Wrap(err, "initialize S3 export client")
	}
	m.exportClient = exportClient

	return nil
}

// ExportBackend returns the export-specific backend. It has no default
// bucket or path; the export scheduler supplies both via
// EXPORT_DEFAULT_BUCKET and EXPORT_DEFAULT_PATH.
func (m *Module) ExportBackend() modulecapabilities.BackupBackend {
	return &exportS3Backend{m.exportClient}
}

// exportS3Backend wraps an s3Client to satisfy the full BackupBackend
// interface (s3Client is missing IsExternal and Name).
type exportS3Backend struct {
	*s3Client
}

func (e *exportS3Backend) IsExternal() bool { return true }
func (e *exportS3Backend) Name() string     { return Name }

func (m *Module) MetaInfo() (map[string]interface{}, error) {
	metaInfo := make(map[string]interface{}, 4)
	metaInfo["endpoint"] = m.config.Endpoint
	metaInfo["bucketName"] = m.config.Bucket
	if root := m.config.BackupPath; root != "" {
		metaInfo["rootName"] = root
	}
	metaInfo["useSSL"] = m.config.UseSSL
	return metaInfo, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.BackupBackend(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.ExportBackendProvider(New())
)
