//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"context"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/usage/types"
	backupent "github.com/weaviate/weaviate/entities/backup"
	entcfg "github.com/weaviate/weaviate/entities/config"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/schema"
)

type Service interface {
	SetJitterInterval(interval time.Duration)
	Usage(ctx context.Context, exactObjectCount bool) (*types.Report, error)
}
type service struct {
	schemaManager  schema.SchemaGetter
	db             *db.DB
	backups        backup.BackupBackendProvider
	logger         logrus.FieldLogger
	jitterInterval time.Duration
}

func NewService(schemaManager schema.SchemaGetter, db *db.DB, backups backup.BackupBackendProvider, logger logrus.FieldLogger) Service {
	return &service{
		schemaManager:  schemaManager,
		db:             db,
		backups:        backups,
		logger:         logger,
		jitterInterval: 0, // Default to no jitter
	}
}

// SetJitterInterval sets the jitter interval for shard processing
func (s *service) SetJitterInterval(interval time.Duration) {
	s.jitterInterval = interval
	s.logger.WithFields(logrus.Fields{"jitter_interval": interval.String()}).Info("shard jitter interval updated")
}

// Usage service collects usage metrics for the node and shall return error in case of any error
// to avoid reporting partial data
func (s *service) Usage(ctx context.Context, exactObjectCount bool) (*types.Report, error) {
	defer func() {
		if !entcfg.Enabled(os.Getenv("RECOVERY_IN_USAGE_MODULE_DISABLED")) {
			if r := recover(); r != nil {
				s.logger.Warn("Could not collect usage data")
			}
		}
	}()

	scheme := s.schemaManager.GetSchemaSkipAuth().Objects
	collections := scheme.Classes
	usage := &types.Report{
		Schema:      scheme,
		Node:        s.schemaManager.NodeName(),
		Collections: make([]*types.CollectionUsage, 0, len(collections)),
		Backups:     make([]*types.BackupUsage, 0),
	}

	// Collect usage for each collection
	for _, collection := range collections {
		collectionUsage, err := s.db.UsageForIndex(ctx, entschema.ClassName(collection.Class), s.jitterInterval, exactObjectCount)
		if err != nil {
			return nil, err
		}

		usage.Collections = append(usage.Collections, collectionUsage)
	}

	// Get backup usage from all enabled backup backends
	for _, backend := range s.backups.EnabledBackupBackends() {
		backups, err := backend.AllBackups(ctx)
		if err != nil {
			s.logger.WithError(err).WithFields(logrus.Fields{"backend": backend}).Error("failed to get backups from backend")
			return nil, err
		}

		for _, backup := range backups {
			if backup.Status != backupent.Success {
				continue
			}
			usage.Backups = append(usage.Backups, &types.BackupUsage{
				ID:             backup.ID,
				CompletionTime: backup.CompletedAt.Format(time.RFC3339),
				SizeInGib:      float64(backup.PreCompressionSizeBytes) / (1024 * 1024 * 1024), // Convert bytes to GiB
				Type:           string(backup.Status),
				Collections:    backup.Classes(),
			})
		}
	}
	return usage, nil
}
