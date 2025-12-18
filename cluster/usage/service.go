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
	"fmt"
	"runtime/debug"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/usage/types"
	backupent "github.com/weaviate/weaviate/entities/backup"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/config"
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
// exactObjectCount will return the correct object count (including memtables) when set to true. This is mainly for
// testing via the debug api. In production, this should be false to avoid the performance hit
func (s *service) Usage(ctx context.Context, exactObjectCount bool) (*types.Report, error) {
	scheme := s.schemaManager.GetSchemaSkipAuth().Objects
	collections := scheme.Classes
	usage := &types.Report{
		Schema:      scheme,
		Node:        s.schemaManager.NodeName(),
		Collections: make([]*types.CollectionUsage, 0, len(collections)),
		Backups:     make([]*types.BackupUsage, 0),
	}

	s.logger.Infof("Creating usage report with %d collections", len(collections))
	// Collect usage for each collection
	for _, collection := range collections {
		vectorConfig, err := config.ExtractVectorConfigs(collection)
		if err != nil {
			return nil, fmt.Errorf("collection %s: %w", collection.Class, err)
		}
		collectionUsage, err := s.db.UsageForIndex(ctx, entschema.ClassName(collection.Class), s.jitterInterval, exactObjectCount, vectorConfig)
		// we lock the local index against being deleted while we collect usage, however we cannot lock the RAFT schema
		// against being changed. If the class was deleted in the RAFT schema, we simply skip it here
		// as it is no longer relevant for the current node usage
		if errors.Is(err, clusterSchema.ErrClassNotFound) || collectionUsage == nil {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("collection %s: %w", collection.Class, err)
		}

		usage.Collections = append(usage.Collections, collectionUsage)
	}
	sort.Sort(usage.Collections)

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

			var sizeInGib float64
			for name, n := range backup.Nodes {
				if name == usage.Node {
					sizeInGib = float64(n.PreCompressionSizeBytes) / (1024 * 1024 * 1024) // Convert bytes to GiB
					break
				}
			}

			usage.Backups = append(usage.Backups, &types.BackupUsage{
				ID:             backup.ID,
				CompletionTime: backup.CompletedAt.Format(time.RFC3339),
				SizeInGib:      sizeInGib,
				Type:           string(backup.Status),
				Collections:    backup.Classes(),
			})
		}
	}

	// -1 returns current limit without changing it
	usage.GoMemLimit = debug.SetMemoryLimit(-1)

	return usage, nil
}
