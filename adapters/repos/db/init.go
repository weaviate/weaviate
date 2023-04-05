//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/replica"
)

// On init we get the current schema and create one index object per class.
// They will in turn create shards which will either read an existing db file
// from disk or create a new one if none exists
func (d *DB) init(ctx context.Context) error {
	if err := os.MkdirAll(d.config.RootPath, 0o777); err != nil {
		return errors.Wrapf(err, "create root path directory at %s", d.config.RootPath)
	}

	objects := d.schemaGetter.GetSchemaSkipAuth().Objects
	if objects != nil {
		for _, class := range objects.Classes {
			invertedConfig := class.InvertedIndexConfig
			if invertedConfig == nil {
				// for backward compatibility, this field was introduced in v1.0.4,
				// prior schemas will not yet have the field. Init with the defaults
				// which were previously hard-coded.
				// In this method we are essentially reading the schema from disk, so
				// it could have been created before v1.0.4
				invertedConfig = &models.InvertedIndexConfig{
					CleanupIntervalSeconds: config.DefaultCleanupIntervalSeconds,
					Bm25: &models.BM25Config{
						K1: config.DefaultBM25k1,
						B:  config.DefaultBM25b,
					},
				}
			}

			if err := replica.ValidateConfig(class); err != nil {
				return fmt.Errorf("replication config: %w", err)
			}

			idx, err := NewIndex(ctx, IndexConfig{
				ClassName:                 schema.ClassName(class.Class),
				RootPath:                  d.config.RootPath,
				ResourceUsage:             d.config.ResourceUsage,
				QueryMaximumResults:       d.config.QueryMaximumResults,
				MemtablesFlushIdleAfter:   d.config.MemtablesFlushIdleAfter,
				MemtablesInitialSizeMB:    d.config.MemtablesInitialSizeMB,
				MemtablesMaxSizeMB:        d.config.MemtablesMaxSizeMB,
				MemtablesMinActiveSeconds: d.config.MemtablesMinActiveSeconds,
				MemtablesMaxActiveSeconds: d.config.MemtablesMaxActiveSeconds,
				TrackVectorDimensions:     d.config.TrackVectorDimensions,
				ReplicationFactor:         class.ReplicationConfig.Factor,
			}, d.schemaGetter.ShardingState(class.Class),
				inverted.ConfigFromModel(invertedConfig),
				class.VectorIndexConfig.(schema.VectorIndexConfig),
				d.schemaGetter, d, d.logger, d.nodeResolver, d.remoteIndex,
				d.replicaClient, d.promMetrics, class, d.jobQueueCh)
			if err != nil {
				return errors.Wrap(err, "create index")
			}

			d.indexLock.Lock()
			d.indices[idx.ID()] = idx
			idx.notifyReady()
			d.indexLock.Unlock()
		}
	}

	return nil
}
