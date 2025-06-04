//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	database "github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modules"
	usecaseSchema "github.com/weaviate/weaviate/usecases/schema"
)

var (
	regxUsage = regexp.MustCompile(`/usage`)
)

type usageManager interface {
	GetUsage() (*models.UsageResponse, error)
}

type usage struct {
	manager usageManager
	// auth    clusterapi.auth // TODO: do we need auth ?
}

func NewUsage(manager usageManager) *usage {
	return &usage{manager: manager}
}

func (m *usage) Usage() http.Handler {
	// return m.auth.handleFunc(m.usageHandler())
	return http.HandlerFunc(m.usageHandler())
}

func (m *usage) usageHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case regxUsage.MatchString(path):
			if r.Method != http.MethodGet {
				http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
				return
			}
			usage, err := m.manager.GetUsage()
			if err != nil {
				http.Error(w, "get usage: "+err.Error(), http.StatusInternalServerError)
				return
			}

			if usage == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			usageBytes, err := json.Marshal(usage)
			if err != nil {
				http.Error(w, "marshal usage response: "+err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.Write(usageBytes)
			return
		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
	}
}

// usageManagerImpl implements the usageManager interface
type usageManagerImpl struct {
	db            *database.DB
	schemaManager *usecaseSchema.Manager
	modules       *modules.Provider
}

func NewManager(db *database.DB, schemaManager *usecaseSchema.Manager, modules *modules.Provider) usageManager {
	return &usageManagerImpl{
		db:            db,
		schemaManager: schemaManager,
		modules:       modules,
	}
}

func (m *usageManagerImpl) GetUsage() (*models.UsageResponse, error) {
	// Get all collections
	collections := m.schemaManager.GetSchemaSkipAuth().Objects.Classes
	usage := &models.UsageResponse{
		Node:                    m.schemaManager.NodeName(),
		SingleTenantCollections: make([]*models.CollectionUsage, 0, len(collections)),
		Backups:                 make([]*models.BackupUsage, 0),
	}

	// Collect usage for each collection
	for _, class := range collections {
		shardingState := m.schemaManager.CopyShardingState(class.Class)
		collectionUsage := &models.CollectionUsage{
			Name:              class.Class,
			ReplicationFactor: class.ReplicationConfig.Factor,
			UniqueShardCount:  int64(len(shardingState.Physical)),
			Shards:            make([]*models.ShardUsage, 0),
		}
		// Get shard usage
		index := m.db.GetIndex(schema.ClassName(class.Class))
		if index != nil {
			// TODO: this will load all shards into memory, which is not efficient
			// we shall collect usage for each shard without loading them into memory
			index.ForEachShard(func(name string, shard database.ShardLike) error {
				shardUsage := &models.ShardUsage{
					Name:                name,
					ObjectsCount:        int64(shard.ObjectCount()),
					ObjectsStorageBytes: shard.ObjectStorageBytes(),
					NamedVectors:        make([]*models.VectorUsage, 0),
				}
				// Get vector usage for each named vector
				_ = shard.ForEachVectorIndex(func(vectorName string, vectorIndex database.VectorIndex) error {
					// TODO: this blocks the main thread needs debugging
					// stats, err := vectorIndex.Stats()
					// if err != nil {
					// 	logrus.Infof("err: %v", err)
					// }

					vectorUsage := &models.VectorUsage{
						Name:                   vectorName,
						VectorIndexType:        "hnsw",                                 //string(stats.IndexType()),
						Compression:            "",                                     // TODO get from stats stats.CompressionType()
						VectorCompressionRatio: 0,                                      // TODO: get from stats stats.CompressionRatio()
						Dimensionalities:       make([]*models.DimensionalityUsage, 0), // TODO: get from stats stats.Dimensions()
					}

					// Get dimensionality usage
					// if dims := stats.Dimensions(); dims > 0 {
					// 	vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &models.DimensionalityUsage{
					// 		Dimensionality: int64(dims),
					// 		Count:          int64(shard.ObjectCountAsync()),
					// 	})
					// }

					shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
					return nil
				})

				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
				return nil
			})
		}

		usage.SingleTenantCollections = append(usage.SingleTenantCollections, collectionUsage)
	}

	// Get backup usage from all enabled backup backends
	for _, backend := range m.modules.EnabledBackupBackends() {
		backups, err := backend.AllBackups(context.TODO())
		if err == nil {
			for _, backup := range backups {
				usage.Backups = append(usage.Backups, &models.BackupUsage{
					ID:             backup.ID,
					CompletionTime: backup.CompletedAt.Format(time.RFC3339),
					SizeInGib:      float64(backup.SizeBytes) / (1024 * 1024 * 1024), // Convert bytes to GiB
					Type:           string(backup.Status),
					Collections:    backup.Classes(),
				})
			}
		}
	}

	return usage, nil
}
