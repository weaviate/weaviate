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

package geo

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const DefaultHNSWEF = 800

// Index wraps another index to provide geo searches. This allows us to reuse
// the hnsw vector index, without making geo searches dependent on
// hnsw-specific features.
//
// In the future we could use this level of abstraction to provide a better
// suited geo-index if we deem it necessary
type Index struct {
	config      Config
	vectorIndex vectorIndex
}

// vectorIndex represents the underlying vector index, typically hnsw
type vectorIndex interface {
	Add(ctx context.Context, id uint64, vector []float32) error
	KnnSearchByVectorMaxDist(ctx context.Context, query []float32, dist float32, ef int,
		allowList helpers.AllowList) ([]uint64, error)
	Delete(id ...uint64) error
	Drop(ctx context.Context, keepFiles bool) error
	Flush() error
	Shutdown(ctx context.Context) error
	PostStartup(ctx context.Context)
	PrepareForBackup(ctx context.Context) error
	ResumeAfterBackup(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
}

// Config is passed to the GeoIndex when its created
type Config struct {
	ID                 string
	CoordinatesForID   CoordinatesForID
	DisablePersistence bool
	RootPath           string
	Logger             logrus.FieldLogger
	ClassName          string
	ShardName          string

	// Store and CoordinatesFromObject let the cache prefill read every
	// coordinate from the objects bucket in storage order, which it does only
	// when WaitForCachePrefill is set. A Store without a decoder is rejected;
	// leaving the Store out keeps the prefill on one lookup per doc ID.
	Store                 *lsmkv.Store
	CoordinatesFromObject CoordinatesFromObject
	WaitForCachePrefill   bool

	HNSWEF int

	SnapshotDisabled                         bool
	SnapshotOnStartup                        bool
	SnapshotCreateInterval                   time.Duration
	SnapshotMinDeltaCommitlogsNumer          int
	SnapshotMinDeltaCommitlogsSizePercentage int
	AllocChecker                             memwatch.AllocChecker
}

func (c Config) hnswEF() int {
	if c.HNSWEF > 0 {
		return c.HNSWEF
	}
	return DefaultHNSWEF
}

func NewIndex(config Config,
	commitLogMaintenanceCallbacks, tombstoneCleanupCallbacks cyclemanager.CycleCallbackGroup,
) (*Index, error) {
	// the commit-logger thunk below captures this Config by value, so hnsw.New
	// substituting a default into its own copy would never reach it
	config.Logger = common.LoggerOrDiscard(config.Logger)

	// without a decoder the prefill scan would read each object's own vector and
	// cache it as a coordinate
	if config.Store != nil && config.CoordinatesFromObject == nil {
		return nil, errors.Errorf("geo index %q: coordinatesFromObject is required alongside a store", config.ID)
	}

	// the underlying index identifies its lines by class, shard and target
	// vector, and a geo index leaves the target vector empty. index_id is the key
	// its prefill lines already give this id, which also names the files on disk.
	config.Logger = config.Logger.WithField("index_id", config.ID)

	var vectorFromObject hnsw.VectorFromObject
	if config.CoordinatesFromObject != nil {
		vectorFromObject = config.CoordinatesFromObject.VectorFromObject
	}

	vi, err := hnsw.New(hnsw.Config{
		VectorForIDThunk:      config.CoordinatesForID.VectorForID,
		VectorFromObject:      vectorFromObject,
		WaitForCachePrefill:   config.WaitForCachePrefill,
		ID:                    config.ID,
		ClassName:             config.ClassName,
		ShardName:             config.ShardName,
		RootPath:              config.RootPath,
		MakeCommitLoggerThunk: makeCommitLoggerFromConfig(config, commitLogMaintenanceCallbacks),
		DistanceProvider:      distancer.NewGeoProvider(),
		DisableSnapshots:      config.SnapshotDisabled,
		SnapshotOnStartup:     config.SnapshotOnStartup,
		AllocChecker:          config.AllocChecker,
		GetViewThunk:          func() common.BucketView { return nil },
		Logger:                config.Logger,
	}, hnswent.UserConfig{
		MaxConnections:         64,
		EFConstruction:         128,
		CleanupIntervalSeconds: hnswent.DefaultCleanupIntervalSeconds,
		// The cache drops every vector once its entry count reaches this maximum,
		// so a zero here empties it every few seconds.
		VectorCacheMaxObjects: vectorIndexCommon.DefaultVectorCacheMaxObjects,
	}, tombstoneCleanupCallbacks, config.Store)
	if err != nil {
		return nil, errors.Wrap(err, "underlying hnsw index")
	}

	i := &Index{
		config:      config,
		vectorIndex: vi,
	}

	return i, nil
}

func (i *Index) Drop(ctx context.Context, keepFiles bool) error {
	if err := i.vectorIndex.Drop(ctx, keepFiles); err != nil {
		return err
	}

	i.vectorIndex = nil
	return nil
}

func (i *Index) PostStartup(ctx context.Context) {
	i.vectorIndex.PostStartup(ctx)
}

func makeCommitLoggerFromConfig(config Config, maintenanceCallbacks cyclemanager.CycleCallbackGroup,
) hnsw.MakeCommitLogger {
	makeCL := hnsw.MakeNoopCommitLogger
	if !config.DisablePersistence {
		makeCL = func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(config.RootPath, config.ID, config.Logger, maintenanceCallbacks,
				hnsw.WithSnapshotDisabled(config.SnapshotDisabled),
				hnsw.WithSnapshotCreateInterval(config.SnapshotCreateInterval),
				hnsw.WithSnapshotMinDeltaCommitlogsNumer(config.SnapshotMinDeltaCommitlogsNumer),
				hnsw.WithSnapshotMinDeltaCommitlogsSizePercentage(config.SnapshotMinDeltaCommitlogsSizePercentage),
			)
		}
	}
	return makeCL
}

// Add extends the index with the specified GeoCoordinates. It is thread-safe
// and can be called concurrently.
func (i *Index) Add(ctx context.Context, id uint64, coordinates *models.GeoCoordinates) error {
	v, err := geoCoordiantesToVector(coordinates)
	if err != nil {
		return errors.Wrap(err, "invalid arguments")
	}

	return i.vectorIndex.Add(ctx, id, v)
}

// WithinGeoRange searches the index by the specified range. It is thread-safe
// and can be called concurrently.
func (i *Index) WithinRange(ctx context.Context,
	geoRange filters.GeoRange,
) ([]uint64, error) {
	if geoRange.GeoCoordinates == nil {
		return nil, fmt.Errorf("invalid arguments: GeoCoordinates in range must be set")
	}

	query, err := geoCoordiantesToVector(geoRange.GeoCoordinates)
	if err != nil {
		return nil, errors.Wrap(err, "invalid arguments")
	}

	return i.vectorIndex.KnnSearchByVectorMaxDist(ctx, query, geoRange.Distance, i.config.hnswEF(), nil)
}

func (i *Index) Delete(id uint64) error {
	return i.vectorIndex.Delete(id)
}

func (i *Index) Flush() error {
	return i.vectorIndex.Flush()
}

func (i *Index) Shutdown(ctx context.Context) error {
	return i.vectorIndex.Shutdown(ctx)
}

// PrepareForBackup readies the geo graph for its files to be copied. The graph
// is an HNSW in its own right, persisted beside the shard rather than inside its
// LSM buckets, so a backup has to halt, list and resume it like any other vector
// index.
func (i *Index) PrepareForBackup(ctx context.Context) error {
	return i.vectorIndex.PrepareForBackup(ctx)
}

func (i *Index) ResumeAfterBackup(ctx context.Context) error {
	return i.vectorIndex.ResumeAfterBackup(ctx)
}

// ListFiles returns the index files a backup has to copy, relative to basePath.
func (i *Index) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return i.vectorIndex.ListFiles(ctx, basePath)
}

// UnderlyingVectorIndex returns the underlying vector index (typically HNSW)
// so it can be wrapped in a VectorIndexQueue for async indexing.
func (i *Index) UnderlyingVectorIndex() interface{} {
	return i.vectorIndex
}
