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

package geo

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	//GW
    //GW"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
    "github.com/weaviate/weaviate/adapters/repos/db/vector/gemini"
    //GW
	//GW "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/gemini/distancer"
    //GW
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	//GW hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	geminient "github.com/weaviate/weaviate/entities/vectorindex/gemini"
    //GW
)

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
	Add(id uint64, vector []float32) error
	KnnSearchByVectorMaxDist(query []float32, dist float32, ef int,
		allowList helpers.AllowList) ([]uint64, error)
	Delete(id uint64) error
	Dump(...string)
	Drop(ctx context.Context) error
	PostStartup()
}

// Config is passed to the GeoIndex when its created
type Config struct {
	ID                 string
	CoordinatesForID   CoordinatesForID
	DisablePersistence bool
	RootPath           string
	Logger             logrus.FieldLogger
}

func NewIndex(config Config) (*Index, error) {
	//GW vi, err := hnsw.New(hnsw.Config{
	vi, err := gemini.New(gemini.Config{
    //GW
		VectorForIDThunk:      config.CoordinatesForID.VectorForID,
		ID:                    config.ID,
		RootPath:              config.RootPath,
		MakeCommitLoggerThunk: makeCommitLoggerFromConfig(config),
		DistanceProvider:      distancer.NewGeoProvider(),
	//GW}, hnswent.UserConfig{
	}, geminient.UserConfig{
    //GW
		MaxConnections:         64,
		EFConstruction:         128,
		//GWCleanupIntervalSeconds: hnswent.DefaultCleanupIntervalSeconds,
		CleanupIntervalSeconds: geminient.DefaultCleanupIntervalSeconds,
        //GW
	})
	if err != nil {
		//GW return nil, errors.Wrap(err, "underlying hnsw index")
		return nil, errors.Wrap(err, "underlying gemini index")
        //GW
	}

	i := &Index{
		config:      config,
		vectorIndex: vi,
	}

	return i, nil
}

func (i *Index) Drop(ctx context.Context) error {
	if err := i.vectorIndex.Drop(ctx); err != nil {
		return err
	}

	i.vectorIndex = nil
	return nil
}

func (i *Index) PostStartup() {
	i.vectorIndex.PostStartup()
}

//GW func makeCommitLoggerFromConfig(config Config) hnsw.MakeCommitLogger {
func makeCommitLoggerFromConfig(config Config) gemini.MakeCommitLogger {
//GW
	//GWmakeCL := hnsw.MakeNoopCommitLogger
	makeCL := gemini.MakeNoopCommitLogger
    //GW
	if !config.DisablePersistence {
		//GW makeCL = func() (hnsw.CommitLogger, error) {
		makeCL = func() (gemini.CommitLogger, error) {
        //GW
			//GW return hnsw.NewCommitLogger(config.RootPath, config.ID, 10*time.Second,
			return gemini.NewCommitLogger(config.RootPath, config.ID, 10*time.Second,
            //GW
				config.Logger)
		}
	}
	return makeCL
}

// Add extends the index with the specified GeoCoordinates. It is thread-safe
// and can be called concurrently.
func (i *Index) Add(id uint64, coordinates *models.GeoCoordinates) error {
	v, err := geoCoordiantesToVector(coordinates)
	if err != nil {
		return errors.Wrap(err, "invalid arguments")
	}

	return i.vectorIndex.Add(id, v)
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

	return i.vectorIndex.KnnSearchByVectorMaxDist(query, geoRange.Distance, 800, nil)
}

func (i *Index) Delete(id uint64) error {
	return i.vectorIndex.Delete(id)
}
