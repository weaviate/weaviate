package geo

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/sirupsen/logrus"
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
	Add(id int, vector []float32) error
	KnnSearchByVectorMaxDist(query []float32, dist float32, ef int,
		allowList helpers.AllowList) ([]int, error)
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
	vi, err := hnsw.New(hnsw.Config{
		VectorForIDThunk:      config.CoordinatesForID.VectorForID,
		ID:                    config.ID,
		RootPath:              config.RootPath,
		EFConstruction:        128,
		MaximumConnections:    64,
		MakeCommitLoggerThunk: makeCommitLoggerFromConfig(config),
	})
	if err != nil {
		return nil, errors.Wrap(err, "underlying hnsw index")
	}

	i := &Index{
		config:      config,
		vectorIndex: vi,
	}

	return i, nil
}

func makeCommitLoggerFromConfig(config Config) hnsw.MakeCommitLogger {
	makeCL := hnsw.MakeNoopCommitLogger
	if !config.DisablePersistence {
		makeCL = func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(config.RootPath, config.ID, 10*time.Second,
				config.Logger)
		}
	}
	return makeCL
}

// Add extends the index with the specified GeoCoordinates. It is thread-safe
// and can be called concurrently.
func (i *Index) Add(id int, coordinates models.GeoCoordinates) error {
	v, err := geoCoordiantesToVector(coordinates)
	if err != nil {
		return errors.Wrap(err, "invalid arguments")
	}

	return i.vectorIndex.Add(id, v)
}

// WithinGeoRange searches the index by the specified range. It is thread-safe
// and can be called concurrently.
func (i *Index) WithinRange(ctx context.Context,
	geoRange filters.GeoRange) ([]int, error) {
	if geoRange.GeoCoordinates == nil {
		return nil, fmt.Errorf("invalid arguments: GeoCoordinates in range must be set")
	}

	query, err := geoCoordiantesToVector(*geoRange.GeoCoordinates)
	if err != nil {
		return nil, errors.Wrap(err, "invalid arguments")
	}

	return i.vectorIndex.KnnSearchByVectorMaxDist(query, geoRange.Distance, 800, nil)
}
