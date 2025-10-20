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

package spfresh

import (
	"fmt"
	"io"
	"math"
	"runtime"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/spfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Config struct {
	Logger                    logrus.FieldLogger
	DistanceProvider          distancer.Provider
	RootPath                  string
	ID                        string
	TargetVector              string
	ShardName                 string
	ClassName                 string
	PrometheusMetrics         *monitoring.PrometheusMetrics
	SplitWorkers              int                             `json:"splitWorkers,omitempty"`              // Number of concurrent workers for split operations
	ReassignWorkers           int                             `json:"reassignWorkers,omitempty"`           // Number of concurrent workers for reassign operations
	InternalPostingCandidates int                             `json:"internalPostingCandidates,omitempty"` // Number of candidates to consider when running a centroid search internally
	ReassignNeighbors         int                             `json:"reassignNeighbors,omitempty"`         // Number of neighboring centroids to consider for reassigning vectors
	MaxDistanceRatio          float32                         `json:"maxDistanceRatio,omitempty"`          // Maximum distance ratio for the search, used to filter out candidates that are too far away
	Store                     StoreConfig                     `json:"store"`                               // Configuration for the underlying LSMKV store
	Centroids                 CentroidConfig                  `json:"centroids"`                           // Configuration for the centroid index
	TombstoneCallbacks        cyclemanager.CycleCallbackGroup // Callbacks for handling tombstones
	Compressed                bool                            `json:"compressed,omitempty"` // Whether to store vectors in compressed format
}

type StoreConfig struct {
	MinMMapSize                  int64                 `json:"minMMapSize,omitempty"`                  // Minimum size of the mmap for the store
	MaxReuseWalSize              int64                 `json:"maxReuseWalSize,omitempty"`              // Maximum size of the reuse wal for the store
	AllocChecker                 memwatch.AllocChecker `json:"allocChecker,omitempty"`                 // Alloc checker for the store
	LazyLoadSegments             bool                  `json:"lazyLoadSegments,omitempty"`             // Lazy load segments for the store
	WriteSegmentInfoIntoFileName bool                  `json:"writeSegmentInfoIntoFileName,omitempty"` // Write segment info into file name for the store
	WriteMetadataFilesEnabled    bool                  `json:"writeMetadataFilesEnabled,omitempty"`    // Write metadata files for the store
}

type CentroidConfig struct {
	IndexType  string       `json:"indexType,omitempty"` // Type of centroid index to use, e.g. "bruteforce" or "hnsw"
	HNSWConfig *hnsw.Config `json:"hnswConfig,omitempty"`
}

func (c *Config) Validate() error {
	if c.Logger == nil {
		logger := logrus.New()
		logger.Out = io.Discard
		c.Logger = logger
	}

	return nil
}

func DefaultConfig() *Config {
	w := runtime.GOMAXPROCS(0)

	return &Config{
		Logger:                    logrus.New(),
		SplitWorkers:              w,
		ReassignWorkers:           w,
		InternalPostingCandidates: 64,
		ReassignNeighbors:         8,
		MaxDistanceRatio:          10_000,
		DistanceProvider:          distancer.NewL2SquaredProvider(),
		Compressed:                false,
	}
}

func ValidateUserConfigUpdate(initial, updated config.VectorIndexConfig) error {
	uc, ok := initial.(ent.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	nuc, ok := updated.(ent.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	if uc.Distance != nuc.Distance {
		return fmt.Errorf("distance function cannot be changed once set (was '%s', cannot change to '%s')",
			uc.Distance, nuc.Distance)
	}

	return nil
}

// MaxPostingVectors returns how many vectors can fit in one posting
// given the dimensions, compression and I/O budget.
// I/O budget: SPANN recommends 12KB per posting for byte vectors
// and 48KB for float32 vectors.
// Dims is the number of dimensions of the vector, after compression
// if applicable.
func computeMaxPostingSize(dims int, compressed bool) uint32 {
	bytesPerDim := 4
	maxBytes := 48 * 1024 // default to float32 budget
	metadata := 8 + 1     // id + version
	if compressed {
		bytesPerDim = 1
		maxBytes = 12 * 1024                          // compressed budget
		metadata += compressionhelpers.RQMetadataSize // RQ metadata
	}

	vBytes := dims*bytesPerDim + metadata

	return uint32(math.Ceil(float64(maxBytes) / float64(vBytes)))
}

func compressedVectorSize(size int) int {
	return size + compressionhelpers.RQMetadataSize
}

func (s *SPFresh) setMaxPostingSize() {
	if s.maxPostingSize == 0 {
		isCompressed := s.Compressed()
		s.maxPostingSize = computeMaxPostingSize(int(s.dims), isCompressed)
	}

	if s.maxPostingSize <= s.minPostingSize {
		s.minPostingSize = s.maxPostingSize / 2
	}
}
