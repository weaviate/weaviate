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

package hfresh

import (
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	maxPostingSizeFloor uint32 = 8 // Minimum vectors per posting
	minPostingSizeFloor uint32 = 3 // Minimum vectors for split threshold
)

type Config struct {
	Logger                    logrus.FieldLogger
	Scheduler                 *queue.Scheduler
	DistanceProvider          distancer.Provider
	RootPath                  string
	ID                        string
	TargetVector              string
	ShardName                 string
	ClassName                 string
	PrometheusMetrics         *monitoring.PrometheusMetrics
	InternalPostingCandidates int                             `json:"internalPostingCandidates,omitempty"` // Number of candidates to consider when running a centroid search internally
	ReassignNeighbors         int                             `json:"reassignNeighbors,omitempty"`         // Number of neighboring centroids to consider for reassigning vectors
	MaxDistanceRatio          float32                         `json:"maxDistanceRatio,omitempty"`          // Maximum distance ratio for the search, used to filter out candidates that are too far away
	Store                     StoreConfig                     `json:"store"`                               // Configuration for the underlying LSMKV store
	Centroids                 CentroidConfig                  `json:"centroids"`                           // Configuration for the centroid index
	TombstoneCallbacks        cyclemanager.CycleCallbackGroup // Callbacks for handling tombstones
	VectorForIDThunk          common.VectorForID[float32]     `json:"vectorForIDThunk,omitempty"` // Function to get a vector by index ID
}

type StoreConfig struct {
	MakeBucketOptions lsmkv.MakeBucketOptions `json:"makeBucketOptions,omitempty"` // Make bucket options for the store
}

type CentroidConfig struct {
	HNSWConfig *hnsw.Config `json:"hnswConfig,omitempty"`
}

const (
	DefaultInternalPostingCandidates = 64
	DefaultReassignNeighbors         = 8
	DefaultMaxDistanceRatio          = 10_000
)

func (c *Config) Validate() error {
	if c.Logger == nil {
		logger := logrus.New()
		logger.Out = io.Discard
		c.Logger = logger
	}

	if c.InternalPostingCandidates <= 0 {
		c.InternalPostingCandidates = DefaultInternalPostingCandidates
	}
	if c.ReassignNeighbors <= 0 {
		c.ReassignNeighbors = DefaultReassignNeighbors
	}
	if c.MaxDistanceRatio <= 0 {
		c.MaxDistanceRatio = DefaultMaxDistanceRatio
	}

	return nil
}

func DefaultConfig() *Config {
	return &Config{
		Logger:                    logrus.New(),
		InternalPostingCandidates: DefaultInternalPostingCandidates,
		ReassignNeighbors:         DefaultReassignNeighbors,
		MaxDistanceRatio:          DefaultMaxDistanceRatio,
		DistanceProvider:          distancer.NewL2SquaredProvider(),
		Store:                     StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions},
	}
}

func ValidateUserConfigUpdate(initial, updated config.VectorIndexConfig) error {
	initialParsed, ok := initial.(ent.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	updatedParsed, ok := updated.(ent.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	immutableFields := []immutableParameter{
		{
			name:     "distance",
			accessor: func(c ent.UserConfig) interface{} { return c.Distance },
		},
		{
			name:     "replicas",
			accessor: func(c ent.UserConfig) interface{} { return c.Replicas },
		},
	}

	for _, u := range immutableFields {
		if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}

	return nil
}

type immutableParameter struct {
	accessor func(c ent.UserConfig) interface{}
	name     string
}

func validateImmutableField(u immutableParameter,
	previous, next ent.UserConfig,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from \"%v\" to \"%v\"",
			u.name, oldField, newField)
	}

	return nil
}

// MaxPostingVectors returns how many vectors can fit in one posting
// given the dimensions and I/O budget.
// I/O budget: 48KB.
// Dims is the number of dimensions of the vector, after compression
// if applicable.
func (h *HFresh) computeMaxPostingSize(dims int) uint32 {
	bytesPerDim := 0.125                                  // RQ1
	maxBytes := h.maxPostingSizeKB * 1024                 // budget
	metadata := 8 + 1 + compressionhelpers.RQMetadataSize // id + version + RQ metadata

	vBytes := float64(dims)*bytesPerDim + float64(metadata)

	return uint32(math.Ceil(float64(maxBytes) / vBytes))
}

func (h *HFresh) setMaxPostingSize() {
	if h.maxPostingSize == 0 {
		h.maxPostingSize = max(h.computeMaxPostingSize(int(h.dims)), maxPostingSizeFloor)
	}

	h.minPostingSize = max(h.maxPostingSize/3, minPostingSizeFloor)
	h.logger.WithFields(
		logrus.Fields{
			"action":           "hfresh_configure",
			"id":               h.id,
			"minPostingSize":   h.minPostingSize,
			"maxPostingSize":   h.maxPostingSize,
			"maxPostingSizeKB": h.maxPostingSizeKB,
		},
	).Warn("hfresh posting sizes configured")
}
