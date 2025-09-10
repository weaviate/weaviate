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
	"io"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const (
	SizeBasedPruningStrategy     = "size"     // Size pruning strategy for brute force search
	DistanceBasedPruningStrategy = "distance" // Distance pruning strategy for brute force search
)

// UserConfig defines the configuration options for the SPFresh index.
type UserConfig struct {
}

type Config struct {
	Logger                    logrus.FieldLogger
	Distancer                 distancer.Provider
	RootPath                  string
	ID                        string
	TargetVector              string
	MaxPostingSize            uint32  `json:"maxPostingSize,omitempty"`            // Maximum number of vectors in a posting
	MinPostingSize            uint32  `json:"minPostingSize,omitempty"`            // Minimum number of vectors in a posting
	SplitWorkers              int     `json:"splitWorkers,omitempty"`              // Number of concurrent workers for split operations
	ReassignWorkers           int     `json:"reassignWorkers,omitempty"`           // Number of concurrent workers for reassign operations
	InternalPostingCandidates int     `json:"internalPostingCandidates,omitempty"` // Number of candidates to consider when running a centroid search internally
	ReassignNeighbors         int     `json:"reassignNeighbors,omitempty"`         // Number of neighboring centroids to consider for reassigning vectors
	Replicas                  int     `json:"replicas,omitempty"`                  // Number of closure replicas to maintain
	RNGFactor                 float32 `json:"rngFactor,omitempty"`                 // Distance factor used by the RNG rule to determine how spread out replica selections are
	MaxDistanceRatio          float32 `json:"maxDistanceRatio,omitempty"`          // Maximum distance ratio for the search, used to filter out candidates that are too far away
	PruningStrategy           string  `json:"pruningStrategy,omitempty"`           // Strategy to use for pruning postings during brute force
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
	return &Config{
		Logger:                    logrus.New(),
		Distancer:                 distancer.NewL2SquaredProvider(),
		MaxPostingSize:            128,
		MinPostingSize:            10,
		SplitWorkers:              64,
		ReassignWorkers:           128,
		InternalPostingCandidates: 64,
		ReassignNeighbors:         64,
		Replicas:                  8,
		RNGFactor:                 3,
		MaxDistanceRatio:          10_000,
		PruningStrategy:           SizeBasedPruningStrategy,
	}
}
