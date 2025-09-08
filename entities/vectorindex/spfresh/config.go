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

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorindexcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultMaxPostingSize            = 100
	DefaultMinPostingSize            = 10
	DefaultSplitWorkers              = 5
	DefaultReassignWorkers           = 5
	DefaultInternalPostingCandidates = 4
	DefaultReassignNeighbors         = 4
	DefaultReplicas                  = 1
	DefaultRNGFactor                 = 1.5
	DefaultMaxDistanceRatio          = 1.5
)

type UserConfig struct {
	// public
	Distance string `json:"distance"`
	// TODO make these private when spfresh goes GA
	MaxPostingSize            uint32  `json:"maxPostingSize,omitempty"`            // Maximum number of vectors in a posting
	MinPostingSize            uint32  `json:"minPostingSize,omitempty"`            // Minimum number of vectors in a posting
	SplitWorkers              int     `json:"splitWorkers,omitempty"`              // Number of concurrent workers for split operations
	ReassignWorkers           int     `json:"reassignWorkers,omitempty"`           // Number of concurrent workers for reassign operations
	InternalPostingCandidates int     `json:"internalPostingCandidates,omitempty"` // Number of candidates to consider when running a centroid search internally
	ReassignNeighbors         int     `json:"reassignNeighbors,omitempty"`         // Number of neighboring centroids to consider for reassigning vectors
	Replicas                  int     `json:"replicas,omitempty"`                  // Number of closure replicas to maintain
	RNGFactor                 float32 `json:"rngFactor,omitempty"`                 // Distance factor used by the RNG rule to determine how spread out replica selections are
	MaxDistanceRatio          float32 `json:"maxDistanceRatio,omitempty"`          // Maximum distance ratio for the search, used to filter out candidates that are too far away
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "spfresh"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.Distance = vectorindexcommon.DefaultDistanceMetric
	u.MaxPostingSize = DefaultMaxPostingSize
	u.MinPostingSize = DefaultMinPostingSize
	u.SplitWorkers = DefaultSplitWorkers
	u.ReassignWorkers = DefaultReassignWorkers
	u.InternalPostingCandidates = DefaultInternalPostingCandidates
	u.ReassignNeighbors = DefaultReassignNeighbors
	u.Replicas = DefaultReplicas
	u.RNGFactor = DefaultRNGFactor
	u.MaxDistanceRatio = DefaultMaxDistanceRatio
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}) (schemaConfig.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := vectorindexcommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := validateCompression(asMap); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "maxPostingSize", func(v int) {
		uc.MaxPostingSize = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "minPostingSize", func(v int) {
		uc.MinPostingSize = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "splitWorkers", func(v int) {
		uc.SplitWorkers = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "reassignWorkers", func(v int) {
		uc.ReassignWorkers = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "internalPostingCandidates", func(v int) {
		uc.InternalPostingCandidates = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "reassignNeighbors", func(v int) {
		uc.ReassignNeighbors = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "replicas", func(v int) {
		uc.Replicas = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalFloat32FromMap(asMap, "rngFactor", func(v float32) {
		uc.RNGFactor = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalFloat32FromMap(asMap, "maxDistanceRatio", func(v float32) {
		uc.MaxDistanceRatio = v
	}); err != nil {
		return uc, err
	}

	return uc, nil
}

func validateCompression(in map[string]interface{}) error {
	_, pqOk := in["pq"]
	_, bqOk := in["bq"]
	_, sqOk := in["sq"]
	_, rqOk := in["rq"]

	if pqOk || bqOk || sqOk || rqOk {
		return fmt.Errorf("BQ, PQ, RQ, and SQ are not currently supported for spfresh indices")
	}

	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
