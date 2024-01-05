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

package hnsw

import (
	"fmt"
	"strings"

	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"

	"github.com/weaviate/weaviate/entities/schema"
)

const (
	// Set these defaults if the user leaves them blank
	DefaultCleanupIntervalSeconds = 5 * 60
	DefaultMaxConnections         = 64
	DefaultEFConstruction         = 128
	DefaultEF                     = -1 // indicates "let Weaviate pick"
	DefaultDynamicEFMin           = 100
	DefaultDynamicEFMax           = 500
	DefaultDynamicEFFactor        = 8
	DefaultSkip                   = false
	DefaultFlatSearchCutoff       = 40000

	// Fail validation if those criteria are not met
	MinmumMaxConnections = 4
	MinmumEFConstruction = 4
)

// UserConfig bundles all values settable by a user in the per-class settings
type UserConfig struct {
	Skip                   bool     `json:"skip"`
	CleanupIntervalSeconds int      `json:"cleanupIntervalSeconds"`
	MaxConnections         int      `json:"maxConnections"`
	EFConstruction         int      `json:"efConstruction"`
	EF                     int      `json:"ef"`
	DynamicEFMin           int      `json:"dynamicEfMin"`
	DynamicEFMax           int      `json:"dynamicEfMax"`
	DynamicEFFactor        int      `json:"dynamicEfFactor"`
	VectorCacheMaxObjects  int      `json:"vectorCacheMaxObjects"`
	FlatSearchCutoff       int      `json:"flatSearchCutoff"`
	Distance               string   `json:"distance"`
	PQ                     PQConfig `json:"pq"`
	BQ                     BQConfig `json:"bq"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "hnsw"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.MaxConnections = DefaultMaxConnections
	u.EFConstruction = DefaultEFConstruction
	u.CleanupIntervalSeconds = DefaultCleanupIntervalSeconds
	u.VectorCacheMaxObjects = vectorIndexCommon.DefaultVectorCacheMaxObjects
	u.EF = DefaultEF
	u.DynamicEFFactor = DefaultDynamicEFFactor
	u.DynamicEFMax = DefaultDynamicEFMax
	u.DynamicEFMin = DefaultDynamicEFMin
	u.Skip = DefaultSkip
	u.FlatSearchCutoff = DefaultFlatSearchCutoff
	u.Distance = vectorIndexCommon.DefaultDistanceMetric
	u.PQ = PQConfig{
		Enabled:        DefaultPQEnabled,
		BitCompression: DefaultPQBitCompression,
		Segments:       DefaultPQSegments,
		Centroids:      DefaultPQCentroids,
		TrainingLimit:  DefaultPQTrainingLimit,
		Encoder: PQEncoder{
			Type:         DefaultPQEncoderType,
			Distribution: DefaultPQEncoderDistribution,
		},
	}
	u.BQ = BQConfig{
		Enabled: DefaultBQEnabled,
	}
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}) (schema.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "maxConnections", func(v int) {
		uc.MaxConnections = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "cleanupIntervalSeconds", func(v int) {
		uc.CleanupIntervalSeconds = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "efConstruction", func(v int) {
		uc.EFConstruction = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "ef", func(v int) {
		uc.EF = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "dynamicEfFactor", func(v int) {
		uc.DynamicEFFactor = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "dynamicEfMax", func(v int) {
		uc.DynamicEFMax = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "dynamicEfMin", func(v int) {
		uc.DynamicEFMin = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.VectorCacheMaxObjects = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "flatSearchCutoff", func(v int) {
		uc.FlatSearchCutoff = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalBoolFromMap(asMap, "skip", func(v bool) {
		uc.Skip = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := parsePQMap(asMap, &uc.PQ); err != nil {
		return uc, err
	}

	if err := parseBQMap(asMap, &uc.BQ); err != nil {
		return uc, err
	}

	return uc, uc.validate()
}

func (u *UserConfig) validate() error {
	var errMsgs []string
	if u.MaxConnections < MinmumMaxConnections {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"maxConnections must be a positive integer with a minimum of %d",
			MinmumMaxConnections,
		))
	}

	if u.EFConstruction < MinmumEFConstruction {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"efConstruction must be a positive integer with a minimum of %d",
			MinmumMaxConnections,
		))
	}

	if len(errMsgs) > 0 {
		return fmt.Errorf("invalid hnsw config: %s",
			strings.Join(errMsgs, ", "))
	}

	if u.PQ.Enabled && u.BQ.Enabled {
		return fmt.Errorf("invalid hnsw config: two compression methods enabled: PQ and BQ")
	}

	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
