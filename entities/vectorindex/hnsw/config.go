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

package hnsw

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	DistanceCosine    = "cosine"
	DistanceDot       = "dot"
	DistanceL2Squared = "l2-squared"
	DistanceManhattan = "manhattan"
	DistanceHamming   = "hamming"
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
	DefaultVectorCacheMaxObjects  = 1e12
	DefaultSkip                   = false
	DefaultFlatSearchCutoff       = 40000
	DefaultDistanceMetric         = DistanceCosine

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
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "hnsw"
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.MaxConnections = DefaultMaxConnections
	u.EFConstruction = DefaultEFConstruction
	u.CleanupIntervalSeconds = DefaultCleanupIntervalSeconds
	u.VectorCacheMaxObjects = DefaultVectorCacheMaxObjects
	u.EF = DefaultEF
	u.DynamicEFFactor = DefaultDynamicEFFactor
	u.DynamicEFMax = DefaultDynamicEFMax
	u.DynamicEFMin = DefaultDynamicEFMin
	u.Skip = DefaultSkip
	u.FlatSearchCutoff = DefaultFlatSearchCutoff
	u.Distance = DefaultDistanceMetric
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
		return nil, fmt.Errorf("input must be a non-nil map")
	}

	if err := optionalIntFromMap(asMap, "maxConnections", func(v int) {
		uc.MaxConnections = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "cleanupIntervalSeconds", func(v int) {
		uc.CleanupIntervalSeconds = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "efConstruction", func(v int) {
		uc.EFConstruction = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "ef", func(v int) {
		uc.EF = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "dynamicEfFactor", func(v int) {
		uc.DynamicEFFactor = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "dynamicEfMax", func(v int) {
		uc.DynamicEFMax = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "dynamicEfMin", func(v int) {
		uc.DynamicEFMin = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.VectorCacheMaxObjects = v
	}); err != nil {
		return nil, err
	}

	if err := optionalIntFromMap(asMap, "flatSearchCutoff", func(v int) {
		uc.FlatSearchCutoff = v
	}); err != nil {
		return nil, err
	}

	if err := optionalBoolFromMap(asMap, "skip", func(v bool) {
		uc.Skip = v
	}); err != nil {
		return nil, err
	}

	if err := optionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return nil, err
	}

	if err := parsePQMap(asMap, &uc.PQ); err != nil {
		return nil, err
	}

	return uc, uc.validate()
}

func ValidateDefaultVectorDistanceMetric(distance string) error {
	switch distance {
	case "", DistanceCosine, DistanceDot, DistanceL2Squared, DistanceManhattan, DistanceHamming:
		return nil
	default:
		return fmt.Errorf("must be one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]")
	}
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

	err := ValidateDefaultVectorDistanceMetric(u.Distance)
	if err != nil {
		errMsgs = append(errMsgs, fmt.Sprintf("distance %v", err))
	}

	if len(errMsgs) > 0 {
		return fmt.Errorf("invalid hnsw config: %s",
			strings.Join(errMsgs, ", "))
	}

	return nil
}

// Tries to parse an int value from a map.
func optionalIntFromMap(in map[string]interface{}, name string,
	setFn func(v int),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	// depending on whether we get the results from disk or from the REST API,
	// numbers may be represented slightly differently
	switch typed := value.(type) {
	case int:
		setFn(typed)
		return nil

	case json.Number:
		asInt64, err := typed.Int64()
		if err != nil {
			return errors.Wrapf(err, "json.Number to int64 for %q", name)
		}
		setFn(int(asInt64))
		return nil

	case float64:
		setFn(int(typed))
		return nil

	default:
		return fmt.Errorf("%s is of the wrong type. Should be of type int, json.Number or float64", name)
	}
}

func optionalBoolFromMap(in map[string]interface{}, name string,
	setFn func(v bool),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asBool, ok := value.(bool)
	if !ok {
		return fmt.Errorf("%s is of the wrong type. Should be of type bool", name)
	}

	setFn(asBool)
	return nil
}

func optionalStringFromMap(in map[string]interface{}, name string,
	setFn func(v string),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return fmt.Errorf("%s is of the wrong type. Should be of type string", name)
	}

	setFn(asString)
	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
