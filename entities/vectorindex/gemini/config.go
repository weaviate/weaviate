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

package gemini

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

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
	DefaultCleanupIntervalSeconds = 5 * 60
	DefaultMaxConnections         = 64
	DefaultEFConstruction         = 128
	DefaultEF                     = -1 // indicates "let Weaviate pick"
	DefaultDynamicEFMin           = 100
	DefaultDynamicEFMax           = 500
	DefaultDynamicEFFactor        = 8
	DefaultVectorCacheMaxObjects  = 1e12
	DefaultSkip                   = false
	//GW DefaultSkip                   = true
	//GW
	DefaultFlatSearchCutoff       = 40000
	DefaultDistanceMetric         = DistanceCosine
)

// UserConfig bundles all values settable by a user in the per-class settings
type UserConfig struct {
	Skip                   bool   `json:"skip"`
	CleanupIntervalSeconds int    `json:"cleanupIntervalSeconds"`
	MaxConnections         int    `json:"maxConnections"`
	EFConstruction         int    `json:"efConstruction"`
	EF                     int    `json:"ef"`
	DynamicEFMin           int    `json:"dynamicEfMin"`
	DynamicEFMax           int    `json:"dynamicEfMax"`
	DynamicEFFactor        int    `json:"dynamicEfFactor"`
	VectorCacheMaxObjects  int    `json:"vectorCacheMaxObjects"`
	FlatSearchCutoff       int    `json:"flatSearchCutoff"`
	Distance               string `json:"distance"`
//GW

//GW
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "gemini"
}

// SetDefaults in the user-specifyable part of the config
func (c *UserConfig) SetDefaults() {
	c.MaxConnections = DefaultMaxConnections
	c.EFConstruction = DefaultEFConstruction
	c.CleanupIntervalSeconds = DefaultCleanupIntervalSeconds
	c.VectorCacheMaxObjects = DefaultVectorCacheMaxObjects
	c.EF = DefaultEF
	c.DynamicEFFactor = DefaultDynamicEFFactor
	c.DynamicEFMax = DefaultDynamicEFMax
	c.DynamicEFMin = DefaultDynamicEFMin
	c.Skip = DefaultSkip
	c.FlatSearchCutoff = DefaultFlatSearchCutoff
	c.Distance = DefaultDistanceMetric
}

// ParseUserConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseUserConfig(input interface{}) (schema.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	//GW
        fmt.Println("Gemini Parse User Config 1!")
        //GW

	if input == nil {
		return uc, nil
	}

	//GW
        fmt.Println("Gemini Parse User Config 2!")
        //GW

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := optionalIntFromMap(asMap, "maxConnections", func(v int) {
		uc.MaxConnections = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "cleanupIntervalSeconds", func(v int) {
		uc.CleanupIntervalSeconds = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "efConstruction", func(v int) {
		uc.EFConstruction = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "ef", func(v int) {
		uc.EF = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "dynamicEfFactor", func(v int) {
		uc.DynamicEFFactor = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "dynamicEfMax", func(v int) {
		uc.DynamicEFMax = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "dynamicEfMin", func(v int) {
		uc.DynamicEFMin = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.VectorCacheMaxObjects = v
	}); err != nil {
		return uc, err
	}

	if err := optionalIntFromMap(asMap, "flatSearchCutoff", func(v int) {
		uc.FlatSearchCutoff = v
	}); err != nil {
		return uc, err
	}

	//GW
        fmt.Println("SKIP BEFORE entities/vectorindex/gemini/config.go !")
        //GW

	if err := optionalBoolFromMap(asMap, "skip", func(v bool) {
		uc.Skip = v
	}); err != nil {
		return uc, err
	}

	if err := optionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	return uc, nil
}

// Tries to parse the int value from the map, if it overflows math.MaxInt64, it
// uses math.MaxInt64 instead. This is to protect from rounding errors from
// json marshalling where the type may be assumed as float64
func optionalIntFromMap(in map[string]interface{}, name string,
	setFn func(v int),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	var asInt64 int64
	var err error

	// depending on whether we get the results from disk or from the REST API,
	// numbers may be represented slightly differently
	switch typed := value.(type) {
	case json.Number:
		asInt64, err = typed.Int64()
	case float64:
		asInt64 = int64(typed)
	}
	if err != nil {
		// try to recover from error
		if errors.Is(err, strconv.ErrRange) {
			setFn(int(math.MaxInt64))
			return nil
		}

		return errors.Wrapf(err, "json.Number to int64 for %q", name)
	}

	setFn(int(asInt64))
	return nil
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
		return nil
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
		return nil
	}

	setFn(asString)
	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
