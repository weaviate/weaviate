//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/sirupsen/logrus"
)

// Config for a new HSNW index, this contains information that is derived
// internally, e.g. by the shard. All User-settable config is specified in
// Config.UserConfig
type Config struct {
	// internal
	RootPath              string
	ID                    string
	MakeCommitLoggerThunk MakeCommitLogger
	VectorForIDThunk      VectorForID
	Logger                logrus.FieldLogger
	DistanceProvider      distancer.Provider
	PrometheusMetrics     *monitoring.PrometheusMetrics

	// metadata for monitoring
	ShardName string
	ClassName string
}

func (c Config) Validate() error {
	ec := &errorcompounder.ErrorCompounder{}

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.Addf("rootPath cannot be empty")
	}

	if c.MakeCommitLoggerThunk == nil {
		ec.Addf("makeCommitLoggerThunk cannot be nil")
	}

	if c.VectorForIDThunk == nil {
		ec.Addf("vectorForIDThunk cannot be nil")
	}

	if c.DistanceProvider == nil {
		ec.Addf("distancerProvider cannot be nil")
	}

	return ec.ToError()
}

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
	DefaultVectorCacheMaxObjects  = math.MaxInt64
	DefaultSkip                   = false
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
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "hnsw"
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

	if input == nil {
		return uc, nil
	}

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
		return errors.Wrapf(err, "%s", name)
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
