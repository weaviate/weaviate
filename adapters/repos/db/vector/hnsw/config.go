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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/schema"
	ent "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
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

// ParseUserConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseUserConfig(input interface{}) (schema.VectorIndexConfig, error) {
	uc := ent.UserConfig{}
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

func NewDefaultUserConfig() ent.UserConfig {
	uc := ent.UserConfig{}
	uc.SetDefaults()
	return uc
}
