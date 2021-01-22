//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
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

	// // TODO: remove
	// // from user config
	// MaximumConnections          int
	// MaximumConnectionsLevelZero int
	// EFConstruction              int
	// VectorCacheMaxObjects       int
}

func (c Config) Validate() error {
	ec := &errorCompounder{}

	if c.ID == "" {
		ec.addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.addf("rootPath cannot be empty")
	}

	// if c.MaximumConnections <= 0 {
	// 	ec.addf("maximumConnections must be greater than 0")
	// }

	// if c.EFConstruction <= 0 {
	// 	ec.addf("efConstruction must be greater than 0")
	// }

	if c.MakeCommitLoggerThunk == nil {
		ec.addf("makeCommitLoggerThunk cannot be nil")
	}

	if c.VectorForIDThunk == nil {
		ec.addf("vectorForIDThunk cannot be nil")
	}

	if c.DistanceProvider == nil {
		ec.addf("distancerProvider cannot be nil")
	}

	return ec.toError()
}

type errorCompounder struct {
	errors []error
}

func (ec *errorCompounder) addf(msg string, args ...interface{}) {
	ec.errors = append(ec.errors, fmt.Errorf(msg, args...))
}

func (ec *errorCompounder) add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *errorCompounder) toError() error {
	if len(ec.errors) == 0 {
		return nil
	}

	var msg strings.Builder
	for i, err := range ec.errors {
		if i != 0 {
			msg.WriteString(", ")
		}

		msg.WriteString(err.Error())
	}

	return errors.New(msg.String())
}

const (
	DefaultCleanupIntervalSeconds = 5 * 60
	DefaultMaxConnections         = 64
	DefaultEFConstruction         = 128
	DefaultVectorCacheMaxObjects  = 500000
)

// UserConfig bundles all values settable by a user in the per-class settings
type UserConfig struct {
	CleanupIntervalSeconds int `json:"cleanupIntervalSeconds"`
	MaxConnections         int `json:"maxConnections"`
	EFConstruction         int `json:"efConstruction"`
	VectorCacheMaxObjects  int `json:"vectorCacheMaxObjects"`
}

// SetDefaults in the user-specifyable part of the config
func (c *UserConfig) SetDefaults() {
	c.MaxConnections = DefaultMaxConnections
	c.EFConstruction = DefaultEFConstruction
	c.CleanupIntervalSeconds = DefaultCleanupIntervalSeconds
	c.VectorCacheMaxObjects = DefaultVectorCacheMaxObjects
}

// ParseUserConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseUserConfig(input interface{}) (UserConfig, error) {
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

	if err := optionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.VectorCacheMaxObjects = v
	}); err != nil {
		return uc, err
	}

	return uc, nil
}

func optionalIntFromMap(in map[string]interface{}, name string,
	setFn func(v int)) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asNumber, ok := value.(json.Number)
	if !ok {
		return fmt.Errorf("%s: must be a number, got: %T", name, value)
	}

	asInt64, err := asNumber.Int64()
	if err != nil {
		return errors.Wrap(err, "maxConnections")
	}

	setFn(int(asInt64))
	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
