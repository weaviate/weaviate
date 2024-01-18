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

package sharding

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

const (
	DefaultVirtualPerPhysical = 128
	DefaultKey                = "_id"
	DefaultStrategy           = "hash"
	DefaultFunction           = "murmur3"
)

type Config struct {
	VirtualPerPhysical  int    `json:"virtualPerPhysical"`
	DesiredCount        int    `json:"desiredCount"`
	ActualCount         int    `json:"actualCount"`
	DesiredVirtualCount int    `json:"desiredVirtualCount"`
	ActualVirtualCount  int    `json:"actualVirtualCount"`
	Key                 string `json:"key"`
	Strategy            string `json:"strategy"`
	Function            string `json:"function"`
}

func (c *Config) setDefaults(nodeCount int) {
	c.VirtualPerPhysical = DefaultVirtualPerPhysical
	c.DesiredCount = nodeCount
	c.DesiredVirtualCount = c.DesiredCount * c.VirtualPerPhysical
	c.Function = DefaultFunction
	c.Key = DefaultKey
	c.Strategy = DefaultStrategy

	// these will only differ once there is an async component through replication
	// or dynamic scaling. For now they have to be the same
	c.ActualCount = c.DesiredCount
	c.ActualVirtualCount = c.DesiredVirtualCount
}

func (c *Config) validate() error {
	if c.Key != "_id" {
		return errors.Errorf("sharding only supported on key '_id' for now, "+
			"got: %s", c.Key)
	}

	if c.Strategy != "hash" {
		return errors.Errorf("sharding only supported with strategy 'hash' for now, "+
			"got: %s", c.Strategy)
	}

	if c.Function != "murmur3" {
		return errors.Errorf("sharding only supported with function 'murmur3' for now, "+
			"got: %s", c.Function)
	}

	return nil
}

func ParseConfig(input interface{}, nodeCount int) (Config, error) {
	out := Config{}
	out.setDefaults(nodeCount)

	if input == nil {
		return out, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return out, fmt.Errorf("input must be a non-nil map")
	}

	if err := optionalIntFromMap(asMap, "virtualPerPhysical", func(v int) {
		out.VirtualPerPhysical = v
	}); err != nil {
		return out, err
	}

	if err := optionalIntFromMap(asMap, "desiredCount", func(v int) {
		out.DesiredCount = v
	}); err != nil {
		return out, err
	}

	out.DesiredVirtualCount = out.DesiredCount * out.VirtualPerPhysical

	if err := optionalIntFromMap(asMap, "desiredCount", func(v int) {
		out.DesiredCount = v
	}); err != nil {
		return out, err
	}

	if err := optionalStringFromMap(asMap, "key", func(v string) {
		out.Key = v
	}); err != nil {
		return out, err
	}

	if err := optionalStringFromMap(asMap, "strategy", func(v string) {
		out.Strategy = v
	}); err != nil {
		return out, err
	}

	if err := optionalStringFromMap(asMap, "function", func(v string) {
		out.Function = v
	}); err != nil {
		return out, err
	}

	// these will only differ once there is an async component through replication
	// or dynamic scaling. For now they have to be the same
	out.ActualCount = out.DesiredCount
	out.ActualVirtualCount = out.DesiredVirtualCount

	if err := out.validate(); err != nil {
		return out, err
	}

	return out, nil
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
	case int:
		asInt64 = int64(typed)
	case float64:
		asInt64 = int64(typed)
	}
	if err != nil {
		return errors.Wrapf(err, "%s", name)
	}

	setFn(int(asInt64))
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
		return errors.Errorf("field %q must be of type string, got: %T", name, value)
	}

	setFn(asString)
	return nil
}
