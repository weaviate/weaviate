package sharding

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

const (
	DefaultVirtualPerPhysical = 128
	DefaultDesiredCount       = 1
	DefaultKey                = "_id"
	DefaultStrategy           = "hash"
	DefaultFunction           = "murmur3"
)

type Config struct {
	VirtualPerPhysical  int
	DesiredCount        int
	ActualCount         int
	DesiredVirtualCount int
	ActualVirtualCount  int
	Key                 string
	Strategy            string
	ShardFunction       string
}

func (c *Config) setDefaults() {
	c.VirtualPerPhysical = DefaultVirtualPerPhysical
	c.DesiredCount = DefaultDesiredCount
	c.DesiredVirtualCount = c.DesiredCount * c.VirtualPerPhysical
	c.ShardFunction = DefaultFunction
	c.Key = DefaultKey
	c.Strategy = DefaultStrategy
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

	if c.ShardFunction != "murmur3" {
		return errors.Errorf("sharding only supported with function 'murmur3' for now, "+
			"got: %s", c.ShardFunction)
	}

	return nil
}

func ParseConfig(input interface{}) (Config, error) {
	out := Config{}
	out.setDefaults()

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
		out.ShardFunction = v
	}); err != nil {
		return out, err
	}

	if err := out.validate(); err != nil {
		return out, err
	}

	return out, nil
}

func optionalIntFromMap(in map[string]interface{}, name string,
	setFn func(v int)) error {
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

func optionalStringFromMap(in map[string]interface{}, name string,
	setFn func(v string)) error {
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
