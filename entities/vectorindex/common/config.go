//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"encoding/json"
	"math"
	"strconv"

	"github.com/pkg/errors"
)

const (
	DistanceCosine    = "cosine"
	DistanceDot       = "dot"
	DistanceL2Squared = "l2-squared"
	DistanceManhattan = "manhattan"
	DistanceHamming   = "hamming"

	// Set these defaults if the user leaves them blank
	DefaultVectorCacheMaxObjects = 1e12
	DefaultDistanceMetric        = DistanceCosine
)

const (
	CompressionBQ = "bq"
	CompressionPQ = "pq"
	CompressionSQ = "sq"
	CompressionRQ = "rq"
	NoCompression = "none"
)

// Tries to parse the int value from the map, if it overflows math.MaxInt64, it
// uses math.MaxInt64 instead. This is to protect from rounding errors from
// json marshalling where the type may be assumed as float64
//
// If the key is present with a value that is neither a json.Number nor a
// float64 (including an explicit JSON `null`), that is a caller error and is
// reported rather than silently defaulting the field to 0.
func OptionalIntFromMap(in map[string]interface{}, name string,
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
	default:
		return errors.Errorf("%q must be an integer, got %T", name, value)
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

// If the key is present with a value that is not a bool (including an
// explicit JSON `null`), that is a caller error and is reported rather than
// silently leaving the field at its current value.
func OptionalBoolFromMap(in map[string]interface{}, name string,
	setFn func(v bool),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asBool, ok := value.(bool)
	if !ok {
		return errors.Errorf("%q must be a boolean, got %T", name, value)
	}

	setFn(asBool)
	return nil
}

// If the key is present with a value that is not a string (including an
// explicit JSON `null`), that is a caller error and is reported rather than
// silently leaving the field at its current value. This is what previously
// let `"distance": null` in a vectorIndexConfig be silently accepted and
// resolved to the default distance metric instead of being rejected.
func OptionalStringFromMap(in map[string]interface{}, name string,
	setFn func(v string),
) error {
	value, ok := in[name]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return errors.Errorf("%q must be a string, got %T", name, value)
	}

	setFn(asString)
	return nil
}
