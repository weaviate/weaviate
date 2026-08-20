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

// ValidateBQCompatibility rejects binary quantization (BQ) combined with a
// distance metric whose vectors it cannot represent. BQ encodes each dimension
// by its sign (a bit is set only when the value is < 0). Hamming vectors are
// 0/1, so every value is non-negative and all vectors collapse to the same
// all-zero code; compressed Top-k then becomes tie/order dependent and can drop
// the true nearest neighbour. See
// https://github.com/weaviate/weaviate/issues/12035.
func ValidateBQCompatibility(distance string, bqEnabled bool) error {
	if bqEnabled && distance == DistanceHamming {
		return errors.Errorf("binary quantization (bq) is not compatible with the %q "+
			"distance metric: BQ encodes each dimension by its sign, but hamming vectors are "+
			"non-negative, so all vectors would collapse to the same code", DistanceHamming)
	}
	return nil
}

// Tries to parse the int value from the map, if it overflows math.MaxInt64, it
// uses math.MaxInt64 instead. This is to protect from rounding errors from
// json marshalling where the type may be assumed as float64
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

func OptionalBoolFromMap(in map[string]interface{}, name string,
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

func OptionalStringFromMap(in map[string]interface{}, name string,
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
