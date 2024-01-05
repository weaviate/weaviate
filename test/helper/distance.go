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

package helper

import (
	"testing"

	"github.com/weaviate/weaviate/entities/additional"
)

func CertaintyToDist(t *testing.T, in float32) float32 {
	asFloat64 := float64(in)
	dist := additional.CertaintyToDistPtr(&asFloat64)
	if dist == nil {
		t.Fatalf(
			"somehow %+v of type %T failed to produce a non-null *float64", in, in)
	}
	return float32(*dist)
}
