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

package lsmkv

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// preComputeSegmentMeta has no side-effects for an already running store. As a
// result this can be run without the need to obtain any locks. All files
// created will have a .tmp suffix so they don't interfere with existing
// segments that might have a similar name.
//
// This function is a partial copy of what happens in newSegment(). Any changes
// made here should likely be made in newSegment, and vice versa. This is
// absolutely not ideal, but in the short time I was able to consider this, I wasn't
// able to find a way to unify the two -- there are subtle differences.
func preComputeSegmentMeta(path string, updatedCountNetAdditions int,
	logger logrus.FieldLogger, useBloomFilter bool, calcCountNetAdditions bool,
	enableChecksumValidation bool, minMMapSize int64, allocChecker memwatch.AllocChecker,
) ([]string, error) {
	out := []string{path}
	return out, nil
}
