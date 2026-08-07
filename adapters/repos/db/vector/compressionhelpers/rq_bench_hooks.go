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

package compressionhelpers

import "slices"

// benchRQCenteringMean is a bench-only side channel for the RQ centering
// mean (see RQOptions.Mean). The dataset mean is a training artifact with no
// home in the user-facing config surface, and threading it through the index
// configuration for a benchmark would put an experimental knob on a
// production API. Nothing in production code sets it; when unset (the
// default) all quantizers behave exactly as before.
//
// Not synchronized: set it once before building indexes, from a single
// goroutine.
var benchRQCenteringMean []float32

// SetBenchRQCenteringMean sets the dataset mean applied by NewRQCompressor
// to newly created rotational quantizers. Pass nil to disable.
func SetBenchRQCenteringMean(mean []float32) {
	benchRQCenteringMean = slices.Clone(mean)
}

// BenchRQCenteringMean returns the currently configured bench centering
// mean, or nil.
func BenchRQCenteringMean() []float32 {
	return benchRQCenteringMean
}
