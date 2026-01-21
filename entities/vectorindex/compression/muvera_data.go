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

package compression

// MuveraData holds the serialization data for Muvera multi-vector encoding.
type MuveraData struct {
	KSim         uint32        // 4 bytes
	NumClusters  uint32        // 4 bytes
	Dimensions   uint32        // 4 bytes
	DProjections uint32        // 4 bytes
	Repetitions  uint32        // 4 bytes
	Gaussians    [][][]float32 // (repetitions, kSim, dimensions)
	S            [][][]float32 // (repetitions, dProjections, dimensions)
}
