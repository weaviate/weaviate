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

// BRQData holds the serialization data for Binary Rotational Quantization compression.
type BRQData struct {
	InputDim uint32
	Rotation FastRotation
	Rounding []float32
	// Mean is the centering mean (centered 1-bit RQ); empty for the
	// uncentered quantizer. Persisted via the AddBRQCentered record.
	Mean []float32
}
