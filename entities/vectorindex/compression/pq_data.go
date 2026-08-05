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

// Encoder constants for PQ encoder types.
const (
	PQEncoderTypeTile              = "tile"
	PQEncoderTypeKMeans            = "kmeans"
	PQEncoderDistributionLogNormal = "log-normal"
	PQEncoderDistributionNormal    = "normal"
)

// Encoder represents the type of encoder used for product quantization.
type Encoder byte

const (
	UseTileEncoder   Encoder = 0
	UseKMeansEncoder Encoder = 1
)

// PQSegmentEncoder is the interface for product quantization segment encoders.
// Implementations include TileEncoder and KMeansEncoder.
type PQSegmentEncoder interface {
	Encode(x []float32) byte
	Centroid(b byte) []float32
	Add(x []float32)
	Fit(data [][]float32) error
	ExposeDataForRestore() []byte
}

// PQData holds the serialization data for Product Quantization compression.
type PQData struct {
	Ks                  uint16
	M                   uint16
	Dimensions          uint16
	EncoderType         Encoder
	EncoderDistribution byte
	Encoders            []PQSegmentEncoder
	UseBitsEncoding     bool
	TrainingLimit       int
}
