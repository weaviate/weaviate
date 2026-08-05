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

package hnsw

import (
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// Type aliases to compression package for backward compatibility.
// All compression-related types are now defined in entities/vectorindex/compression.

// Encoder is an alias for compression.Encoder.
type Encoder = compression.Encoder

// Encoder constants.
const (
	UseTileEncoder   = compression.UseTileEncoder
	UseKMeansEncoder = compression.UseKMeansEncoder
)

// PQSegmentEncoder is an alias for compression.PQSegmentEncoder.
type PQSegmentEncoder = compression.PQSegmentEncoder

// PQData is an alias for compression.PQData.
type PQData = compression.PQData

// SQData is an alias for compression.SQData.
type SQData = compression.SQData

// Swap is an alias for compression.Swap.
type Swap = compression.Swap

// FastRotation is an alias for compression.FastRotation.
type FastRotation = compression.FastRotation

// RQData is an alias for compression.RQData.
type RQData = compression.RQData

// BRQData is an alias for compression.BRQData.
type BRQData = compression.BRQData

// MuveraData is an alias for compression.MuveraData.
type MuveraData = compression.MuveraData
