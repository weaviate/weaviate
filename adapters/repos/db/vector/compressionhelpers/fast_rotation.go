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

// Package compressionhelpers provides vector compression algorithms for HNSW indexes.
// Types like FastRotation, PQData, SQData, RQData, and BRQData are defined in
// entities/vectorindex/compression and should be imported from there.
package compressionhelpers

import (
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// NewFastRotation creates a new FastRotation with the given parameters.
var NewFastRotation = compression.NewFastRotation

// RestoreFastRotation creates a FastRotation from persisted data.
var RestoreFastRotation = compression.RestoreFastRotation

// DefaultFastRotationSeed is the default seed for fast rotation.
const DefaultFastRotationSeed = compression.DefaultFastRotationSeed
