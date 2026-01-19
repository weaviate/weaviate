//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// Swap is an alias for compression.Swap for backward compatibility.
type Swap = compression.Swap

// FastRotation is an alias for compression.FastRotation for backward compatibility.
type FastRotation = compression.FastRotation

// DefaultFastRotationSeed is the default seed for fast rotation.
const DefaultFastRotationSeed = compression.DefaultFastRotationSeed

// NewFastRotation creates a new FastRotation with the given parameters.
var NewFastRotation = compression.NewFastRotation

// RestoreFastRotation creates a FastRotation from persisted data.
var RestoreFastRotation = compression.RestoreFastRotation
