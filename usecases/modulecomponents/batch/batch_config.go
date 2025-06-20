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

package batch

import "github.com/weaviate/weaviate/entities/moduletools"

type Settings struct {
	TokenMultiplier    float32
	MaxTimePerBatch    float64
	MaxObjectsPerBatch int
	MaxTokensPerBatch  func(cfg moduletools.ClassConfig) int
	HasTokenLimit      bool
	ReturnsRateLimit   bool
}
