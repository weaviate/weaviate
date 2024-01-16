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

package modulecapabilities

// TextTransform performs text transformation operation
type TextTransform interface {
	Transform(in []string) ([]string, error)
}

// TextTransformers defines all text transformers
// for given arguments
type TextTransformers interface {
	TextTransformers() map[string]TextTransform
}
