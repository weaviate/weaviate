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

package stopwords

const (
	EnglishPreset = "en"
	NoPreset      = "none"
)

var Presets = map[string][]string{
	EnglishPreset: {
		"a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
		"if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that",
		"the", "their", "then", "there", "these", "they", "this", "to", "was", "will",
		"with",
	},
	NoPreset: {},
}
