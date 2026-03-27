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

package additional

// Highlight represents highlighted text fragments for a single property.
type Highlight struct {
	Property  string   `json:"property"`
	Fragments []string `json:"fragments"`
}

// HighlightConfig holds the user-configurable parameters for highlighting.
type HighlightConfig struct {
	// NumberOfFragments is the max number of fragments to return per property.
	// Defaults to 3.
	NumberOfFragments int `json:"numberOfFragments"`

	// FragmentSize is the approximate character length of each fragment.
	// Defaults to 100.
	FragmentSize int `json:"fragmentSize"`

	// PreTag is the tag inserted before each matching term. Defaults to "<em>".
	PreTag string `json:"preTag"`

	// PostTag is the tag inserted after each matching term. Defaults to "</em>".
	PostTag string `json:"postTag"`
}

// DefaultHighlightConfig returns a HighlightConfig with sensible defaults.
func DefaultHighlightConfig() HighlightConfig {
	return HighlightConfig{
		NumberOfFragments: 3,
		FragmentSize:      100,
		PreTag:            "<em>",
		PostTag:           "</em>",
	}
}
