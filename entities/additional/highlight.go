//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright (c) 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package additional

const (
	DefaultHighlightFragmentCount = 1
	DefaultHighlightFragmentSize  = 160
	DefaultHighlightPreTag        = "<em>"
	DefaultHighlightPostTag       = "</em>"
)

type HighlightProperties struct {
	Properties    []string `json:"properties,omitempty"`
	FragmentCount int      `json:"fragmentCount"`
	FragmentSize  int      `json:"fragmentSize"`
	PreTag        string   `json:"preTag"`
	PostTag       string   `json:"postTag"`
}

func DefaultHighlightProperties() *HighlightProperties {
	return &HighlightProperties{
		FragmentCount: DefaultHighlightFragmentCount,
		FragmentSize:  DefaultHighlightFragmentSize,
		PreTag:        DefaultHighlightPreTag,
		PostTag:       DefaultHighlightPostTag,
	}
}

type Highlight struct {
	Property  string   `json:"property"`
	Fragments []string `json:"fragments"`
}
