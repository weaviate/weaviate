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

// HighlightParams holds the user-supplied configuration for the
// _additional { highlight { ... } } query argument.
type HighlightParams struct {
	// Properties lists the text/string properties to highlight.
	// An empty slice means "all string properties in the result".
	Properties []string

	// FragmentSize is the approximate byte length of each returned snippet.
	// Defaults to 100 when unset (zero value).
	FragmentSize int

	// NumberOfFragments caps how many snippets are returned per property.
	// Defaults to 3 when unset (zero value).
	NumberOfFragments int

	// Prefix is prepended to every matched term in a snippet (e.g. "<em>").
	Prefix string

	// Postfix is appended to every matched term in a snippet (e.g. "</em>").
	Postfix string
}

// HighlightResult is the per-property payload returned in
// _additional { highlight { field snippets } }.
type HighlightResult struct {
	Field    string   `json:"field"`
	Snippets []string `json:"snippets"`
}
