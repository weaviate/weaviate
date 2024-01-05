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

package models

// SpellCheckChange describes the misspellings
type SpellCheckChange struct {
	Original  string `json:"original,omitempty"`
	Corrected string `json:"corrected,omitempty"`
}

// SpellCheck presents proper text without misspellings
// and the list of words that were misspelled
type SpellCheck struct {
	OriginalText        string             `json:"originalText,omitempty"`
	DidYouMean          string             `json:"didYouMean,omitempty"`
	Location            string             `json:"location,omitempty"`
	NumberOfCorrections int                `json:"numberOfCorrections,omitempty"`
	Changes             []SpellCheckChange `json:"changes,omitempty"`
}
