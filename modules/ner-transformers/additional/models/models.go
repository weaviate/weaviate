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

// Tokens used in NER module to represent
// the found entities in a given string property value
type Token struct {
	Property      string  `json:"property,omitempty"`
	Entity        string  `json:"entity,omitempty"`
	Certainty     float64 `json:"certainty,omitempty"`
	Word          string  `json:"word,omitempty"`
	StartPosition int     `json:"startPosition,omitempty"`
	EndPosition   int     `json:"endPosition,omitempty"`
}
