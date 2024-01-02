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

// Answer used in qna module to represent
// the answer to a given question
type Answer struct {
	Result        *string  `json:"result,omitempty"`
	Property      *string  `json:"property,omitempty"`
	StartPosition int      `json:"startPosition,omitempty"`
	EndPosition   int      `json:"endPosition,omitempty"`
	Certainty     *float64 `json:"certainty,omitempty"`
	Distance      *float64 `json:"distance,omitempty"`
	HasAnswer     bool     `json:"hasAnswer,omitempty"`
}
