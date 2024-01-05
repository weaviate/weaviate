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

// GenerateResult used in generative OpenAI module to represent
// the answer to a given question
type GenerateResult struct {
	SingleResult  *string `json:"singleResult,omitempty"`
	GroupedResult *string `json:"groupedResult,omitempty"`
	Error         error   `json:"error,omitempty"`
}

type GenerateResponse struct {
	Result *string
}
