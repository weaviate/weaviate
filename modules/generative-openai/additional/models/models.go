//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package models

// GenerateResult used in generative OpenAI module to represent
// the answer to a given question
type GenerateResult struct {
	SingleResult  *string `json:"singleResult,omitempty"`
	GroupedResult *string `json:"groupedResult,omitempty"`
	Single        *Result `json:"single,omitempty"`
	Grouped       *Result `json:"grouped,omitempty"`
	Usage         *Usage  `json:"usage,omitempty"`
	Error         error   `json:"error,omitempty"`
}

type Result struct {
	Result *string `json:"result,omitempty"`
	Usage  *Usage  `json:"usage,omitempty"`
}

type Usage struct {
	PromptTokens     int `json:"promptTokens,omitempty"`
	CompletionTokens int `json:"completionTokens,omitempty"`
	TotalTokens      int `json:"totalTokens,omitempty"`
}
