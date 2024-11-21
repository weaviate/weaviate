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

package modulecomponents

import "time"

type RateLimits struct {
	LastOverwrite        time.Time
	AfterRequestFunction func(limits *RateLimits, tokensUsed int, deductRequest bool)
	LimitRequests        int
	LimitTokens          int
	RemainingRequests    int
	RemainingTokens      int
	ReservedRequests     int
	ReservedTokens       int
	ResetRequests        time.Time
	ResetTokens          time.Time
}

func (rl *RateLimits) ResetAfterRequestFunction(tokensUsed int) {
	if rl.AfterRequestFunction != nil {
		rl.AfterRequestFunction(rl, tokensUsed, true)
	}
}

func (rl *RateLimits) CheckForReset() {
	if rl.AfterRequestFunction != nil {
		rl.AfterRequestFunction(rl, 0, false)
	}
}

func (rl *RateLimits) CanSendFullBatch(numRequests int, batchTokens int) bool {
	freeRequests := rl.RemainingRequests - rl.ReservedRequests
	freeTokens := rl.RemainingTokens - rl.ReservedTokens

	fitsCurrentBatch := freeRequests >= numRequests && freeTokens >= batchTokens
	if !fitsCurrentBatch {
		return false
	}

	// also make sure that we do not "spend" all the rate limit at once
	var percentageOfRequests, percentageOfTokens int
	if rl.LimitRequests > 0 {
		percentageOfRequests = numRequests * 100 / rl.LimitRequests
	}
	if rl.LimitTokens > 0 {
		percentageOfTokens = batchTokens * 100 / rl.LimitTokens
	}

	// the clients aim for 10s per batch, or 6 batches per minute in sequential-mode. 15% is somewhat below that to
	// account for some variance in the rate limits
	return percentageOfRequests <= 15 && percentageOfTokens <= 15
}

func (rl *RateLimits) UpdateWithRateLimit(other *RateLimits) {
	rl.LimitRequests = other.LimitRequests
	rl.LimitTokens = other.LimitTokens
	rl.ResetRequests = other.ResetRequests
	rl.ResetTokens = other.ResetTokens
	rl.RemainingRequests = other.RemainingRequests
	rl.RemainingTokens = other.RemainingTokens
}

func (rl *RateLimits) IsInitialized() bool {
	return rl.RemainingRequests == 0 && rl.RemainingTokens == 0
}

type VectorizationResult struct {
	Text       []string
	Dimensions int
	Vector     [][]float32
	Errors     []error
}

type Usage struct {
	CompletionTokens int `json:"completion_tokens,omitempty"`
	PromptTokens     int `json:"prompt_tokens,omitempty"`
	TotalTokens      int `json:"total_tokens,omitempty"`
}

func GetTotalTokens(usage *Usage) int {
	if usage == nil {
		return -1
	}
	return usage.TotalTokens
}

type VectorizationCLIPResult struct {
	TextVectors  [][]float32
	ImageVectors [][]float32
}
