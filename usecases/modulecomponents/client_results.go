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
	AfterRequestFunction func(limits *RateLimits)
	LimitRequests        int
	LimitTokens          int
	RemainingRequests    int
	RemainingTokens      int
	ResetRequests        time.Time
	ResetTokens          time.Time
}

func (rl *RateLimits) ResetAfterRequestFunction() {
	if rl.AfterRequestFunction != nil {
		rl.AfterRequestFunction(rl)
	}
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
