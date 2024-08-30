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

package ent

import (
	"net/http"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const dummyLimit = 10000000

func GetRateLimitsFromHeader(header http.Header) *modulecomponents.RateLimits {
	requestsReset, err := time.ParseDuration(header.Get("x-ratelimit-reset-requests"))
	if err != nil {
		requestsReset = 0
	}
	tokensReset, err := time.ParseDuration(header.Get("x-ratelimit-reset-tokens"))
	if err != nil {
		tokensReset = 0
	}
	limitRequests := getHeaderInt(header, "x-ratelimit-limit-requests")
	limitTokens := getHeaderInt(header, "x-ratelimit-limit-tokens")
	remainingRequests := getHeaderInt(header, "x-ratelimit-remaining-requests")
	remainingTokens := getHeaderInt(header, "x-ratelimit-remaining-tokens")

	// azure returns 0 as limit, make sure this does not block anything by setting a high value
	if limitTokens == 0 && remainingTokens > 0 {
		limitTokens = dummyLimit
	}
	if limitRequests == 0 && remainingRequests > 0 {
		limitRequests = dummyLimit
	}
	return &modulecomponents.RateLimits{
		LimitRequests:     limitRequests,
		LimitTokens:       limitTokens,
		RemainingRequests: remainingRequests,
		RemainingTokens:   remainingTokens,
		ResetRequests:     time.Now().Add(requestsReset),
		ResetTokens:       time.Now().Add(tokensReset),
	}
}

func getHeaderInt(header http.Header, key string) int {
	value := header.Get(key)
	if value == "" {
		return 0
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return i
}
