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
)

type RateLimits struct {
	LimitRequests     int
	LimitTokens       int
	RemainingRequests int
	RemainingTokens   int
	ResetRequests     int
	ResetTokens       int
}

type VectorizationResult struct {
	Text       []string
	Dimensions int
	Vector     [][]float32
	Errors     []error
	RateLimits RateLimits
}

func GetRateLimitsFromHeader(header http.Header) RateLimits {
	return RateLimits{
		LimitRequests:     getHeaderInt(header, "x-ratelimit-limit-requests"),
		LimitTokens:       getHeaderInt(header, "x-ratelimit-limit-tokens"),
		RemainingRequests: getHeaderInt(header, "x-ratelimit-remaining-requests"),
		RemainingTokens:   getHeaderInt(header, "x-ratelimit-remaining-tokens"),
		ResetRequests:     getHeaderInt(header, "x-ratelimit-reset-requests"),
		ResetTokens:       getHeaderInt(header, "x-ratelimit-reset-tokens"),
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
