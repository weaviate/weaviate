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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const dummyLimit = 10000000

func GetRateLimitsFromHeader(l *logrusext.Sampler, header http.Header, isAzure bool) *modulecomponents.RateLimits {
	requestsReset, err := time.ParseDuration(header.Get("x-ratelimit-reset-requests"))
	if err != nil {
		requestsReset = 0
	}
	tokensReset, err := time.ParseDuration(header.Get("x-ratelimit-reset-tokens"))
	if err != nil {
		// azure doesn't include the x-ratelimit-reset-tokens header, fallback to default
		tokensReset = time.Duration(1) * time.Minute
	}
	limitRequests := getHeaderInt(header, "x-ratelimit-limit-requests")
	limitTokens := getHeaderInt(header, "x-ratelimit-limit-tokens")
	remainingRequests := getHeaderInt(header, "x-ratelimit-remaining-requests")
	remainingTokens := getHeaderInt(header, "x-ratelimit-remaining-tokens")

	// azure returns 0 as limit, make sure this does not block anything by setting a high value
	if isAzure {
		limitRequests = dummyLimit
		remainingRequests = dummyLimit
	}

	updateWithMissingValues := false
	// the absolute limits should never be 0, while it is possible to use up all tokens/requests which results in the
	// remaining tokens/requests to be 0
	if limitRequests <= 0 || limitTokens <= 0 || remainingRequests < 0 || remainingTokens < 0 {
		updateWithMissingValues = true

		// logging all headers as there should not be anything sensitive according to the documentation:
		// https://platform.openai.com/docs/api-reference/debugging-requests
		l.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("headers", header).
				Debug("rate limit headers are missing or invalid, going to keep using the old values")
		})
	}

	return &modulecomponents.RateLimits{
		LimitRequests:           limitRequests,
		LimitTokens:             limitTokens,
		RemainingRequests:       remainingRequests,
		RemainingTokens:         remainingTokens,
		ResetRequests:           time.Now().Add(requestsReset),
		ResetTokens:             time.Now().Add(tokensReset),
		UpdateWithMissingValues: updateWithMissingValues,
	}
}

func getHeaderInt(header http.Header, key string) int {
	value := header.Get(key)
	if value == "" {
		return -1
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return i
}
