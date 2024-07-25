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

package utils

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

// NewBackoff returns a Backoff that can be used to retry an operation
// We have this function to ensure that we can use the same backoff settings in multiple places in weaviate.
func NewBackoff() backoff.BackOff {
	return ConstantBackoff(3, 50*time.Millisecond)
}

// ConstantBackoff is a backoff configuration used to handle getters
// retry for eventual consistency handling
func ConstantBackoff(maxrtry int, interval time.Duration) backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(interval), uint64(maxrtry))
}

func DefaultExponentialBackOff() *backoff.ExponentialBackOff {
	return NewExponentialBackOff(time.Millisecond*250, time.Second*5, time.Second*10, 2.0, 1.0)
}

func NewExponentialBackOff(initialInterval time.Duration, maxInterval time.Duration,
	maxElapsedTime time.Duration, multiplier float64, randomizationFactor float64) *backoff.ExponentialBackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = initialInterval
	expBackoff.MaxInterval = maxInterval
	expBackoff.MaxElapsedTime = maxElapsedTime
	expBackoff.Multiplier = multiplier
	expBackoff.RandomizationFactor = randomizationFactor
	return expBackoff
}

func CloneExponentialBackoff(oldExpBackoff backoff.ExponentialBackOff) *backoff.ExponentialBackOff {
	newExpBackoff := backoff.NewExponentialBackOff()
	newExpBackoff.InitialInterval = oldExpBackoff.InitialInterval
	newExpBackoff.MaxInterval = oldExpBackoff.MaxInterval
	newExpBackoff.MaxElapsedTime = oldExpBackoff.MaxElapsedTime
	newExpBackoff.Multiplier = oldExpBackoff.Multiplier
	newExpBackoff.RandomizationFactor = oldExpBackoff.RandomizationFactor
	return newExpBackoff
}

func ShortExponentialBackOff() *backoff.ExponentialBackOff {
	return NewExponentialBackOff(time.Millisecond*1, time.Millisecond*2, time.Millisecond*3, 2.0, 1.0)
}

func LongExponentialBackOff() *backoff.ExponentialBackOff {
	return NewExponentialBackOff(time.Second*1, time.Second*30, time.Second*30, 2.0, 1.0)
}
