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

// After MaxElapsedTime the backoff.BackOff returns Stop.
// It never stops if MaxElapsedTime == 0.
func NewExponentialBackoff(initialInverval time.Duration, maxElapsedTime time.Duration) backoff.BackOff {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = initialInverval
	eb.MaxElapsedTime = maxElapsedTime
	return eb
}
