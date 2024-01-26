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
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 500 * time.Millisecond
	expBackoff.RandomizationFactor = 0.5
	expBackoff.Multiplier = 1.5
	expBackoff.MaxElapsedTime = 3 * time.Second
	return backoff.WithMaxRetries(expBackoff, 3)
}
