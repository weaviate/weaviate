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

package cluster

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// backoffConfig creates a backoff configuration based on the election timeout.
// The initial interval is set to 1/20th of the election timeout, and the max interval
// is set to the election timeout itself. This ensures that retries are aggressive
// enough to detect leader changes quickly while not overwhelming the system.
// With exponential backoff, each retry doubles the previous interval, but is capped at the max interval.
// For example, with a 1s election timeout:
// - Initial interval: 50ms (1/20th of election timeout)
// - Max interval: 1s (election timeout)
// - Max retries: 10
// If electionTimeout = 1s → max time ≈ 5.55s (raft default)
// If electionTimeout = 2s → max time ≈ 11.1s
// If electionTimeout = 5s → max time ≈ 27.75s
func backoffConfig(ctx context.Context, electionTimeout time.Duration) backoff.BackOffContext {
	initialInterval := electionTimeout / 20
	return backoff.WithContext(
		backoff.WithMaxRetries(
			backoff.NewExponentialBackOff(
				backoff.WithInitialInterval(initialInterval),
				backoff.WithMaxInterval(electionTimeout),
			),
			10,
		),
		ctx,
	)
}
