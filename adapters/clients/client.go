//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type retryClient struct {
	client *http.Client
	*retryer
}

type retryer struct {
	minBackOff  time.Duration
	maxBackOff  time.Duration
	timeoutUnit time.Duration
}

func newRetryer() *retryer {
	return &retryer{
		minBackOff:  time.Millisecond * 250,
		maxBackOff:  time.Second * 30,
		timeoutUnit: time.Second, // used by unit tests
	}
}

func (r *retryer) retry(ctx context.Context, n int, work func(context.Context) (bool, error)) error {
	delay := r.minBackOff
	for {
		keepTrying, err := work(ctx)
		if !keepTrying || n < 1 || err == nil {
			return err
		}

		n--
		if delay = backOff(delay); delay > r.maxBackOff {
			delay = r.maxBackOff
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%v: %w", err, ctx.Err())
		case <-timer.C:
		}
		timer.Stop()
	}
}
