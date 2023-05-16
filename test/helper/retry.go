//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helper

import (
	"fmt"
	"log"
	"time"
)

func Retry(retryFunc func() error, retries int, timeout time.Duration) error {
	if timeout == 0 {
		return fmt.Errorf("timeout must be larger than 0")
	}
	if retries < 1 {
		return fmt.Errorf("must retry at least once")
	}
	iters := 1 + retries
	for i := 0; i < iters; i++ {
		if err := retryFunc(); err != nil {
			if i == 0 {
				log.Printf("first attempt failed - starting retries, err: %v", err)
			} else {
				log.Printf("retry #%d failed, err: %v", i, err)
			}
		} else {
			return nil
		}
	}
	return fmt.Errorf("all retry attempts failed")
}
