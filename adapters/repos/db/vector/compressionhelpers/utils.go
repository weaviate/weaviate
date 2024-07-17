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

package compressionhelpers

import (
	"math"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type Action func(taskIndex uint64)

func Concurrently(log logrus.FieldLogger, n uint64, action Action) {
	n64 := float64(n)
	workerCount := runtime.GOMAXPROCS(0)
	wg := &sync.WaitGroup{}
	split := uint64(math.Ceil(n64 / float64(workerCount)))
	for worker := uint64(0); worker < uint64(workerCount); worker++ {
		workerID := worker

		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for i := workerID * split; i < uint64(math.Min(float64((workerID+1)*split), n64)); i++ {
				action(i)
			}
		}, log)
	}
	wg.Wait()
}

func ConcurrentlyWithError(log logrus.FieldLogger, n uint64, action func(taskIndex uint64) error) error {
	n64 := float64(n)
	workerCount := runtime.GOMAXPROCS(0)
	eg := enterrors.NewErrorGroupWrapper(log)
	eg.SetLimit(workerCount)
	split := uint64(math.Ceil(n64 / float64(workerCount)))
	for worker := uint64(0); worker < uint64(workerCount); worker++ {
		workerID := worker
		eg.Go(func() error {
			for i := workerID * split; i < uint64(math.Min(float64((workerID+1)*split), n64)); i++ {
				err := action(i)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return eg.Wait()
}
