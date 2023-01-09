package ssdhelpers

import (
	"math"
	"runtime"
	"sync"
)

type Action func(workerId uint64, taskIndex uint64, mutex *sync.Mutex)

func Concurrently(n uint64, action Action) {
	n64 := float64(n)
	workerCount := runtime.GOMAXPROCS(0)
	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	split := uint64(math.Ceil(n64 / float64(workerCount)))
	for worker := uint64(0); worker < uint64(workerCount); worker++ {
		wg.Add(1)
		go func(workerID uint64) {
			defer wg.Done()
			for i := workerID * split; i < uint64(math.Min(float64((workerID+1)*split), n64)); i++ {
				action(workerID, i, mutex)
			}
		}(worker)
	}
	wg.Wait()
}

func FilterSegment(i int, ds int) FilterFunc {
	segment := int(i)
	return func(x []float32) []float32 {
		return extractSegment(segment, x, ds)
	}
}

func extractSegment(i int, v []float32, ds int) []float32 {
	return v[i*ds : (i+1)*ds]
}
