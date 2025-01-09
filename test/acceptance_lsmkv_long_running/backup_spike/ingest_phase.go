//go:build ignore
// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func main() {
	dir := "./data"
	ctx := context.Background()
	c := lsmkv.NewBucketCreator()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	phasePtr := flag.Int("phase", 0, "specify phase")
	flag.Parse()
	phase := *phasePtr

	logger.Infof("writing phase %d", phase)

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", logger, 1)
	flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback, logger)
	flushCycle.Start()
	compactionCallbacks := cyclemanager.NewCallbackGroupNoop()

	h, m, s := time.Now().Clock()

	bucket, err := c.NewBucket(ctx, filepath.Join(dir, "my-bucket"), "", logger, nil,
		compactionCallbacks, flushCallbacks,
		lsmkv.WithPread(true),
	)
	if err != nil {
		panic(err)
	}

	defer bucket.Shutdown(context.Background())

	if phase > 0 {
		for i := 0; i < 100; i++ {
			if i%2 == 1 {
				continue
			}
			key := []byte(fmt.Sprintf("phase-%02d-key-%03d", phase-1, i))
			value := []byte(fmt.Sprintf("written %02d:%02d:%02d", h, m, s))
			err := bucket.Put(key, value)
			if err != nil {
				panic(err)
			}
		}
	}

	if phase > 1 {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("phase-%02d-key-%03d", phase-2, i))
			err := bucket.Delete(key)
			if err != nil {
				panic(err)
			}
		}
	}

	// in all cases, write phase
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("phase-%02d-key-%03d", phase, i))
		value := []byte(fmt.Sprintf("written %02d:%02d:%02d", h, m, s))
		err := bucket.Put(key, value)
		if err != nil {
			panic(err)
		}
	}

	logger.Infof("now have %d entries", bucket.Count())
}
