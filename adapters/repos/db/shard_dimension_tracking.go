package db

import (
	"encoding/binary"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

// TODO WEAVIATE-286, instead of this fake feature flag, this should be read
// from the environment. See usecases/config/environment.go
var temporaryFakeFeatureFlagForWeavite286 = true

func (s *Shard) Dimensions() int {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0
	}

	c := b.MapCursor()
	sum := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		dimLength := binary.LittleEndian.Uint32(k)
		sum += int(dimLength) * len(v)
	}
	c.Close()

	return sum
}

func (s *Shard) initDimensionTracking() {
	// TODO WEAVIATE-286: check real feature flag and disable if not set
	if !temporaryFakeFeatureFlagForWeavite286 {
		return
	}

	// TODO WEAVIATE-286: cancel the cycle when the shard is shut down
	// Does it make sense to use the more elaborate CycleManager here?
	// See entities/cyclemanager/cyclemanager.go

	go func() {
		t := time.Tick(5 * time.Second)

		for {
			<-t
			_ = s.Dimensions()

			// TODO WEAVIATE-286: Track the dimensions using Prometheus Gauge with
			// class name and shardname as labels
		}
	}()
}
