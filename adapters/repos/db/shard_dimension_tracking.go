package db

import (
	"encoding/binary"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

var featureFlag = true // TODO

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
	// TODO: check real feature flag and disable if not set
	if !featureFlag {
		return
	}

	// TODO cancel

	go func() {
		t := time.Tick(5 * time.Second)

		for {
			<-t
			_ = s.Dimensions()
			// fmt.Print(s.Dimensions())
		}
	}()
}
