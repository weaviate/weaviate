package db

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

var featureFlag = false // TODO

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

			b := s.store.Bucket(helpers.DimensionsBucketLSM)
			if b == nil {
				// TODO
				continue
			}

			before := time.Now()
			c := b.MapCursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				dimLength := binary.LittleEndian.Uint32(k)
				amount := len(v)

				fmt.Printf("%d * %d = %d\n", dimLength, amount, int(dimLength)*amount)
			}
			c.Close()
			fmt.Printf("it took %s\n", time.Since(before))
		}
	}()
}
