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

package db

import (
	"encoding/binary"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func (s *Shard) Dimensions() int {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0
	}

	c := b.MapCursor()
	defer c.Close()
	sum := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		dimLength := binary.LittleEndian.Uint32(k)
		sum += int(dimLength) * len(v)
	}

	return sum
}

func (s *Shard) sendVectorDimensionsMetric(count int) {
	if s.promMetrics != nil {
		metric, err := s.promMetrics.VectorDimensionsSum.
			GetMetricWithLabelValues(s.index.Config.ClassName.String(), s.name)
		if err == nil {
			metric.Set(float64(count))
		}
	}
}

func (s *Shard) initDimensionTracking() {
	if s.index.Config.TrackVectorDimensions {
		// always send vector dimensions at startup if tracking is enabled
		s.sendVectorDimensionsMetric(s.Dimensions())
		// start tracking vector dimensions goroutine only when tracking is enabled
		go func() {
			t := time.NewTicker(5 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-s.stopMetrics:
					return
				case <-t.C:
					s.sendVectorDimensionsMetric(s.Dimensions())
				}
			}
		}()
	}
}
